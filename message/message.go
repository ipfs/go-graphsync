package message

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	pb "github.com/ipfs/go-graphsync/message/pb"
)

// IsTerminalSuccessCode returns true if the response code indicates the
// request terminated successfully.
// DEPRECATED: use status.IsSuccess()
func IsTerminalSuccessCode(status graphsync.ResponseStatusCode) bool {
	return status.IsSuccess()
}

// IsTerminalFailureCode returns true if the response code indicates the
// request terminated in failure.
// DEPRECATED: use status.IsFailure()
func IsTerminalFailureCode(status graphsync.ResponseStatusCode) bool {
	return status.IsFailure()
}

// IsTerminalResponseCode returns true if the response code signals
// the end of the request
// DEPRECATED: use status.IsTerminal()
func IsTerminalResponseCode(status graphsync.ResponseStatusCode) bool {
	return status.IsTerminal()
}

// Exportable is an interface that can serialize to a protobuf
type Exportable interface {
	ToProto() (*pb.Message, error)
	ToNet(w io.Writer) error
}

type GraphSyncExtensions struct {
	Keys   []string
	Values map[string]datamodel.Node
}

// GraphSyncRequest is a struct to capture data on a request contained in a
// GraphSyncMessage.
type GraphSyncRequest struct {
	ID graphsync.RequestID

	Root       cid.Cid
	Selector   ipld.Node
	Extensions GraphSyncExtensions
	Priority   graphsync.Priority
	Cancel     bool
	Update     bool
}

type GraphSyncMetadatum struct {
	Link         datamodel.Link
	BlockPresent bool
}

// GraphSyncResponse is an struct to capture data on a response sent back
// in a GraphSyncMessage.
type GraphSyncResponse struct {
	ID graphsync.RequestID

	Status     graphsync.ResponseStatusCode
	Metadata   []GraphSyncMetadatum
	Extensions GraphSyncExtensions
}

type GraphSyncBlock struct {
	Prefix []byte
	Data   []byte
}

func FromBlockFormat(block blocks.Block) GraphSyncBlock {
	return GraphSyncBlock{
		Prefix: block.Cid().Prefix().Bytes(),
		Data:   block.RawData(),
	}
}

func (b GraphSyncBlock) BlockFormat() *blocks.BasicBlock {
	pref, err := cid.PrefixFromBytes(b.Prefix)
	if err != nil {
		panic(err) // should never happen
	}

	c, err := pref.Sum(b.Data)
	if err != nil {
		panic(err) // should never happen
	}

	block, err := blocks.NewBlockWithCid(b.Data, c)
	if err != nil {
		panic(err) // should never happen
	}
	return block
}

func BlockFormatSlice(bs []GraphSyncBlock) []blocks.Block {
	blks := make([]blocks.Block, len(bs))
	for i, b := range bs {
		blks[i] = b.BlockFormat()
	}
	return blks
}

type GraphSyncMessage struct {
	Requests  []GraphSyncRequest
	Responses []GraphSyncResponse
	Blocks    []GraphSyncBlock
}

// NewRequest builds a new Graphsync request
func NewRequest(id graphsync.RequestID,
	root cid.Cid,
	selector ipld.Node,
	priority graphsync.Priority,
	extensions ...NamedExtension) GraphSyncRequest {

	return newRequest(id, root, selector, priority, false, false, toExtensionsMap(extensions))
}

// CancelRequest request generates a request to cancel an in progress request
func CancelRequest(id graphsync.RequestID) GraphSyncRequest {
	return newRequest(id, cid.Cid{}, nil, 0, true, false, GraphSyncExtensions{})
}

// UpdateRequest generates a new request to update an in progress request with the given extensions
func UpdateRequest(id graphsync.RequestID, extensions ...NamedExtension) GraphSyncRequest {
	return newRequest(id, cid.Cid{}, nil, 0, false, true, toExtensionsMap(extensions))
}

// NamedExtension exists just for the purpose of the constructors.
type NamedExtension struct {
	Name graphsync.ExtensionName
	Data ipld.Node
}

func toExtensionsMap(extensions []NamedExtension) (m GraphSyncExtensions) {
	if len(extensions) > 0 {
		m.Keys = make([]string, len(extensions))
		m.Values = make(map[string]ipld.Node, len(extensions))
		for i, ext := range extensions {
			m.Keys[i] = string(ext.Name)
			m.Values[string(ext.Name)] = ext.Data
		}
	}
	return m
}

func fromProtoExtensions(protoExts map[string][]byte) GraphSyncExtensions {
	var exts []NamedExtension
	for name, data := range protoExts {
		exts = append(exts, NamedExtension{graphsync.ExtensionName(name), basicnode.NewBytes(data)})
	}
	// Iterating over the map above is non-deterministic,
	// so sort by the unique names to ensure determinism.
	sort.Slice(exts, func(i, j int) bool { return exts[i].Name < exts[j].Name })
	return toExtensionsMap(exts)
}

func newRequest(id graphsync.RequestID,
	root cid.Cid,
	selector ipld.Node,
	priority graphsync.Priority,
	isCancel bool,
	isUpdate bool,
	extensions GraphSyncExtensions) GraphSyncRequest {
	return GraphSyncRequest{
		ID:         id,
		Root:       root,
		Selector:   selector,
		Priority:   priority,
		Cancel:     isCancel,
		Update:     isUpdate,
		Extensions: extensions,
	}
}

// NewResponse builds a new Graphsync response
func NewResponse(requestID graphsync.RequestID,
	status graphsync.ResponseStatusCode,
	extensions ...NamedExtension) GraphSyncResponse {
	return newResponse(requestID, status, toExtensionsMap(extensions))
}

func newResponse(requestID graphsync.RequestID,
	status graphsync.ResponseStatusCode, extensions GraphSyncExtensions) GraphSyncResponse {
	return GraphSyncResponse{
		ID:         requestID,
		Status:     status,
		Extensions: extensions,
	}
}

func newMessageFromProto(pbm *pb.Message) (GraphSyncMessage, error) {
	requests := make([]GraphSyncRequest, len(pbm.GetRequests()))
	for i, req := range pbm.Requests {
		if req == nil {
			return GraphSyncMessage{}, errors.New("request is nil")
		}
		var root cid.Cid
		var err error
		if !req.Cancel && !req.Update {
			root, err = cid.Cast(req.Root)
			if err != nil {
				return GraphSyncMessage{}, err
			}
		}

		var selector ipld.Node
		if !req.Cancel && !req.Update {
			selector, err = ipldutil.DecodeNode(req.Selector)
			if err != nil {
				return GraphSyncMessage{}, err
			}
		}
		// TODO: we likely need to turn some "core" extensions to fields,
		// as some of those got moved to proper fields in the new protocol.
		// Same for responses above, as well as the "to proto" funcs.
		requests[i] = newRequest(graphsync.RequestID(req.Id), root, selector, graphsync.Priority(req.Priority), req.Cancel, req.Update, fromProtoExtensions(req.GetExtensions()))
	}

	responses := make([]GraphSyncResponse, len(pbm.GetResponses()))
	for i, res := range pbm.Responses {
		if res == nil {
			return GraphSyncMessage{}, errors.New("response is nil")
		}
		responses[i] = newResponse(graphsync.RequestID(res.Id), graphsync.ResponseStatusCode(res.Status), fromProtoExtensions(res.GetExtensions()))
	}

	blks := make([]GraphSyncBlock, len(pbm.GetData()))
	for i, b := range pbm.GetData() {
		if b == nil {
			return GraphSyncMessage{}, errors.New("block is nil")
		}
		blks[i] = GraphSyncBlock{
			Prefix: b.GetPrefix(),
			Data:   b.GetData(),
		}
	}

	return GraphSyncMessage{
		requests, responses, blks,
	}, nil
}

func (gsm GraphSyncMessage) Empty() bool {
	return len(gsm.Blocks) == 0 && len(gsm.Requests) == 0 && len(gsm.Responses) == 0
}

func (gsm GraphSyncMessage) ResponseCodes() map[graphsync.RequestID]graphsync.ResponseStatusCode {
	codes := make(map[graphsync.RequestID]graphsync.ResponseStatusCode, len(gsm.Responses))
	for _, response := range gsm.Responses {
		codes[response.ID] = response.Status
	}
	return codes
}

// FromNet can read a network stream to deserialized a GraphSyncMessage
func FromNet(r io.Reader) (GraphSyncMessage, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return FromMsgReader(reader)
}

// FromMsgReader can deserialize a protobuf message into a GraphySyncMessage.
func FromMsgReader(r msgio.Reader) (GraphSyncMessage, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return GraphSyncMessage{}, err
	}

	var pb pb.Message
	err = proto.Unmarshal(msg, &pb)
	r.ReleaseMsg(msg)
	if err != nil {
		return GraphSyncMessage{}, err
	}

	return newMessageFromProto(&pb)
}

func toProtoExtensions(m GraphSyncExtensions) map[string][]byte {
	protoExts := make(map[string][]byte, len(m.Values))
	for name, node := range m.Values {
		// Only keep those which are plain bytes,
		// as those are the only ones that the older protocol clients understand.
		if node.Kind() != ipld.Kind_Bytes {
			continue
		}
		raw, err := node.AsBytes()
		if err != nil {
			panic(err) // shouldn't happen
		}
		protoExts[name] = raw
	}
	return protoExts
}

func (gsm GraphSyncMessage) ToProto() (*pb.Message, error) {
	pbm := new(pb.Message)
	pbm.Requests = make([]*pb.Message_Request, 0, len(gsm.Requests))
	for _, request := range gsm.Requests {
		var selector []byte
		var err error
		if request.Selector != nil {
			selector, err = ipldutil.EncodeNode(request.Selector)
			if err != nil {
				return nil, err
			}
		}
		pbm.Requests = append(pbm.Requests, &pb.Message_Request{
			Id:         int32(request.ID),
			Root:       request.Root.Bytes(),
			Selector:   selector,
			Priority:   int32(request.Priority),
			Cancel:     request.Cancel,
			Update:     request.Update,
			Extensions: toProtoExtensions(request.Extensions),
		})
	}

	pbm.Responses = make([]*pb.Message_Response, 0, len(gsm.Responses))
	for _, response := range gsm.Responses {
		pbm.Responses = append(pbm.Responses, &pb.Message_Response{
			Id:         int32(response.ID),
			Status:     int32(response.Status),
			Extensions: toProtoExtensions(response.Extensions),
		})
	}

	pbm.Data = make([]*pb.Message_Block, 0, len(gsm.Blocks))
	for _, b := range gsm.Blocks {
		pbm.Data = append(pbm.Data, &pb.Message_Block{
			Prefix: b.Prefix,
			Data:   b.Data,
		})
	}
	return pbm, nil
}

func (gsm GraphSyncMessage) ToNet(w io.Writer) error {
	msg, err := gsm.ToProto()
	if err != nil {
		return err
	}
	size := proto.Size(msg)
	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	out, err := proto.MarshalOptions{}.MarshalAppend(buf[:n], msg)
	if err != nil {
		return err
	}
	_, err = w.Write(out)
	return err
}

func (gsm GraphSyncMessage) Loggable() map[string]interface{} {
	requests := make([]string, 0, len(gsm.Requests))
	for _, request := range gsm.Requests {
		requests = append(requests, fmt.Sprintf("%d", request.ID))
	}
	responses := make([]string, 0, len(gsm.Responses))
	for _, response := range gsm.Responses {
		responses = append(responses, fmt.Sprintf("%d", response.ID))
	}
	return map[string]interface{}{
		"requests":  requests,
		"responses": responses,
	}
}

func (gsm GraphSyncMessage) Clone() GraphSyncMessage {
	requests := append([]GraphSyncRequest{}, gsm.Requests...)
	responses := append([]GraphSyncResponse{}, gsm.Responses...)
	blocks := append([]GraphSyncBlock{}, gsm.Blocks...)
	return GraphSyncMessage{requests, responses, blocks}
}

// Extension returns the content for an extension on a response, or errors
// if extension is not present
func (gsr GraphSyncRequest) Extension(name graphsync.ExtensionName) (ipld.Node, bool) {
	val, ok := gsr.Extensions.Values[string(name)]
	if !ok {
		return nil, false
	}
	return val, true
}

// ExtensionNames returns the names of the extensions included in this request
func (gsr GraphSyncRequest) ExtensionNames() []string {
	return gsr.Extensions.Keys
}

// Extension returns the content for an extension on a response, or errors
// if extension is not present
func (gsr GraphSyncResponse) Extension(name graphsync.ExtensionName) (ipld.Node, bool) {
	val, ok := gsr.Extensions.Values[string(name)]
	if !ok {
		return nil, false
	}
	return val, true
}

// ExtensionNames returns the names of the extensions included in this request
func (gsr GraphSyncResponse) ExtensionNames() []string {
	return gsr.Extensions.Keys
}

// ReplaceExtensions merges the extensions given extensions into the request to create a new request,
// but always uses new data
func (gsr GraphSyncRequest) ReplaceExtensions(extensions []NamedExtension) GraphSyncRequest {
	req, _ := gsr.MergeExtensions(extensions, func(name graphsync.ExtensionName, oldNode, newNode ipld.Node) (ipld.Node, error) {
		return newNode, nil
	})
	return req
}

// MergeExtensions merges the given list of extensions to produce a new request with the combination of the old request
// plus the new extensions. When an old extension and a new extension are both present, mergeFunc is called to produce
// the result
func (gsr GraphSyncRequest) MergeExtensions(extensions []NamedExtension, mergeFunc func(name graphsync.ExtensionName, oldNode, newNode ipld.Node) (ipld.Node, error)) (GraphSyncRequest, error) {
	if len(gsr.Extensions.Keys) == 0 {
		return newRequest(gsr.ID, gsr.Root, gsr.Selector, gsr.Priority, gsr.Cancel, gsr.Update, toExtensionsMap(extensions)), nil
	}
	combinedExtensions := make(map[string]ipld.Node)
	for _, newExt := range extensions {
		oldNode, ok := gsr.Extensions.Values[string(newExt.Name)]
		if !ok {
			combinedExtensions[string(newExt.Name)] = newExt.Data
			continue
		}
		resultNode, err := mergeFunc(graphsync.ExtensionName(newExt.Name), oldNode, newExt.Data)
		if err != nil {
			return GraphSyncRequest{}, err
		}
		combinedExtensions[string(newExt.Name)] = resultNode
	}

	for name, oldNode := range gsr.Extensions.Values {
		_, ok := combinedExtensions[name]
		if ok {
			continue
		}
		combinedExtensions[name] = oldNode
	}
	extNames := make([]string, len(combinedExtensions))
	sort.Strings(extNames) // for reproducibility
	return newRequest(gsr.ID, gsr.Root, gsr.Selector, gsr.Priority, gsr.Cancel, gsr.Update, GraphSyncExtensions{extNames, combinedExtensions}), nil
}
