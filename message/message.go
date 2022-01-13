package message

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/ipfs/go-graphsync"
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

// String returns a human-readable form of a GraphSyncRequest
func (gsr GraphSyncRequest) String() string {
	var buf bytes.Buffer
	dagjson.Encode(gsr.Selector, &buf)
	return fmt.Sprintf("GraphSyncRequest<root=%s, selector=%s, priority=%d, id=%s, cancel=%v, update=%v, exts=%s>",
		gsr.Root.String(),
		buf.String(),
		gsr.Priority,
		gsr.ID.String(),
		gsr.Cancel,
		gsr.Update,
		strings.Join(gsr.ExtensionNames(), "|"),
	)
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

// String returns a human-readable form of a GraphSyncResponse
func (gsr GraphSyncResponse) String() string {
	return fmt.Sprintf("GraphSyncResponse<id=%s, status=%d, exts=%s>",
		gsr.ID.String(),
		gsr.Status,
		strings.Join(gsr.ExtensionNames(), "|"),
	)
}

// GraphSyncMessage is the internal representation form of a message sent or
// received over the wire
type GraphSyncMessage struct {
	Requests  []GraphSyncRequest
	Responses []GraphSyncResponse
	Blocks    []GraphSyncBlock
}

// String returns a human-readable (multi-line) form of a GraphSyncMessage and
// its contents
func (gsm GraphSyncMessage) String() string {
	cts := make([]string, 0)
	for _, req := range gsm.Requests {
		cts = append(cts, req.String())
	}
	for _, resp := range gsm.Responses {
		cts = append(cts, resp.String())
	}
	for _, c := range gsm.Blocks {
		cts = append(cts, fmt.Sprintf("Block<%s>", c.BlockFormat().Cid().String()))
	}
	return fmt.Sprintf("GraphSyncMessage<\n\t%s\n>", strings.Join(cts, ",\n\t"))
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

// Empty returns true if this message has no actionable content
func (gsm GraphSyncMessage) Empty() bool {
	return len(gsm.Blocks) == 0 && len(gsm.Requests) == 0 && len(gsm.Responses) == 0
}

// ResponseCodes returns a list of ResponseStatusCodes contained in the
// responses in this GraphSyncMessage
func (gsm GraphSyncMessage) ResponseCodes() map[graphsync.RequestID]graphsync.ResponseStatusCode {
	codes := make(map[graphsync.RequestID]graphsync.ResponseStatusCode, len(gsm.Responses))
	for _, response := range gsm.Responses {
		codes[response.ID] = response.Status
	}
	return codes
}

// Loggable returns a simplified, single-line log form of this GraphSyncMessage
func (gsm GraphSyncMessage) Loggable() map[string]interface{} {
	requests := make([]string, 0, len(gsm.Requests))
	for _, request := range gsm.Requests {
		requests = append(requests, request.ID.String())
	}
	responses := make([]string, 0, len(gsm.Responses))
	for _, response := range gsm.Responses {
		responses = append(responses, response.ID.String())
	}
	return map[string]interface{}{
		"requests":  requests,
		"responses": responses,
	}
}

// Clone returns a shallow copy of this GraphSyncMessage
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
