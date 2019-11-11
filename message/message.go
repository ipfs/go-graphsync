package message

import (
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"

	ggio "github.com/gogo/protobuf/io"
	cid "github.com/ipfs/go-cid"
	pb "github.com/ipfs/go-graphsync/message/pb"
	"github.com/libp2p/go-libp2p-core/network"
)

// GraphSyncRequestID is a unique identifier for a GraphSync request.
type GraphSyncRequestID int32

// GraphSyncPriority a priority for a GraphSync request.
type GraphSyncPriority int32

// GraphSyncResponseStatusCode is a status returned for a GraphSync Request.
type GraphSyncResponseStatusCode int32

// GraphSyncExtensionName is a name for a GraphSync extension
type GraphSyncExtensionName string

// GraphSyncExtension is a name/data pair for a graphsync extension
type GraphSyncExtension struct {
	Name GraphSyncExtensionName
	Data []byte
}

const (

	// Known Graphsync Extensions

	// ExtensionMetadata provides response metadata for a Graphsync request and is
	// documented at
	// https://github.com/ipld/specs/blob/master/block-layer/graphsync/known_extensions.md
	ExtensionMetadata = GraphSyncExtensionName("graphsync/response-metadata")

	// ExtensionDoNotSendCIDs tells the responding peer not to send certain blocks if they
	// are encountered in a traversal and is documented at
	// https://github.com/ipld/specs/blob/master/block-layer/graphsync/known_extensions.md
	ExtensionDoNotSendCIDs = GraphSyncExtensionName("graphsync/do-not-send-cids")

	// GraphSync Response Status Codes

	// Informational Response Codes (partial)

	// RequestAcknowledged means the request was received and is being worked on.
	RequestAcknowledged = GraphSyncResponseStatusCode(10)
	// AdditionalPeers means additional peers were found that may be able
	// to satisfy the request and contained in the extra block of the response.
	AdditionalPeers = GraphSyncResponseStatusCode(11)
	// NotEnoughGas means fulfilling this request requires payment.
	NotEnoughGas = GraphSyncResponseStatusCode(12)
	// OtherProtocol means a different type of response than GraphSync is
	// contained in extra.
	OtherProtocol = GraphSyncResponseStatusCode(13)
	// PartialResponse may include blocks and metadata about the in progress response
	// in extra.
	PartialResponse = GraphSyncResponseStatusCode(14)

	// Success Response Codes (request terminated)

	// RequestCompletedFull means the entire fulfillment of the GraphSync request
	// was sent back.
	RequestCompletedFull = GraphSyncResponseStatusCode(20)
	// RequestCompletedPartial means the response is completed, and part of the
	// GraphSync request was sent back, but not the complete request.
	RequestCompletedPartial = GraphSyncResponseStatusCode(21)

	// Error Response Codes (request terminated)

	// RequestRejected means the node did not accept the incoming request.
	RequestRejected = GraphSyncResponseStatusCode(30)
	// RequestFailedBusy means the node is too busy, try again later. Backoff may
	// be contained in extra.
	RequestFailedBusy = GraphSyncResponseStatusCode(31)
	// RequestFailedUnknown means the request failed for an unspecified reason. May
	// contain data about why in extra.
	RequestFailedUnknown = GraphSyncResponseStatusCode(32)
	// RequestFailedLegal means the request failed for legal reasons.
	RequestFailedLegal = GraphSyncResponseStatusCode(33)
	// RequestFailedContentNotFound means the respondent does not have the content.
	RequestFailedContentNotFound = GraphSyncResponseStatusCode(34)
)

// IsTerminalSuccessCode returns true if the response code indicates the
// request terminated successfully.
func IsTerminalSuccessCode(status GraphSyncResponseStatusCode) bool {
	return status == RequestCompletedFull ||
		status == RequestCompletedPartial
}

// IsTerminalFailureCode returns true if the response code indicates the
// request terminated in failure.
func IsTerminalFailureCode(status GraphSyncResponseStatusCode) bool {
	return status == RequestFailedBusy ||
		status == RequestFailedContentNotFound ||
		status == RequestFailedLegal ||
		status == RequestFailedUnknown
}

// IsTerminalResponseCode returns true if the response code signals
// the end of the request
func IsTerminalResponseCode(status GraphSyncResponseStatusCode) bool {
	return IsTerminalSuccessCode(status) || IsTerminalFailureCode(status)
}

// GraphSyncMessage is interface that can be serialized and deserialized to send
// over the GraphSync network
type GraphSyncMessage interface {
	Requests() []GraphSyncRequest

	Responses() []GraphSyncResponse

	Blocks() []blocks.Block

	AddRequest(graphSyncRequest GraphSyncRequest)

	AddResponse(graphSyncResponse GraphSyncResponse)

	AddBlock(blocks.Block)

	Empty() bool

	Exportable

	Loggable() map[string]interface{}
}

// Exportable is an interface that can serialize to a protobuf
type Exportable interface {
	ToProto() *pb.Message
	ToNet(w io.Writer) error
}

// GraphSyncRequest is a struct to capture data on a request contained in a
// GraphSyncMessage.
type GraphSyncRequest struct {
	root       cid.Cid
	selector   []byte
	priority   GraphSyncPriority
	id         GraphSyncRequestID
	extensions map[string][]byte
	isCancel   bool
}

// GraphSyncResponse is an struct to capture data on a response sent back
// in a GraphSyncMessage.
type GraphSyncResponse struct {
	requestID  GraphSyncRequestID
	status     GraphSyncResponseStatusCode
	extensions map[string][]byte
}

type graphSyncMessage struct {
	requests  map[GraphSyncRequestID]GraphSyncRequest
	responses map[GraphSyncRequestID]GraphSyncResponse
	blocks    map[cid.Cid]blocks.Block
}

// New initializes a new blank GraphSyncMessage
func New() GraphSyncMessage {
	return newMsg()
}

func newMsg() *graphSyncMessage {
	return &graphSyncMessage{
		requests:  make(map[GraphSyncRequestID]GraphSyncRequest),
		responses: make(map[GraphSyncRequestID]GraphSyncResponse),
		blocks:    make(map[cid.Cid]blocks.Block),
	}
}

// NewRequest builds a new Graphsync request
func NewRequest(id GraphSyncRequestID,
	root cid.Cid,
	selector []byte,
	priority GraphSyncPriority,
	extensions ...GraphSyncExtension) GraphSyncRequest {

	return newRequest(id, root, selector, priority, false, toExtensionsMap(extensions))
}

// CancelRequest request generates a request to cancel an in progress request
func CancelRequest(id GraphSyncRequestID) GraphSyncRequest {
	return newRequest(id, cid.Cid{}, nil, 0, true, nil)
}

func toExtensionsMap(extensions []GraphSyncExtension) (extensionsMap map[string][]byte) {
	if len(extensions) > 0 {
		extensionsMap = make(map[string][]byte, len(extensions))
		for _, extension := range extensions {
			extensionsMap[string(extension.Name)] = extension.Data
		}
	}
	return
}

func newRequest(id GraphSyncRequestID,
	root cid.Cid,
	selector []byte,
	priority GraphSyncPriority,
	isCancel bool,
	extensions map[string][]byte) GraphSyncRequest {
	return GraphSyncRequest{
		id:         id,
		root:       root,
		selector:   selector,
		priority:   priority,
		isCancel:   isCancel,
		extensions: extensions,
	}
}

// NewResponse builds a new Graphsync response
func NewResponse(requestID GraphSyncRequestID,
	status GraphSyncResponseStatusCode,
	extensions ...GraphSyncExtension) GraphSyncResponse {
	return newResponse(requestID, status, toExtensionsMap(extensions))
}

func newResponse(requestID GraphSyncRequestID,
	status GraphSyncResponseStatusCode, extensions map[string][]byte) GraphSyncResponse {
	return GraphSyncResponse{
		requestID:  requestID,
		status:     status,
		extensions: extensions,
	}
}
func newMessageFromProto(pbm pb.Message) (GraphSyncMessage, error) {
	gsm := newMsg()
	for _, req := range pbm.Requests {
		root, err := cid.Cast(req.Root)
		if err != nil {
			return nil, err
		}
		gsm.AddRequest(newRequest(GraphSyncRequestID(req.Id), root, req.Selector, GraphSyncPriority(req.Priority), req.Cancel, req.GetExtensions()))
	}

	for _, res := range pbm.Responses {
		gsm.AddResponse(newResponse(GraphSyncRequestID(res.Id), GraphSyncResponseStatusCode(res.Status), res.GetExtensions()))
	}

	for _, b := range pbm.GetData() {
		pref, err := cid.PrefixFromBytes(b.GetPrefix())
		if err != nil {
			return nil, err
		}

		c, err := pref.Sum(b.GetData())
		if err != nil {
			return nil, err
		}

		blk, err := blocks.NewBlockWithCid(b.GetData(), c)
		if err != nil {
			return nil, err
		}

		gsm.AddBlock(blk)
	}

	return gsm, nil
}

func (gsm *graphSyncMessage) Empty() bool {
	return len(gsm.blocks) == 0 && len(gsm.requests) == 0 && len(gsm.responses) == 0
}

func (gsm *graphSyncMessage) Requests() []GraphSyncRequest {
	requests := make([]GraphSyncRequest, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		requests = append(requests, request)
	}
	return requests
}

func (gsm *graphSyncMessage) Responses() []GraphSyncResponse {
	responses := make([]GraphSyncResponse, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		responses = append(responses, response)
	}
	return responses
}

func (gsm *graphSyncMessage) Blocks() []blocks.Block {
	bs := make([]blocks.Block, 0, len(gsm.blocks))
	for _, block := range gsm.blocks {
		bs = append(bs, block)
	}
	return bs
}

func (gsm *graphSyncMessage) AddRequest(graphSyncRequest GraphSyncRequest) {
	gsm.requests[graphSyncRequest.id] = graphSyncRequest
}

func (gsm *graphSyncMessage) AddResponse(graphSyncResponse GraphSyncResponse) {
	gsm.responses[graphSyncResponse.requestID] = graphSyncResponse
}

func (gsm *graphSyncMessage) AddBlock(b blocks.Block) {
	gsm.blocks[b.Cid()] = b
}

// FromNet can read a network stream to deserialized a GraphSyncMessage
func FromNet(r io.Reader) (GraphSyncMessage, error) {
	pbr := ggio.NewDelimitedReader(r, network.MessageSizeMax)
	return FromPBReader(pbr)
}

// FromPBReader can deserialize a protobuf message into a GraphySyncMessage.
func FromPBReader(pbr ggio.Reader) (GraphSyncMessage, error) {
	pb := new(pb.Message)
	if err := pbr.ReadMsg(pb); err != nil {
		return nil, err
	}

	return newMessageFromProto(*pb)
}

func (gsm *graphSyncMessage) ToProto() *pb.Message {
	pbm := new(pb.Message)
	pbm.Requests = make([]pb.Message_Request, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		pbm.Requests = append(pbm.Requests, pb.Message_Request{
			Id:         int32(request.id),
			Root:       request.root.Bytes(),
			Selector:   request.selector,
			Priority:   int32(request.priority),
			Cancel:     request.isCancel,
			Extensions: request.extensions,
		})
	}

	pbm.Responses = make([]pb.Message_Response, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		pbm.Responses = append(pbm.Responses, pb.Message_Response{
			Id:         int32(response.requestID),
			Status:     int32(response.status),
			Extensions: response.extensions,
		})
	}

	blocks := gsm.Blocks()
	pbm.Data = make([]pb.Message_Block, 0, len(blocks))
	for _, b := range blocks {
		pbm.Data = append(pbm.Data, pb.Message_Block{
			Data:   b.RawData(),
			Prefix: b.Cid().Prefix().Bytes(),
		})
	}
	return pbm
}

func (gsm *graphSyncMessage) ToNet(w io.Writer) error {
	pbw := ggio.NewDelimitedWriter(w)

	return pbw.WriteMsg(gsm.ToProto())
}

func (gsm *graphSyncMessage) Loggable() map[string]interface{} {
	requests := make([]string, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		requests = append(requests, fmt.Sprintf("%d", request.id))
	}
	responses := make([]string, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		responses = append(responses, fmt.Sprintf("%d", response.requestID))
	}
	return map[string]interface{}{
		"requests":  requests,
		"responses": responses,
	}
}

// ID Returns the request ID for this Request
func (gsr GraphSyncRequest) ID() GraphSyncRequestID { return gsr.id }

// Root returns the CID to the root block of this request
func (gsr GraphSyncRequest) Root() cid.Cid { return gsr.root }

// Selector returns the byte representation of the selector for this request
func (gsr GraphSyncRequest) Selector() []byte { return gsr.selector }

// Priority returns the priority of this request
func (gsr GraphSyncRequest) Priority() GraphSyncPriority { return gsr.priority }

// Extension returns the content for an extension on a response, or errors
// if extension is not present
func (gsr GraphSyncRequest) Extension(name GraphSyncExtensionName) ([]byte, bool) {
	if gsr.extensions == nil {
		return nil, false
	}
	val, ok := gsr.extensions[string(name)]
	if !ok {
		return nil, false
	}
	return val, true
}

// IsCancel returns true if this particular request is being cancelled
func (gsr GraphSyncRequest) IsCancel() bool { return gsr.isCancel }

// RequestID returns the request ID for this response
func (gsr GraphSyncResponse) RequestID() GraphSyncRequestID { return gsr.requestID }

// Status returns the status for a response
func (gsr GraphSyncResponse) Status() GraphSyncResponseStatusCode { return gsr.status }

// Extension returns the content for an extension on a response, or errors
// if extension is not present
func (gsr GraphSyncResponse) Extension(name GraphSyncExtensionName) ([]byte, bool) {
	if gsr.extensions == nil {
		return nil, false
	}
	val, ok := gsr.extensions[string(name)]
	if !ok {
		return nil, false
	}
	return val, true

}
