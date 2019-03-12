package message

import (
	"fmt"
	"io"

	"github.com/ipfs/go-block-format"

	ggio "github.com/gogo/protobuf/io"
	cid "github.com/ipfs/go-cid"
	pb "github.com/ipfs/go-graphsync/message/pb"
	inet "github.com/libp2p/go-libp2p-net"
)

// GraphSyncRequestID is a unique identifier for a GraphSync request.
type GraphSyncRequestID int32

// GraphSyncPriority a priority for a GraphSync request.
type GraphSyncPriority int32

// GraphSyncResponseStatusCode is a status returned for a GraphSync Request.
type GraphSyncResponseStatusCode int32

const (

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

// GraphSyncRequest is an interface for accessing data on request contained in a
// GraphSyncMessage.
type GraphSyncRequest interface {
	Selector() []byte
	Priority() GraphSyncPriority
	ID() GraphSyncRequestID
	IsCancel() bool
}

// GraphSyncResponse is an interface for accessing data on a response sent back
// in a GraphSyncMessage.
type GraphSyncResponse interface {
	RequestID() GraphSyncRequestID
	Status() GraphSyncResponseStatusCode
	Extra() []byte
}

// GraphSyncMessage is interface that can be serialized and deserialized to send
// over the GraphSync network
type GraphSyncMessage interface {
	Requests() []GraphSyncRequest

	Responses() []GraphSyncResponse

	Blocks() []blocks.Block

	AddRequest(id GraphSyncRequestID,
		selector []byte,
		priority GraphSyncPriority)

	Cancel(id GraphSyncRequestID)

	AddResponse(
		requestID GraphSyncRequestID,
		status GraphSyncResponseStatusCode,
		extra []byte)

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

type graphSyncRequest struct {
	selector []byte
	priority GraphSyncPriority
	id       GraphSyncRequestID
	isCancel bool
}

type graphSyncResponse struct {
	requestID GraphSyncRequestID
	status    GraphSyncResponseStatusCode
	extra     []byte
}

type graphSyncMessage struct {
	requests  map[GraphSyncRequestID]*graphSyncRequest
	responses map[GraphSyncRequestID]*graphSyncResponse
	blocks    map[cid.Cid]blocks.Block
}

// New initializes a new blank GraphSyncMessage
func New() GraphSyncMessage {
	return newMsg()
}

func newMsg() *graphSyncMessage {
	return &graphSyncMessage{
		requests:  make(map[GraphSyncRequestID]*graphSyncRequest),
		responses: make(map[GraphSyncRequestID]*graphSyncResponse),
		blocks:    make(map[cid.Cid]blocks.Block),
	}
}

func newMessageFromProto(pbm pb.Message) (GraphSyncMessage, error) {
	gsm := newMsg()
	for _, req := range pbm.Requests {
		gsm.addRequest(GraphSyncRequestID(req.Id), req.Selector, GraphSyncPriority(req.Priority), req.Cancel)
	}

	for _, res := range pbm.Responses {
		gsm.AddResponse(GraphSyncRequestID(res.Id), GraphSyncResponseStatusCode(res.Status), res.Extra)
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

func (gsm *graphSyncMessage) Cancel(id GraphSyncRequestID) {
	delete(gsm.requests, id)
	gsm.addRequest(id, nil, 0, true)
}

func (gsm *graphSyncMessage) AddRequest(id GraphSyncRequestID,
	selector []byte,
	priority GraphSyncPriority,
) {
	gsm.addRequest(id, selector, priority, false)
}

func (gsm *graphSyncMessage) addRequest(id GraphSyncRequestID,
	selector []byte,
	priority GraphSyncPriority,
	isCancel bool) {
	gsm.requests[id] = &graphSyncRequest{
		id:       id,
		selector: selector,
		priority: priority,
		isCancel: isCancel,
	}
}

func (gsm *graphSyncMessage) AddResponse(requestID GraphSyncRequestID,
	status GraphSyncResponseStatusCode,
	extra []byte) {
	gsm.responses[requestID] = &graphSyncResponse{
		requestID: requestID,
		status:    status,
		extra:     extra,
	}
}

func (gsm *graphSyncMessage) AddBlock(b blocks.Block) {
	gsm.blocks[b.Cid()] = b
}

// FromNet can read a network stream to deserialized a GraphSyncMessage
func FromNet(r io.Reader) (GraphSyncMessage, error) {
	pbr := ggio.NewDelimitedReader(r, inet.MessageSizeMax)
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
			Id:       int32(request.id),
			Selector: request.selector,
			Priority: int32(request.priority),
			Cancel:   request.isCancel,
		})
	}

	pbm.Responses = make([]pb.Message_Response, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		pbm.Responses = append(pbm.Responses, pb.Message_Response{
			Id:     int32(response.requestID),
			Status: int32(response.status),
			Extra:  response.extra,
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

func (gsr *graphSyncRequest) ID() GraphSyncRequestID      { return gsr.id }
func (gsr *graphSyncRequest) Selector() []byte            { return gsr.selector }
func (gsr *graphSyncRequest) Priority() GraphSyncPriority { return gsr.priority }
func (gsr *graphSyncRequest) IsCancel() bool              { return gsr.isCancel }

func (gsr *graphSyncResponse) RequestID() GraphSyncRequestID       { return gsr.requestID }
func (gsr *graphSyncResponse) Status() GraphSyncResponseStatusCode { return gsr.status }
func (gsr *graphSyncResponse) Extra() []byte                       { return gsr.extra }
