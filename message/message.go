package message

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"

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

// GraphSyncRequest is a struct to capture data on a request contained in a
// GraphSyncMessage.
type GraphSyncRequest struct {
	root       cid.Cid
	selector   ipld.Node
	priority   graphsync.Priority
	id         graphsync.RequestID
	extensions map[string][]byte
	isCancel   bool
	isUpdate   bool
}

// String returns a human-readable form of a GraphSyncRequest
func (gsr GraphSyncRequest) String() string {
	var buf bytes.Buffer
	dagjson.Encode(gsr.selector, &buf)
	ext := make([]string, 0)
	for s := range gsr.extensions {
		ext = append(ext, s)
	}
	return fmt.Sprintf("GraphSyncRequest<root=%s, selector=%s, priority=%d, id=%s, cancel=%v, update=%v, exts=%s>",
		gsr.root.String(),
		buf.String(),
		gsr.priority,
		gsr.id.String(),
		gsr.isCancel,
		gsr.isUpdate,
		strings.Join(ext, "|"),
	)
}

// GraphSyncResponse is an struct to capture data on a response sent back
// in a GraphSyncMessage.
type GraphSyncResponse struct {
	requestID  graphsync.RequestID
	status     graphsync.ResponseStatusCode
	extensions map[string][]byte
}

// String returns a human-readable form of a GraphSyncResponse
func (gsr GraphSyncResponse) String() string {
	ext := make([]string, 0)
	for s := range gsr.extensions {
		ext = append(ext, s)
	}
	return fmt.Sprintf("GraphSyncResponse<id=%s, status=%d, exts=%s>",
		gsr.requestID.String(),
		gsr.status,
		strings.Join(ext, "|"),
	)
}

// GraphSyncMessage is the internal representation form of a message sent or
// received over the wire
type GraphSyncMessage struct {
	requests  map[graphsync.RequestID]GraphSyncRequest
	responses map[graphsync.RequestID]GraphSyncResponse
	blocks    map[cid.Cid]blocks.Block
}

// String returns a human-readable (multi-line) form of a GraphSyncMessage and
// its contents
func (gsm GraphSyncMessage) String() string {
	cts := make([]string, 0)
	for _, req := range gsm.requests {
		cts = append(cts, req.String())
	}
	for _, resp := range gsm.responses {
		cts = append(cts, resp.String())
	}
	for c := range gsm.blocks {
		cts = append(cts, fmt.Sprintf("Block<%s>", c.String()))
	}
	return fmt.Sprintf("GraphSyncMessage<\n\t%s\n>", strings.Join(cts, ",\n\t"))
}

// NewRequest builds a new Graphsync request
func NewRequest(id graphsync.RequestID,
	root cid.Cid,
	selector ipld.Node,
	priority graphsync.Priority,
	extensions ...graphsync.ExtensionData) GraphSyncRequest {

	return newRequest(id, root, selector, priority, false, false, toExtensionsMap(extensions))
}

// CancelRequest request generates a request to cancel an in progress request
func CancelRequest(id graphsync.RequestID) GraphSyncRequest {
	return newRequest(id, cid.Cid{}, nil, 0, true, false, nil)
}

// UpdateRequest generates a new request to update an in progress request with the given extensions
func UpdateRequest(id graphsync.RequestID, extensions ...graphsync.ExtensionData) GraphSyncRequest {
	return newRequest(id, cid.Cid{}, nil, 0, false, true, toExtensionsMap(extensions))
}

func toExtensionsMap(extensions []graphsync.ExtensionData) (extensionsMap map[string][]byte) {
	if len(extensions) > 0 {
		extensionsMap = make(map[string][]byte, len(extensions))
		for _, extension := range extensions {
			extensionsMap[string(extension.Name)] = extension.Data
		}
	}
	return
}

func newRequest(id graphsync.RequestID,
	root cid.Cid,
	selector ipld.Node,
	priority graphsync.Priority,
	isCancel bool,
	isUpdate bool,
	extensions map[string][]byte) GraphSyncRequest {
	return GraphSyncRequest{
		id:         id,
		root:       root,
		selector:   selector,
		priority:   priority,
		isCancel:   isCancel,
		isUpdate:   isUpdate,
		extensions: extensions,
	}
}

// NewResponse builds a new Graphsync response
func NewResponse(requestID graphsync.RequestID,
	status graphsync.ResponseStatusCode,
	extensions ...graphsync.ExtensionData) GraphSyncResponse {
	return newResponse(requestID, status, toExtensionsMap(extensions))
}

func newResponse(requestID graphsync.RequestID,
	status graphsync.ResponseStatusCode, extensions map[string][]byte) GraphSyncResponse {
	return GraphSyncResponse{
		requestID:  requestID,
		status:     status,
		extensions: extensions,
	}
}

// Empty returns true if this message has no actionable content
func (gsm GraphSyncMessage) Empty() bool {
	return len(gsm.blocks) == 0 && len(gsm.requests) == 0 && len(gsm.responses) == 0
}

// Requests returns a copy of the list of GraphSyncRequests in this
// GraphSyncMessage
func (gsm GraphSyncMessage) Requests() []GraphSyncRequest {
	requests := make([]GraphSyncRequest, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		requests = append(requests, request)
	}
	return requests
}

// ResponseCodes returns a list of ResponseStatusCodes contained in the
// responses in this GraphSyncMessage
func (gsm GraphSyncMessage) ResponseCodes() map[graphsync.RequestID]graphsync.ResponseStatusCode {
	codes := make(map[graphsync.RequestID]graphsync.ResponseStatusCode, len(gsm.responses))
	for id, response := range gsm.responses {
		codes[id] = response.Status()
	}
	return codes
}

// Responses returns a copy of the list of GraphSyncResponses in this
// GraphSyncMessage
func (gsm GraphSyncMessage) Responses() []GraphSyncResponse {
	responses := make([]GraphSyncResponse, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		responses = append(responses, response)
	}
	return responses
}

// Blocks returns a copy of the list of Blocks in this GraphSyncMessage
func (gsm GraphSyncMessage) Blocks() []blocks.Block {
	bs := make([]blocks.Block, 0, len(gsm.blocks))
	for _, block := range gsm.blocks {
		bs = append(bs, block)
	}
	return bs
}

// Loggable returns a simplified, single-line log form of this GraphSyncMessage
func (gsm GraphSyncMessage) Loggable() map[string]interface{} {
	requests := make([]string, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		requests = append(requests, request.id.String())
	}
	responses := make([]string, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		responses = append(responses, response.requestID.String())
	}
	return map[string]interface{}{
		"requests":  requests,
		"responses": responses,
	}
}

// Clone returns a shallow copy of this GraphSyncMessage
func (gsm GraphSyncMessage) Clone() GraphSyncMessage {
	requests := make(map[graphsync.RequestID]GraphSyncRequest, len(gsm.requests))
	for id, request := range gsm.requests {
		requests[id] = request
	}
	responses := make(map[graphsync.RequestID]GraphSyncResponse, len(gsm.responses))
	for id, response := range gsm.responses {
		responses[id] = response
	}
	blocks := make(map[cid.Cid]blocks.Block, len(gsm.blocks))
	for cid, block := range gsm.blocks {
		blocks[cid] = block
	}
	return GraphSyncMessage{requests, responses, blocks}
}

// ID Returns the request ID for this Request
func (gsr GraphSyncRequest) ID() graphsync.RequestID { return gsr.id }

// Root returns the CID to the root block of this request
func (gsr GraphSyncRequest) Root() cid.Cid { return gsr.root }

// Selector returns the byte representation of the selector for this request
func (gsr GraphSyncRequest) Selector() ipld.Node { return gsr.selector }

// Priority returns the priority of this request
func (gsr GraphSyncRequest) Priority() graphsync.Priority { return gsr.priority }

// Extension returns the content for an extension on a response, or errors
// if extension is not present
func (gsr GraphSyncRequest) Extension(name graphsync.ExtensionName) ([]byte, bool) {
	if gsr.extensions == nil {
		return nil, false
	}
	val, ok := gsr.extensions[string(name)]
	if !ok {
		return nil, false
	}
	return val, true
}

// ExtensionNames returns the names of the extensions included in this request
func (gsr GraphSyncRequest) ExtensionNames() []string {
	var extNames []string
	for ext := range gsr.extensions {
		extNames = append(extNames, ext)
	}
	return extNames
}

// IsCancel returns true if this particular request is being cancelled
func (gsr GraphSyncRequest) IsCancel() bool { return gsr.isCancel }

// IsUpdate returns true if this particular request is being updated
func (gsr GraphSyncRequest) IsUpdate() bool { return gsr.isUpdate }

// RequestID returns the request ID for this response
func (gsr GraphSyncResponse) RequestID() graphsync.RequestID { return gsr.requestID }

// Status returns the status for a response
func (gsr GraphSyncResponse) Status() graphsync.ResponseStatusCode { return gsr.status }

// Extension returns the content for an extension on a response, or errors
// if extension is not present
func (gsr GraphSyncResponse) Extension(name graphsync.ExtensionName) ([]byte, bool) {
	if gsr.extensions == nil {
		return nil, false
	}
	val, ok := gsr.extensions[string(name)]
	if !ok {
		return nil, false
	}
	return val, true
}

// ExtensionNames returns the names of the extensions included in this request
func (gsr GraphSyncResponse) ExtensionNames() []string {
	var extNames []string
	for ext := range gsr.extensions {
		extNames = append(extNames, ext)
	}
	return extNames
}

// ReplaceExtensions merges the extensions given extensions into the request to create a new request,
// but always uses new data
func (gsr GraphSyncRequest) ReplaceExtensions(extensions []graphsync.ExtensionData) GraphSyncRequest {
	req, _ := gsr.MergeExtensions(extensions, func(name graphsync.ExtensionName, oldData []byte, newData []byte) ([]byte, error) {
		return newData, nil
	})
	return req
}

// MergeExtensions merges the given list of extensions to produce a new request with the combination of the old request
// plus the new extensions. When an old extension and a new extension are both present, mergeFunc is called to produce
// the result
func (gsr GraphSyncRequest) MergeExtensions(extensions []graphsync.ExtensionData, mergeFunc func(name graphsync.ExtensionName, oldData []byte, newData []byte) ([]byte, error)) (GraphSyncRequest, error) {
	if gsr.extensions == nil {
		return newRequest(gsr.id, gsr.root, gsr.selector, gsr.priority, gsr.isCancel, gsr.isUpdate, toExtensionsMap(extensions)), nil
	}
	newExtensionMap := toExtensionsMap(extensions)
	combinedExtensions := make(map[string][]byte)
	for name, newData := range newExtensionMap {
		oldData, ok := gsr.extensions[name]
		if !ok {
			combinedExtensions[name] = newData
			continue
		}
		resultData, err := mergeFunc(graphsync.ExtensionName(name), oldData, newData)
		if err != nil {
			return GraphSyncRequest{}, err
		}
		combinedExtensions[name] = resultData
	}

	for name, oldData := range gsr.extensions {
		_, ok := combinedExtensions[name]
		if ok {
			continue
		}
		combinedExtensions[name] = oldData
	}
	return newRequest(gsr.id, gsr.root, gsr.selector, gsr.priority, gsr.isCancel, gsr.isUpdate, combinedExtensions), nil
}
