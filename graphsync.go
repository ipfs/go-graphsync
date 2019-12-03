package graphsync

import (
	"context"
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	peer "github.com/libp2p/go-libp2p-peer"
)

// RequestID is a unique identifier for a GraphSync request.
type RequestID int32

// Priority a priority for a GraphSync request.
type Priority int32

// ResponseStatusCode is a status returned for a GraphSync Request.
type ResponseStatusCode int32

// ExtensionName is a name for a GraphSync extension
type ExtensionName string

// ExtensionData is a name/data pair for a graphsync extension
type ExtensionData struct {
	Name ExtensionName
	Data []byte
}

const (

	// Known Graphsync Extensions

	// ExtensionMetadata provides response metadata for a Graphsync request and is
	// documented at
	// https://github.com/ipld/specs/blob/master/block-layer/graphsync/known_extensions.md
	ExtensionMetadata = ExtensionName("graphsync/response-metadata")

	// ExtensionDoNotSendCIDs tells the responding peer not to send certain blocks if they
	// are encountered in a traversal and is documented at
	// https://github.com/ipld/specs/blob/master/block-layer/graphsync/known_extensions.md
	ExtensionDoNotSendCIDs = ExtensionName("graphsync/do-not-send-cids")

	// GraphSync Response Status Codes

	// Informational Response Codes (partial)

	// RequestAcknowledged means the request was received and is being worked on.
	RequestAcknowledged = ResponseStatusCode(10)
	// AdditionalPeers means additional peers were found that may be able
	// to satisfy the request and contained in the extra block of the response.
	AdditionalPeers = ResponseStatusCode(11)
	// NotEnoughGas means fulfilling this request requires payment.
	NotEnoughGas = ResponseStatusCode(12)
	// OtherProtocol means a different type of response than GraphSync is
	// contained in extra.
	OtherProtocol = ResponseStatusCode(13)
	// PartialResponse may include blocks and metadata about the in progress response
	// in extra.
	PartialResponse = ResponseStatusCode(14)

	// Success Response Codes (request terminated)

	// RequestCompletedFull means the entire fulfillment of the GraphSync request
	// was sent back.
	RequestCompletedFull = ResponseStatusCode(20)
	// RequestCompletedPartial means the response is completed, and part of the
	// GraphSync request was sent back, but not the complete request.
	RequestCompletedPartial = ResponseStatusCode(21)

	// Error Response Codes (request terminated)

	// RequestRejected means the node did not accept the incoming request.
	RequestRejected = ResponseStatusCode(30)
	// RequestFailedBusy means the node is too busy, try again later. Backoff may
	// be contained in extra.
	RequestFailedBusy = ResponseStatusCode(31)
	// RequestFailedUnknown means the request failed for an unspecified reason. May
	// contain data about why in extra.
	RequestFailedUnknown = ResponseStatusCode(32)
	// RequestFailedLegal means the request failed for legal reasons.
	RequestFailedLegal = ResponseStatusCode(33)
	// RequestFailedContentNotFound means the respondent does not have the content.
	RequestFailedContentNotFound = ResponseStatusCode(34)
)

var (
	// ErrExtensionAlreadyRegistered means a user extension can be registered only once
	ErrExtensionAlreadyRegistered = errors.New("extension already registered")
)

// ResponseProgress is the fundamental unit of responses making progress in Graphsync.
type ResponseProgress struct {
	Node      ipld.Node // a node which matched the graphsync query
	Path      ipld.Path // the path of that node relative to the traversal start
	LastBlock struct {  // LastBlock stores the Path and Link of the last block edge we had to load.
		Path ipld.Path
		Link ipld.Link
	}
}

// RequestData describes a received graphsync request.
type RequestData interface {
	// ID Returns the request ID for this Request
	ID() RequestID

	// Root returns the CID to the root block of this request
	Root() cid.Cid

	// Selector returns the byte representation of the selector for this request
	Selector() []byte

	// Priority returns the priority of this request
	Priority() Priority

	// Extension returns the content for an extension on a response, or errors
	// if extension is not present
	Extension(name ExtensionName) ([]byte, bool)

	// IsCancel returns true if this particular request is being cancelled
	IsCancel() bool
}

// ResponseData describes a received Graphsync response
type ResponseData interface {
	// RequestID returns the request ID for this response
	RequestID() RequestID

	// Status returns the status for a response
	Status() ResponseStatusCode

	// Extension returns the content for an extension on a response, or errors
	// if extension is not present
	Extension(name ExtensionName) ([]byte, bool)
}

// RequestReceivedHookActions are actions that a request hook can take to change
// behavior for the response
type RequestReceivedHookActions interface {
	SendExtensionData(ExtensionData)
	TerminateWithError(error)
	ValidateRequest()
}

// OnRequestReceivedHook is a hook that runs each time a request is received.
// It receives the peer that sent the request and all data about the request.
// It should return:
// extensionData - any extension data to add to the outgoing response
// err - error - if not nil, halt request and return RequestRejected with the responseData
type OnRequestReceivedHook func(p peer.ID, request RequestData, hookActions RequestReceivedHookActions)

// OnResponseReceivedHook is a hook that runs each time a response is received.
// It receives the peer that sent the response and all data about the response.
// If it returns an error processing is halted and the original request is cancelled.
type OnResponseReceivedHook func(p peer.ID, responseData ResponseData) error

// GraphExchange is a protocol that can exchange IPLD graphs based on a selector
type GraphExchange interface {
	// Request initiates a new GraphSync request to the given peer using the given selector spec.
	Request(ctx context.Context, p peer.ID, root ipld.Link, selector ipld.Node, extensions ...ExtensionData) (<-chan ResponseProgress, <-chan error)

	// RegisterRequestReceivedHook adds a hook that runs when a request is received
	// If overrideDefaultValidation is set to true, then if the hook does not error,
	// it is considered to have "validated" the request -- and that validation supersedes
	// the normal validation of requests Graphsync does (i.e. all selectors can be accepted)
	RegisterRequestReceivedHook(hook OnRequestReceivedHook) error

	// RegisterResponseReceivedHook adds a hook that runs when a response is received
	RegisterResponseReceivedHook(OnResponseReceivedHook) error
}
