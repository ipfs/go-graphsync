package graphsync

import (
	"context"
	"errors"

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

// OnRequestReceivedHook processes extension data in a request
// responseData - the value that should be sent for the extension in the reply
// err - error - if not nil, halt request and return RequestRejected with the responseData
type OnRequestReceivedHook func(requestData []byte) (responseData ExtensionData, err error)

// OnResponseReceivedHook processes extension data in a response
// When it returns an error processing is halted and the original request is cancelled
type OnResponseReceivedHook func(responseData []byte) error

// ExtensionConfig defines behavior for user supplied extension
type ExtensionConfig struct {
	Name ExtensionName
	// PerformsValidation specifies if the request received hook can be considered "a validator"
	// if true, and the hook does not error, then one can assume the request is valid and this
	// should override default validation schemes
	PerformsValidation bool

	// OnRequestReceived is called whenever a request is received that has data for this extension
	OnRequestReceived OnRequestReceivedHook

	// OnResponseReceived is called whenever a response comes back that has data for this extension
	OnResponseReceived OnResponseReceivedHook
}

// GraphExchange is a protocol that can exchange IPLD graphs based on a selector
type GraphExchange interface {
	// Request initiates a new GraphSync request to the given peer using the given selector spec.
	Request(ctx context.Context, p peer.ID, root ipld.Link, selector ipld.Node, extensions ...ExtensionData) (<-chan ResponseProgress, <-chan error)

	// RegisterExtension adds a user supplied extension with the given extension config
	RegisterExtension(config ExtensionConfig) error
}
