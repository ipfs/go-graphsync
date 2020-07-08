/*
Package transport defines interfaces for implementing a transport layer for data
transfer. Where the data transfer manager will coordinate setting up push and
pull requests, validation, etc, the transport layer is responsible for moving
data back and forth, and may be medium specific. For example, some transports
may have the ability to pause and resume requests, while others may not.
Some may support individual data events, while others may only support message
events. Some transport layers may opt to use the actual data transfer network
protocols directly while others may be able to encode messages in their own
data protocol.
*/
package transport

import (
	"context"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/message"
	ipld "github.com/ipld/go-ipld-prime"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type errorType string

func (e errorType) Error() string {
	return string(e)
}

// ErrHandlerAlreadySet means an event handler was already set for this instance of
// hooks
const ErrHandlerAlreadySet = errorType("already set event handler")

// ErrHandlerNotSet means you cannot issue commands to this interface because the
// handler has not been set
const ErrHandlerNotSet = errorType("event handler has not been set")

// ErrChannelNotFound means the channel this command was issued for does not exist
const ErrChannelNotFound = errorType("channel not found")

// ErrPause is a special error that the DataReceived / DataSent hooks can
// use to pause the channel
const ErrPause = errorType("pause channel")

// ErrResume is a special error that the RequestReceived / ResponseReceived hooks can
// use to resume the channel
const ErrResume = errorType("resume channel")

// Events are semantic data transfer events that happen as a result of graphsync hooks
type Events interface {
	// OnChannelOpened is called when we ask the other peer to send us data on the
	// given channel ID
	// return values are:
	// - nil = this channel is recognized
	// - error = ignore incoming data for this channel
	OnChannelOpened(chid datatransfer.ChannelID) error
	// OnResponseReceived is called when we receive a response to a request
	// - nil = continue receiving data
	// - error = cancel this request
	OnResponseReceived(chid datatransfer.ChannelID, msg message.DataTransferResponse) error
	// OnDataReceive is called when we receive data for the given channel ID
	// return values are:
	// - nil = proceed with sending data
	// - error = cancel this request
	// - err == ErrPause - pause this request
	OnDataReceived(chid datatransfer.ChannelID, link ipld.Link, size uint64) error
	// OnDataSent is called when we send data for the given channel ID
	// return values are:
	// message = data transfer message along with data
	// err = error
	// - nil = proceed with sending data
	// - error = cancel this request
	// - err == ErrPause - pause this request
	OnDataSent(chid datatransfer.ChannelID, link ipld.Link, size uint64) (message.DataTransferMessage, error)
	// OnRequestReceived is called when we receive a new request to send data
	// for the given channel ID
	// return values are:
	// message = data transfer message along with reply
	// err = error
	// - nil = proceed with sending data
	// - error = cancel this request
	// - err == ErrPause - pause this request (only for new requests)
	// - err == ErrResume - resume this request (only for update requests)
	OnRequestReceived(chid datatransfer.ChannelID, msg message.DataTransferRequest) (message.DataTransferResponse, error)
	// OnResponseCompleted is called when we finish sending data for the given channel ID
	// Error returns are logged but otherwise have not effect
	OnChannelCompleted(chid datatransfer.ChannelID, success bool) error
}

// Transport is the minimum interface that must be satisfied to serve as a datatransfer
// transport layer. Transports must be able to open
// (open is always called by the receiving peer)
// and close channels, and set at an event handler
type Transport interface {
	// OpenChannel initiates an outgoing request for the other peer to send data
	// to us on this channel
	// Note: from a data transfer symantic standpoint, it doesn't matter if the
	// request is push or pull -- OpenChannel is called by the party that is
	// intending to receive data
	OpenChannel(ctx context.Context,
		dataSender peer.ID,
		channelID datatransfer.ChannelID,
		root ipld.Link,
		stor ipld.Node,
		msg message.DataTransferMessage) error

	// CloseChannel closes the given channel
	CloseChannel(ctx context.Context, chid datatransfer.ChannelID) error
	// SetEventHandler sets the handler for events on channels
	SetEventHandler(events Events) error
	// CleanupChannel is called on the otherside of a cancel - removes any associated
	// data for the channel
	CleanupChannel(chid datatransfer.ChannelID)
}

// PauseableTransport is a transport that can also pause and resume channels
type PauseableTransport interface {
	Transport
	// PauseChannel paused the given channel ID
	PauseChannel(ctx context.Context,
		chid datatransfer.ChannelID,
	) error
	// ResumeChannel resumes the given channel
	ResumeChannel(ctx context.Context,
		msg message.DataTransferMessage,
		chid datatransfer.ChannelID,
	) error
}
