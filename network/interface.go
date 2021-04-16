package network

import (
	"context"

	"github.com/libp2p/go-libp2p-core/peer"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

// DataTransferNetwork provides network connectivity for GraphSync.
type DataTransferNetwork interface {
	Protect(id peer.ID, tag string)
	Unprotect(id peer.ID, tag string) bool

	// SendMessage sends a GraphSync message to a peer.
	SendMessage(
		context.Context,
		peer.ID,
		datatransfer.Message) error

	// SetDelegate registers the Reciver to handle messages received from the
	// network.
	SetDelegate(Receiver)

	// ConnectTo establishes a connection to the given peer
	ConnectTo(context.Context, peer.ID) error

	// ConnectWithRetry establishes a connection to the given peer, retrying if
	// necessary, and opens a stream on the data-transfer protocol to verify
	// the peer will accept messages on the protocol
	ConnectWithRetry(ctx context.Context, p peer.ID) error

	// ID returns the peer id of this libp2p host
	ID() peer.ID
}

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type Receiver interface {
	ReceiveRequest(
		ctx context.Context,
		sender peer.ID,
		incoming datatransfer.Request)

	ReceiveResponse(
		ctx context.Context,
		sender peer.ID,
		incoming datatransfer.Response)

	ReceiveRestartExistingChannelRequest(ctx context.Context, sender peer.ID, incoming datatransfer.Request)

	ReceiveError(error)
}
