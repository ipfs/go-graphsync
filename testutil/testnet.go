package testutil

import (
	"context"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/network"
)

// FakeSentMessage is a recording of a message sent on the FakeNetwork
type FakeSentMessage struct {
	PeerID  peer.ID
	Message datatransfer.Message
}

// FakeNetwork is a network that satisfies the DataTransferNetwork interface but
// does not actually do anything
type FakeNetwork struct {
	PeerID       peer.ID
	SentMessages []FakeSentMessage
	Delegate     network.Receiver
}

// NewFakeNetwork returns a new fake data transfer network instance
func NewFakeNetwork(id peer.ID) *FakeNetwork {
	return &FakeNetwork{PeerID: id}
}

var _ network.DataTransferNetwork = (*FakeNetwork)(nil)

// SendMessage sends a GraphSync message to a peer.
func (fn *FakeNetwork) SendMessage(ctx context.Context, p peer.ID, m datatransfer.Message) error {
	fn.SentMessages = append(fn.SentMessages, FakeSentMessage{p, m})
	return nil
}

// SetDelegate registers the Reciver to handle messages received from the
// network.
func (fn *FakeNetwork) SetDelegate(receiver network.Receiver) {
	fn.Delegate = receiver
}

// ConnectTo establishes a connection to the given peer
func (fn *FakeNetwork) ConnectTo(_ context.Context, _ peer.ID) error {
	panic("not implemented")
}

func (fn *FakeNetwork) ConnectWithRetry(ctx context.Context, p peer.ID) error {
	panic("implement me")
}

// ID returns a stubbed id for host of this network
func (fn *FakeNetwork) ID() peer.ID {
	return fn.PeerID
}

// Protect does nothing on the fake network
func (fn *FakeNetwork) Protect(id peer.ID, tag string) {
}

// Unprotect does nothing on the fake network
func (fn *FakeNetwork) Unprotect(id peer.ID, tag string) bool {
	return false
}

func (fn *FakeNetwork) Protocol(ctx context.Context, id peer.ID) (protocol.ID, error) {
	return datatransfer.ProtocolDataTransfer1_2, nil
}
