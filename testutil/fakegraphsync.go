package testutil

import (
	"context"
	"testing"

	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"
)

// ReceivedGraphSyncRequest contains data about a received graphsync request
type ReceivedGraphSyncRequest struct {
	P          peer.ID
	Root       ipld.Link
	Selector   ipld.Node
	Extensions []graphsync.ExtensionData
}

// FakeGraphSync implements a GraphExchange but does nothing
type FakeGraphSync struct {
	requests chan ReceivedGraphSyncRequest // records calls to fakeGraphSync.Request
}

// NewFakeGraphSync returns a new fake graphsync implementation
func NewFakeGraphSync() *FakeGraphSync {
	return &FakeGraphSync{
		requests: make(chan ReceivedGraphSyncRequest, 1),
	}
}

// AssertNoRequestReceived asserts that no requests should ahve been received by this graphsync implementation
func (fgs *FakeGraphSync) AssertNoRequestReceived(t *testing.T) {
	require.Empty(t, fgs.requests, "should not receive request")
}

// AssertRequestReceived asserts a request should be received before the context closes (and returns said request)
func (fgs *FakeGraphSync) AssertRequestReceived(ctx context.Context, t *testing.T) ReceivedGraphSyncRequest {
	var requestReceived ReceivedGraphSyncRequest
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case requestReceived = <-fgs.requests:
	}
	return requestReceived
}

// Request initiates a new GraphSync request to the given peer using the given selector spec.
func (fgs *FakeGraphSync) Request(ctx context.Context, p peer.ID, root ipld.Link, selector ipld.Node, extensions ...graphsync.ExtensionData) (<-chan graphsync.ResponseProgress, <-chan error) {

	fgs.requests <- ReceivedGraphSyncRequest{p, root, selector, extensions}
	responses := make(chan graphsync.ResponseProgress)
	errors := make(chan error)
	close(responses)
	close(errors)
	return responses, errors
}

// RegisterPersistenceOption registers an alternate loader/storer combo that can be substituted for the default
func (fgs *FakeGraphSync) RegisterPersistenceOption(name string, loader ipld.Loader, storer ipld.Storer) error {
	return nil
}

// RegisterIncomingRequestHook adds a hook that runs when a request is received
func (fgs *FakeGraphSync) RegisterIncomingRequestHook(hook graphsync.OnIncomingRequestHook) graphsync.UnregisterHookFunc {
	return nil
}

// RegisterIncomingResponseHook adds a hook that runs when a response is received
func (fgs *FakeGraphSync) RegisterIncomingResponseHook(_ graphsync.OnIncomingResponseHook) graphsync.UnregisterHookFunc {
	return nil
}

// RegisterOutgoingRequestHook adds a hook that runs immediately prior to sending a new request
func (fgs *FakeGraphSync) RegisterOutgoingRequestHook(hook graphsync.OnOutgoingRequestHook) graphsync.UnregisterHookFunc {
	return nil
}

var _ graphsync.GraphExchange = &FakeGraphSync{}
