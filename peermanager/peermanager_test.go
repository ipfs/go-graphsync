package peermanager

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync/testutil"
)

type fakePeerProcess struct {
}

func (fp *fakePeerProcess) Startup()  {}
func (fp *fakePeerProcess) Shutdown() {}

func TestAddingAndRemovingPeers(t *testing.T) {
	ctx := context.Background()
	peerProcessFatory := func(ctx context.Context, p peer.ID) PeerHandler {
		return &fakePeerProcess{}
	}

	tp := testutil.GeneratePeers(5)
	peer1, peer2, peer3, peer4, peer5 := tp[0], tp[1], tp[2], tp[3], tp[4]
	peerManager := New(ctx, peerProcessFatory)

	peerManager.Connected(peer1)
	peerManager.Connected(peer2)
	peerManager.Connected(peer3)

	connectedPeers := peerManager.ConnectedPeers()

	testutil.AssertContainsPeer(t, connectedPeers, peer1)
	testutil.AssertContainsPeer(t, connectedPeers, peer2)
	testutil.AssertContainsPeer(t, connectedPeers, peer3)

	testutil.RefuteContainsPeer(t, connectedPeers, peer4)
	testutil.RefuteContainsPeer(t, connectedPeers, peer5)

	// removing a peer with only one reference
	peerManager.Disconnected(peer1)
	connectedPeers = peerManager.ConnectedPeers()

	testutil.RefuteContainsPeer(t, connectedPeers, peer1)

	// connecting a peer twice, then disconnecting once, should stay in queue
	peerManager.Connected(peer2)
	peerManager.Disconnected(peer2)
	connectedPeers = peerManager.ConnectedPeers()

	testutil.AssertContainsPeer(t, connectedPeers, peer2)
}
