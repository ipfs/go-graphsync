package peermanager

import (
	"context"
	"testing"

	"github.com/ipfs/go-graphsync/testutil"
	"github.com/libp2p/go-libp2p-core/peer"
)

type fakePeerProcess struct {
}

func (fp *fakePeerProcess) Startup()  {}
func (fp *fakePeerProcess) Shutdown() {}

func TestAddingAndRemovingPeers(t *testing.T) {
	ctx := context.Background()
	peerProcessFatory := func(ctx context.Context, p peer.ID) PeerProcess {
		return &fakePeerProcess{}
	}

	tp := testutil.GeneratePeers(5)
	peer1, peer2, peer3, peer4, peer5 := tp[0], tp[1], tp[2], tp[3], tp[4]
	peerManager := New(ctx, peerProcessFatory)

	peerManager.Connected(peer1)
	peerManager.Connected(peer2)
	peerManager.Connected(peer3)

	connectedPeers := peerManager.ConnectedPeers()

	if !testutil.ContainsPeer(connectedPeers, peer1) ||
		!testutil.ContainsPeer(connectedPeers, peer2) ||
		!testutil.ContainsPeer(connectedPeers, peer3) {
		t.Fatal("Peers not connected that should be connected")
	}

	if testutil.ContainsPeer(connectedPeers, peer4) ||
		testutil.ContainsPeer(connectedPeers, peer5) {
		t.Fatal("Peers connected that shouldn't be connected")
	}

	// removing a peer with only one reference
	peerManager.Disconnected(peer1)
	connectedPeers = peerManager.ConnectedPeers()

	if testutil.ContainsPeer(connectedPeers, peer1) {
		t.Fatal("Peer should have been disconnected but was not")
	}

	// connecting a peer twice, then disconnecting once, should stay in queue
	peerManager.Connected(peer2)
	peerManager.Disconnected(peer2)
	connectedPeers = peerManager.ConnectedPeers()

	if !testutil.ContainsPeer(connectedPeers, peer2) {
		t.Fatal("Peer was disconnected but should not have been")
	}
}
