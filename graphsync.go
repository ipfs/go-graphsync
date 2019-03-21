package graphsync

import (
	"context"

	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/peermanager"

	"github.com/ipfs/go-graphsync/ipldbridge"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/requestmanager"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-peer"
)

// ResponseProgress is the fundamental unit of responses making progress in
// Graphsync.
type ResponseProgress = requestmanager.ResponseProgress

// ResponseError is an error that occurred during a traversal.
type ResponseError = requestmanager.ResponseError

// GraphSync is an instance of a GraphSync exchange that implements
// the graphsync protocol.
type GraphSync struct {
	ipldBridge     ipldbridge.IPLDBridge
	network        gsnet.GraphSyncNetwork
	loader         ipldbridge.Loader
	requestManager *requestmanager.RequestManager
	peerManager    *peermanager.PeerManager
	ctx            context.Context
	cancel         context.CancelFunc
}

// New creates a new GraphSync Exchange on the given network,
// using the given bridge to IPLD and the given link loader.
func New(parent context.Context, network gsnet.GraphSyncNetwork,
	ipldBridge ipldbridge.IPLDBridge, loader ipldbridge.Loader) *GraphSync {
	ctx, cancel := context.WithCancel(parent)

	createMessageQueue := func(ctx context.Context, p peer.ID) peermanager.PeerQueue {
		return messagequeue.New(ctx, p, network)
	}
	peerManager := peermanager.New(ctx, createMessageQueue)
	requestManager := requestmanager.New(ctx, ipldBridge)
	graphSync := &GraphSync{
		ipldBridge:     ipldBridge,
		network:        network,
		loader:         loader,
		requestManager: requestManager,
		peerManager:    peerManager,
		ctx:            ctx,
		cancel:         cancel,
	}

	requestManager.SetDelegate(peerManager)
	requestManager.Startup()

	return graphSync
}

// Request initiates a new GraphSync request to the given peer using the given selector spec.
func (gs *GraphSync) Request(ctx context.Context, p peer.ID, rootedSelector ipld.Node) (<-chan ResponseProgress, <-chan ResponseError) {
	return gs.requestManager.SendRequest(ctx, p, rootedSelector)
}
