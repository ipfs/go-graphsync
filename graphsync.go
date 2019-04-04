package graphsync

import (
	"context"

	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/peermanager"
	"github.com/ipfs/go-graphsync/requestmanager"
	"github.com/ipfs/go-graphsync/responsemanager"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	"github.com/ipfs/go-graphsync/responsemanager/peertaskqueue"
	logging "github.com/ipfs/go-log"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-peer"
)

var log = logging.Logger("graphsync")

// ResponseProgress is the fundamental unit of responses making progress in
// Graphsync.
type ResponseProgress = requestmanager.ResponseProgress

// ResponseError is an error that occurred during a traversal.
type ResponseError = requestmanager.ResponseError

// GraphSync is an instance of a GraphSync exchange that implements
// the graphsync protocol.
type GraphSync struct {
	ipldBridge          ipldbridge.IPLDBridge
	network             gsnet.GraphSyncNetwork
	loader              ipldbridge.Loader
	requestManager      *requestmanager.RequestManager
	responseManager     *responsemanager.ResponseManager
	peerResponseManager *peerresponsemanager.PeerResponseManager
	peerTaskQueue       *peertaskqueue.PeerTaskQueue
	peerManager         *peermanager.PeerMessageManager
	ctx                 context.Context
	cancel              context.CancelFunc
}

// New creates a new GraphSync Exchange on the given network,
// using the given bridge to IPLD and the given link loader.
func New(parent context.Context, network gsnet.GraphSyncNetwork,
	ipldBridge ipldbridge.IPLDBridge, loader ipldbridge.Loader) *GraphSync {
	ctx, cancel := context.WithCancel(parent)

	createMessageQueue := func(ctx context.Context, p peer.ID) peermanager.PeerQueue {
		return messagequeue.New(ctx, p, network)
	}
	peerManager := peermanager.NewMessageManager(ctx, createMessageQueue)
	requestManager := requestmanager.New(ctx, ipldBridge)
	peerTaskQueue := peertaskqueue.New()
	createdResponseQueue := func(ctx context.Context, p peer.ID) peerresponsemanager.PeerResponseSender {
		return peerresponsemanager.NewResponseSender(ctx, p, peerManager, ipldBridge)
	}
	peerResponseManager := peerresponsemanager.New(ctx, createdResponseQueue)
	responseManager := responsemanager.New(ctx, loader, ipldBridge, peerResponseManager, peerTaskQueue)
	graphSync := &GraphSync{
		ipldBridge:          ipldBridge,
		network:             network,
		loader:              loader,
		requestManager:      requestManager,
		peerManager:         peerManager,
		peerTaskQueue:       peerTaskQueue,
		peerResponseManager: peerResponseManager,
		responseManager:     responseManager,
		ctx:                 ctx,
		cancel:              cancel,
	}

	requestManager.SetDelegate(peerManager)
	requestManager.Startup()
	responseManager.Startup()
	network.SetDelegate(graphSync)
	return graphSync
}

// Request initiates a new GraphSync request to the given peer using the given selector spec.
func (gs *GraphSync) Request(ctx context.Context, p peer.ID, rootedSelector ipld.Node) (<-chan ResponseProgress, <-chan ResponseError) {
	return gs.requestManager.SendRequest(ctx, p, rootedSelector)
}

// ReceiveMessage is part of the networks Receiver interface and receives
// incoming messages from the network
func (gs *GraphSync) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming gsmsg.GraphSyncMessage) {
	gs.responseManager.ProcessRequests(ctx, sender, incoming.Requests())
}

// ReceiveError is part of the network's Receiver interface and handles incoming
// errors from the network.
func (gs *GraphSync) ReceiveError(err error) {
	log.Errorf("Error: %s", err.Error())
}
