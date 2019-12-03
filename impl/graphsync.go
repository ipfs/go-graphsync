package graphsync

import (
	"context"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/asyncloader"

	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/peermanager"
	"github.com/ipfs/go-graphsync/requestmanager"
	"github.com/ipfs/go-graphsync/responsemanager"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-peertaskqueue"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("graphsync")

// GraphSync is an instance of a GraphSync exchange that implements
// the graphsync protocol.
type GraphSync struct {
	ipldBridge          ipldbridge.IPLDBridge
	network             gsnet.GraphSyncNetwork
	loader              ipldbridge.Loader
	storer              ipldbridge.Storer
	requestManager      *requestmanager.RequestManager
	responseManager     *responsemanager.ResponseManager
	asyncLoader         *asyncloader.AsyncLoader
	peerResponseManager *peerresponsemanager.PeerResponseManager
	peerTaskQueue       *peertaskqueue.PeerTaskQueue
	peerManager         *peermanager.PeerMessageManager
	ctx                 context.Context
	cancel              context.CancelFunc
}

// New creates a new GraphSync Exchange on the given network,
// using the given bridge to IPLD and the given link loader.
func New(parent context.Context, network gsnet.GraphSyncNetwork,
	ipldBridge ipldbridge.IPLDBridge, loader ipldbridge.Loader,
	storer ipldbridge.Storer) graphsync.GraphExchange {
	ctx, cancel := context.WithCancel(parent)

	createMessageQueue := func(ctx context.Context, p peer.ID) peermanager.PeerQueue {
		return messagequeue.New(ctx, p, network)
	}
	peerManager := peermanager.NewMessageManager(ctx, createMessageQueue)
	asyncLoader := asyncloader.New(ctx, loader, storer)
	requestManager := requestmanager.New(ctx, asyncLoader, ipldBridge)
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
		storer:              storer,
		asyncLoader:         asyncLoader,
		requestManager:      requestManager,
		peerManager:         peerManager,
		peerTaskQueue:       peerTaskQueue,
		peerResponseManager: peerResponseManager,
		responseManager:     responseManager,
		ctx:                 ctx,
		cancel:              cancel,
	}

	asyncLoader.Startup()
	requestManager.SetDelegate(peerManager)
	requestManager.Startup()
	responseManager.Startup()
	network.SetDelegate((*graphSyncReceiver)(graphSync))
	return graphSync
}

// Request initiates a new GraphSync request to the given peer using the given selector spec.
func (gs *GraphSync) Request(ctx context.Context, p peer.ID, root ipld.Link, selector ipld.Node, extensions ...graphsync.ExtensionData) (<-chan graphsync.ResponseProgress, <-chan error) {
	return gs.requestManager.SendRequest(ctx, p, root, selector, extensions...)
}

// RegisterRequestReceivedHook adds a hook that runs when a request is received
// If overrideDefaultValidation is set to true, then if the hook does not error,
// it is considered to have "validated" the request -- and that validation supersedes
// the normal validation of requests Graphsync does (i.e. all selectors can be accepted)
func (gs *GraphSync) RegisterRequestReceivedHook(hook graphsync.OnRequestReceivedHook) error {
	gs.responseManager.RegisterHook(hook)
	return nil
}

// RegisterResponseReceivedHook adds a hook that runs when a response is received
func (gs *GraphSync) RegisterResponseReceivedHook(hook graphsync.OnResponseReceivedHook) error {
	gs.requestManager.RegisterHook(hook)
	return nil
}

type graphSyncReceiver GraphSync

func (gsr *graphSyncReceiver) graphSync() *GraphSync {
	return (*GraphSync)(gsr)
}

// ReceiveMessage is part of the networks Receiver interface and receives
// incoming messages from the network
func (gsr *graphSyncReceiver) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming gsmsg.GraphSyncMessage) {
	gsr.graphSync().responseManager.ProcessRequests(ctx, sender, incoming.Requests())
	gsr.graphSync().requestManager.ProcessResponses(sender, incoming.Responses(), incoming.Blocks())
}

// ReceiveError is part of the network's Receiver interface and handles incoming
// errors from the network.
func (gsr *graphSyncReceiver) ReceiveError(err error) {
	log.Infof("Graphsync ReceiveError: %s", err)
	// TODO log the network error
	// TODO bubble the network error up to the parent context/error logger
}

// Connected is part of the networks 's Receiver interface and handles peers connecting
// on the network
func (gsr *graphSyncReceiver) Connected(p peer.ID) {
	gsr.graphSync().peerManager.Connected(p)
	gsr.graphSync().peerResponseManager.Connected(p)
}

// Connected is part of the networks 's Receiver interface and handles peers connecting
// on the network
func (gsr *graphSyncReceiver) Disconnected(p peer.ID) {
	gsr.graphSync().peerManager.Disconnected(p)
	gsr.graphSync().peerResponseManager.Disconnected(p)
}
