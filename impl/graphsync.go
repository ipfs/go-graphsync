package graphsync

import (
	"context"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/peermanager"
	"github.com/ipfs/go-graphsync/requestmanager"
	"github.com/ipfs/go-graphsync/requestmanager/asyncloader"
	requestorhooks "github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager"
	responderhooks "github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	"github.com/ipfs/go-graphsync/responsemanager/persistenceoptions"
	"github.com/ipfs/go-graphsync/selectorvalidator"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-peertaskqueue"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("graphsync")

const maxRecursionDepth = 100

// GraphSync is an instance of a GraphSync exchange that implements
// the graphsync protocol.
type GraphSync struct {
	network                    gsnet.GraphSyncNetwork
	loader                     ipld.Loader
	storer                     ipld.Storer
	requestManager             *requestmanager.RequestManager
	responseManager            *responsemanager.ResponseManager
	asyncLoader                *asyncloader.AsyncLoader
	peerResponseManager        *peerresponsemanager.PeerResponseManager
	peerTaskQueue              *peertaskqueue.PeerTaskQueue
	peerManager                *peermanager.PeerMessageManager
	incomingRequestHooks       *responderhooks.IncomingRequestHooks
	outgoingBlockHooks         *responderhooks.OutgoingBlockHooks
	requestUpdatedHooks        *responderhooks.RequestUpdatedHooks
	incomingResponseHooks      *requestorhooks.IncomingResponseHooks
	outgoingRequestHooks       *requestorhooks.OutgoingRequestHooks
	persistenceOptions         *persistenceoptions.PersistenceOptions
	ctx                        context.Context
	cancel                     context.CancelFunc
	unregisterDefaultValidator graphsync.UnregisterHookFunc
}

// Option defines the functional option type that can be used to configure
// graphsync instances
type Option func(*GraphSync)

// RejectAllRequestsByDefault means that without hooks registered
// that perform their own request validation, all requests are rejected
func RejectAllRequestsByDefault() Option {
	return func(gs *GraphSync) {
		gs.unregisterDefaultValidator()
	}
}

// New creates a new GraphSync Exchange on the given network,
// and the given link loader+storer.
func New(parent context.Context, network gsnet.GraphSyncNetwork,
	loader ipld.Loader, storer ipld.Storer, options ...Option) graphsync.GraphExchange {
	ctx, cancel := context.WithCancel(parent)

	createMessageQueue := func(ctx context.Context, p peer.ID) peermanager.PeerQueue {
		return messagequeue.New(ctx, p, network)
	}
	peerManager := peermanager.NewMessageManager(ctx, createMessageQueue)
	asyncLoader := asyncloader.New(ctx, loader, storer)
	incomingResponseHooks := requestorhooks.NewResponseHooks()
	outgoingRequestHooks := requestorhooks.NewRequestHooks()
	requestManager := requestmanager.New(ctx, asyncLoader, outgoingRequestHooks, incomingResponseHooks)
	peerTaskQueue := peertaskqueue.New()
	createdResponseQueue := func(ctx context.Context, p peer.ID) peerresponsemanager.PeerResponseSender {
		return peerresponsemanager.NewResponseSender(ctx, p, peerManager)
	}
	peerResponseManager := peerresponsemanager.New(ctx, createdResponseQueue)
	persistenceOptions := persistenceoptions.New()
	incomingRequestHooks := responderhooks.NewRequestHooks(persistenceOptions)
	outgoingBlockHooks := responderhooks.NewBlockHooks()
	requestUpdatedHooks := responderhooks.NewUpdateHooks()
	responseManager := responsemanager.New(ctx, loader, peerResponseManager, peerTaskQueue, incomingRequestHooks, outgoingBlockHooks, requestUpdatedHooks)
	unregisterDefaultValidator := incomingRequestHooks.Register(selectorvalidator.SelectorValidator(maxRecursionDepth))
	graphSync := &GraphSync{
		network:                    network,
		loader:                     loader,
		storer:                     storer,
		asyncLoader:                asyncLoader,
		requestManager:             requestManager,
		peerManager:                peerManager,
		persistenceOptions:         persistenceOptions,
		incomingRequestHooks:       incomingRequestHooks,
		outgoingBlockHooks:         outgoingBlockHooks,
		requestUpdatedHooks:        requestUpdatedHooks,
		incomingResponseHooks:      incomingResponseHooks,
		outgoingRequestHooks:       outgoingRequestHooks,
		peerTaskQueue:              peerTaskQueue,
		peerResponseManager:        peerResponseManager,
		responseManager:            responseManager,
		ctx:                        ctx,
		cancel:                     cancel,
		unregisterDefaultValidator: unregisterDefaultValidator,
	}

	for _, option := range options {
		option(graphSync)
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

// RegisterIncomingRequestHook adds a hook that runs when a request is received
// If overrideDefaultValidation is set to true, then if the hook does not error,
// it is considered to have "validated" the request -- and that validation supersedes
// the normal validation of requests Graphsync does (i.e. all selectors can be accepted)
func (gs *GraphSync) RegisterIncomingRequestHook(hook graphsync.OnIncomingRequestHook) graphsync.UnregisterHookFunc {
	return gs.incomingRequestHooks.Register(hook)
}

// RegisterIncomingResponseHook adds a hook that runs when a response is received
func (gs *GraphSync) RegisterIncomingResponseHook(hook graphsync.OnIncomingResponseHook) graphsync.UnregisterHookFunc {
	return gs.incomingResponseHooks.Register(hook)
}

// RegisterOutgoingRequestHook adds a hook that runs immediately prior to sending a new request
func (gs *GraphSync) RegisterOutgoingRequestHook(hook graphsync.OnOutgoingRequestHook) graphsync.UnregisterHookFunc {
	return gs.outgoingRequestHooks.Register(hook)
}

// RegisterPersistenceOption registers an alternate loader/storer combo that can be substituted for the default
func (gs *GraphSync) RegisterPersistenceOption(name string, loader ipld.Loader, storer ipld.Storer) error {
	err := gs.asyncLoader.RegisterPersistenceOption(name, loader, storer)
	if err != nil {
		return err
	}
	return gs.persistenceOptions.Register(name, loader)
}

// RegisterOutgoingBlockHook registers a hook that runs after each block is sent in a response
func (gs *GraphSync) RegisterOutgoingBlockHook(hook graphsync.OnOutgoingBlockHook) graphsync.UnregisterHookFunc {
	return gs.outgoingBlockHooks.Register(hook)
}

// RegisterRequestUpdatedHook registers a hook that runs when an update to a request is received
func (gs *GraphSync) RegisterRequestUpdatedHook(hook graphsync.OnRequestUpdatedHook) graphsync.UnregisterHookFunc {
	return gs.requestUpdatedHooks.Register(hook)
}

// UnpauseResponse unpauses a response that was paused in a block hook based on peer ID and request ID
func (gs *GraphSync) UnpauseResponse(p peer.ID, requestID graphsync.RequestID) error {
	return gs.responseManager.UnpauseResponse(p, requestID)
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
