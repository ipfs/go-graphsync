package graphsync

import (
	"context"

	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-peertaskqueue"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/allocator"
	"github.com/ipfs/go-graphsync/listeners"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/peermanager"
	"github.com/ipfs/go-graphsync/requestmanager"
	"github.com/ipfs/go-graphsync/requestmanager/asyncloader"
	requestorhooks "github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager"
	responderhooks "github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/persistenceoptions"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
	"github.com/ipfs/go-graphsync/selectorvalidator"
)

var log = logging.Logger("graphsync")

const maxRecursionDepth = 100
const defaultTotalMaxMemory = uint64(256 << 20)
const defaultMaxMemoryPerPeer = uint64(16 << 20)
const defaultMaxInProgressRequests = uint64(6)

// GraphSync is an instance of a GraphSync exchange that implements
// the graphsync protocol.
type GraphSync struct {
	network                     gsnet.GraphSyncNetwork
	loader                      ipld.Loader
	storer                      ipld.Storer
	requestManager              *requestmanager.RequestManager
	responseManager             *responsemanager.ResponseManager
	asyncLoader                 *asyncloader.AsyncLoader
	responseAssembler           *responseassembler.ResponseAssembler
	peerTaskQueue               *peertaskqueue.PeerTaskQueue
	peerManager                 *peermanager.PeerMessageManager
	incomingRequestHooks        *responderhooks.IncomingRequestHooks
	outgoingBlockHooks          *responderhooks.OutgoingBlockHooks
	requestUpdatedHooks         *responderhooks.RequestUpdatedHooks
	completedResponseListeners  *listeners.CompletedResponseListeners
	requestorCancelledListeners *listeners.RequestorCancelledListeners
	blockSentListeners          *listeners.BlockSentListeners
	networkErrorListeners       *listeners.NetworkErrorListeners
	receiverErrorListeners      *listeners.NetworkReceiverErrorListeners
	incomingResponseHooks       *requestorhooks.IncomingResponseHooks
	outgoingRequestHooks        *requestorhooks.OutgoingRequestHooks
	incomingBlockHooks          *requestorhooks.IncomingBlockHooks
	persistenceOptions          *persistenceoptions.PersistenceOptions
	ctx                         context.Context
	cancel                      context.CancelFunc
	allocator                   *allocator.Allocator
}

type graphsyncConfigOptions struct {
	totalMaxMemory           uint64
	maxMemoryPerPeer         uint64
	maxInProgressRequests    uint64
	registerDefaultValidator bool
}

// Option defines the functional option type that can be used to configure
// graphsync instances
type Option func(*graphsyncConfigOptions)

// RejectAllRequestsByDefault means that without hooks registered
// that perform their own request validation, all requests are rejected
func RejectAllRequestsByDefault() Option {
	return func(gs *graphsyncConfigOptions) {
		gs.registerDefaultValidator = false
	}
}

// MaxMemoryResponder defines the maximum amount of memory the responder
// may consume queueing up messages for a response in total
func MaxMemoryResponder(totalMaxMemory uint64) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.totalMaxMemory = totalMaxMemory
	}
}

// MaxMemoryPerPeerResponder defines the maximum amount of memory a peer
// may consume queueing up messages for a response
func MaxMemoryPerPeerResponder(maxMemoryPerPeer uint64) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.maxMemoryPerPeer = maxMemoryPerPeer
	}
}

// MaxInProgressRequests changes the maximum number of
// graphsync requests that are processed in parallel (default 6)
func MaxInProgressRequests(maxInProgressRequests uint64) Option {
	return func(gs *graphsyncConfigOptions) {
		gs.maxInProgressRequests = maxInProgressRequests
	}
}

// New creates a new GraphSync Exchange on the given network,
// and the given link loader+storer.
func New(parent context.Context, network gsnet.GraphSyncNetwork,
	loader ipld.Loader, storer ipld.Storer, options ...Option) graphsync.GraphExchange {
	ctx, cancel := context.WithCancel(parent)

	gsConfig := &graphsyncConfigOptions{
		totalMaxMemory:           defaultTotalMaxMemory,
		maxMemoryPerPeer:         defaultMaxMemoryPerPeer,
		maxInProgressRequests:    defaultMaxInProgressRequests,
		registerDefaultValidator: true,
	}
	for _, option := range options {
		option(gsConfig)
	}
	incomingResponseHooks := requestorhooks.NewResponseHooks()
	outgoingRequestHooks := requestorhooks.NewRequestHooks()
	incomingBlockHooks := requestorhooks.NewBlockHooks()
	networkErrorListeners := listeners.NewNetworkErrorListeners()
	receiverErrorListeners := listeners.NewReceiverNetworkErrorListeners()
	persistenceOptions := persistenceoptions.New()
	incomingRequestHooks := responderhooks.NewRequestHooks(persistenceOptions)
	outgoingBlockHooks := responderhooks.NewBlockHooks()
	requestUpdatedHooks := responderhooks.NewUpdateHooks()
	completedResponseListeners := listeners.NewCompletedResponseListeners()
	requestorCancelledListeners := listeners.NewRequestorCancelledListeners()
	blockSentListeners := listeners.NewBlockSentListeners()
	if gsConfig.registerDefaultValidator {
		incomingRequestHooks.Register(selectorvalidator.SelectorValidator(maxRecursionDepth))
	}
	allocator := allocator.NewAllocator(gsConfig.totalMaxMemory, gsConfig.maxMemoryPerPeer)
	createMessageQueue := func(ctx context.Context, p peer.ID) peermanager.PeerQueue {
		return messagequeue.New(ctx, p, network, allocator)
	}
	peerManager := peermanager.NewMessageManager(ctx, createMessageQueue)
	asyncLoader := asyncloader.New(ctx, loader, storer)
	requestManager := requestmanager.New(ctx, asyncLoader, outgoingRequestHooks, incomingResponseHooks, incomingBlockHooks, networkErrorListeners)
	responseAssembler := responseassembler.New(ctx, peerManager)
	peerTaskQueue := peertaskqueue.New()
	responseManager := responsemanager.New(ctx, loader, responseAssembler, peerTaskQueue, incomingRequestHooks, outgoingBlockHooks, requestUpdatedHooks, completedResponseListeners, requestorCancelledListeners, blockSentListeners, networkErrorListeners, gsConfig.maxInProgressRequests)
	graphSync := &GraphSync{
		network:                     network,
		loader:                      loader,
		storer:                      storer,
		requestManager:              requestManager,
		responseManager:             responseManager,
		asyncLoader:                 asyncLoader,
		responseAssembler:           responseAssembler,
		peerTaskQueue:               peerTaskQueue,
		peerManager:                 peerManager,
		incomingRequestHooks:        incomingRequestHooks,
		outgoingBlockHooks:          outgoingBlockHooks,
		requestUpdatedHooks:         requestUpdatedHooks,
		completedResponseListeners:  completedResponseListeners,
		requestorCancelledListeners: requestorCancelledListeners,
		blockSentListeners:          blockSentListeners,
		networkErrorListeners:       networkErrorListeners,
		receiverErrorListeners:      receiverErrorListeners,
		incomingResponseHooks:       incomingResponseHooks,
		outgoingRequestHooks:        outgoingRequestHooks,
		incomingBlockHooks:          incomingBlockHooks,
		persistenceOptions:          persistenceOptions,
		ctx:                         ctx,
		cancel:                      cancel,
		allocator:                   allocator,
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

// UnregisterPersistenceOption unregisters an alternate loader/storer combo
func (gs *GraphSync) UnregisterPersistenceOption(name string) error {
	err := gs.asyncLoader.UnregisterPersistenceOption(name)
	if err != nil {
		return err
	}
	return gs.persistenceOptions.Unregister(name)
}

// RegisterOutgoingBlockHook registers a hook that runs after each block is sent in a response
func (gs *GraphSync) RegisterOutgoingBlockHook(hook graphsync.OnOutgoingBlockHook) graphsync.UnregisterHookFunc {
	return gs.outgoingBlockHooks.Register(hook)
}

// RegisterRequestUpdatedHook registers a hook that runs when an update to a request is received
func (gs *GraphSync) RegisterRequestUpdatedHook(hook graphsync.OnRequestUpdatedHook) graphsync.UnregisterHookFunc {
	return gs.requestUpdatedHooks.Register(hook)
}

// RegisterCompletedResponseListener adds a listener on the responder for completed responses
func (gs *GraphSync) RegisterCompletedResponseListener(listener graphsync.OnResponseCompletedListener) graphsync.UnregisterHookFunc {
	return gs.completedResponseListeners.Register(listener)
}

// RegisterIncomingBlockHook adds a hook that runs when a block is received and validated (put in block store)
func (gs *GraphSync) RegisterIncomingBlockHook(hook graphsync.OnIncomingBlockHook) graphsync.UnregisterHookFunc {
	return gs.incomingBlockHooks.Register(hook)
}

// RegisterRequestorCancelledListener adds a listener on the responder for
// responses cancelled by the requestor
func (gs *GraphSync) RegisterRequestorCancelledListener(listener graphsync.OnRequestorCancelledListener) graphsync.UnregisterHookFunc {
	return gs.requestorCancelledListeners.Register(listener)
}

// RegisterBlockSentListener adds a listener for when blocks are actually sent over the wire
func (gs *GraphSync) RegisterBlockSentListener(listener graphsync.OnBlockSentListener) graphsync.UnregisterHookFunc {
	return gs.blockSentListeners.Register(listener)
}

// RegisterNetworkErrorListener adds a listener for when errors occur sending data over the wire
func (gs *GraphSync) RegisterNetworkErrorListener(listener graphsync.OnNetworkErrorListener) graphsync.UnregisterHookFunc {
	return gs.networkErrorListeners.Register(listener)
}

// RegisterReceiverNetworkErrorListener adds a listener for when errors occur receiving data over the wire
func (gs *GraphSync) RegisterReceiverNetworkErrorListener(listener graphsync.OnReceiverNetworkErrorListener) graphsync.UnregisterHookFunc {
	return gs.receiverErrorListeners.Register(listener)
}

// UnpauseRequest unpauses a request that was paused in a block hook based request ID
// Can also send extensions with unpause
func (gs *GraphSync) UnpauseRequest(requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	return gs.requestManager.UnpauseRequest(requestID, extensions...)
}

// PauseRequest pauses an in progress request (may take 1 or more blocks to process)
func (gs *GraphSync) PauseRequest(requestID graphsync.RequestID) error {
	return gs.requestManager.PauseRequest(requestID)
}

// UnpauseResponse unpauses a response that was paused in a block hook based on peer ID and request ID
func (gs *GraphSync) UnpauseResponse(p peer.ID, requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	return gs.responseManager.UnpauseResponse(p, requestID, extensions...)
}

// PauseResponse pauses an in progress response (may take 1 or more blocks to process)
func (gs *GraphSync) PauseResponse(p peer.ID, requestID graphsync.RequestID) error {
	return gs.responseManager.PauseResponse(p, requestID)
}

// CancelResponse cancels an in progress response
func (gs *GraphSync) CancelResponse(p peer.ID, requestID graphsync.RequestID) error {
	return gs.responseManager.CancelResponse(p, requestID)
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
func (gsr *graphSyncReceiver) ReceiveError(p peer.ID, err error) {
	log.Infof("Graphsync ReceiveError from %s: %s", p, err)
	gsr.receiverErrorListeners.NotifyNetworkErrorListeners(p, err)
}

// Connected is part of the networks 's Receiver interface and handles peers connecting
// on the network
func (gsr *graphSyncReceiver) Connected(p peer.ID) {
	gsr.graphSync().peerManager.Connected(p)
}

// Connected is part of the networks 's Receiver interface and handles peers connecting
// on the network
func (gsr *graphSyncReceiver) Disconnected(p peer.ID) {
	gsr.graphSync().peerManager.Disconnected(p)
}
