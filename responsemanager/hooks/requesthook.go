package hooks

import (
	"context"
	"errors"

	"github.com/hannahhoward/go-pubsub"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal"
	peer "github.com/libp2p/go-libp2p/core/peer"

	"github.com/ipfs/go-graphsync"
)

// PersistenceOptions is an interface for getting loaders by name
type PersistenceOptions interface {
	GetLinkSystem(name string) (ipld.LinkSystem, bool)
}

// IncomingRequestHooks is a set of incoming request hooks that can be processed
type IncomingRequestHooks struct {
	persistenceOptions PersistenceOptions
	pubSub             *pubsub.PubSub
}

type internalRequestHookEvent struct {
	p       peer.ID
	request graphsync.RequestData
	rha     *requestHookActions
}

func requestHookDispatcher(event pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	ie := event.(internalRequestHookEvent)
	hook := subscriberFn.(graphsync.OnIncomingRequestHook)
	hook(ie.p, ie.request, ie.rha)
	return ie.rha.err
}

// NewRequestHooks returns a new list of incoming request hooks
func NewRequestHooks(persistenceOptions PersistenceOptions) *IncomingRequestHooks {
	return &IncomingRequestHooks{
		persistenceOptions: persistenceOptions,
		pubSub:             pubsub.New(requestHookDispatcher),
	}
}

// Register registers an extension to process new incoming requests
func (irh *IncomingRequestHooks) Register(hook graphsync.OnIncomingRequestHook) graphsync.UnregisterHookFunc {
	return graphsync.UnregisterHookFunc(irh.pubSub.Subscribe(hook))
}

// RequestResult is the outcome of running requesthooks
type RequestResult struct {
	IsValidated      bool
	IsPaused         bool
	CustomLinkSystem ipld.LinkSystem
	CustomChooser    traversal.LinkTargetNodePrototypeChooser
	Err              error
	Extensions       []graphsync.ExtensionData
	Ctx              context.Context
	MaxLinks         uint64
}

// ProcessRequestHooks runs request hooks against an incoming request
func (irh *IncomingRequestHooks) ProcessRequestHooks(p peer.ID, request graphsync.RequestData, reqCtx context.Context) RequestResult {
	ha := &requestHookActions{
		persistenceOptions: irh.persistenceOptions,
		ctx:                reqCtx,
	}
	_ = irh.pubSub.Publish(internalRequestHookEvent{p, request, ha})
	return ha.result()
}

type requestHookActions struct {
	persistenceOptions PersistenceOptions
	isValidated        bool
	isPaused           bool
	err                error
	linkSystem         ipld.LinkSystem
	chooser            traversal.LinkTargetNodePrototypeChooser
	extensions         []graphsync.ExtensionData
	ctx                context.Context
	maxLinks           uint64
}

func (ha *requestHookActions) result() RequestResult {
	return RequestResult{
		IsValidated:      ha.isValidated,
		IsPaused:         ha.isPaused,
		CustomLinkSystem: ha.linkSystem,
		CustomChooser:    ha.chooser,
		Err:              ha.err,
		Extensions:       ha.extensions,
		Ctx:              ha.ctx,
		MaxLinks:         ha.maxLinks,
	}
}

func (ha *requestHookActions) SendExtensionData(ext graphsync.ExtensionData) {
	ha.extensions = append(ha.extensions, ext)
}

func (ha *requestHookActions) TerminateWithError(err error) {
	ha.err = err
}

func (ha *requestHookActions) ValidateRequest() {
	ha.isValidated = true
}

func (ha *requestHookActions) UsePersistenceOption(name string) {
	linkSystem, ok := ha.persistenceOptions.GetLinkSystem(name)
	if !ok {
		ha.TerminateWithError(errors.New("unknown loader option"))
		return
	}
	ha.linkSystem = linkSystem
}

func (ha *requestHookActions) MaxLinks(maxLinks uint64) {
	ha.maxLinks = maxLinks
}

func (ha *requestHookActions) UseLinkTargetNodePrototypeChooser(chooser traversal.LinkTargetNodePrototypeChooser) {
	ha.chooser = chooser
}

func (ha *requestHookActions) PauseResponse() {
	ha.isPaused = true
}

func (ha *requestHookActions) AugmentContext(augment func(reqCtx context.Context) context.Context) {
	ha.ctx = augment(ha.ctx)
}
