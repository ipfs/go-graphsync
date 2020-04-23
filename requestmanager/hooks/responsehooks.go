package hooks

import (
	"github.com/hannahhoward/go-pubsub"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
)

// IncomingResponseHooks is a set of incoming response hooks that can be processed
type IncomingResponseHooks struct {
	pubSub *pubsub.PubSub
}

type internalResponseHookEvent struct {
	p        peer.ID
	response graphsync.ResponseData
	rha      *responseHookActions
}

func responseHookDispatcher(event pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	ie := event.(internalResponseHookEvent)
	hook := subscriberFn.(graphsync.OnIncomingResponseHook)
	hook(ie.p, ie.response, ie.rha)
	return ie.rha.err
}

// NewResponseHooks returns a new list of incoming request hooks
func NewResponseHooks() *IncomingResponseHooks {
	return &IncomingResponseHooks{pubSub: pubsub.New(responseHookDispatcher)}
}

// Register registers an extension to process incoming responses
func (irh *IncomingResponseHooks) Register(hook graphsync.OnIncomingResponseHook) graphsync.UnregisterHookFunc {
	return graphsync.UnregisterHookFunc(irh.pubSub.Subscribe(hook))
}

// ResponseResult is the outcome of running response hooks
type ResponseResult struct {
	Err        error
	Extensions []graphsync.ExtensionData
}

// ProcessResponseHooks runs response hooks against an incoming response
func (irh *IncomingResponseHooks) ProcessResponseHooks(p peer.ID, response graphsync.ResponseData) ResponseResult {
	rha := &responseHookActions{}
	_ = irh.pubSub.Publish(internalResponseHookEvent{p, response, rha})
	return rha.result()
}

type responseHookActions struct {
	err        error
	extensions []graphsync.ExtensionData
}

func (rha *responseHookActions) result() ResponseResult {
	return ResponseResult{
		Err:        rha.err,
		Extensions: rha.extensions,
	}
}

func (rha *responseHookActions) TerminateWithError(err error) {
	rha.err = err
}

func (rha *responseHookActions) UpdateRequestWithExtensions(extensions ...graphsync.ExtensionData) {
	rha.extensions = append(rha.extensions, extensions...)
}
