package hooks

import (
	"github.com/hannahhoward/go-pubsub"
	"github.com/ipfs/go-graphsync"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// CompletedResponseHooks is a set of hooks for completed responses
type CompletedResponseHooks struct {
	pubSub *pubsub.PubSub
}

type internalCompletedResponseEvent struct {
	p       peer.ID
	request graphsync.RequestData
	status  graphsync.ResponseStatusCode
	cha     *completeHookActions
}

func completedResponseDispatcher(event pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	ie := event.(internalCompletedResponseEvent)
	hook := subscriberFn.(graphsync.OnResponseCompletedHook)
	hook(ie.p, ie.request, ie.status, ie.cha)
	return nil
}

// NewCompletedResponseHooks returns a new list of completed response hooks
func NewCompletedResponseHooks() *CompletedResponseHooks {
	return &CompletedResponseHooks{pubSub: pubsub.New(completedResponseDispatcher)}
}

// Register registers an hook for completed responses
func (crl *CompletedResponseHooks) Register(hook graphsync.OnResponseCompletedHook) graphsync.UnregisterHookFunc {
	return graphsync.UnregisterHookFunc(crl.pubSub.Subscribe(hook))
}

// ProcessCompleteHooks runs notifies all completed hooks that a response has completed
func (crl *CompletedResponseHooks) ProcessCompleteHooks(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) CompleteResult {
	ha := &completeHookActions{}
	_ = crl.pubSub.Publish(internalCompletedResponseEvent{p, request, status, ha})
	return ha.result()
}

// CompleteResult is the outcome of running complete response hooks
type CompleteResult struct {
	Extensions []graphsync.ExtensionData
}

type completeHookActions struct {
	extensions []graphsync.ExtensionData
}

func (ha *completeHookActions) result() CompleteResult {
	return CompleteResult{
		Extensions: ha.extensions,
	}
}

func (ha *completeHookActions) SendExtensionData(ext graphsync.ExtensionData) {
	ha.extensions = append(ha.extensions, ext)
}

// RequestorCancelledListeners is a set of listeners for when requestors cancel
type RequestorCancelledListeners struct {
	pubSub *pubsub.PubSub
}

type internalRequestorCancelledEvent struct {
	p       peer.ID
	request graphsync.RequestData
}

func requestorCancelledDispatcher(event pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	ie := event.(internalRequestorCancelledEvent)
	listener := subscriberFn.(graphsync.OnRequestorCancelledListener)
	listener(ie.p, ie.request)
	return nil
}

// NewRequestorCancelledListeners returns a new list of listeners for when requestors cancel
func NewRequestorCancelledListeners() *RequestorCancelledListeners {
	return &RequestorCancelledListeners{pubSub: pubsub.New(requestorCancelledDispatcher)}
}

// Register registers an listener for completed responses
func (rcl *RequestorCancelledListeners) Register(listener graphsync.OnRequestorCancelledListener) graphsync.UnregisterHookFunc {
	return graphsync.UnregisterHookFunc(rcl.pubSub.Subscribe(listener))
}

// NotifyCancelledListeners notifies all listeners that a requestor cancelled a response
func (rcl *RequestorCancelledListeners) NotifyCancelledListeners(p peer.ID, request graphsync.RequestData) {
	_ = rcl.pubSub.Publish(internalRequestorCancelledEvent{p, request})
}
