package hooks

import (
	"github.com/hannahhoward/go-pubsub"
	"github.com/ipfs/go-graphsync"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// CompletedResponseListeners is a set of listeners for completed responses
type CompletedResponseListeners struct {
	pubSub *pubsub.PubSub
}

type internalCompletedResponseEvent struct {
	p       peer.ID
	request graphsync.RequestData
	status  graphsync.ResponseStatusCode
}

func completedResponseDispatcher(event pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	ie := event.(internalCompletedResponseEvent)
	listener := subscriberFn.(graphsync.OnResponseCompletedListener)
	listener(ie.p, ie.request, ie.status)
	return nil
}

// NewCompletedResponseListeners returns a new list of completed response listeners
func NewCompletedResponseListeners() *CompletedResponseListeners {
	return &CompletedResponseListeners{pubSub: pubsub.New(completedResponseDispatcher)}
}

// Register registers an listener for completed responses
func (crl *CompletedResponseListeners) Register(listener graphsync.OnResponseCompletedListener) graphsync.UnregisterHookFunc {
	return graphsync.UnregisterHookFunc(crl.pubSub.Subscribe(listener))
}

// NotifyCompletedListeners runs notifies all completed listeners that a response has completed
func (crl *CompletedResponseListeners) NotifyCompletedListeners(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
	_ = crl.pubSub.Publish(internalCompletedResponseEvent{p, request, status})
}
