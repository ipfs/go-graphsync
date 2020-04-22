package pubsub

import (
	"sync"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

type subscriber struct {
	fn  datatransfer.Subscriber
	key uint64
}

// PubSub is a simple emitter of data transfer events
type PubSub struct {
	subscribersLk sync.RWMutex
	subscribers   []subscriber
	nextKey       uint64
}

// New returns a new PubSub
func New() *PubSub {
	return &PubSub{}
}

// Subscribe adds the given subscriber to the list of subscribers for this Pubsub
func (ps *PubSub) Subscribe(subscriberFn datatransfer.Subscriber) datatransfer.Unsubscribe {
	ps.subscribersLk.Lock()
	subscriber := subscriber{subscriberFn, ps.nextKey}
	ps.nextKey++
	ps.subscribers = append(ps.subscribers, subscriber)
	ps.subscribersLk.Unlock()
	return ps.unsubscribeAt(subscriber)
}

// unsubscribeAt returns a function that removes an item from ps.subscribers. Does not preserve order.
// Subsequent, repeated calls to the func with the same Subscriber are a no-op.
func (ps *PubSub) unsubscribeAt(sub subscriber) datatransfer.Unsubscribe {
	return func() {
		ps.subscribersLk.Lock()
		defer ps.subscribersLk.Unlock()
		curLen := len(ps.subscribers)
		for i, el := range ps.subscribers {
			if sub.key == el.key {
				ps.subscribers[i] = ps.subscribers[curLen-1]
				ps.subscribers = ps.subscribers[:curLen-1]
				return
			}
		}
	}
}

// Publish publishes the given event and channel state to all subscribers
func (ps *PubSub) Publish(evt datatransfer.Event, cs datatransfer.ChannelState) {
	ps.subscribersLk.RLock()
	defer ps.subscribersLk.RUnlock()
	for _, sub := range ps.subscribers {
		sub.fn(evt, cs)
	}
}
