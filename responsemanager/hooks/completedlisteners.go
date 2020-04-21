package hooks

import (
	"sync"

	"github.com/ipfs/go-graphsync"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type completedListener struct {
	key      uint64
	listener graphsync.OnResponseCompletedListener
}

// CompletedResponseListeners is a set of listeners for completed responses
type CompletedResponseListeners struct {
	listenersLk sync.RWMutex
	nextKey     uint64
	listeners   []completedListener
}

// NewCompletedResponseListeners returns a new list of completed response listeners
func NewCompletedResponseListeners() *CompletedResponseListeners {
	return &CompletedResponseListeners{}
}

// Register registers an listener for completed responses
func (crl *CompletedResponseListeners) Register(listener graphsync.OnResponseCompletedListener) graphsync.UnregisterHookFunc {
	crl.listenersLk.Lock()
	cl := completedListener{crl.nextKey, listener}
	crl.nextKey++
	crl.listeners = append(crl.listeners, cl)
	crl.listenersLk.Unlock()
	return func() {
		crl.listenersLk.Lock()
		defer crl.listenersLk.Unlock()
		for i, matchListener := range crl.listeners {
			if cl.key == matchListener.key {
				crl.listeners = append(crl.listeners[:i], crl.listeners[i+1:]...)
				return
			}
		}
	}
}

// NotifyCompletedListeners runs notifies all completed listeners that a response has completed
func (crl *CompletedResponseListeners) NotifyCompletedListeners(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
	crl.listenersLk.RLock()
	defer crl.listenersLk.RUnlock()
	for _, listener := range crl.listeners {
		listener.listener(p, request, status)
	}
}
