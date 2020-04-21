package hooks

import (
	"sync"

	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime/traversal"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type requestHook struct {
	key  uint64
	hook graphsync.OnOutgoingRequestHook
}

// OutgoingRequestHooks is a set of incoming request hooks that can be processed
type OutgoingRequestHooks struct {
	nextKey uint64
	hooksLk sync.RWMutex
	hooks   []requestHook
}

// NewRequestHooks returns a new list of incoming request hooks
func NewRequestHooks() *OutgoingRequestHooks {
	return &OutgoingRequestHooks{}
}

// Register registers an extension to process outgoing requests
func (orh *OutgoingRequestHooks) Register(hook graphsync.OnOutgoingRequestHook) graphsync.UnregisterHookFunc {
	orh.hooksLk.Lock()
	rh := requestHook{orh.nextKey, hook}
	orh.nextKey++
	orh.hooks = append(orh.hooks, rh)
	orh.hooksLk.Unlock()
	return func() {
		orh.hooksLk.Lock()
		defer orh.hooksLk.Unlock()
		for i, matchHook := range orh.hooks {
			if rh.key == matchHook.key {
				orh.hooks = append(orh.hooks[:i], orh.hooks[i+1:]...)
				return
			}
		}
	}
}

// RequestResult is the outcome of running requesthooks
type RequestResult struct {
	PersistenceOption string
	CustomChooser     traversal.NodeBuilderChooser
}

// ProcessRequestHooks runs request hooks against an outgoing request
func (orh *OutgoingRequestHooks) ProcessRequestHooks(p peer.ID, request graphsync.RequestData) RequestResult {
	orh.hooksLk.RLock()
	defer orh.hooksLk.RUnlock()
	rha := &requestHookActions{}
	for _, requestHook := range orh.hooks {
		requestHook.hook(p, request, rha)
	}
	return rha.result()
}

type requestHookActions struct {
	persistenceOption  string
	nodeBuilderChooser traversal.NodeBuilderChooser
}

func (rha *requestHookActions) result() RequestResult {
	return RequestResult{
		PersistenceOption: rha.persistenceOption,
		CustomChooser:     rha.nodeBuilderChooser,
	}
}

func (rha *requestHookActions) UsePersistenceOption(name string) {
	rha.persistenceOption = name
}

func (rha *requestHookActions) UseNodeBuilderChooser(nodeBuilderChooser traversal.NodeBuilderChooser) {
	rha.nodeBuilderChooser = nodeBuilderChooser
}
