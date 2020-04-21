package hooks

import (
	"sync"

	"github.com/ipfs/go-graphsync"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type requestUpdatedHook struct {
	key  uint64
	hook graphsync.OnRequestUpdatedHook
}

// RequestUpdatedHooks manages and runs hooks for request updates
type RequestUpdatedHooks struct {
	nextKey uint64
	hooksLk sync.RWMutex
	hooks   []requestUpdatedHook
}

// NewUpdateHooks returns a new list of request updated hooks
func NewUpdateHooks() *RequestUpdatedHooks {
	return &RequestUpdatedHooks{}
}

// Register registers an hook to process updates to requests
func (ruh *RequestUpdatedHooks) Register(hook graphsync.OnRequestUpdatedHook) graphsync.UnregisterHookFunc {
	ruh.hooksLk.Lock()
	rh := requestUpdatedHook{ruh.nextKey, hook}
	ruh.nextKey++
	ruh.hooks = append(ruh.hooks, rh)
	ruh.hooksLk.Unlock()
	return func() {
		ruh.hooksLk.Lock()
		defer ruh.hooksLk.Unlock()
		for i, matchHook := range ruh.hooks {
			if rh.key == matchHook.key {
				ruh.hooks = append(ruh.hooks[:i], ruh.hooks[i+1:]...)
				return
			}
		}
	}
}

// UpdateResult is the result of running update hooks
type UpdateResult struct {
	Err        error
	Unpause    bool
	Extensions []graphsync.ExtensionData
}

// ProcessUpdateHooks runs request hooks against an incoming request
func (ruh *RequestUpdatedHooks) ProcessUpdateHooks(p peer.ID, request graphsync.RequestData, update graphsync.RequestData) UpdateResult {
	ruh.hooksLk.RLock()
	defer ruh.hooksLk.RUnlock()
	ha := &updateHookActions{}
	for _, updateHook := range ruh.hooks {
		updateHook.hook(p, request, update, ha)
		if ha.hasError() {
			break
		}
	}
	return ha.result()
}

type updateHookActions struct {
	err        error
	unpause    bool
	extensions []graphsync.ExtensionData
}

func (uha *updateHookActions) hasError() bool {
	return uha.err != nil
}

func (uha *updateHookActions) result() UpdateResult {
	return UpdateResult{uha.err, uha.unpause, uha.extensions}
}

func (uha *updateHookActions) SendExtensionData(data graphsync.ExtensionData) {
	uha.extensions = append(uha.extensions, data)
}

func (uha *updateHookActions) TerminateWithError(err error) {
	uha.err = err
}

func (uha *updateHookActions) UnpauseResponse() {
	uha.unpause = true
}
