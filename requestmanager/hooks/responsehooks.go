package hooks

import (
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
)

type responseHook struct {
	key  uint64
	hook graphsync.OnIncomingResponseHook
}

// IncomingResponseHooks is a set of incoming response hooks that can be processed
type IncomingResponseHooks struct {
	nextKey uint64
	hooksLk sync.RWMutex
	hooks   []responseHook
}

// NewResponseHooks returns a new list of incoming request hooks
func NewResponseHooks() *IncomingResponseHooks {
	return &IncomingResponseHooks{}
}

// Register registers an extension to process incoming responses
func (irh *IncomingResponseHooks) Register(hook graphsync.OnIncomingResponseHook) graphsync.UnregisterHookFunc {
	irh.hooksLk.Lock()
	rh := responseHook{irh.nextKey, hook}
	irh.nextKey++
	irh.hooks = append(irh.hooks, rh)
	irh.hooksLk.Unlock()
	return func() {
		irh.hooksLk.Lock()
		defer irh.hooksLk.Unlock()
		for i, matchHook := range irh.hooks {
			if rh.key == matchHook.key {
				irh.hooks = append(irh.hooks[:i], irh.hooks[i+1:]...)
				return
			}
		}
	}
}

// ResponseResult is the outcome of running response hooks
type ResponseResult struct {
	Err        error
	Extensions []graphsync.ExtensionData
}

// ProcessResponseHooks runs response hooks against an incoming response
func (irh *IncomingResponseHooks) ProcessResponseHooks(p peer.ID, response graphsync.ResponseData) ResponseResult {
	irh.hooksLk.Lock()
	defer irh.hooksLk.Unlock()
	rha := &responseHookActions{}
	for _, responseHooks := range irh.hooks {
		responseHooks.hook(p, response, rha)
		if rha.hasError() {
			break
		}
	}
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

func (rha *responseHookActions) hasError() bool {
	return rha.err != nil
}

func (rha *responseHookActions) TerminateWithError(err error) {
	rha.err = err
}

func (rha *responseHookActions) UpdateRequestWithExtensions(extensions ...graphsync.ExtensionData) {
	rha.extensions = append(rha.extensions, extensions...)
}
