package hooks

import (
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime/traversal"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type responseHook struct {
	key  uint64
	hook graphsync.OnIncomingResponseHook
}

type requestHook struct {
	key  uint64
	hook graphsync.OnOutgoingRequestHook
}

// Hooks is a set of incoming request hooks that can be processed
type Hooks struct {
	hooksNextKey  uint64
	responseHooks []responseHook
	requestHooks  []requestHook
}

// New returns a new list of incoming request hooks
func New() *Hooks {
	return &Hooks{}
}

// RegisterRequestHook registers an extension to process outgoing requests
func (irh *Hooks) RegisterRequestHook(hook graphsync.OnOutgoingRequestHook) graphsync.UnregisterHookFunc {
	rh := requestHook{irh.hooksNextKey, hook}
	irh.hooksNextKey++
	irh.requestHooks = append(irh.requestHooks, rh)
	return func() {
		for i, matchHook := range irh.requestHooks {
			if rh.key == matchHook.key {
				irh.requestHooks = append(irh.requestHooks[:i], irh.requestHooks[i+1:]...)
				return
			}
		}
	}
}

// RegisterResponseHook registers an extension to process incoming responses
func (irh *Hooks) RegisterResponseHook(hook graphsync.OnIncomingResponseHook) graphsync.UnregisterHookFunc {
	rh := responseHook{irh.hooksNextKey, hook}
	irh.hooksNextKey++
	irh.responseHooks = append(irh.responseHooks, rh)
	return func() {
		for i, matchHook := range irh.responseHooks {
			if rh.key == matchHook.key {
				irh.responseHooks = append(irh.responseHooks[:i], irh.responseHooks[i+1:]...)
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
func (irh *Hooks) ProcessRequestHooks(p peer.ID, request graphsync.RequestData) RequestResult {
	rha := &requestHookActions{}
	for _, requestHook := range irh.requestHooks {
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

// ResponseResult is the outcome of running response hooks
type ResponseResult struct {
	Err        error
	Extensions []graphsync.ExtensionData
}

// ProcessResponseHooks runs response hooks against an incoming response
func (irh *Hooks) ProcessResponseHooks(p peer.ID, response graphsync.ResponseData) ResponseResult {
	rha := &responseHookActions{}
	for _, responseHooks := range irh.responseHooks {
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
