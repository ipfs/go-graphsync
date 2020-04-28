package hooks

import (
	"errors"
	"sync"

	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type requestHook struct {
	key  uint64
	hook graphsync.OnIncomingRequestHook
}

// PersistenceOptions is an interface for getting loaders by name
type PersistenceOptions interface {
	GetLoader(name string) (ipld.Loader, bool)
}

// IncomingRequestHooks is a set of incoming request hooks that can be processed
type IncomingRequestHooks struct {
	persistenceOptions PersistenceOptions
	hooksLk            sync.RWMutex
	nextKey            uint64
	hooks              []requestHook
}

// NewRequestHooks returns a new list of incoming request hooks
func NewRequestHooks(persistenceOptions PersistenceOptions) *IncomingRequestHooks {
	return &IncomingRequestHooks{
		persistenceOptions: persistenceOptions,
	}
}

// Register registers an extension to process new incoming requests
func (irh *IncomingRequestHooks) Register(hook graphsync.OnIncomingRequestHook) graphsync.UnregisterHookFunc {
	irh.hooksLk.Lock()
	rh := requestHook{irh.nextKey, hook}
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

// RequestResult is the outcome of running requesthooks
type RequestResult struct {
	IsValidated   bool
	CustomLoader  ipld.Loader
	CustomChooser traversal.LinkTargetNodeStyleChooser
	Err           error
	Extensions    []graphsync.ExtensionData
}

// ProcessRequestHooks runs request hooks against an incoming request
func (irh *IncomingRequestHooks) ProcessRequestHooks(p peer.ID, request graphsync.RequestData) RequestResult {
	irh.hooksLk.RLock()
	defer irh.hooksLk.RUnlock()
	ha := &requestHookActions{
		persistenceOptions: irh.persistenceOptions,
	}
	for _, requestHook := range irh.hooks {
		requestHook.hook(p, request, ha)
		if ha.hasError() {
			break
		}
	}
	return ha.result()
}

type requestHookActions struct {
	persistenceOptions PersistenceOptions
	isValidated        bool
	err                error
	loader             ipld.Loader
	chooser            traversal.LinkTargetNodeStyleChooser
	extensions         []graphsync.ExtensionData
}

func (ha *requestHookActions) hasError() bool {
	return ha.err != nil
}

func (ha *requestHookActions) result() RequestResult {
	return RequestResult{
		IsValidated:   ha.isValidated,
		CustomLoader:  ha.loader,
		CustomChooser: ha.chooser,
		Err:           ha.err,
		Extensions:    ha.extensions,
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
	loader, ok := ha.persistenceOptions.GetLoader(name)
	if !ok {
		ha.TerminateWithError(errors.New("unknown loader option"))
		return
	}
	ha.loader = loader
}

func (ha *requestHookActions) UseLinkTargetNodeStyleChooser(chooser traversal.LinkTargetNodeStyleChooser) {
	ha.chooser = chooser
}
