package requesthooks

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
	requestHooksLk     sync.RWMutex
	requestHookNextKey uint64
	requestHooks       []requestHook
}

// New returns a new list of incoming request hooks
func New(persistenceOptions PersistenceOptions) *IncomingRequestHooks {
	return &IncomingRequestHooks{
		persistenceOptions: persistenceOptions,
	}
}

// Register registers an extension to process new incoming requests
func (irh *IncomingRequestHooks) Register(hook graphsync.OnIncomingRequestHook) graphsync.UnregisterHookFunc {
	irh.requestHooksLk.Lock()
	rh := requestHook{irh.requestHookNextKey, hook}
	irh.requestHookNextKey++
	irh.requestHooks = append(irh.requestHooks, rh)
	irh.requestHooksLk.Unlock()
	return func() {
		irh.requestHooksLk.Lock()
		defer irh.requestHooksLk.Unlock()
		for i, matchHook := range irh.requestHooks {
			if rh.key == matchHook.key {
				irh.requestHooks = append(irh.requestHooks[:i], irh.requestHooks[i+1:]...)
				return
			}
		}
	}
}

// Result is the outcome of running requesthooks
type Result struct {
	IsValidated   bool
	CustomLoader  ipld.Loader
	CustomChooser traversal.NodeBuilderChooser
	Err           error
	Extensions    []graphsync.ExtensionData
}

// ProcessRequestHooks runs request hooks against an incoming request
func (irh *IncomingRequestHooks) ProcessRequestHooks(p peer.ID, request graphsync.RequestData) Result {
	irh.requestHooksLk.RLock()
	defer irh.requestHooksLk.RUnlock()
	ha := &hookActions{
		persistenceOptions: irh.persistenceOptions,
	}
	for _, requestHook := range irh.requestHooks {
		requestHook.hook(p, request, ha)
		if ha.hasError() {
			break
		}
	}
	return ha.result()
}

type hookActions struct {
	persistenceOptions PersistenceOptions
	isValidated        bool
	err                error
	loader             ipld.Loader
	chooser            traversal.NodeBuilderChooser
	extensions         []graphsync.ExtensionData
}

func (ha *hookActions) hasError() bool {
	return ha.err != nil
}

func (ha *hookActions) result() Result {
	return Result{
		IsValidated:   ha.isValidated,
		CustomLoader:  ha.loader,
		CustomChooser: ha.chooser,
		Err:           ha.err,
		Extensions:    ha.extensions,
	}
}

func (ha *hookActions) SendExtensionData(ext graphsync.ExtensionData) {
	ha.extensions = append(ha.extensions, ext)
}

func (ha *hookActions) TerminateWithError(err error) {
	ha.err = err
}

func (ha *hookActions) ValidateRequest() {
	ha.isValidated = true
}

func (ha *hookActions) UsePersistenceOption(name string) {
	loader, ok := ha.persistenceOptions.GetLoader(name)
	if !ok {
		ha.TerminateWithError(errors.New("unknown loader option"))
		return
	}
	ha.loader = loader
}

func (ha *hookActions) UseNodeBuilderChooser(chooser traversal.NodeBuilderChooser) {
	ha.chooser = chooser
}
