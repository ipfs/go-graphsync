package blockhooks

import (
	"errors"
	"sync"

	"github.com/ipfs/go-graphsync"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// ErrPaused indicates a request should stop processing, but only cause it's paused
var ErrPaused = errors.New("request has been paused")

type blockHook struct {
	key  uint64
	hook graphsync.OnOutgoingBlockHook
}

// OutgoingBlockHooks is a set of outgoing block hooks that can be processed
type OutgoingBlockHooks struct {
	blockHooksLk      sync.RWMutex
	blockHooksNextKey uint64
	blockHooks        []blockHook
}

// New returns a new list of outgoing block hooks
func New() *OutgoingBlockHooks {
	return &OutgoingBlockHooks{}
}

// Register registers an hook to process outgoing blocks in a response
func (obh *OutgoingBlockHooks) Register(hook graphsync.OnOutgoingBlockHook) graphsync.UnregisterHookFunc {
	obh.blockHooksLk.Lock()
	bh := blockHook{obh.blockHooksNextKey, hook}
	obh.blockHooksNextKey++
	obh.blockHooks = append(obh.blockHooks, bh)
	obh.blockHooksLk.Unlock()
	return func() {
		obh.blockHooksLk.Lock()
		defer obh.blockHooksLk.Unlock()
		for i, matchHook := range obh.blockHooks {
			if bh.key == matchHook.key {
				obh.blockHooks = append(obh.blockHooks[:i], obh.blockHooks[i+1:]...)
				return
			}
		}
	}
}

// Result is the result of processing block hooks
type Result struct {
	Err        error
	Extensions []graphsync.ExtensionData
}

// ProcessBlockHooks runs block hooks against a request and block data
func (obh *OutgoingBlockHooks) ProcessBlockHooks(p peer.ID, request graphsync.RequestData, blockData graphsync.BlockData) Result {
	obh.blockHooksLk.RLock()
	defer obh.blockHooksLk.RUnlock()
	bha := &blockHookActions{}
	for _, bh := range obh.blockHooks {
		bh.hook(p, request, blockData, bha)
		if bha.hasError() {
			break
		}
	}
	return bha.result()
}

type blockHookActions struct {
	err        error
	extensions []graphsync.ExtensionData
}

func (bha *blockHookActions) hasError() bool {
	return bha.err != nil
}

func (bha *blockHookActions) result() Result {
	return Result{bha.err, bha.extensions}
}

func (bha *blockHookActions) SendExtensionData(data graphsync.ExtensionData) {
	bha.extensions = append(bha.extensions, data)
}

func (bha *blockHookActions) TerminateWithError(err error) {
	bha.err = err
}

func (bha *blockHookActions) PauseResponse() {
	bha.err = ErrPaused
}
