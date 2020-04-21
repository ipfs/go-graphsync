package hooks

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
	hooksLk sync.RWMutex
	nextKey uint64
	hooks   []blockHook
}

// NewBlockHooks returns a new list of outgoing block hooks
func NewBlockHooks() *OutgoingBlockHooks {
	return &OutgoingBlockHooks{}
}

// Register registers an hook to process outgoing blocks in a response
func (obh *OutgoingBlockHooks) Register(hook graphsync.OnOutgoingBlockHook) graphsync.UnregisterHookFunc {
	obh.hooksLk.Lock()
	bh := blockHook{obh.nextKey, hook}
	obh.nextKey++
	obh.hooks = append(obh.hooks, bh)
	obh.hooksLk.Unlock()
	return func() {
		obh.hooksLk.Lock()
		defer obh.hooksLk.Unlock()
		for i, matchHook := range obh.hooks {
			if bh.key == matchHook.key {
				obh.hooks = append(obh.hooks[:i], obh.hooks[i+1:]...)
				return
			}
		}
	}
}

// BlockResult is the result of processing block hooks
type BlockResult struct {
	Err        error
	Extensions []graphsync.ExtensionData
}

// ProcessBlockHooks runs block hooks against a request and block data
func (obh *OutgoingBlockHooks) ProcessBlockHooks(p peer.ID, request graphsync.RequestData, blockData graphsync.BlockData) BlockResult {
	obh.hooksLk.RLock()
	defer obh.hooksLk.RUnlock()
	bha := &blockHookActions{}
	for _, bh := range obh.hooks {
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

func (bha *blockHookActions) result() BlockResult {
	return BlockResult{bha.err, bha.extensions}
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
