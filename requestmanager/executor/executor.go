package executor

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	peer "github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/requestmanager/types"
)

// AsyncLoadFn is a function which given a request id and an ipld.Link, returns
// a channel which will eventually return data for the link or an err
type AsyncLoadFn func(graphsync.RequestID, ipld.Link) <-chan types.AsyncLoadResult

// ExecutionEnv are request parameters that last between requests
type ExecutionEnv struct {
	Ctx              context.Context
	SendRequest      func(peer.ID, gsmsg.GraphSyncRequest)
	RunBlockHooks    func(p peer.ID, response graphsync.ResponseData, blk graphsync.BlockData) error
	TerminateRequest func(graphsync.RequestID)
	WaitForMessages  func(ctx context.Context, resumeMessages chan graphsync.ExtensionData) ([]graphsync.ExtensionData, error)
	Loader           AsyncLoadFn
}

// RequestExecution are parameters for a single request execution
type RequestExecution struct {
	Ctx              context.Context
	P                peer.ID
	NetworkError     chan error
	Request          gsmsg.GraphSyncRequest
	LastResponse     *atomic.Value
	DoNotSendCids    *cid.Set
	NodePrototypeChooser traversal.LinkTargetNodePrototypeChooser
	ResumeMessages   chan []graphsync.ExtensionData
	PauseMessages    chan struct{}
}

// Start begins execution of a request in a go routine
func (ee ExecutionEnv) Start(re RequestExecution) (chan graphsync.ResponseProgress, chan error) {
	executor := &requestExecutor{
		inProgressChan:   make(chan graphsync.ResponseProgress),
		inProgressErr:    make(chan error),
		ctx:              re.Ctx,
		p:                re.P,
		networkError:     re.NetworkError,
		request:          re.Request,
		lastResponse:     re.LastResponse,
		doNotSendCids:    re.DoNotSendCids,
		nodeStyleChooser: re.NodePrototypeChooser,
		resumeMessages:   re.ResumeMessages,
		pauseMessages:    re.PauseMessages,
		env:              ee,
	}
	executor.sendRequest(executor.request)
	go executor.run()
	return executor.inProgressChan, executor.inProgressErr
}

type requestExecutor struct {
	inProgressChan    chan graphsync.ResponseProgress
	inProgressErr     chan error
	ctx               context.Context
	p                 peer.ID
	networkError      chan error
	request           gsmsg.GraphSyncRequest
	lastResponse      *atomic.Value
	nodeStyleChooser  traversal.LinkTargetNodePrototypeChooser
	resumeMessages    chan []graphsync.ExtensionData
	pauseMessages     chan struct{}
	doNotSendCids     *cid.Set
	env               ExecutionEnv
	restartNeeded     bool
	pendingExtensions []graphsync.ExtensionData
}

func (re *requestExecutor) visitor(tp traversal.Progress, node ipld.Node, tr traversal.VisitReason) error {
	select {
	case <-re.ctx.Done():
	case re.inProgressChan <- graphsync.ResponseProgress{
		Node:      node,
		Path:      tp.Path,
		LastBlock: tp.LastBlock,
	}:
	}
	return nil
}

func (re *requestExecutor) traverse() error {
	traverser := ipldutil.TraversalBuilder{
		Root:     cidlink.Link{Cid: re.request.Root()},
		Selector: re.request.Selector(),
		Visitor:  re.visitor,
		Chooser:  re.nodeStyleChooser,
	}.Start(re.ctx)
	defer traverser.Shutdown(context.Background())
	for {
		isComplete, err := traverser.IsComplete()
		if isComplete {
			return err
		}
		lnk, _ := traverser.CurrentRequest()
		resultChan := re.env.Loader(re.request.ID(), lnk)
		var result types.AsyncLoadResult
		select {
		case result = <-resultChan:
		default:
			err := re.sendRestartAsNeeded()
			if err != nil {
				return err
			}
			select {
			case <-re.ctx.Done():
				return ipldutil.ContextCancelError{}
			case result = <-resultChan:
			}
		}
		err = re.processResult(traverser, lnk, result)
		if _, ok := err.(hooks.ErrPaused); ok {
			err = re.waitForResume()
			if err != nil {
				return err
			}
			err = traverser.Advance(bytes.NewBuffer(result.Data))
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
}

func (re *requestExecutor) run() {
	err := re.traverse()
	if err != nil {
		if !isContextErr(err) {
			select {
			case <-re.ctx.Done():
			case re.inProgressErr <- err:
			}
		}
	}
	select {
	case networkError := <-re.networkError:
		select {
		case re.inProgressErr <- networkError:
		case <-re.env.Ctx.Done():
		}
	default:
	}
	re.terminateRequest()
	close(re.inProgressChan)
	close(re.inProgressErr)
}

func (re *requestExecutor) sendRequest(request gsmsg.GraphSyncRequest) {
	re.env.SendRequest(re.p, request)
}

func (re *requestExecutor) terminateRequest() {
	re.env.TerminateRequest(re.request.ID())
}

func (re *requestExecutor) runBlockHooks(blk graphsync.BlockData) error {
	response := re.lastResponse.Load().(gsmsg.GraphSyncResponse)
	return re.env.RunBlockHooks(re.p, response, blk)
}

func (re *requestExecutor) waitForResume() error {
	select {
	case <-re.ctx.Done():
		return ipldutil.ContextCancelError{}
	case re.pendingExtensions = <-re.resumeMessages:
		re.restartNeeded = true
		return nil
	}
}

func (re *requestExecutor) onNewBlockWithPause(block graphsync.BlockData) error {
	err := re.onNewBlock(block)
	select {
	case <-re.pauseMessages:
		re.sendRequest(gsmsg.CancelRequest(re.request.ID()))
		if err == nil {
			err = hooks.ErrPaused{}
		}
	default:
	}
	return err
}

func (re *requestExecutor) onNewBlock(block graphsync.BlockData) error {
	re.doNotSendCids.Add(block.Link().(cidlink.Link).Cid)
	return re.runBlockHooks(block)
}

func (re *requestExecutor) processResult(traverser ipldutil.Traverser, link ipld.Link, result types.AsyncLoadResult) error {
	if result.Err != nil {
		select {
		case <-re.ctx.Done():
			return ipldutil.ContextCancelError{}
		case re.inProgressErr <- result.Err:
			traverser.Error(traversal.SkipMe{})
			return nil
		}
	}
	err := re.onNewBlockWithPause(&blockData{link, result.Local, uint64(len(result.Data))})
	if err != nil {
		return err
	}
	err = traverser.Advance(bytes.NewBuffer(result.Data))
	if err != nil {
		return err
	}
	return nil
}

func (re *requestExecutor) sendRestartAsNeeded() error {
	if !re.restartNeeded {
		return nil
	}
	extensions := re.pendingExtensions
	re.pendingExtensions = nil
	re.restartNeeded = false
	cidsData, err := cidset.EncodeCidSet(re.doNotSendCids)
	if err != nil {
		return err
	}
	extensions = append(extensions, graphsync.ExtensionData{Name: graphsync.ExtensionDoNotSendCIDs, Data: cidsData})
	re.request = re.request.ReplaceExtensions(extensions)
	re.sendRequest(re.request)
	return nil
}

func isContextErr(err error) bool {
	// TODO: Match with errors.Is when https://github.com/ipld/go-ipld-prime/issues/58 is resolved
	return strings.Contains(err.Error(), ipldutil.ContextCancelError{}.Error())
}

type blockData struct {
	link  ipld.Link
	local bool
	size  uint64
}

// Link is the link/cid for the block
func (bd *blockData) Link() ipld.Link {
	return bd.link
}

// BlockSize specifies the size of the block
func (bd *blockData) BlockSize() uint64 {
	return bd.size
}

// BlockSize specifies the amount of data actually transmitted over the network
func (bd *blockData) BlockSizeOnWire() uint64 {
	if bd.local {
		return 0
	}
	return bd.size
}
