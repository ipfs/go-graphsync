package executor

import (
	"context"
	"regexp"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/requestmanager/loader"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	peer "github.com/libp2p/go-libp2p-peer"
)

// ExecutionEnv are request parameters that last between requests
type ExecutionEnv struct {
	SendRequest      func(peer.ID, gsmsg.GraphSyncRequest)
	RunBlockHooks    func(p peer.ID, response graphsync.ResponseData, blk graphsync.BlockData) error
	TerminateRequest func(graphsync.RequestID)
	WaitForMessages  func(ctx context.Context, resumeMessages chan graphsync.ExtensionData) ([]graphsync.ExtensionData, error)
	Loader           loader.AsyncLoadFn
}

// RequestExecution are parameters for a single request execution
type RequestExecution struct {
	Ctx              context.Context
	P                peer.ID
	Request          gsmsg.GraphSyncRequest
	LastResponse     *atomic.Value
	DoNotSendCids    *cid.Set
	NodeStyleChooser traversal.LinkTargetNodeStyleChooser
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
		request:          re.Request,
		lastResponse:     re.LastResponse,
		doNotSendCids:    re.DoNotSendCids,
		nodeStyleChooser: re.NodeStyleChooser,
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
	request           gsmsg.GraphSyncRequest
	lastResponse      *atomic.Value
	nodeStyleChooser  traversal.LinkTargetNodeStyleChooser
	resumeMessages    chan []graphsync.ExtensionData
	pauseMessages     chan struct{}
	blocksReceived    uint64
	maxBlocksReceived uint64
	doNotSendCids     *cid.Set
	env               ExecutionEnv
}

func (re *requestExecutor) visitor(tp traversal.Progress, node ipld.Node, tr traversal.VisitReason) error {
	if re.blocksReceived >= re.maxBlocksReceived {
		select {
		case <-re.ctx.Done():
		case re.inProgressChan <- graphsync.ResponseProgress{
			Node:      node,
			Path:      tp.Path,
			LastBlock: tp.LastBlock,
		}:
		}
	}
	return nil
}

func (re *requestExecutor) onNewBlockWithPause(block graphsync.BlockData) error {
	err := re.onNewBlock(block)
	select {
	case <-re.pauseMessages:
		if err == nil {
			err = hooks.ErrPaused{}
		}
	default:
	}
	return err
}

func (re *requestExecutor) onNewBlock(block graphsync.BlockData) error {
	re.blocksReceived++
	re.doNotSendCids.Add(block.Link().(cidlink.Link).Cid)
	if re.blocksReceived > re.maxBlocksReceived {
		re.maxBlocksReceived = re.blocksReceived
		return re.runBlockHooks(block)
	}
	return nil
}

func (re *requestExecutor) executeOnce(selector selector.Selector, loaderFn ipld.Loader) (bool, error) {
	err := ipldutil.Traverse(re.ctx, loaderFn, re.nodeStyleChooser, cidlink.Link{Cid: re.request.Root()}, selector, re.visitor)
	if err == nil || !isPausedErr(err) {
		return true, err
	}
	re.blocksReceived = 0
	extensions, err := re.waitForResume()
	if err != nil {
		return true, err
	}
	cidsData, err := cidset.EncodeCidSet(re.doNotSendCids)
	if err != nil {
		return true, err
	}
	extensions = append(extensions, graphsync.ExtensionData{Name: graphsync.ExtensionDoNotSendCIDs, Data: cidsData})
	re.request = re.request.ReplaceExtensions(extensions)
	re.sendRequest(re.request)
	return false, nil
}

func (re *requestExecutor) run() {
	selector, _ := ipldutil.ParseSelector(re.request.Selector())
	loaderFn := loader.WrapAsyncLoader(re.ctx, re.env.Loader, re.request.ID(), re.inProgressErr, re.onNewBlockWithPause)
	var err error
	var finished bool
	for !finished {
		finished, err = re.executeOnce(selector, loaderFn)
	}
	if err != nil {
		if !isContextErr(err) {
			select {
			case <-re.ctx.Done():
			case re.inProgressErr <- err:
			}
		}
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

func (re *requestExecutor) waitForResume() ([]graphsync.ExtensionData, error) {
	select {
	case <-re.ctx.Done():
		return nil, loader.ContextCancelError{}
	case extensions := <-re.resumeMessages:
		return extensions, nil
	}
}
func isPausedErr(err error) bool {
	// TODO: Match with errors.Is when https://github.com/ipld/go-ipld-prime/issues/58 is resolved
	match, _ := regexp.MatchString(hooks.ErrPaused{}.Error(), err.Error())
	return match
}

func isContextErr(err error) bool {
	// TODO: Match with errors.Is when https://github.com/ipld/go-ipld-prime/issues/58 is resolved
	match, _ := regexp.MatchString(loader.ContextCancelError{}.Error(), err.Error())
	return match
}
