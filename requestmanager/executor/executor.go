package executor

import (
	"context"
	"regexp"

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
)

// RequestExecution runs a single graphsync request with data loaded from the
// asynchronous loader
type RequestExecution struct {
	Request          gsmsg.GraphSyncRequest
	SendRequest      func(gsmsg.GraphSyncRequest)
	Loader           loader.AsyncLoadFn
	RunBlockHooks    func(blk graphsync.BlockData) error
	DoNotSendCids    *cid.Set
	TerminateRequest func()
	WaitForResume    func() ([]graphsync.ExtensionData, error)
	NodeStyleChooser traversal.LinkTargetNodeStyleChooser
}

// Start begins execution of a request in a go routine
func (re RequestExecution) Start(ctx context.Context) (chan graphsync.ResponseProgress, chan error) {
	executor := &requestExecutor{
		inProgressChan:   make(chan graphsync.ResponseProgress),
		inProgressErr:    make(chan error),
		ctx:              ctx,
		request:          re.Request,
		sendRequest:      re.SendRequest,
		loader:           re.Loader,
		runBlockHooks:    re.RunBlockHooks,
		terminateRequest: re.TerminateRequest,
		nodeStyleChooser: re.NodeStyleChooser,
		waitForResume:    re.WaitForResume,
		doNotSendCids:    re.DoNotSendCids,
	}
	executor.sendRequest(executor.request)
	go executor.run()
	return executor.inProgressChan, executor.inProgressErr
}

type requestExecutor struct {
	inProgressChan    chan graphsync.ResponseProgress
	inProgressErr     chan error
	ctx               context.Context
	request           gsmsg.GraphSyncRequest
	sendRequest       func(gsmsg.GraphSyncRequest)
	loader            loader.AsyncLoadFn
	runBlockHooks     func(blk graphsync.BlockData) error
	terminateRequest  func()
	nodeStyleChooser  traversal.LinkTargetNodeStyleChooser
	blocksReceived    uint64
	maxBlocksReceived uint64
	doNotSendCids     *cid.Set
	waitForResume     func() ([]graphsync.ExtensionData, error)
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
	loaderFn := loader.WrapAsyncLoader(re.ctx, re.loader, re.request.ID(), re.inProgressErr, re.onNewBlock)
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
