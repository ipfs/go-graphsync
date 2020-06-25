package executor

import (
	"context"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/requestmanager/loader"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
)

// RequestExecution runs a single graphsync request with data loaded from the
// asynchronous loader
type RequestExecution struct {
	Request          gsmsg.GraphSyncRequest
	SendRequest      func(gsmsg.GraphSyncRequest)
	Loader           loader.AsyncLoadFn
	RunBlockHooks    func(blk graphsync.BlockData) error
	TerminateRequest func()
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
	}
	executor.sendRequest(executor.request)
	go executor.run()
	return executor.inProgressChan, executor.inProgressErr
}

type requestExecutor struct {
	inProgressChan   chan graphsync.ResponseProgress
	inProgressErr    chan error
	ctx              context.Context
	request          gsmsg.GraphSyncRequest
	sendRequest      func(gsmsg.GraphSyncRequest)
	loader           loader.AsyncLoadFn
	runBlockHooks    func(blk graphsync.BlockData) error
	terminateRequest func()
	nodeStyleChooser traversal.LinkTargetNodeStyleChooser
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

func (re *requestExecutor) run() {
	selector, _ := ipldutil.ParseSelector(re.request.Selector())
	loaderFn := loader.WrapAsyncLoader(re.ctx, re.loader, re.request.ID(), re.inProgressErr, re.runBlockHooks)
	_ = ipldutil.Traverse(re.ctx, loaderFn, re.nodeStyleChooser, cidlink.Link{Cid: re.request.Root()}, selector, re.visitor)
	re.terminateRequest()
	close(re.inProgressChan)
	close(re.inProgressErr)
}
