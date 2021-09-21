package executor

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-peertaskqueue"
	"github.com/ipfs/go-peertaskqueue/peertask"
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
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("gs_request_executor")

// AsyncLoadFn is a function which given a request id and an ipld.Link, returns
// a channel which will eventually return data for the link or an err
type AsyncLoadFn func(peer.ID, graphsync.RequestID, ipld.Link, ipld.LinkContext) <-chan types.AsyncLoadResult

type Executor struct {
	ctx             context.Context
	sendRequest     func(peer.ID, gsmsg.GraphSyncRequest)
	runBlockHooks   func(p peer.ID, response graphsync.ResponseData, blk graphsync.BlockData) error
	getRequest      func(peer.ID, *peertask.Task, chan RequestExecution)
	releaseRequest  func(peer.ID, *peertask.Task, error)
	waitForMessages func(ctx context.Context, resumeMessages chan graphsync.ExtensionData) ([]graphsync.ExtensionData, error)
	loader          AsyncLoadFn
	linkSystem      ipld.LinkSystem
	queue           *peertaskqueue.PeerTaskQueue
	workSignal      chan struct{}
	ticker          *time.Ticker
}

func (e *Executor) processRequestsWorker() {
	const targetWork = 1
	requestExecutionChan := make(chan RequestExecution)
	var requestExecution RequestExecution
	for {
		pid, tasks, _ := e.queue.PopTasks(targetWork)
		for len(tasks) == 0 {
			select {
			case <-e.ctx.Done():
				return
			case <-e.workSignal:
				pid, tasks, _ = e.queue.PopTasks(targetWork)
			case <-e.ticker.C:
				e.queue.ThawRound()
				pid, tasks, _ = e.queue.PopTasks(targetWork)
			}
		}
		for _, task := range tasks {
			e.getRequest(pid, task, requestExecutionChan)
			select {
			case requestExecution = <-requestExecutionChan:
			case <-e.ctx.Done():
				return
			}
			if requestExecution.Empty {
				log.Info("Empty task on peer request stack")
				continue
			}
			log.Debugw("beginning request execution", "id", requestExecution.Request.ID(), "peer", pid.String(), "root_cid", requestExecution.Request.Root().String())
			err := e.traverse(requestExecution)
			if err != nil && !isContextErr(err) && !isPausedErr(err) {
				select {
				case <-requestExecution.Ctx.Done():
				case requestExecution.InProgressErr <- err:
				}
			}
			e.releaseRequest(pid, task, err)
			log.Debugw("finishing response execution", "id", requestExecution.Request.ID(), "peer", pid.String(), "root_cid", requestExecution.Request.Root().String())
		}
	}
}

// RequestExecution are parameters for a single request execution
type RequestExecution struct {
	Ctx           context.Context
	Request       gsmsg.GraphSyncRequest
	LastResponse  *atomic.Value
	DoNotSendCids *cid.Set
	PauseMessages chan struct{}
	Traverser     ipldutil.Traverser
	P             peer.ID
	InProgressErr chan error
	Empty         bool
}

func (e *Executor) traverse(re RequestExecution) error {
	for {
		isComplete, err := re.Traverser.IsComplete()
		if isComplete {
			return err
		}
		lnk, linkContext := re.Traverser.CurrentRequest()
		resultChan := e.loader(re.P, re.Request.ID(), lnk, linkContext)
		var result types.AsyncLoadResult
		select {
		case result = <-resultChan:
		default:
			if err := e.startRemoteRequest(re); err != nil {
				return err
			}
			select {
			case <-e.ctx.Done():
				return ipldutil.ContextCancelError{}
			case result = <-resultChan:
			}
		}
		err = e.processResult(re, lnk, result)
		if err != nil {
			return err
		}
	}
}

func (e *Executor) onNewBlockWithPause(re RequestExecution, block graphsync.BlockData) error {
	err := e.onNewBlock(re, block)
	select {
	case <-re.PauseMessages:
		e.sendRequest(re.P, gsmsg.CancelRequest(re.Request.ID()))
		if err == nil {
			err = hooks.ErrPaused{}
		}
	default:
	}
	return err
}

func (e *Executor) onNewBlock(re RequestExecution, block graphsync.BlockData) error {
	re.DoNotSendCids.Add(block.Link().(cidlink.Link).Cid)
	response := re.LastResponse.Load().(gsmsg.GraphSyncResponse)
	return e.runBlockHooks(re.P, response, block)
}

func (e *Executor) processResult(re RequestExecution, link ipld.Link, result types.AsyncLoadResult) error {
	if result.Err != nil {
		select {
		case <-re.Ctx.Done():
			return ipldutil.ContextCancelError{}
		case re.InProgressErr <- result.Err:
			re.Traverser.Error(traversal.SkipMe{})
			return nil
		}
	}
	err := e.onNewBlockWithPause(re, &blockData{link, result.Local, uint64(len(result.Data))})
	if err != nil {
		return err
	}
	err = re.Traverser.Advance(bytes.NewBuffer(result.Data))
	if err != nil {
		return err
	}
	return nil
}

func (e *Executor) startRemoteRequest(re RequestExecution) error {
	cidsData, err := cidset.EncodeCidSet(re.DoNotSendCids)
	if err != nil {
		return err
	}
	request := re.Request.ReplaceExtensions([]graphsync.ExtensionData{{Name: graphsync.ExtensionDoNotSendCIDs, Data: cidsData}})
	e.sendRequest(re.P, request)
	return nil
}

func isContextErr(err error) bool {
	// TODO: Match with errors.Is when https://github.com/ipld/go-ipld-prime/issues/58 is resolved
	return strings.Contains(err.Error(), ipldutil.ContextCancelError{}.Error())
}

func isPausedErr(err error) bool {
	_, isPaused := err.(hooks.ErrPaused)
	return isPaused
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
