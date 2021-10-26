package queryexecutor

import (
	"context"
	"errors"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-peertaskqueue/peertask"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
	"github.com/ipfs/go-graphsync/responsemanager/runtraversal"
)

const (
	ThawSpeed = time.Millisecond * 100
)

var log = logging.Logger("graphsync")
var ErrNetworkError = errors.New("network error")
var ErrCancelledByCommand = errors.New("response cancelled by responder")

// ResponseTaskData returns all information needed to execute a given response
type ResponseTaskData struct {
	Empty      bool
	Subscriber *notifications.TopicDataSubscriber
	Ctx        context.Context
	Request    gsmsg.GraphSyncRequest
	Loader     ipld.BlockReadOpener
	Traverser  ipldutil.Traverser
	Signals    ResponseSignals
}

// ResponseSignals are message channels to communicate between the manager and the QueryExecutor
type ResponseSignals struct {
	PauseSignal  chan struct{}
	UpdateSignal chan struct{}
	ErrSignal    chan error
}

// QueryExecutor is responsible for performing individual requests by executing their traversals
type QueryExecutor struct {
	ctx                context.Context
	manager            Manager
	blockHooks         BlockHooks
	updateHooks        UpdateHooks
	cancelledListeners CancelledListeners
	responseAssembler  ResponseAssembler
	workSignal         chan struct{}
	ticker             *time.Ticker
	connManager        network.ConnManager
}

// New creates a new QueryExecutor
func New(ctx context.Context,
	manager Manager,
	blockHooks BlockHooks,
	updateHooks UpdateHooks,
	cancelledListeners CancelledListeners,
	responseAssembler ResponseAssembler,
	workSignal chan struct{},
	ticker *time.Ticker,
	connManager network.ConnManager,
) *QueryExecutor {
	qm := &QueryExecutor{
		blockHooks:         blockHooks,
		updateHooks:        updateHooks,
		cancelledListeners: cancelledListeners,
		responseAssembler:  responseAssembler,
		manager:            manager,
		ctx:                ctx,
		workSignal:         workSignal,
		ticker:             ticker,
		connManager:        connManager,
	}
	return qm
}

// ExecuteTask takes a single task and executes its traversal it describes. For each block, it
// checks for signals on the task's ResponseSignals, updates on the QueryExecutor's UpdateHooks,
// and uses the ResponseAssembler to build and send a response, while also triggering any of
// the QueryExecutor's BlockHooks. Traversal continues until complete, or a signal or hook
// suggests we should stop or pause.
func (qe *QueryExecutor) ExecuteTask(ctx context.Context, pid peer.ID, task *peertask.Task) bool {
	// StartTask lets us block until this task is at the top of the execution stack
	taskDataChan := make(chan ResponseTaskData)
	var taskData ResponseTaskData
	qe.manager.StartTask(task, taskDataChan)
	select {
	case taskData = <-taskDataChan:
	case <-qe.ctx.Done():
		return true
	}
	if taskData.Empty {
		log.Info("Empty task on peer request stack")
		return false
	}

	log.Debugw("beginning response execution", "id", taskData.Request.ID(), "peer", pid.String(), "root_cid", taskData.Request.Root().String())
	_, err := qe.executeQuery(pid, taskData.Request, taskData.Loader, taskData.Traverser, taskData.Signals, taskData.Subscriber)
	isCancelled := err != nil && ipldutil.IsContextCancelErr(err) // TODO(rv): this won't match ErrCancelledByCommand, only
	if isCancelled {
		qe.connManager.Unprotect(pid, taskData.Request.ID().Tag())
		qe.cancelledListeners.NotifyCancelledListeners(pid, taskData.Request)
	}
	qe.manager.FinishTask(task, err)
	log.Debugw("finishing response execution", "id", taskData.Request.ID(), "peer", pid.String(), "root_cid", taskData.Request.Root().String())
	return false
}

func (qe *QueryExecutor) executeQuery(
	p peer.ID,
	request gsmsg.GraphSyncRequest,
	loader ipld.BlockReadOpener,
	traverser ipldutil.Traverser,
	signals ResponseSignals,
	sub *notifications.TopicDataSubscriber) (graphsync.ResponseStatusCode, error) {

	// Execute the traversal operation, continue until we have reason to stop (error, pause, complete)
	updateChan := make(chan []gsmsg.GraphSyncRequest)
	err := runtraversal.RunTraversal(loader, traverser, func(link ipld.Link, data []byte) error {
		// Execute a transaction for this block, including any other queued operations
		var err error
		_ = qe.responseAssembler.Transaction(p, request.ID(), func(rb responseassembler.ResponseBuilder) error {
			// Ensure that any updates that have occurred till now are integrated into the response
			err = qe.checkForUpdates(p, request, signals, updateChan, rb)
			// On any error other than a pause, we bail, if it's a pause then we continue processing _this_ block
			if _, ok := err.(hooks.ErrPaused); !ok && err != nil {
				return nil
			}
			blockData := rb.SendResponse(link, data)
			rb.AddNotifee(notifications.Notifee{Data: blockData, Subscriber: sub})
			if blockData.BlockSize() > 0 {
				result := qe.blockHooks.ProcessBlockHooks(p, request, blockData)
				for _, extension := range result.Extensions {
					rb.SendExtensionData(extension)
				}
				if _, ok := result.Err.(hooks.ErrPaused); ok {
					rb.PauseRequest()
				}
				if result.Err != nil {
					// Halts the traversal and returns to the top-level `err`
					err = result.Err
				}
			}
			return nil
		})
		return err
	})

	// Close out the response, either temporarily (pause) or permanently (cancel, fail, complete)
	var code graphsync.ResponseStatusCode
	_ = qe.responseAssembler.Transaction(p, request.ID(), func(rb responseassembler.ResponseBuilder) error {
		if err != nil {
			// TODO(rv): in the pause, cancel and network-error cases, the `code` is dropped on the floor,
			// should we be capturing that or passing it on? Why set it otherwise?
			_, isPaused := err.(hooks.ErrPaused)
			if isPaused {
				code = graphsync.RequestPaused
				return nil
			}
			if ipldutil.IsContextCancelErr(err) {
				rb.ClearRequest()
				code = graphsync.RequestCancelled
				return nil
			}
			if err == ErrNetworkError {
				rb.ClearRequest()
				code = graphsync.RequestFailedUnknown
				return nil
			}
			if err == runtraversal.ErrFirstBlockLoad {
				code = graphsync.RequestFailedContentNotFound
			} else if err == ErrCancelledByCommand {
				code = graphsync.RequestCancelled
			} else {
				code = graphsync.RequestFailedUnknown
			}
			rb.FinishWithError(code)
		} else {
			code = rb.FinishRequest()
		}
		rb.AddNotifee(notifications.Notifee{Data: code, Subscriber: sub})
		return nil
	})
	return code, err
}

// checkForUpdates is called on each block traversed to ensure no outstanding signals
// or updates need to be handled during the current transaction
func (qe *QueryExecutor) checkForUpdates(
	p peer.ID,
	request gsmsg.GraphSyncRequest,
	signals ResponseSignals,
	updateChan chan []gsmsg.GraphSyncRequest,
	rb responseassembler.ResponseBuilder) error {

	for {
		select {
		case <-signals.PauseSignal:
			rb.PauseRequest()
			return hooks.ErrPaused{}
		case err := <-signals.ErrSignal:
			return err
		case <-signals.UpdateSignal:
			qe.manager.GetUpdates(p, request.ID(), updateChan)
			select {
			case updates := <-updateChan:
				for _, update := range updates {
					result := qe.updateHooks.ProcessUpdateHooks(p, request, update)
					for _, extension := range result.Extensions {
						// if there is something to send to the client for this update, build it into the
						// response that will be sent with the current transaction
						rb.SendExtensionData(extension)
					}
					if result.Err != nil {
						return result.Err
					}
				}
			case <-qe.ctx.Done():
			}
		default:
			return nil
		}
	}
}

// Manager providers an interface to the response manager
type Manager interface {
	StartTask(task *peertask.Task, responseTaskDataChan chan<- ResponseTaskData)
	GetUpdates(p peer.ID, requestID graphsync.RequestID, updatesChan chan<- []gsmsg.GraphSyncRequest)
	FinishTask(task *peertask.Task, err error)
}

// BlockHooks is an interface for processing block hooks
type BlockHooks interface {
	ProcessBlockHooks(p peer.ID, request graphsync.RequestData, blockData graphsync.BlockData) hooks.BlockResult
}

// UpdateHooks is an interface for processing update hooks
type UpdateHooks interface {
	ProcessUpdateHooks(p peer.ID, request graphsync.RequestData, update graphsync.RequestData) hooks.UpdateResult
}

// CancelledListeners is an interface for notifying listeners that requestor cancelled
type CancelledListeners interface {
	NotifyCancelledListeners(p peer.ID, request graphsync.RequestData)
}

// ResponseAssembler is an interface that returns sender interfaces for peer responses.
type ResponseAssembler interface {
	Transaction(p peer.ID, requestID graphsync.RequestID, transaction responseassembler.Transaction) error
}
