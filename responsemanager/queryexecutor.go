package responsemanager

import (
	"context"
	"errors"
	"strings"
	"time"

	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
	"github.com/ipfs/go-graphsync/responsemanager/runtraversal"
	"github.com/ipfs/go-peertaskqueue/peertask"
)

var errCancelledByCommand = errors.New("response cancelled by responder")

// Manager providers an interface to the response manager
type Manager interface {
	StartTask(task *peertask.Task, responseTaskDataChan chan<- ResponseTaskData)
	GetUpdates(p peer.ID, requestID graphsync.RequestID, updatesChan chan<- []gsmsg.GraphSyncRequest)
	FinishTask(task *peertask.Task, err error)
}

// TODO: Move this into a seperate module and fully seperate from the ResponseManager
type queryExecutor struct {
	manager            Manager
	blockHooks         BlockHooks
	updateHooks        UpdateHooks
	cancelledListeners CancelledListeners
	responseAssembler  ResponseAssembler
	queryQueue         QueryQueue
	ctx                context.Context
	workSignal         chan struct{}
	ticker             *time.Ticker
}

func (qe *queryExecutor) processQueriesWorker() {
	const targetWork = 1
	taskDataChan := make(chan ResponseTaskData)
	var taskData ResponseTaskData
	for {
		pid, tasks, _ := qe.queryQueue.PopTasks(targetWork)
		for len(tasks) == 0 {
			select {
			case <-qe.ctx.Done():
				return
			case <-qe.workSignal:
				pid, tasks, _ = qe.queryQueue.PopTasks(targetWork)
			case <-qe.ticker.C:
				qe.queryQueue.ThawRound()
				pid, tasks, _ = qe.queryQueue.PopTasks(targetWork)
			}
		}
		for _, task := range tasks {
			qe.manager.StartTask(task, taskDataChan)
			select {
			case taskData = <-taskDataChan:
			case <-qe.ctx.Done():
				return
			}
			if taskData.Empty {
				log.Info("Empty task on peer request stack")
				continue
			}
			log.Debugw("beginning response execution", "id", taskData.Request.ID(), "peer", pid.String(), "root_cid", taskData.Request.Root().String())
			_, err := qe.executeQuery(pid, taskData.Request, taskData.Loader, taskData.Traverser, taskData.Signals, taskData.Subscriber)
			isCancelled := err != nil && isContextErr(err)
			if isCancelled {
				qe.cancelledListeners.NotifyCancelledListeners(pid, taskData.Request)
			}
			qe.manager.FinishTask(task, err)
			log.Debugw("finishing response execution", "id", taskData.Request.ID(), "peer", pid.String(), "root_cid", taskData.Request.Root().String())
		}
	}
}

func (qe *queryExecutor) executeQuery(
	p peer.ID,
	request gsmsg.GraphSyncRequest,
	loader ipld.BlockReadOpener,
	traverser ipldutil.Traverser,
	signals ResponseSignals,
	sub *notifications.TopicDataSubscriber) (graphsync.ResponseStatusCode, error) {
	updateChan := make(chan []gsmsg.GraphSyncRequest)
	err := runtraversal.RunTraversal(loader, traverser, func(link ipld.Link, data []byte) error {
		var err error
		_ = qe.responseAssembler.Transaction(p, request.ID(), func(rb responseassembler.ResponseBuilder) error {
			err = qe.checkForUpdates(p, request, signals, updateChan, rb)
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
					err = result.Err
				}
			}
			return nil
		})
		return err
	})
	var code graphsync.ResponseStatusCode
	_ = qe.responseAssembler.Transaction(p, request.ID(), func(rb responseassembler.ResponseBuilder) error {
		if err != nil {
			_, isPaused := err.(hooks.ErrPaused)
			if isPaused {
				code = graphsync.RequestPaused
				return nil
			}
			if isContextErr(err) {
				rb.ClearRequest()
				code = graphsync.RequestCancelled
				return nil
			}
			if err == errNetworkError {
				rb.ClearRequest()
				code = graphsync.RequestFailedUnknown
				return nil
			}
			if err == runtraversal.ErrFirstBlockLoad {
				code = graphsync.RequestFailedContentNotFound
			} else if err == errCancelledByCommand {
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

func (qe *queryExecutor) checkForUpdates(
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

func isContextErr(err error) bool {
	// TODO: Match with errors.Is when https://github.com/ipld/go-ipld-prime/issues/58 is resolved
	return strings.Contains(err.Error(), ipldutil.ContextCancelError{}.Error())
}
