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
)

var errCancelledByCommand = errors.New("response cancelled by responder")

// TODO: Move this into a seperate module and fully seperate from the ResponseManager
type queryExecutor struct {
	blockHooks         BlockHooks
	updateHooks        UpdateHooks
	cancelledListeners CancelledListeners
	responseAssembler  ResponseAssembler
	queryQueue         QueryQueue
	messages           chan responseManagerMessage
	ctx                context.Context
	workSignal         chan struct{}
	ticker             *time.Ticker
}

func (qe *queryExecutor) processQueriesWorker() {
	const targetWork = 1
	taskDataChan := make(chan responseTaskData)
	var taskData responseTaskData
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
			select {
			case qe.messages <- &startTaskRequest{task, taskDataChan}:
			case <-qe.ctx.Done():
				return
			}
			select {
			case taskData = <-taskDataChan:
			case <-qe.ctx.Done():
				return
			}
			if taskData.empty {
				log.Info("Empty task on peer request stack")
				continue
			}
			log.Debugw("beginning response execution", "id", taskData.request.ID(), "peer", pid.String(), "root_cid", taskData.request.Root().String())
			status, err := qe.executeQuery(pid, taskData.request, taskData.loader, taskData.traverser, taskData.signals, taskData.subscriber)
			isCancelled := err != nil && isContextErr(err)
			if isCancelled {
				qe.cancelledListeners.NotifyCancelledListeners(pid, taskData.request)
			}
			select {
			case qe.messages <- &finishTaskRequest{task, status, err}:
			case <-qe.ctx.Done():
			}
			log.Debugw("finishing response execution", "id", taskData.request.ID(), "peer", pid.String(), "root_cid", taskData.request.Root().String())
		}
	}
}

func (qe *queryExecutor) executeQuery(
	p peer.ID,
	request gsmsg.GraphSyncRequest,
	loader ipld.BlockReadOpener,
	traverser ipldutil.Traverser,
	signals signals,
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
	signals signals,
	updateChan chan []gsmsg.GraphSyncRequest,
	rb responseassembler.ResponseBuilder) error {
	for {
		select {
		case <-signals.pauseSignal:
			rb.PauseRequest()
			return hooks.ErrPaused{}
		case err := <-signals.errSignal:
			return err
		case <-signals.updateSignal:
			select {
			case qe.messages <- &responseUpdateRequest{responseKey{p, request.ID()}, updateChan}:
			case <-qe.ctx.Done():
			}
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
