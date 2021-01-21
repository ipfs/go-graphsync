package responsemanager

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/dedupkey"
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
	requestHooks       RequestHooks
	blockHooks         BlockHooks
	updateHooks        UpdateHooks
	cancelledListeners CancelledListeners
	responseAssembler  ResponseAssembler
	loader             ipld.Loader
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
			key := task.Topic.(responseKey)
			select {
			case qe.messages <- &responseDataRequest{key, taskDataChan}:
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
			status, err := qe.executeTask(key, taskData)
			isCancelled := err != nil && isContextErr(err)
			if isCancelled {
				qe.cancelledListeners.NotifyCancelledListeners(key.p, taskData.request)
			}
			select {
			case qe.messages <- &finishTaskRequest{key, status, err}:
			case <-qe.ctx.Done():
			}
		}
		qe.queryQueue.TasksDone(pid, tasks...)

	}

}

func (qe *queryExecutor) executeTask(key responseKey, taskData responseTaskData) (graphsync.ResponseStatusCode, error) {
	var err error
	loader := taskData.loader
	traverser := taskData.traverser
	if loader == nil || traverser == nil {
		var isPaused bool
		loader, traverser, isPaused, err = qe.prepareQuery(taskData.ctx, key.p, taskData.request, taskData.signals, taskData.subscriber)
		if err != nil {
			return graphsync.RequestFailedUnknown, err
		}
		select {
		case <-qe.ctx.Done():
			return graphsync.RequestFailedUnknown, errors.New("context cancelled")
		case qe.messages <- &setResponseDataRequest{key, loader, traverser}:
		}
		if isPaused {
			return graphsync.RequestPaused, hooks.ErrPaused{}
		}
	}
	return qe.executeQuery(key.p, taskData.request, loader, traverser, taskData.signals, taskData.subscriber)
}

func (qe *queryExecutor) prepareQuery(ctx context.Context,
	p peer.ID,
	request gsmsg.GraphSyncRequest, signals signals, sub *notifications.TopicDataSubscriber) (ipld.Loader, ipldutil.Traverser, bool, error) {
	result := qe.requestHooks.ProcessRequestHooks(p, request)
	var transactionError error
	var isPaused bool
	failNotifee := notifications.Notifee{Data: graphsync.RequestFailedUnknown, Subscriber: sub}
	err := qe.responseAssembler.Transaction(p, request.ID(), func(rb responseassembler.ResponseBuilder) error {
		for _, extension := range result.Extensions {
			rb.SendExtensionData(extension)
		}
		if result.Err != nil || !result.IsValidated {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			rb.AddNotifee(failNotifee)
			transactionError = errors.New("request not valid")
		} else if result.IsPaused {
			rb.PauseRequest()
			isPaused = true
		}
		return nil
	})
	if err != nil {
		return nil, nil, false, err
	}
	if transactionError != nil {
		return nil, nil, false, transactionError
	}
	if err := qe.processDedupByKey(request, p, failNotifee); err != nil {
		return nil, nil, false, err
	}
	if err := qe.processDoNoSendCids(request, p, failNotifee); err != nil {
		return nil, nil, false, err
	}
	rootLink := cidlink.Link{Cid: request.Root()}
	traverser := ipldutil.TraversalBuilder{
		Root:     rootLink,
		Selector: request.Selector(),
		Chooser:  result.CustomChooser,
	}.Start(ctx)
	loader := result.CustomLoader
	if loader == nil {
		loader = qe.loader
	}
	return loader, traverser, isPaused, nil
}

func (qe *queryExecutor) processDedupByKey(request gsmsg.GraphSyncRequest, p peer.ID, failNotifee notifications.Notifee) error {
	dedupData, has := request.Extension(graphsync.ExtensionDeDupByKey)
	if !has {
		return nil
	}
	key, err := dedupkey.DecodeDedupKey(dedupData)
	if err != nil {
		_ = qe.responseAssembler.Transaction(p, request.ID(), func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			rb.AddNotifee(failNotifee)
			return nil
		})
		return err
	}
	qe.responseAssembler.DedupKey(p, request.ID(), key)
	return nil
}

func (qe *queryExecutor) processDoNoSendCids(request gsmsg.GraphSyncRequest, p peer.ID, failNotifee notifications.Notifee) error {
	doNotSendCidsData, has := request.Extension(graphsync.ExtensionDoNotSendCIDs)
	if !has {
		return nil
	}
	cidSet, err := cidset.DecodeCidSet(doNotSendCidsData)
	if err != nil {
		_ = qe.responseAssembler.Transaction(p, request.ID(), func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			rb.AddNotifee(failNotifee)
			return nil
		})
		return err
	}
	links := make([]ipld.Link, 0, cidSet.Len())
	err = cidSet.ForEach(func(c cid.Cid) error {
		links = append(links, cidlink.Link{Cid: c})
		return nil
	})
	if err != nil {
		return err
	}
	qe.responseAssembler.IgnoreBlocks(p, request.ID(), links)
	return nil
}

func (qe *queryExecutor) executeQuery(
	p peer.ID,
	request gsmsg.GraphSyncRequest,
	loader ipld.Loader,
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
			if err == errCancelledByCommand {
				code = graphsync.RequestCancelled
			} else {
				code = graphsync.RequestFailedUnknown
			}
			rb.FinishWithError(graphsync.RequestCancelled)
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
