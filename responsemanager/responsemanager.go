package responsemanager

import (
	"context"
	"time"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/loader"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	"github.com/ipfs/go-graphsync/responsemanager/selectorvalidator"
	"github.com/ipfs/go-peertaskqueue/peertask"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	maxInProcessRequests = 6
	maxRecursionDepth    = 100
	thawSpeed            = time.Millisecond * 100
)

type inProgressResponseStatus struct {
	ctx      context.Context
	cancelFn func()
	request  gsmsg.GraphSyncRequest
}

type responseKey struct {
	p         peer.ID
	requestID graphsync.RequestID
}

type responseTaskData struct {
	ctx     context.Context
	request gsmsg.GraphSyncRequest
}

type requestHook struct {
	hook graphsync.OnRequestReceivedHook
}

// QueryQueue is an interface that can receive new selector query tasks
// and prioritize them as needed, and pop them off later
type QueryQueue interface {
	PushTasks(to peer.ID, tasks ...peertask.Task)
	PopTasks(targetMinWork int) (peer.ID, []*peertask.Task, int)
	Remove(topic peertask.Topic, p peer.ID)
	TasksDone(to peer.ID, tasks ...*peertask.Task)
	ThawRound()
}

// PeerManager is an interface that returns sender interfaces for peer responses.
type PeerManager interface {
	SenderForPeer(p peer.ID) peerresponsemanager.PeerResponseSender
}

type responseManagerMessage interface {
	handle(rm *ResponseManager)
}

// ResponseManager handles incoming requests from the network, initiates selector
// traversals, and transmits responses
type ResponseManager struct {
	ctx         context.Context
	cancelFn    context.CancelFunc
	loader      ipldbridge.Loader
	ipldBridge  ipldbridge.IPLDBridge
	peerManager PeerManager
	queryQueue  QueryQueue

	messages            chan responseManagerMessage
	workSignal          chan struct{}
	ticker              *time.Ticker
	inProgressResponses map[responseKey]inProgressResponseStatus
	requestHooks        []requestHook
}

// New creates a new response manager from the given context, loader,
// bridge to IPLD interface, peerManager, and queryQueue.
func New(ctx context.Context,
	loader ipldbridge.Loader,
	ipldBridge ipldbridge.IPLDBridge,
	peerManager PeerManager,
	queryQueue QueryQueue) *ResponseManager {
	ctx, cancelFn := context.WithCancel(ctx)
	return &ResponseManager{
		ctx:                 ctx,
		cancelFn:            cancelFn,
		loader:              loader,
		ipldBridge:          ipldBridge,
		peerManager:         peerManager,
		queryQueue:          queryQueue,
		messages:            make(chan responseManagerMessage, 16),
		workSignal:          make(chan struct{}, 1),
		ticker:              time.NewTicker(thawSpeed),
		inProgressResponses: make(map[responseKey]inProgressResponseStatus),
	}
}

type processRequestMessage struct {
	p        peer.ID
	requests []gsmsg.GraphSyncRequest
}

// ProcessRequests processes incoming requests for the given peer
func (rm *ResponseManager) ProcessRequests(ctx context.Context, p peer.ID, requests []gsmsg.GraphSyncRequest) {
	select {
	case rm.messages <- &processRequestMessage{p, requests}:
	case <-rm.ctx.Done():
	case <-ctx.Done():
	}
}

// RegisterHook registers an extension to process new incoming requests
func (rm *ResponseManager) RegisterHook(hook graphsync.OnRequestReceivedHook) {
	select {
	case rm.messages <- &requestHook{hook}:
	case <-rm.ctx.Done():
	}
}

type synchronizeMessage struct {
	sync chan struct{}
}

// this is a test utility method to force all messages to get processed
func (rm *ResponseManager) synchronize() {
	sync := make(chan struct{})
	select {
	case rm.messages <- &synchronizeMessage{sync}:
	case <-rm.ctx.Done():
	}
	select {
	case <-sync:
	case <-rm.ctx.Done():
	}
}

type responseDataRequest struct {
	key          responseKey
	taskDataChan chan *responseTaskData
}

type finishResponseRequest struct {
	key responseKey
}

func (rm *ResponseManager) processQueriesWorker() {
	const targetWork = 1
	taskDataChan := make(chan *responseTaskData)
	var taskData *responseTaskData
	for {
		pid, tasks, _ := rm.queryQueue.PopTasks(targetWork)
		for len(tasks) == 0 {
			select {
			case <-rm.ctx.Done():
				return
			case <-rm.workSignal:
				pid, tasks, _ = rm.queryQueue.PopTasks(targetWork)
			case <-rm.ticker.C:
				rm.queryQueue.ThawRound()
				pid, tasks, _ = rm.queryQueue.PopTasks(targetWork)
			}
		}
		for _, task := range tasks {
			key := task.Topic.(responseKey)
			select {
			case rm.messages <- &responseDataRequest{key, taskDataChan}:
			case <-rm.ctx.Done():
				return
			}
			select {
			case taskData = <-taskDataChan:
			case <-rm.ctx.Done():
				return
			}
			rm.executeQuery(taskData.ctx, key.p, taskData.request)
			select {
			case rm.messages <- &finishResponseRequest{key}:
			case <-rm.ctx.Done():
			}
		}
		rm.queryQueue.TasksDone(pid, tasks...)

	}

}

func noopVisitor(tp ipldbridge.TraversalProgress, n ipld.Node, tr ipldbridge.TraversalReason) error {
	return nil
}

type hookActions struct {
	isValidated        bool
	requestID          graphsync.RequestID
	peerResponseSender peerresponsemanager.PeerResponseSender
	err                error
}

func (ha *hookActions) SendExtensionData(ext graphsync.ExtensionData) {
	ha.peerResponseSender.SendExtensionData(ha.requestID, ext)
}

func (ha *hookActions) TerminateWithError(err error) {
	ha.err = err
	ha.peerResponseSender.FinishWithError(ha.requestID, graphsync.RequestFailedUnknown)
}

func (ha *hookActions) ValidateRequest() {
	ha.isValidated = true
}

func (rm *ResponseManager) executeQuery(ctx context.Context,
	p peer.ID,
	request gsmsg.GraphSyncRequest) {
	peerResponseSender := rm.peerManager.SenderForPeer(p)
	selectorSpec, err := rm.ipldBridge.DecodeNode(request.Selector())
	if err != nil {
		peerResponseSender.FinishWithError(request.ID(), graphsync.RequestFailedUnknown)
		return
	}
	ha := &hookActions{false, request.ID(), peerResponseSender, nil}
	for _, requestHook := range rm.requestHooks {
		requestHook.hook(p, request, ha)
		if ha.err != nil {
			return
		}
	}
	if !ha.isValidated {
		err = selectorvalidator.ValidateSelector(rm.ipldBridge, selectorSpec, maxRecursionDepth)
		if err != nil {
			peerResponseSender.FinishWithError(request.ID(), graphsync.RequestFailedUnknown)
			return
		}
	}
	selector, err := rm.ipldBridge.ParseSelector(selectorSpec)
	if err != nil {
		peerResponseSender.FinishWithError(request.ID(), graphsync.RequestFailedUnknown)
		return
	}
	rootLink := cidlink.Link{Cid: request.Root()}
	wrappedLoader := loader.WrapLoader(rm.loader, request.ID(), peerResponseSender)
	err = rm.ipldBridge.Traverse(ctx, wrappedLoader, rootLink, selector, noopVisitor)
	if err != nil {
		peerResponseSender.FinishWithError(request.ID(), graphsync.RequestFailedUnknown)
		return
	}
	peerResponseSender.FinishRequest(request.ID())
}

// Startup starts processing for the WantManager.
func (rm *ResponseManager) Startup() {
	go rm.run()
}

// Shutdown ends processing for the want manager.
func (rm *ResponseManager) Shutdown() {
	rm.cancelFn()
}

func (rm *ResponseManager) cleanupInProcessResponses() {
	for _, response := range rm.inProgressResponses {
		response.cancelFn()
	}
}

func (rm *ResponseManager) run() {
	defer rm.cleanupInProcessResponses()
	for i := 0; i < maxInProcessRequests; i++ {
		go rm.processQueriesWorker()
	}

	for {
		select {
		case <-rm.ctx.Done():
			return
		case message := <-rm.messages:
			message.handle(rm)
		}
	}
}

func (prm *processRequestMessage) handle(rm *ResponseManager) {
	for _, request := range prm.requests {
		key := responseKey{p: prm.p, requestID: request.ID()}
		if !request.IsCancel() {
			ctx, cancelFn := context.WithCancel(rm.ctx)
			rm.inProgressResponses[key] =
				inProgressResponseStatus{
					ctx:      ctx,
					cancelFn: cancelFn,
					request:  request,
				}
			// TODO: Use a better work estimation metric.
			rm.queryQueue.PushTasks(prm.p, peertask.Task{Topic: key, Priority: int(request.Priority()), Work: 1})
			select {
			case rm.workSignal <- struct{}{}:
			default:
			}
		} else {
			rm.queryQueue.Remove(key, key.p)
			response, ok := rm.inProgressResponses[key]
			if ok {
				response.cancelFn()
			}
		}
	}
}

func (rh *requestHook) handle(rm *ResponseManager) {
	rm.requestHooks = append(rm.requestHooks, *rh)
}

func (rdr *responseDataRequest) handle(rm *ResponseManager) {
	response, ok := rm.inProgressResponses[rdr.key]
	var taskData *responseTaskData
	if ok {
		taskData = &responseTaskData{response.ctx, response.request}
	} else {
		taskData = nil
	}
	select {
	case <-rm.ctx.Done():
	case rdr.taskDataChan <- taskData:
	}
}

func (frr *finishResponseRequest) handle(rm *ResponseManager) {
	response, ok := rm.inProgressResponses[frr.key]
	if !ok {
		return
	}
	delete(rm.inProgressResponses, frr.key)
	response.cancelFn()
}

func (sm *synchronizeMessage) handle(rm *ResponseManager) {
	select {
	case <-rm.ctx.Done():
	case sm.sync <- struct{}{}:
	}
}
