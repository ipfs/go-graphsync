package responsemanager

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/ipfs/go-graphsync/responsemanager/hooks"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-peertaskqueue/peertask"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("graphsync")

const (
	maxInProcessRequests = 6
	thawSpeed            = time.Millisecond * 100
)

type inProgressResponseStatus struct {
	ctx       context.Context
	cancelFn  func()
	request   gsmsg.GraphSyncRequest
	loader    ipld.Loader
	traverser ipldutil.Traverser
	signals   signals
	updates   []gsmsg.GraphSyncRequest
	isPaused  bool
}

type responseKey struct {
	p         peer.ID
	requestID graphsync.RequestID
}

type signals struct {
	pauseSignal  chan struct{}
	updateSignal chan struct{}
	stopSignal   chan bool
}

type responseTaskData struct {
	empty     bool
	ctx       context.Context
	request   gsmsg.GraphSyncRequest
	loader    ipld.Loader
	traverser ipldutil.Traverser
	signals   signals
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

// RequestHooks is an interface for processing request hooks
type RequestHooks interface {
	ProcessRequestHooks(p peer.ID, request graphsync.RequestData) hooks.RequestResult
}

// BlockHooks is an interface for processing block hooks
type BlockHooks interface {
	ProcessBlockHooks(p peer.ID, request graphsync.RequestData, blockData graphsync.BlockData) hooks.BlockResult
}

// UpdateHooks is an interface for processing update hooks
type UpdateHooks interface {
	ProcessUpdateHooks(p peer.ID, request graphsync.RequestData, update graphsync.RequestData) hooks.UpdateResult
}

// CompletedListeners is an interface for notifying listeners that responses are complete
type CompletedListeners interface {
	NotifyCompletedListeners(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode)
}

// CancelledListeners is an interface for notifying listeners that requestor cancelled
type CancelledListeners interface {
	NotifyCancelledListeners(p peer.ID, request graphsync.RequestData)
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
	ctx                 context.Context
	cancelFn            context.CancelFunc
	peerManager         PeerManager
	queryQueue          QueryQueue
	updateHooks         UpdateHooks
	cancelledListeners  CancelledListeners
	completedListeners  CompletedListeners
	messages            chan responseManagerMessage
	workSignal          chan struct{}
	qe                  *queryExecutor
	inProgressResponses map[responseKey]*inProgressResponseStatus
}

// New creates a new response manager from the given context, loader,
// bridge to IPLD interface, peerManager, and queryQueue.
func New(ctx context.Context,
	loader ipld.Loader,
	peerManager PeerManager,
	queryQueue QueryQueue,
	requestHooks RequestHooks,
	blockHooks BlockHooks,
	updateHooks UpdateHooks,
	completedListeners CompletedListeners,
	cancelledListeners CancelledListeners,
) *ResponseManager {
	ctx, cancelFn := context.WithCancel(ctx)
	messages := make(chan responseManagerMessage, 16)
	workSignal := make(chan struct{}, 1)
	qe := &queryExecutor{
		requestHooks:       requestHooks,
		blockHooks:         blockHooks,
		updateHooks:        updateHooks,
		completedListeners: completedListeners,
		cancelledListeners: cancelledListeners,
		peerManager:        peerManager,
		loader:             loader,
		queryQueue:         queryQueue,
		messages:           messages,
		ctx:                ctx,
		workSignal:         workSignal,
		ticker:             time.NewTicker(thawSpeed),
	}
	return &ResponseManager{
		ctx:                 ctx,
		cancelFn:            cancelFn,
		peerManager:         peerManager,
		queryQueue:          queryQueue,
		updateHooks:         updateHooks,
		completedListeners:  completedListeners,
		cancelledListeners:  cancelledListeners,
		messages:            messages,
		workSignal:          workSignal,
		qe:                  qe,
		inProgressResponses: make(map[responseKey]*inProgressResponseStatus),
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

type unpauseRequestMessage struct {
	p          peer.ID
	requestID  graphsync.RequestID
	response   chan error
	extensions []graphsync.ExtensionData
}

// UnpauseResponse unpauses a response that was previously paused
func (rm *ResponseManager) UnpauseResponse(p peer.ID, requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	response := make(chan error, 1)
	return rm.sendSyncMessage(&unpauseRequestMessage{p, requestID, response, extensions}, response)
}

type pauseRequestMessage struct {
	p         peer.ID
	requestID graphsync.RequestID
	response  chan error
}

// PauseResponse pauses an in progress response (may take 1 or more blocks to process)
func (rm *ResponseManager) PauseResponse(p peer.ID, requestID graphsync.RequestID) error {
	response := make(chan error, 1)
	return rm.sendSyncMessage(&pauseRequestMessage{p, requestID, response}, response)
}

type cancelRequestMessage struct {
	p         peer.ID
	requestID graphsync.RequestID
	response  chan error
}

// CancelResponse cancels an in progress response
func (rm *ResponseManager) CancelResponse(p peer.ID, requestID graphsync.RequestID) error {
	response := make(chan error, 1)
	return rm.sendSyncMessage(&cancelRequestMessage{p, requestID, response}, response)
}

func (rm *ResponseManager) sendSyncMessage(message responseManagerMessage, response chan error) error {
	select {
	case <-rm.ctx.Done():
		return errors.New("Context Cancelled")
	case rm.messages <- message:
	}
	select {
	case <-rm.ctx.Done():
		return errors.New("Context Cancelled")
	case err := <-response:
		return err
	}
}

type synchronizeMessage struct {
	sync chan error
}

// this is a test utility method to force all messages to get processed
func (rm *ResponseManager) synchronize() {
	sync := make(chan error)
	_ = rm.sendSyncMessage(&synchronizeMessage{sync}, sync)
}

type responseDataRequest struct {
	key          responseKey
	taskDataChan chan responseTaskData
}

type finishTaskRequest struct {
	key    responseKey
	status graphsync.ResponseStatusCode
	err    error
}

type setResponseDataRequest struct {
	key       responseKey
	loader    ipld.Loader
	traverser ipldutil.Traverser
}

type responseUpdateRequest struct {
	key        responseKey
	updateChan chan []gsmsg.GraphSyncRequest
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
		go rm.qe.processQueriesWorker()
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

func (rm *ResponseManager) processUpdate(key responseKey, update gsmsg.GraphSyncRequest) {
	response, ok := rm.inProgressResponses[key]
	if !ok {
		log.Warnf("received update for non existent request, peer %s, request ID %d", key.p.Pretty(), key.requestID)
		return
	}
	if !response.isPaused {
		response.updates = append(response.updates, update)
		select {
		case response.signals.updateSignal <- struct{}{}:
		default:
		}
		return
	}
	result := rm.updateHooks.ProcessUpdateHooks(key.p, response.request, update)
	peerResponseSender := rm.peerManager.SenderForPeer(key.p)
	err := peerResponseSender.Transaction(key.requestID, func(transaction peerresponsemanager.PeerResponseTransactionSender) error {
		for _, extension := range result.Extensions {
			transaction.SendExtensionData(extension)
		}
		if result.Err != nil {
			transaction.FinishWithError(graphsync.RequestFailedUnknown)
		}
		return nil
	})
	if err != nil {
		log.Errorf("Error processing update: %s", err)
	}
	if result.Err != nil {
		delete(rm.inProgressResponses, key)
		response.cancelFn()
		return
	}
	if result.Unpause {
		err := rm.unpauseRequest(key.p, key.requestID)
		if err != nil {
			log.Warnf("error unpausing request: %s", err.Error())
		}
	}

}

func (rm *ResponseManager) unpauseRequest(p peer.ID, requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	key := responseKey{p, requestID}
	inProgressResponse, ok := rm.inProgressResponses[key]
	if !ok {
		return errors.New("could not find request")
	}
	if !inProgressResponse.isPaused {
		return errors.New("request is not paused")
	}
	inProgressResponse.isPaused = false
	if len(extensions) > 0 {
		peerResponseSender := rm.peerManager.SenderForPeer(key.p)
		_ = peerResponseSender.Transaction(requestID, func(transaction peerresponsemanager.PeerResponseTransactionSender) error {
			for _, extension := range extensions {
				transaction.SendExtensionData(extension)
			}
			return nil
		})
	}
	rm.queryQueue.PushTasks(p, peertask.Task{Topic: key, Priority: math.MaxInt32, Work: 1})
	select {
	case rm.workSignal <- struct{}{}:
	default:
	}
	return nil
}

func (rm *ResponseManager) cancelRequest(p peer.ID, requestID graphsync.RequestID, selfCancel bool) error {
	key := responseKey{p, requestID}
	rm.queryQueue.Remove(key, key.p)
	response, ok := rm.inProgressResponses[key]
	if !ok {
		return errors.New("could not find request")
	}

	if response.isPaused {
		peerResponseSender := rm.peerManager.SenderForPeer(key.p)
		if selfCancel {
			rm.completedListeners.NotifyCompletedListeners(p, response.request, graphsync.RequestCancelled)
			peerResponseSender.FinishWithError(requestID, graphsync.RequestCancelled)
		} else {
			rm.cancelledListeners.NotifyCancelledListeners(p, response.request)
			peerResponseSender.FinishWithCancel(requestID)
		}
		delete(rm.inProgressResponses, key)
		response.cancelFn()
		return nil
	}
	select {
	case response.signals.stopSignal <- selfCancel:
	default:
	}
	return nil
}

func (prm *processRequestMessage) handle(rm *ResponseManager) {
	for _, request := range prm.requests {
		key := responseKey{p: prm.p, requestID: request.ID()}
		if request.IsCancel() {
			_ = rm.cancelRequest(prm.p, request.ID(), false)
			continue
		}
		if request.IsUpdate() {
			rm.processUpdate(key, request)
			continue
		}
		ctx, cancelFn := context.WithCancel(rm.ctx)
		rm.inProgressResponses[key] =
			&inProgressResponseStatus{
				ctx:      ctx,
				cancelFn: cancelFn,
				request:  request,
				signals: signals{
					pauseSignal:  make(chan struct{}, 1),
					updateSignal: make(chan struct{}, 1),
					stopSignal:   make(chan bool, 1),
				},
			}
		// TODO: Use a better work estimation metric.
		rm.queryQueue.PushTasks(prm.p, peertask.Task{Topic: key, Priority: int(request.Priority()), Work: 1})
		select {
		case rm.workSignal <- struct{}{}:
		default:
		}
	}
}

func (rdr *responseDataRequest) handle(rm *ResponseManager) {
	response, ok := rm.inProgressResponses[rdr.key]
	var taskData responseTaskData
	if ok {
		taskData = responseTaskData{false, response.ctx, response.request, response.loader, response.traverser, response.signals}
	} else {
		taskData = responseTaskData{empty: true}
	}
	select {
	case <-rm.ctx.Done():
	case rdr.taskDataChan <- taskData:
	}
}

func (ftr *finishTaskRequest) handle(rm *ResponseManager) {
	response, ok := rm.inProgressResponses[ftr.key]
	if !ok {
		return
	}
	if _, ok := ftr.err.(hooks.ErrPaused); ok {
		response.isPaused = true
		return
	}
	if ftr.err != nil {
		log.Infof("response failed: %w", ftr.err)
	}
	delete(rm.inProgressResponses, ftr.key)
	response.cancelFn()
}

func (srdr *setResponseDataRequest) handle(rm *ResponseManager) {
	response, ok := rm.inProgressResponses[srdr.key]
	if !ok {
		return
	}
	response.loader = srdr.loader
	response.traverser = srdr.traverser
}

func (rur *responseUpdateRequest) handle(rm *ResponseManager) {
	response, ok := rm.inProgressResponses[rur.key]
	var updates []gsmsg.GraphSyncRequest
	if ok {
		updates = response.updates
		response.updates = nil
	} else {
		updates = nil
	}
	select {
	case <-rm.ctx.Done():
	case rur.updateChan <- updates:
	}
}

func (sm *synchronizeMessage) handle(rm *ResponseManager) {
	select {
	case <-rm.ctx.Done():
	case sm.sync <- nil:
	}
}

func (urm *unpauseRequestMessage) handle(rm *ResponseManager) {
	err := rm.unpauseRequest(urm.p, urm.requestID, urm.extensions...)
	select {
	case <-rm.ctx.Done():
	case urm.response <- err:
	}
}

func (prm *pauseRequestMessage) pauseRequest(rm *ResponseManager) error {
	key := responseKey{prm.p, prm.requestID}
	inProgressResponse, ok := rm.inProgressResponses[key]
	if !ok {
		return errors.New("could not find request")
	}
	if inProgressResponse.isPaused {
		return errors.New("request is already paused")
	}
	select {
	case inProgressResponse.signals.pauseSignal <- struct{}{}:
	default:
	}
	return nil
}

func (prm *pauseRequestMessage) handle(rm *ResponseManager) {
	err := prm.pauseRequest(rm)
	select {
	case <-rm.ctx.Done():
	case prm.response <- err:
	}
}

func (crm *cancelRequestMessage) handle(rm *ResponseManager) {
	err := rm.cancelRequest(crm.p, crm.requestID, true)
	select {
	case <-rm.ctx.Done():
	case crm.response <- err:
	}
}
