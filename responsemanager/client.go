package responsemanager

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
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
)

var log = logging.Logger("graphsync")

const (
	thawSpeed = time.Millisecond * 100
)

type inProgressResponseStatus struct {
	ctx        context.Context
	cancelFn   func()
	request    gsmsg.GraphSyncRequest
	loader     ipld.BlockReadOpener
	traverser  ipldutil.Traverser
	signals    ResponseSignals
	updates    []gsmsg.GraphSyncRequest
	isPaused   bool
	subscriber *notifications.TopicDataSubscriber
}

type responseKey struct {
	p         peer.ID
	requestID graphsync.RequestID
}

// ResponseSignals are message channels to communicate between the manager and the query
type ResponseSignals struct {
	PauseSignal  chan struct{}
	UpdateSignal chan struct{}
	ErrSignal    chan error
}

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

// RequestQueuedHooks is an interface for processing request queued hooks
type RequestQueuedHooks interface {
	ProcessRequestQueuedHooks(p peer.ID, request graphsync.RequestData)
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

// BlockSentListeners is an interface for notifying listeners that of a block send occuring over the wire
type BlockSentListeners interface {
	NotifyBlockSentListeners(p peer.ID, request graphsync.RequestData, block graphsync.BlockData)
}

// NetworkErrorListeners is an interface for notifying listeners that an error occurred sending a data on the wire
type NetworkErrorListeners interface {
	NotifyNetworkErrorListeners(p peer.ID, request graphsync.RequestData, err error)
}

// ResponseAssembler is an interface that returns sender interfaces for peer responses.
type ResponseAssembler interface {
	DedupKey(p peer.ID, requestID graphsync.RequestID, key string)
	IgnoreBlocks(p peer.ID, requestID graphsync.RequestID, links []ipld.Link)
	Transaction(p peer.ID, requestID graphsync.RequestID, transaction responseassembler.Transaction) error
}

type responseManagerMessage interface {
	handle(rm *ResponseManager)
}

// ResponseManager handles incoming requests from the network, initiates selector
// traversals, and transmits responses
type ResponseManager struct {
	ctx                   context.Context
	cancelFn              context.CancelFunc
	responseAssembler     ResponseAssembler
	queryQueue            QueryQueue
	requestHooks          RequestHooks
	linkSystem            ipld.LinkSystem
	requestQueuedHooks    RequestQueuedHooks
	updateHooks           UpdateHooks
	cancelledListeners    CancelledListeners
	completedListeners    CompletedListeners
	blockSentListeners    BlockSentListeners
	networkErrorListeners NetworkErrorListeners
	messages              chan responseManagerMessage
	workSignal            chan struct{}
	qe                    *queryExecutor
	inProgressResponses   map[responseKey]*inProgressResponseStatus
	maxInProcessRequests  uint64
}

// New creates a new response manager for responding to requests
func New(ctx context.Context,
	linkSystem ipld.LinkSystem,
	responseAssembler ResponseAssembler,
	queryQueue QueryQueue,
	requestQueuedHooks RequestQueuedHooks,
	requestHooks RequestHooks,
	blockHooks BlockHooks,
	updateHooks UpdateHooks,
	completedListeners CompletedListeners,
	cancelledListeners CancelledListeners,
	blockSentListeners BlockSentListeners,
	networkErrorListeners NetworkErrorListeners,
	maxInProcessRequests uint64,
) *ResponseManager {
	ctx, cancelFn := context.WithCancel(ctx)
	messages := make(chan responseManagerMessage, 16)
	workSignal := make(chan struct{}, 1)
	rm := &ResponseManager{
		ctx:                   ctx,
		cancelFn:              cancelFn,
		requestHooks:          requestHooks,
		linkSystem:            linkSystem,
		responseAssembler:     responseAssembler,
		queryQueue:            queryQueue,
		requestQueuedHooks:    requestQueuedHooks,
		updateHooks:           updateHooks,
		cancelledListeners:    cancelledListeners,
		completedListeners:    completedListeners,
		blockSentListeners:    blockSentListeners,
		networkErrorListeners: networkErrorListeners,
		messages:              messages,
		workSignal:            workSignal,
		inProgressResponses:   make(map[responseKey]*inProgressResponseStatus),
		maxInProcessRequests:  maxInProcessRequests,
	}
	rm.qe = &queryExecutor{
		blockHooks:         blockHooks,
		updateHooks:        updateHooks,
		cancelledListeners: cancelledListeners,
		responseAssembler:  responseAssembler,
		queryQueue:         queryQueue,
		manager:            rm,
		ctx:                ctx,
		workSignal:         workSignal,
		ticker:             time.NewTicker(thawSpeed),
	}
	return rm
}

// ProcessRequests processes incoming requests for the given peer
func (rm *ResponseManager) ProcessRequests(p peer.ID, requests []gsmsg.GraphSyncRequest) {
	rm.cast(&processRequestMessage{p, requests})
}

// UnpauseResponse unpauses a response that was previously paused
func (rm *ResponseManager) UnpauseResponse(p peer.ID, requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	response := make(chan error, 1)
	return rm.call(&unpauseRequestMessage{p, requestID, response, extensions}, response)
}

// PauseResponse pauses an in progress response (may take 1 or more blocks to process)
func (rm *ResponseManager) PauseResponse(p peer.ID, requestID graphsync.RequestID) error {
	response := make(chan error, 1)
	return rm.call(&pauseRequestMessage{p, requestID, response}, response)
}

// CancelResponse cancels an in progress response
func (rm *ResponseManager) CancelResponse(p peer.ID, requestID graphsync.RequestID) error {
	response := make(chan error, 1)
	return rm.call(&errorRequestMessage{p, requestID, errCancelledByCommand, response}, response)
}

// this is a test utility method to force all messages to get processed
func (rm *ResponseManager) synchronize() {
	sync := make(chan error)
	_ = rm.call(&synchronizeMessage{sync}, sync)
}

// StartTask starts the given task from the peer task queue
func (rm *ResponseManager) StartTask(task *peertask.Task, responseTaskDataChan chan<- ResponseTaskData) {
	rm.cast(&startTaskRequest{task, responseTaskDataChan})
}

// GetUpdates is called to read pending updates for a task and clear them
func (rm *ResponseManager) GetUpdates(p peer.ID, requestID graphsync.RequestID, updatesChan chan<- []gsmsg.GraphSyncRequest) {
	rm.cast(&responseUpdateRequest{responseKey{p, requestID}, updatesChan})
}

// FinishTask marks a task from the task queue as done
func (rm *ResponseManager) FinishTask(task *peertask.Task, err error) {
	rm.cast(&finishTaskRequest{task, err})
}

func (rm *ResponseManager) call(message responseManagerMessage, response chan error) error {
	select {
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case rm.messages <- message:
	}
	select {
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case err := <-response:
		return err
	}
}

func (rm *ResponseManager) cast(message responseManagerMessage) {
	select {
	case <-rm.ctx.Done():
	case rm.messages <- message:
	}
}

// Startup starts processing for the WantManager.
func (rm *ResponseManager) Startup() {
	go rm.run()
}

// Shutdown ends processing for the want manager.
func (rm *ResponseManager) Shutdown() {
	rm.cancelFn()
}
