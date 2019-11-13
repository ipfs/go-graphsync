package responsemanager

import (
	"context"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/loader"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	"github.com/ipfs/go-peertaskqueue/peertask"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	maxInProcessRequests = 6
	thawSpeed            = time.Millisecond * 100
)

type inProgressResponseStatus struct {
	ctx      context.Context
	cancelFn func()
	root     cid.Cid
	selector []byte
}

type responseKey struct {
	p         peer.ID
	requestID graphsync.RequestID
}

type responseTaskData struct {
	ctx      context.Context
	root     cid.Cid
	selector []byte
}

// QueryQueue is an interface that can receive new selector query tasks
// and prioritize them as needed, and pop them off later
type QueryQueue interface {
	PushBlock(to peer.ID, tasks ...peertask.Task)
	PopBlock() *peertask.TaskBlock
	Remove(identifier peertask.Identifier, p peer.ID)
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
	taskDataChan := make(chan *responseTaskData)
	var taskData *responseTaskData
	for {
		nextTaskBlock := rm.queryQueue.PopBlock()
		for nextTaskBlock == nil {
			select {
			case <-rm.ctx.Done():
				return
			case <-rm.workSignal:
				nextTaskBlock = rm.queryQueue.PopBlock()
			case <-rm.ticker.C:
				rm.queryQueue.ThawRound()
				nextTaskBlock = rm.queryQueue.PopBlock()
			}
		}
		for _, task := range nextTaskBlock.Tasks {
			key := task.Identifier.(responseKey)
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
			rm.executeQuery(taskData.ctx, key.p, key.requestID, taskData.root, taskData.selector)
			select {
			case rm.messages <- &finishResponseRequest{key}:
			case <-rm.ctx.Done():
			}
		}
		nextTaskBlock.Done(nextTaskBlock.Tasks)

	}

}

func noopVisitor(tp ipldbridge.TraversalProgress, n ipld.Node, tr ipldbridge.TraversalReason) error {
	return nil
}

func (rm *ResponseManager) executeQuery(ctx context.Context,
	p peer.ID,
	requestID graphsync.RequestID,
	root cid.Cid,
	selectorBytes []byte) {
	peerResponseSender := rm.peerManager.SenderForPeer(p)
	selectorSpec, err := rm.ipldBridge.DecodeNode(selectorBytes)
	if err != nil {
		peerResponseSender.FinishWithError(requestID, graphsync.RequestFailedUnknown)
		return
	}
	rootLink := cidlink.Link{Cid: root}
	selector, err := rm.ipldBridge.ParseSelector(selectorSpec)
	if err != nil {
		peerResponseSender.FinishWithError(requestID, graphsync.RequestFailedUnknown)
		return
	}
	wrappedLoader := loader.WrapLoader(rm.loader, requestID, peerResponseSender)
	err = rm.ipldBridge.Traverse(ctx, wrappedLoader, rootLink, selector, noopVisitor)
	if err != nil {
		peerResponseSender.FinishWithError(requestID, graphsync.RequestFailedUnknown)
		return
	}
	peerResponseSender.FinishRequest(requestID)
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
					root:     request.Root(),
					selector: request.Selector(),
				}
			rm.queryQueue.PushBlock(prm.p, peertask.Task{Identifier: key, Priority: int(request.Priority())})
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

func (rdr *responseDataRequest) handle(rm *ResponseManager) {
	response, ok := rm.inProgressResponses[rdr.key]
	var taskData *responseTaskData
	if ok {
		taskData = &responseTaskData{response.ctx, response.root, response.selector}
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
