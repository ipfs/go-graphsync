package asyncloader

import (
	"context"
	"errors"
	"sync"

	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipld/go-ipld-prime"
)

type loadRequest struct {
	requestID  gsmsg.GraphSyncRequestID
	link       ipld.Link
	resultChan chan AsyncLoadResult
}

var loadRequestPool = sync.Pool{
	New: func() interface{} {
		return new(loadRequest)
	},
}

func newLoadRequest(requestID gsmsg.GraphSyncRequestID,
	link ipld.Link,
	resultChan chan AsyncLoadResult) *loadRequest {
	lr := loadRequestPool.Get().(*loadRequest)
	lr.requestID = requestID
	lr.link = link
	lr.resultChan = resultChan
	return lr
}

func returnLoadRequest(lr *loadRequest) {
	*lr = loadRequest{}
	loadRequestPool.Put(lr)
}

type loaderMessage interface {
	handle(abl *AsyncLoader)
}

type newResponsesAvailableMessage struct{}

type startRequestMessage struct {
	requestID gsmsg.GraphSyncRequestID
}

type finishRequestMessage struct {
	requestID gsmsg.GraphSyncRequestID
}

// LoadAttempter attempts to load a link to an array of bytes
// it has three results:
// bytes present, error nil = success
// bytes nil, error present = error
// bytes nil, error nil = did not load, but try again later
type LoadAttempter func(gsmsg.GraphSyncRequestID, ipld.Link) ([]byte, error)

// AsyncLoadResult is sent once over the channel returned by an async load.
type AsyncLoadResult struct {
	Data []byte
	Err  error
}

// AsyncLoader is used to make multiple attempts to load a blocks over the
// course of a request - as long as a request is in progress, it will make multiple
// attempts to load a block until it gets a definitive result of whether the block
// is present or missing in the response
type AsyncLoader struct {
	ctx              context.Context
	cancel           context.CancelFunc
	loadAttempter    LoadAttempter
	incomingMessages chan loaderMessage
	outgoingMessages chan loaderMessage
	activeRequests   map[gsmsg.GraphSyncRequestID]struct{}
	pausedRequests   []*loadRequest
}

// New initializes a new AsyncLoader from the given context and loadAttempter function
func New(ctx context.Context, loadAttempter LoadAttempter) *AsyncLoader {
	ctx, cancel := context.WithCancel(ctx)
	return &AsyncLoader{
		ctx:              ctx,
		cancel:           cancel,
		loadAttempter:    loadAttempter,
		incomingMessages: make(chan loaderMessage),
		outgoingMessages: make(chan loaderMessage),
		activeRequests:   make(map[gsmsg.GraphSyncRequestID]struct{}),
	}
}

// AsyncLoad asynchronously loads the given link for the given request ID. It returns a channel for data and a channel
// for errors -- only one message will be sent over either.
func (abl *AsyncLoader) AsyncLoad(requestID gsmsg.GraphSyncRequestID, link ipld.Link) <-chan AsyncLoadResult {
	resultChan := make(chan AsyncLoadResult, 1)
	lr := newLoadRequest(requestID, link, resultChan)
	select {
	case <-abl.ctx.Done():
		abl.terminateWithError("Context Closed", resultChan)
	case abl.incomingMessages <- lr:
	}
	return resultChan
}

// NewResponsesAvailable indicates that the async loader should make another attempt to load
// the links that are currently pending.
func (abl *AsyncLoader) NewResponsesAvailable() {
	select {
	case <-abl.ctx.Done():
	case abl.incomingMessages <- &newResponsesAvailableMessage{}:
	}
}

// StartRequest indicates the given request has started and the loader should
// accepting link load requests for this requestID.
func (abl *AsyncLoader) StartRequest(requestID gsmsg.GraphSyncRequestID) {
	select {
	case <-abl.ctx.Done():
	case abl.incomingMessages <- &startRequestMessage{requestID}:
	}
}

// FinishRequest indicates the given request is completed or cancelled, and all in
// progress link load requests for this request ID should error
func (abl *AsyncLoader) FinishRequest(requestID gsmsg.GraphSyncRequestID) {
	select {
	case <-abl.ctx.Done():
	case abl.incomingMessages <- &finishRequestMessage{requestID}:
	}
}

// Startup starts processing of messages
func (abl *AsyncLoader) Startup() {
	go abl.messageQueueWorker()
	go abl.run()
}

// Shutdown stops processing of messages
func (abl *AsyncLoader) Shutdown() {
	abl.cancel()
}

func (abl *AsyncLoader) run() {
	for {
		select {
		case <-abl.ctx.Done():
			return
		case message := <-abl.outgoingMessages:
			message.handle(abl)
		}
	}
}

func (abl *AsyncLoader) messageQueueWorker() {
	var messageBuffer []loaderMessage
	nextMessage := func() loaderMessage {
		if len(messageBuffer) == 0 {
			return nil
		}
		return messageBuffer[0]
	}
	outgoingMessages := func() chan<- loaderMessage {
		if len(messageBuffer) == 0 {
			return nil
		}
		return abl.outgoingMessages
	}
	for {
		select {
		case incomingMessage := <-abl.incomingMessages:
			messageBuffer = append(messageBuffer, incomingMessage)
		case outgoingMessages() <- nextMessage():
			messageBuffer = messageBuffer[1:]
		case <-abl.ctx.Done():
			return
		}
	}
}

func (lr *loadRequest) handle(abl *AsyncLoader) {
	_, ok := abl.activeRequests[lr.requestID]
	if !ok {
		abl.terminateWithError("No active request", lr.resultChan)
		returnLoadRequest(lr)
		return
	}
	response, err := abl.loadAttempter(lr.requestID, lr.link)
	if err != nil {
		lr.resultChan <- AsyncLoadResult{nil, err}
		close(lr.resultChan)
		returnLoadRequest(lr)
		return
	}
	if response != nil {
		lr.resultChan <- AsyncLoadResult{response, nil}
		close(lr.resultChan)
		returnLoadRequest(lr)
		return
	}
	abl.pausedRequests = append(abl.pausedRequests, lr)
}

func (srm *startRequestMessage) handle(abl *AsyncLoader) {
	abl.activeRequests[srm.requestID] = struct{}{}
}

func (frm *finishRequestMessage) handle(abl *AsyncLoader) {
	delete(abl.activeRequests, frm.requestID)
	pausedRequests := abl.pausedRequests
	abl.pausedRequests = nil
	for _, lr := range pausedRequests {
		if lr.requestID == frm.requestID {
			abl.terminateWithError("No active request", lr.resultChan)
			returnLoadRequest(lr)
		} else {
			abl.pausedRequests = append(abl.pausedRequests, lr)
		}
	}
}

func (nram *newResponsesAvailableMessage) handle(abl *AsyncLoader) {
	// drain buffered
	pausedRequests := abl.pausedRequests
	abl.pausedRequests = nil
	for _, lr := range pausedRequests {
		select {
		case <-abl.ctx.Done():
			return
		case abl.incomingMessages <- lr:
		}
	}
}

func (abl *AsyncLoader) terminateWithError(errMsg string, resultChan chan<- AsyncLoadResult) {
	resultChan <- AsyncLoadResult{nil, errors.New(errMsg)}
	close(resultChan)
}
