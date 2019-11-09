package asyncloader

import (
	"context"
	"errors"
	"io/ioutil"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-graphsync"

	"github.com/ipfs/go-graphsync/ipldbridge"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/requestmanager/asyncloader/loadattemptqueue"
	"github.com/ipfs/go-graphsync/requestmanager/asyncloader/responsecache"
	"github.com/ipfs/go-graphsync/requestmanager/asyncloader/unverifiedblockstore"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipld/go-ipld-prime"
)

type loaderMessage interface {
	handle(al *AsyncLoader)
}

// AsyncLoader manages loading links asynchronously in as new responses
// come in from the network
type AsyncLoader struct {
	ctx              context.Context
	cancel           context.CancelFunc
	incomingMessages chan loaderMessage
	outgoingMessages chan loaderMessage

	activeRequests   map[graphsync.RequestID]bool
	loadAttemptQueue *loadattemptqueue.LoadAttemptQueue
	responseCache    *responsecache.ResponseCache
}

// New initializes a new link loading manager for asynchronous loads from the given context
// and local store loading and storing function
func New(ctx context.Context, loader ipld.Loader, storer ipld.Storer) *AsyncLoader {
	unverifiedBlockStore := unverifiedblockstore.New(storer)
	responseCache := responsecache.New(unverifiedBlockStore)
	loadAttemptQueue := loadattemptqueue.New(func(requestID graphsync.RequestID, link ipld.Link) ([]byte, error) {
		// load from response cache
		data, err := responseCache.AttemptLoad(requestID, link)
		if data == nil && err == nil {
			// fall back to local store
			stream, loadErr := loader(link, ipldbridge.LinkContext{})
			if stream != nil && loadErr == nil {
				localData, loadErr := ioutil.ReadAll(stream)
				if loadErr == nil && localData != nil {
					return localData, nil
				}
			}
		}
		return data, err
	})
	ctx, cancel := context.WithCancel(ctx)
	return &AsyncLoader{
		ctx:              ctx,
		cancel:           cancel,
		incomingMessages: make(chan loaderMessage),
		outgoingMessages: make(chan loaderMessage),
		activeRequests:   make(map[graphsync.RequestID]bool),
		responseCache:    responseCache,
		loadAttemptQueue: loadAttemptQueue,
	}
}

// Startup starts processing of messages
func (al *AsyncLoader) Startup() {
	go al.messageQueueWorker()
	go al.run()
}

// Shutdown finishes processing of messages
func (al *AsyncLoader) Shutdown() {
	al.cancel()
}

// StartRequest indicates the given request has started and the manager should
// continually attempt to load links for this request as new responses come in
func (al *AsyncLoader) StartRequest(requestID graphsync.RequestID) {
	select {
	case <-al.ctx.Done():
	case al.incomingMessages <- &startRequestMessage{requestID}:
	}
}

// ProcessResponse injests new responses and completes asynchronous loads as
// neccesary
func (al *AsyncLoader) ProcessResponse(responses map[graphsync.RequestID]metadata.Metadata,
	blks []blocks.Block) {
	al.responseCache.ProcessResponse(responses, blks)
	select {
	case <-al.ctx.Done():
	case al.incomingMessages <- &newResponsesAvailableMessage{}:
	}
}

// AsyncLoad asynchronously loads the given link for the given request ID. It returns a channel for data and a channel
// for errors -- only one message will be sent over either.
func (al *AsyncLoader) AsyncLoad(requestID graphsync.RequestID, link ipld.Link) <-chan types.AsyncLoadResult {
	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := loadattemptqueue.NewLoadRequest(requestID, link, resultChan)
	select {
	case <-al.ctx.Done():
		resultChan <- types.AsyncLoadResult{Data: nil, Err: errors.New("Context closed")}
		close(resultChan)
	case al.incomingMessages <- &loadRequestMessage{requestID, lr}:
	}
	return resultChan
}

// CompleteResponsesFor indicates no further responses will come in for the given
// requestID, so if no responses are in the cache or local store, a link load
// should not retry
func (al *AsyncLoader) CompleteResponsesFor(requestID graphsync.RequestID) {
	select {
	case <-al.ctx.Done():
	case al.incomingMessages <- &finishRequestMessage{requestID}:
	}
}

// CleanupRequest indicates the given request is complete on the client side,
// and no further attempts will be made to load links for this request,
// so any cached response data is invalid can be cleaned
func (al *AsyncLoader) CleanupRequest(requestID graphsync.RequestID) {
	al.responseCache.FinishRequest(requestID)
}

type loadRequestMessage struct {
	requestID   graphsync.RequestID
	loadRequest loadattemptqueue.LoadRequest
}

type newResponsesAvailableMessage struct {
}

type startRequestMessage struct {
	requestID graphsync.RequestID
}

type finishRequestMessage struct {
	requestID graphsync.RequestID
}

func (al *AsyncLoader) run() {
	for {
		select {
		case <-al.ctx.Done():
			return
		case message := <-al.outgoingMessages:
			message.handle(al)
		}
	}
}

func (al *AsyncLoader) messageQueueWorker() {
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
		return al.outgoingMessages
	}
	for {
		select {
		case incomingMessage := <-al.incomingMessages:
			messageBuffer = append(messageBuffer, incomingMessage)
		case outgoingMessages() <- nextMessage():
			messageBuffer = messageBuffer[1:]
		case <-al.ctx.Done():
			return
		}
	}
}

func (lrm *loadRequestMessage) handle(al *AsyncLoader) {
	retry := al.activeRequests[lrm.requestID]
	al.loadAttemptQueue.AttemptLoad(lrm.loadRequest, retry)
}

func (srm *startRequestMessage) handle(al *AsyncLoader) {
	al.activeRequests[srm.requestID] = true
}

func (frm *finishRequestMessage) handle(al *AsyncLoader) {
	delete(al.activeRequests, frm.requestID)
	al.loadAttemptQueue.ClearRequest(frm.requestID)
}

func (nram *newResponsesAvailableMessage) handle(al *AsyncLoader) {
	al.loadAttemptQueue.RetryLoads()
}
