package asyncloader

import (
	"context"
	"errors"
	"io/ioutil"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-ipld-prime"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/requestmanager/asyncloader/loadattemptqueue"
	"github.com/ipfs/go-graphsync/requestmanager/asyncloader/responsecache"
	"github.com/ipfs/go-graphsync/requestmanager/asyncloader/unverifiedblockstore"
	"github.com/ipfs/go-graphsync/requestmanager/types"
)

type loaderMessage interface {
	handle(al *AsyncLoader)
}

type alternateQueue struct {
	responseCache    *responsecache.ResponseCache
	loadAttemptQueue *loadattemptqueue.LoadAttemptQueue
}

// AsyncLoader manages loading links asynchronously in as new responses
// come in from the network
type AsyncLoader struct {
	ctx              context.Context
	cancel           context.CancelFunc
	incomingMessages chan loaderMessage
	outgoingMessages chan loaderMessage

	defaultLoader    ipld.Loader
	defaultStorer    ipld.Storer
	activeRequests   map[graphsync.RequestID]struct{}
	requestQueues    map[graphsync.RequestID]string
	alternateQueues  map[string]alternateQueue
	responseCache    *responsecache.ResponseCache
	loadAttemptQueue *loadattemptqueue.LoadAttemptQueue
}

// New initializes a new link loading manager for asynchronous loads from the given context
// and local store loading and storing function
func New(ctx context.Context, loader ipld.Loader, storer ipld.Storer) *AsyncLoader {
	responseCache, loadAttemptQueue := setupAttemptQueue(loader, storer)
	ctx, cancel := context.WithCancel(ctx)
	return &AsyncLoader{
		ctx:              ctx,
		cancel:           cancel,
		incomingMessages: make(chan loaderMessage),
		outgoingMessages: make(chan loaderMessage),
		defaultLoader:    loader,
		defaultStorer:    storer,
		activeRequests:   make(map[graphsync.RequestID]struct{}),
		requestQueues:    make(map[graphsync.RequestID]string),
		alternateQueues:  make(map[string]alternateQueue),
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

// RegisterPersistenceOption registers a new loader/storer option for processing requests
func (al *AsyncLoader) RegisterPersistenceOption(name string, loader ipld.Loader, storer ipld.Storer) error {
	if name == "" {
		return errors.New("Persistence option must have a name")
	}
	response := make(chan error, 1)
	err := al.sendSyncMessage(&registerPersistenceOptionMessage{name, loader, storer, response}, response)
	return err
}

// UnregisterPersistenceOption unregisters an existing loader/storer option for processing requests
func (al *AsyncLoader) UnregisterPersistenceOption(name string) error {
	if name == "" {
		return errors.New("Persistence option must have a name")
	}
	response := make(chan error, 1)
	err := al.sendSyncMessage(&unregisterPersistenceOptionMessage{name, response}, response)
	return err
}

// StartRequest indicates the given request has started and the manager should
// continually attempt to load links for this request as new responses come in
func (al *AsyncLoader) StartRequest(requestID graphsync.RequestID, persistenceOption string) error {
	response := make(chan error, 1)
	err := al.sendSyncMessage(&startRequestMessage{requestID, persistenceOption, response}, response)
	return err
}

// ProcessResponse injests new responses and completes asynchronous loads as
// neccesary
func (al *AsyncLoader) ProcessResponse(responses map[graphsync.RequestID]metadata.Metadata,
	blks []blocks.Block) {
	select {
	case <-al.ctx.Done():
	case al.incomingMessages <- &newResponsesAvailableMessage{responses, blks}:
	}
}

// AsyncLoad asynchronously loads the given link for the given request ID. It returns a channel for data and a channel
// for errors -- only one message will be sent over either.
func (al *AsyncLoader) AsyncLoad(requestID graphsync.RequestID, link ipld.Link) <-chan types.AsyncLoadResult {
	resultChan := make(chan types.AsyncLoadResult, 1)
	response := make(chan error, 1)
	lr := loadattemptqueue.NewLoadRequest(requestID, link, resultChan)
	_ = al.sendSyncMessage(&loadRequestMessage{response, requestID, lr}, response)
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
	select {
	case <-al.ctx.Done():
	case al.incomingMessages <- &cleanupRequestMessage{requestID}:
	}
}

func (al *AsyncLoader) sendSyncMessage(message loaderMessage, response chan error) error {
	select {
	case <-al.ctx.Done():
		return errors.New("Context Closed")
	case al.incomingMessages <- message:
	}
	select {
	case <-al.ctx.Done():
		return errors.New("Context Closed")
	case err := <-response:
		return err
	}
}

type loadRequestMessage struct {
	response    chan error
	requestID   graphsync.RequestID
	loadRequest loadattemptqueue.LoadRequest
}

type newResponsesAvailableMessage struct {
	responses map[graphsync.RequestID]metadata.Metadata
	blks      []blocks.Block
}

type registerPersistenceOptionMessage struct {
	name     string
	loader   ipld.Loader
	storer   ipld.Storer
	response chan error
}

type unregisterPersistenceOptionMessage struct {
	name     string
	response chan error
}

type startRequestMessage struct {
	requestID         graphsync.RequestID
	persistenceOption string
	response          chan error
}

type finishRequestMessage struct {
	requestID graphsync.RequestID
}

type cleanupRequestMessage struct {
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

func (al *AsyncLoader) getLoadAttemptQueue(queue string) *loadattemptqueue.LoadAttemptQueue {
	if queue == "" {
		return al.loadAttemptQueue
	}
	return al.alternateQueues[queue].loadAttemptQueue
}

func (al *AsyncLoader) getResponseCache(queue string) *responsecache.ResponseCache {
	if queue == "" {
		return al.responseCache
	}
	return al.alternateQueues[queue].responseCache
}

func (lrm *loadRequestMessage) handle(al *AsyncLoader) {
	_, retry := al.activeRequests[lrm.requestID]
	loadAttemptQueue := al.getLoadAttemptQueue(al.requestQueues[lrm.requestID])
	loadAttemptQueue.AttemptLoad(lrm.loadRequest, retry)
	select {
	case <-al.ctx.Done():
	case lrm.response <- nil:
	}
}

func (rpom *registerPersistenceOptionMessage) register(al *AsyncLoader) error {
	_, existing := al.alternateQueues[rpom.name]
	if existing {
		return errors.New("already registerd a persistence option with this name")
	}
	responseCache, loadAttemptQueue := setupAttemptQueue(rpom.loader, rpom.storer)
	al.alternateQueues[rpom.name] = alternateQueue{responseCache, loadAttemptQueue}
	return nil
}

func (rpom *registerPersistenceOptionMessage) handle(al *AsyncLoader) {
	err := rpom.register(al)
	select {
	case <-al.ctx.Done():
	case rpom.response <- err:
	}
}

func (upom *unregisterPersistenceOptionMessage) unregister(al *AsyncLoader) error {
	_, ok := al.alternateQueues[upom.name]
	if !ok {
		return errors.New("Unknown persistence option")
	}
	for _, requestQueue := range al.requestQueues {
		if upom.name == requestQueue {
			return errors.New("cannot unregister while requests are in progress")
		}
	}
	delete(al.alternateQueues, upom.name)
	return nil
}

func (upom *unregisterPersistenceOptionMessage) handle(al *AsyncLoader) {
	err := upom.unregister(al)
	select {
	case <-al.ctx.Done():
	case upom.response <- err:
	}
}

func (srm *startRequestMessage) startRequest(al *AsyncLoader) error {
	if srm.persistenceOption != "" {
		_, ok := al.alternateQueues[srm.persistenceOption]
		if !ok {
			return errors.New("Unknown persistence option")
		}
		al.requestQueues[srm.requestID] = srm.persistenceOption
	}
	al.activeRequests[srm.requestID] = struct{}{}
	return nil
}

func (srm *startRequestMessage) handle(al *AsyncLoader) {
	err := srm.startRequest(al)
	select {
	case <-al.ctx.Done():
	case srm.response <- err:
	}
}

func (frm *finishRequestMessage) handle(al *AsyncLoader) {
	delete(al.activeRequests, frm.requestID)
	loadAttemptQueue := al.getLoadAttemptQueue(al.requestQueues[frm.requestID])
	loadAttemptQueue.ClearRequest(frm.requestID)
}

func (nram *newResponsesAvailableMessage) handle(al *AsyncLoader) {
	byQueue := make(map[string][]graphsync.RequestID)
	for requestID := range nram.responses {
		queue := al.requestQueues[requestID]
		byQueue[queue] = append(byQueue[queue], requestID)
	}
	for queue, requestIDs := range byQueue {
		loadAttemptQueue := al.getLoadAttemptQueue(queue)
		responseCache := al.getResponseCache(queue)
		responses := make(map[graphsync.RequestID]metadata.Metadata, len(requestIDs))
		for _, requestID := range requestIDs {
			responses[requestID] = nram.responses[requestID]
		}
		responseCache.ProcessResponse(responses, nram.blks)
		loadAttemptQueue.RetryLoads()
	}
}

func (crm *cleanupRequestMessage) handle(al *AsyncLoader) {
	aq, ok := al.requestQueues[crm.requestID]
	if ok {
		al.alternateQueues[aq].responseCache.FinishRequest(crm.requestID)
		delete(al.requestQueues, crm.requestID)
		return
	}
	al.responseCache.FinishRequest(crm.requestID)
}

func setupAttemptQueue(loader ipld.Loader, storer ipld.Storer) (*responsecache.ResponseCache, *loadattemptqueue.LoadAttemptQueue) {

	unverifiedBlockStore := unverifiedblockstore.New(storer)
	responseCache := responsecache.New(unverifiedBlockStore)
	loadAttemptQueue := loadattemptqueue.New(func(requestID graphsync.RequestID, link ipld.Link) types.AsyncLoadResult {
		// load from response cache
		data, err := responseCache.AttemptLoad(requestID, link)
		if data == nil && err == nil {
			// fall back to local store
			stream, loadErr := loader(link, ipld.LinkContext{})
			if stream != nil && loadErr == nil {
				localData, loadErr := ioutil.ReadAll(stream)
				if loadErr == nil && localData != nil {
					return types.AsyncLoadResult{
						Data:  localData,
						Err:   nil,
						Local: true,
					}
				}
			}
		}
		return types.AsyncLoadResult{
			Data:  data,
			Err:   err,
			Local: false,
		}
	})

	return responseCache, loadAttemptQueue
}
