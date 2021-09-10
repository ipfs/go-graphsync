package requestmanager

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hannahhoward/go-pubsub"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/dedupkey"
	ipldutil "github.com/ipfs/go-graphsync/ipldutil"
	"github.com/ipfs/go-graphsync/listeners"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/requestmanager/executor"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/requestmanager/types"
)

var log = logging.Logger("graphsync")

const (
	// defaultPriority is the default priority for requests sent by graphsync
	defaultPriority = graphsync.Priority(0)
)

type inProgressRequestStatus struct {
	ctx            context.Context
	startTime      time.Time
	cancelFn       func()
	p              peer.ID
	terminalError  chan error
	resumeMessages chan []graphsync.ExtensionData
	pauseMessages  chan struct{}
	paused         bool
	lastResponse   atomic.Value
	onTerminated   []chan error
}

// PeerHandler is an interface that can send requests to peers
type PeerHandler interface {
	AllocateAndBuildMessage(p peer.ID, blkSize uint64, buildMessageFn func(*gsmsg.Builder), notifees []notifications.Notifee)
}

// AsyncLoader is an interface for loading links asynchronously, returning
// results as new responses are processed
type AsyncLoader interface {
	StartRequest(graphsync.RequestID, string) error
	ProcessResponse(p peer.ID, responses map[graphsync.RequestID]metadata.Metadata,
		blks []blocks.Block)
	AsyncLoad(p peer.ID, requestID graphsync.RequestID, link ipld.Link, linkContext ipld.LinkContext) <-chan types.AsyncLoadResult
	CompleteResponsesFor(requestID graphsync.RequestID)
	CleanupRequest(requestID graphsync.RequestID)
}

// RequestManager tracks outgoing requests and processes incoming reponses
// to them.
type RequestManager struct {
	ctx             context.Context
	cancel          func()
	messages        chan requestManagerMessage
	peerHandler     PeerHandler
	rc              *responseCollector
	asyncLoader     AsyncLoader
	disconnectNotif *pubsub.PubSub
	linkSystem      ipld.LinkSystem

	// dont touch out side of run loop
	nextRequestID             graphsync.RequestID
	inProgressRequestStatuses map[graphsync.RequestID]*inProgressRequestStatus
	requestHooks              RequestHooks
	responseHooks             ResponseHooks
	blockHooks                BlockHooks
	networkErrorListeners     *listeners.NetworkErrorListeners
}

type requestManagerMessage interface {
	handle(rm *RequestManager)
}

// RequestHooks run for new requests
type RequestHooks interface {
	ProcessRequestHooks(p peer.ID, request graphsync.RequestData) hooks.RequestResult
}

// ResponseHooks run for new responses
type ResponseHooks interface {
	ProcessResponseHooks(p peer.ID, response graphsync.ResponseData) hooks.UpdateResult
}

// BlockHooks run for each block loaded
type BlockHooks interface {
	ProcessBlockHooks(p peer.ID, response graphsync.ResponseData, block graphsync.BlockData) hooks.UpdateResult
}

// New generates a new request manager from a context, network, and selectorQuerier
func New(ctx context.Context,
	asyncLoader AsyncLoader,
	linkSystem ipld.LinkSystem,
	requestHooks RequestHooks,
	responseHooks ResponseHooks,
	blockHooks BlockHooks,
	networkErrorListeners *listeners.NetworkErrorListeners,
) *RequestManager {
	ctx, cancel := context.WithCancel(ctx)
	return &RequestManager{
		ctx:                       ctx,
		cancel:                    cancel,
		asyncLoader:               asyncLoader,
		disconnectNotif:           pubsub.New(disconnectDispatcher),
		linkSystem:                linkSystem,
		rc:                        newResponseCollector(ctx),
		messages:                  make(chan requestManagerMessage, 16),
		inProgressRequestStatuses: make(map[graphsync.RequestID]*inProgressRequestStatus),
		requestHooks:              requestHooks,
		responseHooks:             responseHooks,
		blockHooks:                blockHooks,
		networkErrorListeners:     networkErrorListeners,
	}
}

// SetDelegate specifies who will send messages out to the internet.
func (rm *RequestManager) SetDelegate(peerHandler PeerHandler) {
	rm.peerHandler = peerHandler
}

type inProgressRequest struct {
	requestID     graphsync.RequestID
	request       gsmsg.GraphSyncRequest
	incoming      chan graphsync.ResponseProgress
	incomingError chan error
}

type newRequestMessage struct {
	p                     peer.ID
	root                  ipld.Link
	selector              ipld.Node
	extensions            []graphsync.ExtensionData
	inProgressRequestChan chan<- inProgressRequest
}

// SendRequest initiates a new GraphSync request to the given peer.
func (rm *RequestManager) SendRequest(ctx context.Context,
	p peer.ID,
	root ipld.Link,
	selectorNode ipld.Node,
	extensions ...graphsync.ExtensionData) (<-chan graphsync.ResponseProgress, <-chan error) {
	if _, err := selector.ParseSelector(selectorNode); err != nil {
		return rm.singleErrorResponse(fmt.Errorf("invalid selector spec"))
	}

	inProgressRequestChan := make(chan inProgressRequest)

	select {
	case rm.messages <- &newRequestMessage{p, root, selectorNode, extensions, inProgressRequestChan}:
	case <-rm.ctx.Done():
		return rm.emptyResponse()
	case <-ctx.Done():
		return rm.emptyResponse()
	}
	var receivedInProgressRequest inProgressRequest
	select {
	case <-rm.ctx.Done():
		return rm.emptyResponse()
	case receivedInProgressRequest = <-inProgressRequestChan:
	}

	// If the connection to the peer is disconnected, fire an error
	unsub := rm.listenForDisconnect(p, func(neterr error) {
		rm.networkErrorListeners.NotifyNetworkErrorListeners(p, receivedInProgressRequest.request, neterr)
	})

	return rm.rc.collectResponses(ctx,
		receivedInProgressRequest.incoming,
		receivedInProgressRequest.incomingError,
		func() {
			rm.cancelRequest(receivedInProgressRequest.requestID,
				receivedInProgressRequest.incoming,
				receivedInProgressRequest.incomingError)
		},
		// Once the request has completed, stop listening for disconnect events
		unsub,
	)
}

// Dispatch the Disconnect event to subscribers
func disconnectDispatcher(p pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	listener := subscriberFn.(func(peer.ID))
	listener(p.(peer.ID))
	return nil
}

// Listen for the Disconnect event for the given peer
func (rm *RequestManager) listenForDisconnect(p peer.ID, onDisconnect func(neterr error)) func() {
	// Subscribe to Disconnect notifications
	return rm.disconnectNotif.Subscribe(func(evtPeer peer.ID) {
		// If the peer is the one we're interested in, call the listener
		if evtPeer == p {
			onDisconnect(fmt.Errorf("disconnected from peer %s", p))
		}
	})
}

// Disconnected is called when a peer disconnects
func (rm *RequestManager) Disconnected(p peer.ID) {
	// Notify any listeners that a peer has disconnected
	_ = rm.disconnectNotif.Publish(p)
}

func (rm *RequestManager) emptyResponse() (chan graphsync.ResponseProgress, chan error) {
	ch := make(chan graphsync.ResponseProgress)
	close(ch)
	errCh := make(chan error)
	close(errCh)
	return ch, errCh
}

func (rm *RequestManager) singleErrorResponse(err error) (chan graphsync.ResponseProgress, chan error) {
	ch := make(chan graphsync.ResponseProgress)
	close(ch)
	errCh := make(chan error, 1)
	errCh <- err
	close(errCh)
	return ch, errCh
}

type cancelRequestMessage struct {
	requestID     graphsync.RequestID
	isPause       bool
	onTerminated  chan error
	terminalError error
}

func (rm *RequestManager) cancelRequest(requestID graphsync.RequestID,
	incomingResponses chan graphsync.ResponseProgress,
	incomingErrors chan error) {
	cancelMessageChannel := rm.messages
	for cancelMessageChannel != nil || incomingResponses != nil || incomingErrors != nil {
		select {
		case cancelMessageChannel <- &cancelRequestMessage{requestID, false, nil, nil}:
			cancelMessageChannel = nil
		// clear out any remaining responses, in case and "incoming reponse"
		// messages get processed before our cancel message
		case _, ok := <-incomingResponses:
			if !ok {
				incomingResponses = nil
			}
		case _, ok := <-incomingErrors:
			if !ok {
				incomingErrors = nil
			}
		case <-rm.ctx.Done():
			return
		}
	}
}

// CancelRequest cancels the given request ID and waits for the request to terminate
func (rm *RequestManager) CancelRequest(ctx context.Context, requestID graphsync.RequestID) error {
	terminated := make(chan error, 1)
	return rm.sendSyncMessage(&cancelRequestMessage{requestID, false, terminated, graphsync.RequestClientCancelledErr{}}, terminated, ctx.Done())
}

type processResponseMessage struct {
	p         peer.ID
	responses []gsmsg.GraphSyncResponse
	blks      []blocks.Block
	response  chan error
}

// ProcessResponses ingests the given responses from the network and
// and updates the in progress requests based on those responses.
func (rm *RequestManager) ProcessResponses(p peer.ID, responses []gsmsg.GraphSyncResponse,
	blks []blocks.Block) {
	response := make(chan error, 1)
	err := rm.sendSyncMessage(&processResponseMessage{p, responses, blks, response}, response, nil)
	if err != nil {
		log.Warnf("ProcessResponses: %s", err)
	}
}

type unpauseRequestMessage struct {
	id         graphsync.RequestID
	extensions []graphsync.ExtensionData
	response   chan error
}

// UnpauseRequest unpauses a request that was paused in a block hook based request ID
// Can also send extensions with unpause
func (rm *RequestManager) UnpauseRequest(requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	response := make(chan error, 1)
	return rm.sendSyncMessage(&unpauseRequestMessage{requestID, extensions, response}, response, nil)
}

type pauseRequestMessage struct {
	id       graphsync.RequestID
	response chan error
}

// PauseRequest pauses an in progress request (may take 1 or more blocks to process)
func (rm *RequestManager) PauseRequest(requestID graphsync.RequestID) error {
	response := make(chan error, 1)
	return rm.sendSyncMessage(&pauseRequestMessage{requestID, response}, response, nil)
}

func (rm *RequestManager) sendSyncMessage(message requestManagerMessage, response chan error, done <-chan struct{}) error {
	select {
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case <-done:
		return errors.New("context cancelled")
	case rm.messages <- message:
	}
	select {
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case <-done:
		return errors.New("context cancelled")
	case err := <-response:
		return err
	}
}

// Startup starts processing for the WantManager.
func (rm *RequestManager) Startup() {
	go rm.run()
}

// Shutdown ends processing for the want manager.
func (rm *RequestManager) Shutdown() {
	rm.cancel()
}

func (rm *RequestManager) run() {
	// NOTE: Do not open any streams or connections from anywhere in this
	// event loop. Really, just don't do anything likely to block.
	defer rm.cleanupInProcessRequests()

	for {
		select {
		case message := <-rm.messages:
			message.handle(rm)
		case <-rm.ctx.Done():
			return
		}
	}
}

func (rm *RequestManager) cleanupInProcessRequests() {
	for _, requestStatus := range rm.inProgressRequestStatuses {
		requestStatus.cancelFn()
	}
}

type terminateRequestMessage struct {
	requestID graphsync.RequestID
}

func (nrm *newRequestMessage) setupRequest(requestID graphsync.RequestID, rm *RequestManager) (gsmsg.GraphSyncRequest, chan graphsync.ResponseProgress, chan error) {
	log.Infow("graphsync request initiated", "request id", requestID, "peer", nrm.p, "root", nrm.root)

	request, hooksResult, err := rm.validateRequest(requestID, nrm.p, nrm.root, nrm.selector, nrm.extensions)
	if err != nil {
		rp, err := rm.singleErrorResponse(err)
		return request, rp, err
	}
	doNotSendCidsData, has := request.Extension(graphsync.ExtensionDoNotSendCIDs)
	var doNotSendCids *cid.Set
	if has {
		doNotSendCids, err = cidset.DecodeCidSet(doNotSendCidsData)
		if err != nil {
			rp, err := rm.singleErrorResponse(err)
			return request, rp, err
		}
	} else {
		doNotSendCids = cid.NewSet()
	}
	ctx, cancel := context.WithCancel(rm.ctx)
	p := nrm.p
	resumeMessages := make(chan []graphsync.ExtensionData, 1)
	pauseMessages := make(chan struct{}, 1)
	terminalError := make(chan error, 1)
	requestStatus := &inProgressRequestStatus{
		ctx: ctx, startTime: time.Now(), cancelFn: cancel, p: p, resumeMessages: resumeMessages, pauseMessages: pauseMessages, terminalError: terminalError,
	}
	lastResponse := &requestStatus.lastResponse
	lastResponse.Store(gsmsg.NewResponse(request.ID(), graphsync.RequestAcknowledged))
	rm.inProgressRequestStatuses[request.ID()] = requestStatus
	incoming, incomingError := executor.ExecutionEnv{
		Ctx:              rm.ctx,
		SendRequest:      rm.sendRequest,
		TerminateRequest: rm.terminateRequest,
		RunBlockHooks:    rm.processBlockHooks,
		Loader:           rm.asyncLoader.AsyncLoad,
		LinkSystem:       rm.linkSystem,
	}.Start(
		executor.RequestExecution{
			Ctx:                  ctx,
			P:                    p,
			Request:              request,
			TerminalError:        terminalError,
			LastResponse:         lastResponse,
			DoNotSendCids:        doNotSendCids,
			NodePrototypeChooser: hooksResult.CustomChooser,
			ResumeMessages:       resumeMessages,
			PauseMessages:        pauseMessages,
		})
	return request, incoming, incomingError
}

func (nrm *newRequestMessage) handle(rm *RequestManager) {
	var ipr inProgressRequest
	ipr.requestID = rm.nextRequestID
	rm.nextRequestID++
	ipr.request, ipr.incoming, ipr.incomingError = nrm.setupRequest(ipr.requestID, rm)

	select {
	case nrm.inProgressRequestChan <- ipr:
	case <-rm.ctx.Done():
	}
}

func (trm *terminateRequestMessage) handle(rm *RequestManager) {
	ipr, ok := rm.inProgressRequestStatuses[trm.requestID]
	if ok {
		log.Infow("graphsync request complete", "request id", trm.requestID, "peer", ipr.p, "total time", time.Since(ipr.startTime))
	}
	delete(rm.inProgressRequestStatuses, trm.requestID)
	rm.asyncLoader.CleanupRequest(trm.requestID)
	if ok {
		for _, onTerminated := range ipr.onTerminated {
			select {
			case <-rm.ctx.Done():
			case onTerminated <- nil:
			}
		}
	}
}

func (crm *cancelRequestMessage) handle(rm *RequestManager) {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[crm.requestID]
	if !ok {
		if crm.onTerminated != nil {
			select {
			case crm.onTerminated <- graphsync.RequestNotFoundErr{}:
			case <-rm.ctx.Done():
			}
		}
		return
	}

	if crm.onTerminated != nil {
		inProgressRequestStatus.onTerminated = append(inProgressRequestStatus.onTerminated, crm.onTerminated)
	}
	if crm.terminalError != nil {
		select {
		case inProgressRequestStatus.terminalError <- crm.terminalError:
		default:
		}
	}

	rm.sendRequest(inProgressRequestStatus.p, gsmsg.CancelRequest(crm.requestID))
	if crm.isPause {
		inProgressRequestStatus.paused = true
	} else {
		inProgressRequestStatus.cancelFn()
	}
}

func (prm *processResponseMessage) handle(rm *RequestManager) {
	filteredResponses := rm.processExtensions(prm.responses, prm.p)
	filteredResponses = rm.filterResponsesForPeer(filteredResponses, prm.p)
	rm.updateLastResponses(filteredResponses)
	responseMetadata := metadataForResponses(filteredResponses)
	rm.asyncLoader.ProcessResponse(prm.p, responseMetadata, prm.blks)
	rm.processTerminations(filteredResponses)
	select {
	case <-rm.ctx.Done():
	case prm.response <- nil:
	}
}

func (rm *RequestManager) filterResponsesForPeer(responses []gsmsg.GraphSyncResponse, p peer.ID) []gsmsg.GraphSyncResponse {
	responsesForPeer := make([]gsmsg.GraphSyncResponse, 0, len(responses))
	for _, response := range responses {
		requestStatus, ok := rm.inProgressRequestStatuses[response.RequestID()]
		if !ok || requestStatus.p != p {
			continue
		}
		responsesForPeer = append(responsesForPeer, response)
	}
	return responsesForPeer
}

func (rm *RequestManager) processExtensions(responses []gsmsg.GraphSyncResponse, p peer.ID) []gsmsg.GraphSyncResponse {
	remainingResponses := make([]gsmsg.GraphSyncResponse, 0, len(responses))
	for _, response := range responses {
		success := rm.processExtensionsForResponse(p, response)
		if success {
			remainingResponses = append(remainingResponses, response)
		}
	}
	return remainingResponses
}

func (rm *RequestManager) updateLastResponses(responses []gsmsg.GraphSyncResponse) {
	for _, response := range responses {
		rm.inProgressRequestStatuses[response.RequestID()].lastResponse.Store(response)
	}
}

func (rm *RequestManager) processExtensionsForResponse(p peer.ID, response gsmsg.GraphSyncResponse) bool {
	result := rm.responseHooks.ProcessResponseHooks(p, response)
	if len(result.Extensions) > 0 {
		updateRequest := gsmsg.UpdateRequest(response.RequestID(), result.Extensions...)
		rm.sendRequest(p, updateRequest)
	}
	if result.Err != nil {
		requestStatus, ok := rm.inProgressRequestStatuses[response.RequestID()]
		if !ok {
			return false
		}
		responseError := rm.generateResponseErrorFromStatus(graphsync.RequestFailedUnknown)
		select {
		case requestStatus.terminalError <- responseError:
		default:
		}
		rm.sendRequest(p, gsmsg.CancelRequest(response.RequestID()))
		requestStatus.cancelFn()
		return false
	}
	return true
}

func (rm *RequestManager) processTerminations(responses []gsmsg.GraphSyncResponse) {
	for _, response := range responses {
		if gsmsg.IsTerminalResponseCode(response.Status()) {
			if gsmsg.IsTerminalFailureCode(response.Status()) {
				requestStatus := rm.inProgressRequestStatuses[response.RequestID()]
				responseError := rm.generateResponseErrorFromStatus(response.Status())
				select {
				case requestStatus.terminalError <- responseError:
				default:
				}
				requestStatus.cancelFn()
			}
			rm.asyncLoader.CompleteResponsesFor(response.RequestID())
		}
	}
}

func (rm *RequestManager) generateResponseErrorFromStatus(status graphsync.ResponseStatusCode) error {
	switch status {
	case graphsync.RequestFailedBusy:
		return graphsync.RequestFailedBusyErr{}
	case graphsync.RequestFailedContentNotFound:
		return graphsync.RequestFailedContentNotFoundErr{}
	case graphsync.RequestFailedLegal:
		return graphsync.RequestFailedLegalErr{}
	case graphsync.RequestFailedUnknown:
		return graphsync.RequestFailedUnknownErr{}
	case graphsync.RequestCancelled:
		return graphsync.RequestCancelledErr{}
	default:
		return fmt.Errorf("Unknown")
	}
}

func (rm *RequestManager) processBlockHooks(p peer.ID, response graphsync.ResponseData, block graphsync.BlockData) error {
	result := rm.blockHooks.ProcessBlockHooks(p, response, block)
	if len(result.Extensions) > 0 {
		updateRequest := gsmsg.UpdateRequest(response.RequestID(), result.Extensions...)
		rm.sendRequest(p, updateRequest)
	}
	if result.Err != nil {
		_, isPause := result.Err.(hooks.ErrPaused)
		select {
		case <-rm.ctx.Done():
		case rm.messages <- &cancelRequestMessage{response.RequestID(), isPause, nil, nil}:
		}
	}
	return result.Err
}

func (rm *RequestManager) terminateRequest(requestID graphsync.RequestID) {
	select {
	case <-rm.ctx.Done():
	case rm.messages <- &terminateRequestMessage{requestID}:
	}
}

func (rm *RequestManager) validateRequest(requestID graphsync.RequestID, p peer.ID, root ipld.Link, selectorSpec ipld.Node, extensions []graphsync.ExtensionData) (gsmsg.GraphSyncRequest, hooks.RequestResult, error) {
	_, err := ipldutil.EncodeNode(selectorSpec)
	if err != nil {
		return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, err
	}
	_, err = selector.ParseSelector(selectorSpec)
	if err != nil {
		return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, err
	}
	asCidLink, ok := root.(cidlink.Link)
	if !ok {
		return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, fmt.Errorf("request failed: link has no cid")
	}
	request := gsmsg.NewRequest(requestID, asCidLink.Cid, selectorSpec, defaultPriority, extensions...)
	hooksResult := rm.requestHooks.ProcessRequestHooks(p, request)
	if hooksResult.PersistenceOption != "" {
		dedupData, err := dedupkey.EncodeDedupKey(hooksResult.PersistenceOption)
		if err != nil {
			return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, err
		}
		request = request.ReplaceExtensions([]graphsync.ExtensionData{
			{
				Name: graphsync.ExtensionDeDupByKey,
				Data: dedupData,
			},
		})
	}
	err = rm.asyncLoader.StartRequest(requestID, hooksResult.PersistenceOption)
	if err != nil {
		return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, err
	}
	return request, hooksResult, nil
}

type reqSubscriber struct {
	p                     peer.ID
	request               gsmsg.GraphSyncRequest
	networkErrorListeners *listeners.NetworkErrorListeners
}

func (r *reqSubscriber) OnNext(topic notifications.Topic, event notifications.Event) {
	mqEvt, isMQEvt := event.(messagequeue.Event)
	if !isMQEvt || mqEvt.Name != messagequeue.Error {
		return
	}

	r.networkErrorListeners.NotifyNetworkErrorListeners(r.p, r.request, mqEvt.Err)
	//r.re.networkError <- mqEvt.Err
	//r.re.terminateRequest()
}

func (r reqSubscriber) OnClose(topic notifications.Topic) {
}

const requestNetworkError = "request_network_error"

func (rm *RequestManager) sendRequest(p peer.ID, request gsmsg.GraphSyncRequest) {
	sub := notifications.NewTopicDataSubscriber(&reqSubscriber{p, request, rm.networkErrorListeners})
	failNotifee := notifications.Notifee{Data: requestNetworkError, Subscriber: sub}
	rm.peerHandler.AllocateAndBuildMessage(p, 0, func(builder *gsmsg.Builder) {
		builder.AddRequest(request)
	}, []notifications.Notifee{failNotifee})
}

func (urm *unpauseRequestMessage) unpause(rm *RequestManager) error {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[urm.id]
	if !ok {
		return graphsync.RequestNotFoundErr{}
	}
	if !inProgressRequestStatus.paused {
		return errors.New("request is not paused")
	}
	inProgressRequestStatus.paused = false
	select {
	case <-inProgressRequestStatus.pauseMessages:
		rm.sendRequest(inProgressRequestStatus.p, gsmsg.UpdateRequest(urm.id, urm.extensions...))
		return nil
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case inProgressRequestStatus.resumeMessages <- urm.extensions:
		return nil
	}
}
func (urm *unpauseRequestMessage) handle(rm *RequestManager) {
	err := urm.unpause(rm)
	select {
	case <-rm.ctx.Done():
	case urm.response <- err:
	}
}
func (prm *pauseRequestMessage) pause(rm *RequestManager) error {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[prm.id]
	if !ok {
		return graphsync.RequestNotFoundErr{}
	}
	if inProgressRequestStatus.paused {
		return errors.New("request is already paused")
	}
	inProgressRequestStatus.paused = true
	select {
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case inProgressRequestStatus.pauseMessages <- struct{}{}:
		return nil
	}
}
func (prm *pauseRequestMessage) handle(rm *RequestManager) {
	err := prm.pause(rm)
	select {
	case <-rm.ctx.Done():
	case prm.response <- err:
	}
}
