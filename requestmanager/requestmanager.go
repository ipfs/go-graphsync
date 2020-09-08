package requestmanager

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/dedupkey"
	ipldutil "github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/metadata"
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
	cancelFn       func()
	p              peer.ID
	networkError   chan error
	resumeMessages chan []graphsync.ExtensionData
	pauseMessages  chan struct{}
	paused         bool
	lastResponse   atomic.Value
}

// PeerHandler is an interface that can send requests to peers
type PeerHandler interface {
	SendRequest(p peer.ID, graphSyncRequest gsmsg.GraphSyncRequest)
}

// AsyncLoader is an interface for loading links asynchronously, returning
// results as new responses are processed
type AsyncLoader interface {
	StartRequest(graphsync.RequestID, string) error
	ProcessResponse(responses map[graphsync.RequestID]metadata.Metadata,
		blks []blocks.Block)
	AsyncLoad(requestID graphsync.RequestID, link ipld.Link) <-chan types.AsyncLoadResult
	CompleteResponsesFor(requestID graphsync.RequestID)
	CleanupRequest(requestID graphsync.RequestID)
}

// RequestManager tracks outgoing requests and processes incoming reponses
// to them.
type RequestManager struct {
	ctx         context.Context
	cancel      func()
	messages    chan requestManagerMessage
	peerHandler PeerHandler
	rc          *responseCollector
	asyncLoader AsyncLoader
	// dont touch out side of run loop
	nextRequestID             graphsync.RequestID
	inProgressRequestStatuses map[graphsync.RequestID]*inProgressRequestStatus
	requestHooks              RequestHooks
	responseHooks             ResponseHooks
	blockHooks                BlockHooks
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
	requestHooks RequestHooks,
	responseHooks ResponseHooks,
	blockHooks BlockHooks) *RequestManager {
	ctx, cancel := context.WithCancel(ctx)
	return &RequestManager{
		ctx:                       ctx,
		cancel:                    cancel,
		asyncLoader:               asyncLoader,
		rc:                        newResponseCollector(ctx),
		messages:                  make(chan requestManagerMessage, 16),
		inProgressRequestStatuses: make(map[graphsync.RequestID]*inProgressRequestStatus),
		requestHooks:              requestHooks,
		responseHooks:             responseHooks,
		blockHooks:                blockHooks,
	}
}

// SetDelegate specifies who will send messages out to the internet.
func (rm *RequestManager) SetDelegate(peerHandler PeerHandler) {
	rm.peerHandler = peerHandler
}

type inProgressRequest struct {
	requestID     graphsync.RequestID
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
	selector ipld.Node,
	extensions ...graphsync.ExtensionData) (<-chan graphsync.ResponseProgress, <-chan error) {
	if _, err := ipldutil.ParseSelector(selector); err != nil {
		return rm.singleErrorResponse(fmt.Errorf("Invalid Selector Spec"))
	}

	inProgressRequestChan := make(chan inProgressRequest)

	select {
	case rm.messages <- &newRequestMessage{p, root, selector, extensions, inProgressRequestChan}:
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

	return rm.rc.collectResponses(ctx,
		receivedInProgressRequest.incoming,
		receivedInProgressRequest.incomingError,
		func() {
			rm.cancelRequest(receivedInProgressRequest.requestID,
				receivedInProgressRequest.incoming,
				receivedInProgressRequest.incomingError)
		})
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
	requestID graphsync.RequestID
	isPause   bool
}

func (rm *RequestManager) cancelRequest(requestID graphsync.RequestID,
	incomingResponses chan graphsync.ResponseProgress,
	incomingErrors chan error) {
	cancelMessageChannel := rm.messages
	for cancelMessageChannel != nil || incomingResponses != nil || incomingErrors != nil {
		select {
		case cancelMessageChannel <- &cancelRequestMessage{requestID, false}:
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

type processResponseMessage struct {
	p         peer.ID
	responses []gsmsg.GraphSyncResponse
	blks      []blocks.Block
}

// ProcessResponses ingests the given responses from the network and
// and updates the in progress requests based on those responses.
func (rm *RequestManager) ProcessResponses(p peer.ID, responses []gsmsg.GraphSyncResponse,
	blks []blocks.Block) {
	select {
	case rm.messages <- &processResponseMessage{p, responses, blks}:
	case <-rm.ctx.Done():
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
	return rm.sendSyncMessage(&unpauseRequestMessage{requestID, extensions, response}, response)
}

type pauseRequestMessage struct {
	id       graphsync.RequestID
	response chan error
}

// PauseRequest pauses an in progress request (may take 1 or more blocks to process)
func (rm *RequestManager) PauseRequest(requestID graphsync.RequestID) error {
	response := make(chan error, 1)
	return rm.sendSyncMessage(&pauseRequestMessage{requestID, response}, response)
}

func (rm *RequestManager) sendSyncMessage(message requestManagerMessage, response chan error) error {
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

func (nrm *newRequestMessage) setupRequest(requestID graphsync.RequestID, rm *RequestManager) (chan graphsync.ResponseProgress, chan error) {
	request, hooksResult, err := rm.validateRequest(requestID, nrm.p, nrm.root, nrm.selector, nrm.extensions)
	if err != nil {
		return rm.singleErrorResponse(err)
	}
	doNotSendCidsData, has := request.Extension(graphsync.ExtensionDoNotSendCIDs)
	var doNotSendCids *cid.Set
	if has {
		doNotSendCids, err = cidset.DecodeCidSet(doNotSendCidsData)
		if err != nil {
			return rm.singleErrorResponse(err)
		}
	} else {
		doNotSendCids = cid.NewSet()
	}
	ctx, cancel := context.WithCancel(rm.ctx)
	p := nrm.p
	resumeMessages := make(chan []graphsync.ExtensionData, 1)
	pauseMessages := make(chan struct{}, 1)
	networkError := make(chan error, 1)
	requestStatus := &inProgressRequestStatus{
		ctx: ctx, cancelFn: cancel, p: p, resumeMessages: resumeMessages, pauseMessages: pauseMessages, networkError: networkError,
	}
	lastResponse := &requestStatus.lastResponse
	lastResponse.Store(gsmsg.NewResponse(request.ID(), graphsync.RequestAcknowledged))
	rm.inProgressRequestStatuses[request.ID()] = requestStatus
	incoming, incomingError := executor.ExecutionEnv{
		Ctx:              rm.ctx,
		SendRequest:      rm.peerHandler.SendRequest,
		TerminateRequest: rm.terminateRequest,
		RunBlockHooks:    rm.processBlockHooks,
		Loader:           rm.asyncLoader.AsyncLoad,
	}.Start(
		executor.RequestExecution{
			Ctx:              ctx,
			P:                p,
			Request:          request,
			NetworkError:     networkError,
			LastResponse:     lastResponse,
			DoNotSendCids:    doNotSendCids,
			NodePrototypeChooser: hooksResult.CustomChooser,
			ResumeMessages:   resumeMessages,
			PauseMessages:    pauseMessages,
		})
	return incoming, incomingError
}

func (nrm *newRequestMessage) handle(rm *RequestManager) {
	var ipr inProgressRequest
	ipr.requestID = rm.nextRequestID
	rm.nextRequestID++
	ipr.incoming, ipr.incomingError = nrm.setupRequest(ipr.requestID, rm)

	select {
	case nrm.inProgressRequestChan <- ipr:
	case <-rm.ctx.Done():
	}
}

func (trm *terminateRequestMessage) handle(rm *RequestManager) {
	delete(rm.inProgressRequestStatuses, trm.requestID)
	rm.asyncLoader.CleanupRequest(trm.requestID)
}

func (crm *cancelRequestMessage) handle(rm *RequestManager) {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[crm.requestID]
	if !ok {
		return
	}

	rm.peerHandler.SendRequest(inProgressRequestStatus.p, gsmsg.CancelRequest(crm.requestID))
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
	rm.asyncLoader.ProcessResponse(responseMetadata, prm.blks)
	rm.processTerminations(filteredResponses)
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
		rm.peerHandler.SendRequest(p, updateRequest)
	}
	if result.Err != nil {
		requestStatus, ok := rm.inProgressRequestStatuses[response.RequestID()]
		if !ok {
			return false
		}
		responseError := rm.generateResponseErrorFromStatus(graphsync.RequestFailedUnknown)
		select {
		case requestStatus.networkError <- responseError:
		case <-requestStatus.ctx.Done():
		}
		rm.peerHandler.SendRequest(p, gsmsg.CancelRequest(response.RequestID()))
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
				case requestStatus.networkError <- responseError:
				case <-requestStatus.ctx.Done():
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
		rm.peerHandler.SendRequest(p, updateRequest)
	}
	if result.Err != nil {
		_, isPause := result.Err.(hooks.ErrPaused)
		select {
		case <-rm.ctx.Done():
		case rm.messages <- &cancelRequestMessage{response.RequestID(), isPause}:
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
	_, err = ipldutil.ParseSelector(selectorSpec)
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

func (urm *unpauseRequestMessage) unpause(rm *RequestManager) error {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[urm.id]
	if !ok {
		return errors.New("request not found")
	}
	if !inProgressRequestStatus.paused {
		return errors.New("request is not paused")
	}
	inProgressRequestStatus.paused = false
	select {
	case <-inProgressRequestStatus.pauseMessages:
		rm.peerHandler.SendRequest(inProgressRequestStatus.p, gsmsg.UpdateRequest(urm.id, urm.extensions...))
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
		return errors.New("request not found")
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
