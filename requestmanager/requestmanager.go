package requestmanager

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/ipfs/go-graphsync/requestmanager/executor"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-graphsync"
	ipldutil "github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	logging "github.com/ipfs/go-log"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("graphsync")

const (
	// defaultPriority is the default priority for requests sent by graphsync
	defaultPriority = graphsync.Priority(0)
)

type inProgressRequestStatus struct {
	ctx          context.Context
	cancelFn     func()
	p            peer.ID
	networkError chan error
	lastResponse atomic.Value
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
}

func (rm *RequestManager) cancelRequest(requestID graphsync.RequestID,
	incomingResponses chan graphsync.ResponseProgress,
	incomingErrors chan error) {
	cancelMessageChannel := rm.messages
	for cancelMessageChannel != nil || incomingResponses != nil || incomingErrors != nil {
		select {
		case cancelMessageChannel <- &cancelRequestMessage{requestID}:
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

func (nrm *newRequestMessage) handle(rm *RequestManager) {
	var ipr inProgressRequest
	ipr.requestID = rm.nextRequestID
	rm.nextRequestID++
	request, hooksResult, err := rm.validateRequest(ipr.requestID, nrm.p, nrm.root, nrm.selector, nrm.extensions)
	if err != nil {
		ipr.incoming, ipr.incomingError = rm.singleErrorResponse(err)
	} else {
		ctx, cancel := context.WithCancel(rm.ctx)
		p := nrm.p
		requestStatus := &inProgressRequestStatus{
			ctx: ctx, cancelFn: cancel, p: p,
		}
		lastResponse := &requestStatus.lastResponse
		lastResponse.Store(gsmsg.NewResponse(request.ID(), graphsync.RequestAcknowledged))
		rm.inProgressRequestStatuses[request.ID()] = requestStatus
		ipr.incoming, ipr.incomingError = executor.RequestExecution{
			Request:     request,
			SendRequest: func(gsRequest gsmsg.GraphSyncRequest) { rm.peerHandler.SendRequest(p, gsRequest) },
			Loader:      rm.asyncLoader.AsyncLoad,
			RunBlockHooks: func(bd graphsync.BlockData) error {
				response := lastResponse.Load().(gsmsg.GraphSyncResponse)
				return rm.processBlockHooks(p, response, bd)
			},
			TerminateRequest: func() {
				select {
				case <-ctx.Done():
				case rm.messages <- &terminateRequestMessage{request.ID()}:
				}
			},
			NodeStyleChooser: hooksResult.CustomChooser,
		}.Start(ctx)
		requestStatus.networkError = ipr.incomingError
	}

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
	delete(rm.inProgressRequestStatuses, crm.requestID)
	inProgressRequestStatus.cancelFn()
}

func (prm *processResponseMessage) handle(rm *RequestManager) {
	filteredResponses := rm.filterResponsesForPeer(prm.responses, prm.p)
	filteredResponses = rm.processExtensions(filteredResponses, prm.p)
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
		requestStatus := rm.inProgressRequestStatuses[response.RequestID()]
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
			delete(rm.inProgressRequestStatuses, response.RequestID())
		}
	}
}

func (rm *RequestManager) generateResponseErrorFromStatus(status graphsync.ResponseStatusCode) error {
	switch status {
	case graphsync.RequestFailedBusy:
		return fmt.Errorf("Request Failed - Peer Is Busy")
	case graphsync.RequestFailedContentNotFound:
		return fmt.Errorf("Request Failed - Content Not Found")
	case graphsync.RequestFailedLegal:
		return fmt.Errorf("Request Failed - For Legal Reasons")
	case graphsync.RequestFailedUnknown:
		return fmt.Errorf("Request Failed - Unknown Reason")
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
		select {
		case <-rm.ctx.Done():
		case rm.messages <- &cancelRequestMessage{response.RequestID()}:
		}
	}
	return result.Err
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
	err = rm.asyncLoader.StartRequest(requestID, hooksResult.PersistenceOption)
	if err != nil {
		return gsmsg.GraphSyncRequest{}, hooks.RequestResult{}, err
	}
	return request, hooksResult, nil
}
