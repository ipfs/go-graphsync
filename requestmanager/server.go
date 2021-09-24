package requestmanager

import (
	"context"
	"errors"
	"fmt"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/dedupkey"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/requestmanager/executor"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/libp2p/go-libp2p-core/peer"
)

// The code in this file implements the internal thread for the request manager.
// These functions can modify the internal state of the RequestManager

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

func (rm *RequestManager) setupRequest(p peer.ID, root ipld.Link, selector ipld.Node, extensions []graphsync.ExtensionData) (gsmsg.GraphSyncRequest, chan graphsync.ResponseProgress, chan error) {
	requestID := rm.nextRequestID
	rm.nextRequestID++

	log.Infow("graphsync request initiated", "request id", requestID, "peer", p, "root", root)

	request, hooksResult, err := rm.validateRequest(requestID, p, root, selector, extensions)
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
		SendRequest:      rm.SendRequest,
		TerminateRequest: rm.TerminateRequest,
		RunBlockHooks:    rm.ProcessBlockHooks,
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

func (rm *RequestManager) terminateRequest(requestID graphsync.RequestID) {
	ipr, ok := rm.inProgressRequestStatuses[requestID]
	if ok {
		log.Infow("graphsync request complete", "request id", requestID, "peer", ipr.p, "total time", time.Since(ipr.startTime))
	}
	delete(rm.inProgressRequestStatuses, requestID)
	rm.asyncLoader.CleanupRequest(requestID)
	if ok {
		for _, onTerminated := range ipr.onTerminated {
			select {
			case <-rm.ctx.Done():
			case onTerminated <- nil:
			}
		}
	}
}

func (rm *RequestManager) cancelRequest(requestID graphsync.RequestID, isPause bool, onTerminated chan<- error, terminalError error) {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[requestID]
	if !ok {
		if onTerminated != nil {
			select {
			case onTerminated <- graphsync.RequestNotFoundErr{}:
			case <-rm.ctx.Done():
			}
		}
		return
	}

	if onTerminated != nil {
		inProgressRequestStatus.onTerminated = append(inProgressRequestStatus.onTerminated, onTerminated)
	}
	if terminalError != nil {
		select {
		case inProgressRequestStatus.terminalError <- terminalError:
		default:
		}
	}

	rm.SendRequest(inProgressRequestStatus.p, gsmsg.CancelRequest(requestID))
	if isPause {
		inProgressRequestStatus.paused = true
	} else {
		inProgressRequestStatus.cancelFn()
	}
}

func (rm *RequestManager) processResponseMessage(p peer.ID, responses []gsmsg.GraphSyncResponse, blks []blocks.Block) {
	filteredResponses := rm.processExtensions(responses, p)
	filteredResponses = rm.filterResponsesForPeer(filteredResponses, p)
	rm.updateLastResponses(filteredResponses)
	responseMetadata := metadataForResponses(filteredResponses)
	rm.asyncLoader.ProcessResponse(responseMetadata, blks)
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
		rm.SendRequest(p, updateRequest)
	}
	if result.Err != nil {
		requestStatus, ok := rm.inProgressRequestStatuses[response.RequestID()]
		if !ok {
			return false
		}
		responseError := graphsync.RequestFailedUnknown.AsError()
		select {
		case requestStatus.terminalError <- responseError:
		default:
		}
		rm.SendRequest(p, gsmsg.CancelRequest(response.RequestID()))
		requestStatus.cancelFn()
		return false
	}
	return true
}

func (rm *RequestManager) processTerminations(responses []gsmsg.GraphSyncResponse) {
	for _, response := range responses {
		if response.Status().IsTerminal() {
			if response.Status().IsFailure() {
				requestStatus := rm.inProgressRequestStatuses[response.RequestID()]
				responseError := response.Status().AsError()
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

func (rm *RequestManager) unpause(id graphsync.RequestID, extensions []graphsync.ExtensionData) error {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[id]
	if !ok {
		return graphsync.RequestNotFoundErr{}
	}
	if !inProgressRequestStatus.paused {
		return errors.New("request is not paused")
	}
	inProgressRequestStatus.paused = false
	select {
	case <-inProgressRequestStatus.pauseMessages:
		rm.SendRequest(inProgressRequestStatus.p, gsmsg.UpdateRequest(id, extensions...))
		return nil
	case <-rm.ctx.Done():
		return errors.New("context cancelled")
	case inProgressRequestStatus.resumeMessages <- extensions:
		return nil
	}
}

func (rm *RequestManager) pause(id graphsync.RequestID) error {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[id]
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
