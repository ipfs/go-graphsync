package responsemanager

import (
	"context"
	"errors"
	"io"
	"math"
	"time"

	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/peertracker"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/peerstate"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/queryexecutor"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
)

// The code in this file implements the internal thread for the response manager.
// These functions can modify the internal state of the ResponseManager

// cleanupInProcessResponses cleans all responses when the response manager shuts down
func (rm *ResponseManager) cleanupInProcessResponses() {
	for _, response := range rm.inProgressResponses {
		response.cancelFn()
	}
}

// run runs the internal loop for the response manager
func (rm *ResponseManager) run() {
	defer rm.cleanupInProcessResponses()

	for {
		select {
		case <-rm.ctx.Done():
			return
		case message := <-rm.messages:
			message.handle(rm)
		}
	}
}

// processRequests is called to process new incoming requests from the network
func (rm *ResponseManager) processRequests(p peer.ID, requests []gsmsg.GraphSyncRequest) {
	ctx, messageSpan := otel.Tracer("graphsync").Start(
		rm.ctx,
		"processRequests",
		trace.WithAttributes(attribute.String("peerID", p.Pretty())),
	)
	defer messageSpan.End()

	for _, request := range requests {
		switch request.Type() {
		case graphsync.RequestTypeCancel:
			_ = rm.abortRequest(ctx, request.ID(), ipldutil.ContextCancelError{})
		case graphsync.RequestTypeUpdate:
			rm.processUpdate(ctx, request.ID(), request)
		case graphsync.RequestTypeNew:
			rm.newRequest(ctx, p, request)
		default:
			log.Errorf("unrecognized request type: %s", request.Type())
		}
	}
}

// processUpdate handles a graphsync update message
func (rm *ResponseManager) processUpdate(ctx context.Context, requestID graphsync.RequestID, update gsmsg.GraphSyncRequest) {
	response, ok := rm.inProgressResponses[requestID]
	if !ok || response.state == graphsync.CompletingSend {
		log.Warnf("received update for non existent request ID %s", requestID.String())
		return
	}

	_, span := otel.Tracer("graphsync").Start(
		trace.ContextWithSpan(ctx, response.span),
		"processUpdate",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithAttributes(
			attribute.String("id", update.ID().String()),
			attribute.StringSlice("extensions", func() []string {
				names := update.ExtensionNames()
				st := make([]string, 0, len(names))
				for _, n := range names {
					st = append(st, string(n))
				}
				return st
			}()),
		))

	defer span.End()

	if response.state != graphsync.Paused {
		response.updates = append(response.updates, update)
		select {
		case response.signals.UpdateSignal <- struct{}{}:
		default:
		}
		return
	} // else this is a paused response, so the update needs to be handled here and not in the executor
	result := rm.updateHooks.ProcessUpdateHooks(response.peer, response.request, update)
	_ = response.responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
		for _, extension := range result.Extensions {
			rb.SendExtensionData(extension)
		}
		if result.Err != nil {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
		}
		return nil
	})
	if result.Err != nil {
		response.state = graphsync.CompletingSend
		response.span.RecordError(result.Err)
		response.span.SetStatus(codes.Error, result.Err.Error())
		return
	}
	if result.Unpause {
		err := rm.unpauseRequest(requestID)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, result.Err.Error())
			log.Warnf("error unpausing request: %s", err.Error())
		}
	}
}

// abor request cancels an in progress request
func (rm *ResponseManager) abortRequest(ctx context.Context, requestID graphsync.RequestID, err error) error {
	response, ok := rm.inProgressResponses[requestID]
	if ok {
		rm.responseQueue.Remove(requestID, response.peer)
	}
	if !ok || (response.state == graphsync.CompletingSend && !errors.Is(err, queryexecutor.ErrNetworkError)) {
		return graphsync.RequestNotFoundErr{}
	}

	_, span := otel.Tracer("graphsync").Start(trace.ContextWithSpan(ctx, response.span),
		"abortRequest",
		trace.WithLinks(trace.LinkFromContext(ctx)),
	)
	defer span.End()
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	response.span.RecordError(err)
	response.span.SetStatus(codes.Error, err.Error())

	if response.state != graphsync.Running {
		if ipldutil.IsContextCancelErr(err) {
			response.responseStream.ClearRequest()
			rm.terminateRequest(requestID)
			rm.cancelledListeners.NotifyCancelledListeners(response.peer, response.request)
			return nil
		}
		if err == queryexecutor.ErrNetworkError {
			response.responseStream.ClearRequest()
			rm.terminateRequest(requestID)
			return nil
		}
		response.state = graphsync.CompletingSend
		return response.responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestCancelled)
			return nil
		})
	}
	select {
	case response.signals.ErrSignal <- err:
	default:
	}
	return nil
}

// new request sets up a new request
func (rm *ResponseManager) newRequest(ctx context.Context, p peer.ID, request gsmsg.GraphSyncRequest) {

	// protect the connection
	rm.connManager.Protect(p, request.ID().Tag())

	// Run request hooks
	// Don't use `ctx` which has the "message" trace, but rm.ctx for a fresh trace which allows
	// for a request hook to join this particular response up to an existing external trace
	result := rm.requestHooks.ProcessRequestHooks(p, request, rm.ctx)

	// setup request data

	rctx, responseSpan := otel.Tracer("graphsync").Start(
		result.Ctx,
		"response",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithAttributes(
			attribute.String("id", request.ID().String()),
			attribute.Int("priority", int(request.Priority())),
			attribute.String("root", request.Root().String()),
			attribute.StringSlice("extensions", func() []string {
				names := request.ExtensionNames()
				st := make([]string, 0, len(names))
				for _, n := range names {
					st = append(st, string(n))
				}
				return st
			}()),
		))

	rctx, cancelFn := context.WithCancel(rctx)

	subscriber := &subscriber{
		p:                     p,
		request:               request,
		requestCloser:         rm,
		blockSentListeners:    rm.blockSentListeners,
		completedListeners:    rm.completedListeners,
		networkErrorListeners: rm.networkErrorListeners,
		connManager:           rm.connManager,
	}

	linkSystem := result.CustomLinkSystem
	if linkSystem.StorageReadOpener == nil {
		linkSystem = rm.linkSystem
	}

	signals := queryexecutor.ResponseSignals{
		PauseSignal:  make(chan struct{}, 1),
		UpdateSignal: make(chan struct{}, 1),
		ErrSignal:    make(chan error, 1),
	}

	responseStream := rm.responseAssembler.NewStream(rctx, p, request.ID(), subscriber)

	response := &inProgressResponseStatus{
		ctx:            rctx,
		span:           responseSpan,
		cancelFn:       cancelFn,
		peer:           p,
		request:        request,
		linkSystem:     linkSystem,
		customChooser:  result.CustomChooser,
		maxLinks:       result.MaxLinks,
		signals:        signals,
		startTime:      time.Now(),
		responseStream: responseStream,
	}

	// setup query for processing
	err := prepareQuery(rctx, p, request, result, responseStream)

	// based on the results of previous hooks and preparing the query, we can now
	// decide what to do. the request will either be a rejection, paused, or ready to be queued
	// for processing
	if err != nil {
		// error occurred in request hooks or setting up the query --
		// now we're just waiting for the error response to finish sending
		// termination will happen when the message subscriber sees the termination
		// response has been sent (or had a network failure)
		response.state = graphsync.CompletingSend
		response.span.RecordError(err)
		response.span.SetStatus(codes.Error, err.Error())
	} else if result.IsPaused {
		// if  the request is paused, don't queue it. just leave in place
		response.state = graphsync.Paused
	} else {
		// no error and the request is not paused, queue for procesisng
		response.state = graphsync.Queued
		// TODO: Use a better work estimation metric.
		rm.responseQueue.PushTask(p, peertask.Task{Topic: request.ID(), Priority: int(request.Priority()), Work: 1})
	}

	// save request state
	log.Infow("graphsync request initiated", "request id", request.ID().String(), "peer", p, "root", request.Root())
	ipr, ok := rm.inProgressResponses[request.ID()]
	if ok && ipr.state == graphsync.Running {
		log.Warnf("there is an identical request already in progress", "request id", request.ID().String(), "peer", p)
	}

	rm.inProgressResponses[request.ID()] = response
}

func (rm *ResponseManager) taskDataForKey(requestID graphsync.RequestID) queryexecutor.ResponseTask {
	response, hasResponse := rm.inProgressResponses[requestID]
	if !hasResponse || response.state == graphsync.CompletingSend {
		return queryexecutor.ResponseTask{Empty: true}
	}
	log.Infow("graphsync response processing begins", "request id", requestID.String(), "peer", response.peer, "total time", time.Since(response.startTime))

	if response.traverser == nil {
		// this is the first time this request has started processing, so call request processing listerners
		inProgressCount := len(rm.inProgressResponses)
		rm.requestProcessingListeners.NotifyRequestProcessingListeners(response.peer, response.request, inProgressCount)

		// setup traversal
		rootLink := cidlink.Link{Cid: response.request.Root()}

		var budget *traversal.Budget
		maxLinks := rm.maxLinksPerRequest
		if maxLinks == 0 || (response.maxLinks != 0 && response.maxLinks < maxLinks) {
			// take the lowest nonzero budget (global or per-request)
			maxLinks = response.maxLinks
		}
		if maxLinks > 0 {
			budget = &traversal.Budget{
				NodeBudget: math.MaxInt64,
				LinkBudget: int64(maxLinks),
			}
		}
		traverser := ipldutil.TraversalBuilder{
			Root:          rootLink,
			Selector:      response.request.Selector(),
			LinkSystem:    response.linkSystem,
			Chooser:       response.customChooser,
			Budget:        budget,
			PanicCallback: rm.panicCallback,
			Visitor: func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error {
				if vr != traversal.VisitReason_SelectionMatch {
					return nil
				}
				if lbn, ok := n.(datamodel.LargeBytesNode); ok {
					s, err := lbn.AsLargeBytes()
					if err != nil {
						log.Warnf("error %s in AsLargeBytes at path %s", err.Error(), p.Path)
					}
					_, err = io.Copy(io.Discard, s)
					if err != nil {
						log.Warnf("error %s reading bytes from reader at path %s", err.Error(), p.Path)
					}
				}
				return nil
			},
		}.Start(response.ctx)

		response.traverser = traverser
	}
	response.state = graphsync.Running
	return queryexecutor.ResponseTask{
		Ctx:            response.ctx,
		Span:           response.span,
		Empty:          false,
		Request:        response.request,
		Loader:         response.linkSystem.StorageReadOpener,
		Traverser:      response.traverser,
		Signals:        response.signals,
		ResponseStream: response.responseStream,
	}
}

// start task is called when the request reaches the top of the execution queue and is
// ready to be processed.
func (rm *ResponseManager) startTask(task *peertask.Task, p peer.ID) queryexecutor.ResponseTask {
	requestID := task.Topic.(graphsync.RequestID)
	taskData := rm.taskDataForKey(requestID)
	if taskData.Empty {
		rm.responseQueue.TaskDone(p, task)
	}

	return taskData
}

func (rm *ResponseManager) terminateRequest(requestID graphsync.RequestID) {
	ipr, ok := rm.inProgressResponses[requestID]
	if !ok {
		return
	}
	rm.connManager.Unprotect(ipr.peer, requestID.Tag())
	delete(rm.inProgressResponses, requestID)
	ipr.cancelFn()
	ipr.span.End()
}

func (rm *ResponseManager) finishTask(task *peertask.Task, p peer.ID, err error) {
	requestID := task.Topic.(graphsync.RequestID)
	rm.responseQueue.TaskDone(p, task)
	response, ok := rm.inProgressResponses[requestID]
	if !ok {
		return
	}
	if _, ok := err.(hooks.ErrPaused); ok {
		response.state = graphsync.Paused
		return
	}
	log.Infow("graphsync response processing complete (messages stil sending)", "request id", requestID.String(), "peer", p, "total time", time.Since(response.startTime))

	if err != nil {
		response.span.RecordError(err)
		response.span.SetStatus(codes.Error, err.Error())
		log.Infof("response failed: %w", err)
	}

	if ipldutil.IsContextCancelErr(err) {
		rm.cancelledListeners.NotifyCancelledListeners(p, response.request)
		rm.terminateRequest(requestID)
		return
	}

	if err == queryexecutor.ErrNetworkError {
		rm.terminateRequest(requestID)
		return
	}

	response.state = graphsync.CompletingSend
}

func (rm *ResponseManager) getUpdates(requestID graphsync.RequestID) []gsmsg.GraphSyncRequest {
	response, ok := rm.inProgressResponses[requestID]
	if !ok {
		return nil
	}
	updates := response.updates
	response.updates = nil
	return updates
}

func (rm *ResponseManager) pauseRequest(requestID graphsync.RequestID) error {
	inProgressResponse, ok := rm.inProgressResponses[requestID]
	if !ok || inProgressResponse.state == graphsync.CompletingSend {
		return graphsync.RequestNotFoundErr{}
	}
	if inProgressResponse.state == graphsync.Paused {
		return errors.New("request is already paused")
	}
	select {
	case inProgressResponse.signals.PauseSignal <- struct{}{}:
	default:
	}
	return nil
}

func (rm *ResponseManager) unpauseRequest(requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	inProgressResponse, ok := rm.inProgressResponses[requestID]
	if !ok {
		return graphsync.RequestNotFoundErr{}
	}
	if inProgressResponse.state != graphsync.Paused {
		return errors.New("request is not paused")
	}
	inProgressResponse.state = graphsync.Queued
	if len(extensions) > 0 {
		_ = inProgressResponse.responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
			for _, extension := range extensions {
				rb.SendExtensionData(extension)
			}
			return nil
		})
	}
	rm.responseQueue.PushTask(inProgressResponse.peer, peertask.Task{Topic: requestID, Priority: math.MaxInt32, Work: 1})
	return nil
}

func (rm *ResponseManager) updateRequest(requestID graphsync.RequestID, extensions []graphsync.ExtensionData) error {
	inProgressResponse, ok := rm.inProgressResponses[requestID]
	if !ok {
		return graphsync.RequestNotFoundErr{}
	}
	_ = inProgressResponse.responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
		rb.SendUpdates(extensions)
		return nil
	})

	return nil
}

func (rm *ResponseManager) peerState(p peer.ID) peerstate.PeerState {
	var peerState peerstate.PeerState
	rm.responseQueue.WithPeerTopics(p, func(peerTopics *peertracker.PeerTrackerTopics) {
		requestStates := make(graphsync.RequestStates)
		for key, ipr := range rm.inProgressResponses {
			if ipr.peer == p {
				requestStates[key] = ipr.state
			}
		}
		peerState = peerstate.PeerState{RequestStates: requestStates, TaskQueueState: fromPeerTopics(peerTopics)}
	})
	return peerState
}

func fromPeerTopics(pt *peertracker.PeerTrackerTopics) peerstate.TaskQueueState {
	if pt == nil {
		return peerstate.TaskQueueState{}
	}
	active := make([]graphsync.RequestID, 0, len(pt.Active))
	for _, topic := range pt.Active {
		active = append(active, topic.(graphsync.RequestID))
	}
	pending := make([]graphsync.RequestID, 0, len(pt.Pending))
	for _, topic := range pt.Pending {
		pending = append(pending, topic.(graphsync.RequestID))
	}
	return peerstate.TaskQueueState{
		Active:  active,
		Pending: pending,
	}
}
