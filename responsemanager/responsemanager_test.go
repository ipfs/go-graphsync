package responsemanager

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/peertracker"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	graphsync "github.com/filecoin-project/boost-graphsync"
	"github.com/filecoin-project/boost-graphsync/cidset"
	"github.com/filecoin-project/boost-graphsync/dedupkey"
	"github.com/filecoin-project/boost-graphsync/donotsendfirstblocks"
	"github.com/filecoin-project/boost-graphsync/listeners"
	gsmsg "github.com/filecoin-project/boost-graphsync/message"
	"github.com/filecoin-project/boost-graphsync/messagequeue"
	"github.com/filecoin-project/boost-graphsync/notifications"
	"github.com/filecoin-project/boost-graphsync/persistenceoptions"
	"github.com/filecoin-project/boost-graphsync/responsemanager/hooks"
	"github.com/filecoin-project/boost-graphsync/responsemanager/queryexecutor"
	"github.com/filecoin-project/boost-graphsync/responsemanager/responseassembler"
	"github.com/filecoin-project/boost-graphsync/selectorvalidator"
	"github.com/filecoin-project/boost-graphsync/taskqueue"
	"github.com/filecoin-project/boost-graphsync/testutil"
)

func TestIncomingQuery(t *testing.T) {

	td := newTestData(t)
	defer td.cancel()
	blks := td.blockChain.AllBlocks()

	responseManager := td.newResponseManager()

	completedResponse := make(chan struct{}, 1)
	td.completedListeners.Register(func(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
		completedResponse <- struct{}{}
	})

	_, testSpan := otel.Tracer("graphsync").Start(td.ctx, "TestIncomingQuery")

	type queuedHook struct {
		p       peer.ID
		request graphsync.RequestData
	}
	qhc := make(chan *queuedHook, 1)
	td.requestQueuedHooks.Register(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.RequestQueuedHookActions) {
		td.connManager.AssertProtectedWithTags(t, p, request.ID().Tag())
		hookActions.AugmentContext(func(reqCtx context.Context) context.Context {
			return trace.ContextWithSpan(reqCtx, testSpan)
		})
		qhc <- &queuedHook{
			p:       p,
			request: request,
		}
	})
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	responseManager.Startup()

	responseManager.ProcessRequests(td.ctx, td.p, td.requests)
	for i := 0; i < len(blks); i++ {
		td.assertSendBlock()
	}
	td.assertCompleteRequestWith(graphsync.RequestCompletedFull)

	testSpan.End()
	td.taskqueue.WaitForNoActiveTasks()
	testutil.AssertDoesReceive(td.ctx, t, completedResponse, "request never completed")

	// ensure request queued hook fires.
	out := <-qhc
	require.Equal(t, td.p, out.p)
	require.Equal(t, out.request.ID(), td.requestID)
	td.connManager.RefuteProtected(t, td.p)

	tracing := td.collectTracing(t)
	require.ElementsMatch(t, append(append(
		[]string{"processRequests(0)"},
		testutil.RepeatTraceStrings("TestIncomingQuery(0)->response(0)->executeTask(0)->processBlock({})->loadBlock(0)", td.blockChainLength)...),
		testutil.RepeatTraceStrings("TestIncomingQuery(0)->response(0)->executeTask(0)->processBlock({})->sendBlock(0)->processBlockHooks(0)", td.blockChainLength)..., // half of the full chain
	), tracing.TracesToStrings())
	messageSpan := tracing.FindSpanByTraceString("processRequests(0)")
	responseSpan := tracing.FindSpanByTraceString("TestIncomingQuery(0)->response(0)")
	require.Len(t, responseSpan.Links, 1)
	require.Equal(t, responseSpan.Links[0].SpanContext.SpanID(), messageSpan.SpanContext.SpanID())
}

func TestCancellationQueryInProgress(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	responseManager := td.newResponseManager()
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	// This block hook is simply used to pause block hook processing after 1 block until the cancel command is sent
	blkCount := 0
	waitForCancel := make(chan struct{})
	td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		if blkCount == 1 {
			<-waitForCancel
		}
		blkCount++
	})
	cancelledListenerCalled := make(chan struct{}, 1)
	td.cancelledListeners.Register(func(p peer.ID, request graphsync.RequestData) {
		cancelledListenerCalled <- struct{}{}
	})
	responseManager.Startup()
	responseManager.ProcessRequests(td.ctx, td.p, td.requests)

	// read one block
	td.assertSendBlock()

	// send a cancellation
	cancelRequests := []gsmsg.GraphSyncRequest{
		gsmsg.NewCancelRequest(td.requestID),
	}
	responseManager.ProcessRequests(td.ctx, td.p, cancelRequests)
	responseManager.synchronize()
	close(waitForCancel)

	testutil.AssertDoesReceive(td.ctx, t, cancelledListenerCalled, "should call cancelled listener")
	td.connManager.RefuteProtected(t, td.p)

	td.assertRequestCleared()

	tracing := td.collectTracing(t)
	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "processRequests(0)")
	require.Contains(t, traceStrings, "response(0)->abortRequest(0)")
	require.Contains(t, traceStrings, "processRequests(1)")
	message0Span := tracing.FindSpanByTraceString("processRequests(0)")
	message1Span := tracing.FindSpanByTraceString("processRequests(1)")
	responseSpan := tracing.FindSpanByTraceString("response(0)")
	abortRequestSpan := tracing.FindSpanByTraceString("response(0)->abortRequest(0)")
	// response(0) originates in processRequests(0)
	require.Len(t, responseSpan.Links, 1)
	require.Equal(t, responseSpan.Links[0].SpanContext.SpanID(), message0Span.SpanContext.SpanID())
	// response(0)->abortRequest(0) occurs thanks to processRequests(1)
	require.Len(t, abortRequestSpan.Links, 1)
	require.Equal(t, abortRequestSpan.Links[0].SpanContext.SpanID(), message1Span.SpanContext.SpanID())
}

func TestCancellationViaCommand(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	responseManager := td.newResponseManager()
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	// This block hook is simply used to pause block hook processing after 1 block until the cancel command is sent
	blkCount := 0
	waitForCancel := make(chan struct{})
	td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		if blkCount == 1 {
			<-waitForCancel
		}
		blkCount++
	})
	responseManager.Startup()
	responseManager.ProcessRequests(td.ctx, td.p, td.requests)

	// read one block
	td.assertSendBlock()

	// send a cancellation
	err := responseManager.CancelResponse(td.ctx, td.requestID)
	require.NoError(t, err)
	close(waitForCancel)

	td.assertCompleteRequestWith(graphsync.RequestCancelled)
	td.connManager.RefuteProtected(t, td.p)
}

func TestEarlyCancellation(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	// we're not testing the queryexeuctor or taskqueue here, we're testing
	// that cancellation inside the responsemanager itself is properly
	// activated, so we won't let responses get far enough to race our
	// cancellation
	responseManager := td.nullTaskQueueResponseManager()
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	responseManager.Startup()
	responseManager.ProcessRequests(td.ctx, td.p, td.requests)
	responseManager.synchronize()
	td.connManager.AssertProtectedWithTags(t, td.p, td.requests[0].ID().Tag())

	// send a cancellation
	cancelRequests := []gsmsg.GraphSyncRequest{
		gsmsg.NewCancelRequest(td.requestID),
	}
	responseManager.ProcessRequests(td.ctx, td.p, cancelRequests)

	responseManager.synchronize()

	td.assertNoResponses()
	td.connManager.RefuteProtected(t, td.p)
}

func TestStats(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	// we're not testing the queryexeuctor or taskqueue here, we're testing
	// that cancellation inside the responsemanager itself is properly
	// activated, so we won't let responses get far enough to race our
	// cancellation
	responseManager := td.nullTaskQueueResponseManager()
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	responseManager.Startup()

	p1 := td.p
	reqid1 := td.requestID
	req1 := td.requests

	p2 := testutil.GeneratePeers(1)[0]
	reqid2 := graphsync.NewRequestID()
	req2 := []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(reqid2, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0), td.extension),
	}

	responseManager.ProcessRequests(td.ctx, p1, req1)
	responseManager.ProcessRequests(td.ctx, p2, req2)

	peerState := responseManager.PeerState(p1)
	require.Len(t, peerState.RequestStates, 1)
	require.Equal(t, peerState.RequestStates[reqid1], graphsync.Queued)
	require.Len(t, peerState.Pending, 1)
	require.Equal(t, peerState.Pending[0], reqid1)
	require.Len(t, peerState.Active, 0)
	// no inconsistencies
	require.Len(t, peerState.Diagnostics(), 0)
	peerState = responseManager.PeerState(p2)
	require.Len(t, peerState.RequestStates, 1)
	require.Equal(t, peerState.RequestStates[reqid2], graphsync.Queued)
	require.Len(t, peerState.Pending, 1)
	require.Equal(t, peerState.Pending[0], reqid2)
	require.Len(t, peerState.Active, 0)
	// no inconsistencies
	require.Len(t, peerState.Diagnostics(), 0)

}
func TestMissingContent(t *testing.T) {
	t.Run("missing root block", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		// delete the root block
		delete(td.blockStore, cidlink.Link{Cid: td.requests[0].Root()})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestFailedContentNotFound)
	})

	t.Run("missing other block", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		// delete a block that isn't the root
		for link := range td.blockStore {
			if link.String() != td.requests[0].Root().String() {
				delete(td.blockStore, link)
				break
			}
		}
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestCompletedPartial)
	})
}

func TestValidationAndExtensions(t *testing.T) {
	t.Run("on its own, should fail validation", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestRejected)
		td.connManager.RefuteProtected(t, td.p)
	})

	t.Run("if non validating hook succeeds, does not pass validation", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			td.connManager.AssertProtectedWithTags(t, td.p, td.requests[0].ID().Tag())
			hookActions.SendExtensionData(td.extensionResponse)
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestRejected)
		td.assertReceiveExtensionResponse()
		td.connManager.RefuteProtected(t, td.p)
	})

	t.Run("if validating hook succeeds, should pass validation", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			td.connManager.AssertProtectedWithTags(t, td.p, td.requests[0].ID().Tag())
			hookActions.ValidateRequest()
			hookActions.SendExtensionData(td.extensionResponse)
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		td.assertReceiveExtensionResponse()
		td.connManager.RefuteProtected(t, td.p)
	})

	t.Run("if any hook fails, should fail", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.SendExtensionData(td.extensionResponse)
			hookActions.TerminateWithError(errors.New("everything went to crap"))
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestFailedUnknown)
		td.assertReceiveExtensionResponse()
	})

	t.Run("hooks can be unregistered", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		unregister := td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
			hookActions.SendExtensionData(td.extensionResponse)
		})

		// hook validates request
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		td.assertReceiveExtensionResponse()

		// unregister
		unregister()

		// now same request should fail
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestRejected)
	})

	t.Run("hooks can alter the loader", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.alternateLoaderResponseManager()
		responseManager.Startup()
		// add validating hook -- so the request SHOULD succeed
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})

		// request fails with base loader reading from block store that's missing data
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestFailedContentNotFound)

		err := td.peristenceOptions.Register("chainstore", td.persistence)
		require.NoError(t, err)
		// register hook to use different loader
		_ = td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			if _, found := requestData.Extension(td.extensionName); found {
				hookActions.UsePersistenceOption("chainstore")
				hookActions.SendExtensionData(td.extensionResponse)
			}
		})
		// hook uses different loader that should make request succeed
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		td.assertReceiveExtensionResponse()
	})

	t.Run("hooks can alter the node builder chooser", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()

		customChooserCallCount := 0
		customChooser := func(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
			customChooserCallCount++
			return basicnode.Prototype.Any, nil
		}

		// add validating hook -- so the request SHOULD succeed
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})

		// with default chooser, customer chooser not called
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		require.Equal(t, 0, customChooserCallCount)

		// register hook to use custom chooser
		_ = td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			if _, found := requestData.Extension(td.extensionName); found {
				hookActions.UseLinkTargetNodePrototypeChooser(customChooser)
				hookActions.SendExtensionData(td.extensionResponse)
			}
		})

		// verify now that request succeeds and uses custom chooser
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		td.assertReceiveExtensionResponse()
		require.Equal(t, 5, customChooserCallCount)
	})

	t.Run("do-not-send-cids extension", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		set := cid.NewSet()
		blks := td.blockChain.Blocks(0, 5)
		for _, blk := range blks {
			set.Add(blk.Cid())
		}
		data := cidset.EncodeCidSet(set)
		requests := []gsmsg.GraphSyncRequest{
			gsmsg.NewRequest(td.requestID, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0),
				graphsync.ExtensionData{
					Name: graphsync.ExtensionDoNotSendCIDs,
					Data: data,
				}),
		}
		responseManager.ProcessRequests(td.ctx, td.p, requests)
		td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		td.assertIgnoredCids(set)
	})

	t.Run("do-not-send-first-blocks extension", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		data := donotsendfirstblocks.EncodeDoNotSendFirstBlocks(4)
		requests := []gsmsg.GraphSyncRequest{
			gsmsg.NewRequest(td.requestID, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0),
				graphsync.ExtensionData{
					Name: graphsync.ExtensionsDoNotSendFirstBlocks,
					Data: data,
				}),
		}
		responseManager.ProcessRequests(td.ctx, td.p, requests)
		td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		td.assertSkippedFirstBlocks(4)
	})

	t.Run("dedup-by-key extension", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		data, err := dedupkey.EncodeDedupKey("applesauce")
		require.NoError(t, err)
		requests := []gsmsg.GraphSyncRequest{
			gsmsg.NewRequest(td.requestID, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0),
				graphsync.ExtensionData{
					Name: graphsync.ExtensionDeDupByKey,
					Data: data,
				}),
		}
		responseManager.ProcessRequests(td.ctx, td.p, requests)
		td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		td.assertDedupKey("applesauce")
	})

	t.Run("test pause/resume", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
			hookActions.PauseResponse()
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertPausedRequest()
		td.assertRequestDoesNotCompleteWhilePaused()
		testutil.AssertChannelEmpty(t, td.sentResponses, "should not send more blocks")
		err := responseManager.UnpauseResponse(td.ctx, td.requestID)
		require.NoError(t, err)
		td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
	})

	t.Run("test block hook processing", func(t *testing.T) {
		t.Run("can send extension data", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				hookActions.SendExtensionData(td.extensionResponse)
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
			for i := 0; i < td.blockChainLength; i++ {
				td.assertReceiveExtensionResponse()
			}
		})

		t.Run("can send errors", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				hookActions.TerminateWithError(errors.New("failed"))
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertCompleteRequestWith(graphsync.RequestFailedUnknown)
		})

		t.Run("can pause/unpause", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			blkIndex := 0
			blockCount := 3
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				blkIndex++
				if blkIndex == blockCount {
					hookActions.PauseResponse()
				}
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertRequestDoesNotCompleteWhilePaused()
			td.verifyNResponses(blockCount)
			td.assertPausedRequest()
			err := responseManager.UnpauseResponse(td.ctx, td.requestID, td.extensionResponse)
			require.NoError(t, err)
			td.assertReceiveExtensionResponse()
			td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		})

		t.Run("can pause/unpause externally", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			blkIndex := 0
			blockCount := 3
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				blkIndex++
				if blkIndex == blockCount {
					err := responseManager.PauseResponse(td.ctx, requestData.ID())
					require.NoError(t, err)
				}
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertRequestDoesNotCompleteWhilePaused()
			td.verifyNResponses(blockCount + 1)
			td.assertPausedRequest()
			err := responseManager.UnpauseResponse(td.ctx, td.requestID)
			require.NoError(t, err)
			td.verifyNResponses(td.blockChainLength - (blockCount + 1))
			td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		})

		t.Run("if started paused, unpausing always works", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			advance := make(chan struct{})
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
				hookActions.PauseResponse()
				close(advance)
			})
			go func() {
				<-advance
				err := responseManager.UnpauseResponse(td.ctx, td.requestID)
				require.NoError(t, err)
			}()
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertPausedRequest()
			td.verifyNResponses(td.blockChainLength)
			td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		})
	})

	t.Run("test update hook processing", func(t *testing.T) {
		t.Run("can pause/unpause", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			blkIndex := 0
			blockCount := 3
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				blkIndex++
				if blkIndex == blockCount {
					hookActions.PauseResponse()
				}
			})
			td.updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
				if _, found := updateData.Extension(td.extensionName); found {
					hookActions.UnpauseResponse()
				}
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertRequestDoesNotCompleteWhilePaused()
			td.verifyNResponses(blockCount)
			td.assertPausedRequest()

			responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)
			td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
		})

		t.Run("can send extension data", func(t *testing.T) {
			t.Run("when unpaused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := td.newResponseManager()
				responseManager.Startup()
				td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
				})
				blkIndex := 0
				blockCount := 3
				wait := make(chan struct{})
				sent := make(chan struct{})
				td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					blkIndex++
					if blkIndex == blockCount {
						close(sent)
						<-wait
					}
				})
				td.updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					if _, found := updateData.Extension(td.extensionName); found {
						hookActions.SendExtensionData(td.extensionResponse)
					}
				})
				responseManager.ProcessRequests(td.ctx, td.p, td.requests)
				testutil.AssertDoesReceive(td.ctx, t, sent, "sends blocks")
				responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)
				responseManager.synchronize()
				close(wait)
				td.assertCompleteRequestWith(graphsync.RequestCompletedFull)
				td.assertReceiveExtensionResponse()
			})

			t.Run("when paused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := td.newResponseManager()
				responseManager.Startup()
				td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
				})
				blkIndex := 0
				blockCount := 3
				td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					blkIndex++
					if blkIndex == blockCount {
						hookActions.PauseResponse()
					}
				})
				td.updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					if _, found := updateData.Extension(td.extensionName); found {
						hookActions.SendExtensionData(td.extensionResponse)
					}
				})
				responseManager.ProcessRequests(td.ctx, td.p, td.requests)
				td.verifyNResponses(blockCount)
				td.assertPausedRequest()

				// send update
				responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)

				// receive data
				td.assertReceiveExtensionResponse()

				// should still be paused
				td.assertRequestDoesNotCompleteWhilePaused()
			})
		})

		t.Run("can send errors", func(t *testing.T) {
			t.Run("when unpaused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := td.newResponseManager()
				responseManager.Startup()
				td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
				})
				blkIndex := 0
				blockCount := 3
				wait := make(chan struct{})
				sent := make(chan struct{})
				td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					blkIndex++
					if blkIndex == blockCount {
						close(sent)
						<-wait
					}
				})
				td.updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					if _, found := updateData.Extension(td.extensionName); found {
						hookActions.TerminateWithError(errors.New("something went wrong"))
					}
				})
				responseManager.ProcessRequests(td.ctx, td.p, td.requests)
				testutil.AssertDoesReceive(td.ctx, t, sent, "sends blocks")
				responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)
				responseManager.synchronize()
				close(wait)
				td.assertCompleteRequestWith(graphsync.RequestFailedUnknown)
			})

			t.Run("when paused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := td.newResponseManager()
				responseManager.Startup()
				td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
				})
				blkIndex := 0
				blockCount := 3
				td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					blkIndex++
					if blkIndex == blockCount {
						hookActions.PauseResponse()
					}
				})
				td.updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					if _, found := updateData.Extension(td.extensionName); found {
						hookActions.TerminateWithError(errors.New("something went wrong"))
					}
				})
				responseManager.ProcessRequests(td.ctx, td.p, td.requests)
				td.verifyNResponses(blockCount)
				td.assertPausedRequest()

				// send update
				responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)
				td.assertCompleteRequestWith(graphsync.RequestFailedUnknown)

				// cannot unpause
				err := responseManager.UnpauseResponse(td.ctx, td.requestID)
				require.Error(t, err)
			})
		})

	})
}

func TestNetworkErrors(t *testing.T) {
	t.Run("network error final status - success", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.verifyNResponses(td.blockChainLength)
		td.assertOnlyCompleteProcessingWith(graphsync.RequestCompletedFull)
		err := errors.New("something went wrong")
		td.notifyStatusMessagesNetworkError(err)
		td.assertNetworkErrors(err, 1)
		td.assertNoCompletedResponseStatuses()
	})

	t.Run("network error final status - failure", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertOnlyCompleteProcessingWith(graphsync.RequestRejected)
		err := errors.New("something went wrong")
		td.notifyStatusMessagesNetworkError(err)
		td.assertNetworkErrors(err, 1)
		td.assertNoCompletedResponseStatuses()
	})

	t.Run("network error block send", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertSendBlock()
		err := errors.New("something went wrong")
		td.notifyBlockSendsNetworkError(err)
		td.assertHasNetworkErrors(err)
		td.assertNoCompletedResponseStatuses()
	})

	t.Run("network error while paused", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		blkIndex := 0
		blockCount := 3
		td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			blkIndex++
			if blkIndex == blockCount {
				hookActions.PauseResponse()
			}
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertRequestDoesNotCompleteWhilePaused()
		td.verifyNResponsesOnlyProcessing(blockCount)
		td.assertPausedRequest()
		err := errors.New("something went wrong")
		td.notifyBlockSendsNetworkError(err)
		td.assertNetworkErrors(err, 1)
		td.assertRequestCleared()
		err = responseManager.UnpauseResponse(td.ctx, td.requestID, td.extensionResponse)
		require.Error(t, err)
	})
}

func TestUpdateResponse(t *testing.T) {
	t.Run("while unpaused", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertSendBlock()
		responseManager.synchronize()

		// send an update with some custom extensions
		ext1 := graphsync.ExtensionData{Name: graphsync.ExtensionName("grip grop"), Data: basicnode.NewString("flim flam, blim blam")}
		ext2 := graphsync.ExtensionData{Name: graphsync.ExtensionName("Humpty/Dumpty"), Data: basicnode.NewInt(101)}

		responseManager.UpdateResponse(td.ctx, td.requestID, ext1, ext2)

		var receivedExtension sentExtension
		testutil.AssertReceive(td.ctx, td.t, td.sentExtensions, &receivedExtension, "should send first extension response")
		require.Equal(td.t, ext1, receivedExtension.extension, "incorrect first extension response sent")
		testutil.AssertReceive(td.ctx, td.t, td.sentExtensions, &receivedExtension, "should send second extension response")
		require.Equal(td.t, ext2, receivedExtension.extension, "incorrect second extension response sent")
		td.assertNoCompletedResponseStatuses()
	})

	t.Run("while paused", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		blkIndex := 0
		blockCount := 3
		td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			blkIndex++
			if blkIndex == blockCount {
				hookActions.PauseResponse()
			}
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertRequestDoesNotCompleteWhilePaused()
		td.verifyNResponsesOnlyProcessing(blockCount)
		td.assertPausedRequest()

		// send an update with some custom extensions
		ext1 := graphsync.ExtensionData{Name: graphsync.ExtensionName("grip grop"), Data: basicnode.NewString("flim flam, blim blam")}
		ext2 := graphsync.ExtensionData{Name: graphsync.ExtensionName("Humpty/Dumpty"), Data: basicnode.NewInt(101)}

		responseManager.UpdateResponse(td.ctx, td.requestID, ext1, ext2)
		responseManager.synchronize()

		var receivedExtension sentExtension
		testutil.AssertReceive(td.ctx, td.t, td.sentExtensions, &receivedExtension, "should send first extension response")
		require.Equal(td.t, ext1, receivedExtension.extension, "incorrect first extension response sent")
		testutil.AssertReceive(td.ctx, td.t, td.sentExtensions, &receivedExtension, "should send second extension response")
		require.Equal(td.t, ext2, receivedExtension.extension, "incorrect second extension response sent")
		td.assertNoCompletedResponseStatuses()
	})
}

type fakeResponseAssembler struct {
	transactionLk        *sync.Mutex
	sentResponses        chan sentResponse
	sentExtensions       chan sentExtension
	lastCompletedRequest chan completedRequest
	pausedRequests       chan pausedRequest
	clearedRequests      chan clearedRequest
	ignoredLinks         chan []ipld.Link
	skippedFirstBlocks   chan int64

	completedNotifications map[graphsync.RequestID]graphsync.ResponseStatusCode
	blkNotifications       map[graphsync.RequestID][]graphsync.BlockData
	notifeePublisher       *testutil.MockPublisher
	dedupKeys              chan string
	missingBlock           bool
}

func (fra *fakeResponseAssembler) NewStream(ctx context.Context, p peer.ID, requestID graphsync.RequestID, subscriber notifications.Subscriber) responseassembler.ResponseStream {
	fra.notifeePublisher.AddSubscriber(subscriber)
	return &fakeResponseStream{fra, requestID}
}

type fakeResponseStream struct {
	fra       *fakeResponseAssembler
	requestID graphsync.RequestID
}

func (frs *fakeResponseStream) Transaction(transaction responseassembler.Transaction) error {
	frs.fra.transactionLk.Lock()
	defer frs.fra.transactionLk.Unlock()
	frb := &fakeResponseBuilder{frs.requestID, frs.fra}
	return transaction(frb)
}

func (frs *fakeResponseStream) IgnoreBlocks(links []ipld.Link) {
	frs.fra.ignoredLinks <- links
}

func (frs *fakeResponseStream) SkipFirstBlocks(skipCount int64) {
	frs.fra.skippedFirstBlocks <- skipCount
}
func (frs *fakeResponseStream) DedupKey(key string) {
	frs.fra.dedupKeys <- key
}

func (frs *fakeResponseStream) ClearRequest() {
	frs.fra.clearRequest(frs.requestID)
}

type sentResponse struct {
	requestID graphsync.RequestID
	link      ipld.Link
	data      []byte
}

type sentExtension struct {
	requestID graphsync.RequestID
	extension graphsync.ExtensionData
}

type completedRequest struct {
	requestID graphsync.RequestID
	result    graphsync.ResponseStatusCode
}
type pausedRequest struct {
	requestID graphsync.RequestID
}

type clearedRequest struct {
	requestID graphsync.RequestID
}

type fakeBlkData struct {
	link ipld.Link
	size uint64
}

func (fbd fakeBlkData) Link() ipld.Link {
	return fbd.link
}

func (fbd fakeBlkData) BlockSize() uint64 {
	return fbd.size
}

func (fbd fakeBlkData) BlockSizeOnWire() uint64 {
	return fbd.size
}

func (fbd fakeBlkData) Index() int64 {
	return 0
}

func (fra *fakeResponseAssembler) sendResponse(
	requestID graphsync.RequestID,
	link ipld.Link,
	data []byte,
) graphsync.BlockData {
	fra.sentResponses <- sentResponse{requestID, link, data}
	if data == nil {
		fra.missingBlock = true
	}
	blkData := fakeBlkData{link, uint64(len(data))}
	fra.blkNotifications[requestID] = append(fra.blkNotifications[requestID], blkData)
	return blkData
}

func (fra *fakeResponseAssembler) sendExtensionData(
	requestID graphsync.RequestID,
	extension graphsync.ExtensionData,
) {
	fra.sentExtensions <- sentExtension{requestID, extension}
}

func (fra *fakeResponseAssembler) finishRequest(requestID graphsync.RequestID) graphsync.ResponseStatusCode {
	code := graphsync.RequestCompletedFull
	if fra.missingBlock {
		code = graphsync.RequestCompletedPartial
	}
	fra.missingBlock = false
	cr := completedRequest{requestID, code}
	fra.lastCompletedRequest <- cr
	fra.completedNotifications[requestID] = code
	return code
}

func (fra *fakeResponseAssembler) finishWithError(requestID graphsync.RequestID, status graphsync.ResponseStatusCode) {
	fra.missingBlock = false
	cr := completedRequest{requestID, status}
	fra.lastCompletedRequest <- cr
	fra.completedNotifications[requestID] = status
}

func (fra *fakeResponseAssembler) pauseRequest(requestID graphsync.RequestID) {
	fra.pausedRequests <- pausedRequest{requestID}
}

func (fra *fakeResponseAssembler) clearRequest(requestID graphsync.RequestID) {
	fra.missingBlock = false
	fra.clearedRequests <- clearedRequest{requestID}
}

type fakeResponseBuilder struct {
	requestID graphsync.RequestID
	fra       *fakeResponseAssembler
}

func (frb *fakeResponseBuilder) SendResponse(link ipld.Link, data []byte) graphsync.BlockData {
	return frb.fra.sendResponse(frb.requestID, link, data)
}

func (frb *fakeResponseBuilder) SendExtensionData(extension graphsync.ExtensionData) {
	frb.fra.sendExtensionData(frb.requestID, extension)
}

func (frb *fakeResponseBuilder) SendUpdates(extensions []graphsync.ExtensionData) {
	for _, ext := range extensions {
		frb.fra.sendExtensionData(frb.requestID, ext)
	}
}

func (frb *fakeResponseBuilder) FinishRequest() graphsync.ResponseStatusCode {
	return frb.fra.finishRequest(frb.requestID)
}

func (frb *fakeResponseBuilder) FinishWithError(status graphsync.ResponseStatusCode) {
	frb.fra.finishWithError(frb.requestID, status)
}

func (frb *fakeResponseBuilder) PauseRequest() {
	frb.fra.pauseRequest(frb.requestID)
}

func (frb *fakeResponseBuilder) Context() context.Context {
	return context.TODO()
}

type testData struct {
	ctx                       context.Context
	t                         *testing.T
	cancel                    context.CancelFunc
	blockStore                map[ipld.Link][]byte
	persistence               ipld.LinkSystem
	blockChainLength          int
	blockChain                *testutil.TestBlockChain
	completedRequestChan      chan completedRequest
	sentResponses             chan sentResponse
	sentExtensions            chan sentExtension
	pausedRequests            chan pausedRequest
	clearedRequests           chan clearedRequest
	completedNotifications    map[graphsync.RequestID]graphsync.ResponseStatusCode
	blkNotifications          map[graphsync.RequestID][]graphsync.BlockData
	ignoredLinks              chan []ipld.Link
	skippedFirstBlocks        chan int64
	dedupKeys                 chan string
	responseAssembler         *fakeResponseAssembler
	extensionData             datamodel.Node
	extensionName             graphsync.ExtensionName
	extension                 graphsync.ExtensionData
	extensionResponseData     datamodel.Node
	extensionResponse         graphsync.ExtensionData
	extensionUpdateData       datamodel.Node
	extensionUpdate           graphsync.ExtensionData
	requestID                 graphsync.RequestID
	requests                  []gsmsg.GraphSyncRequest
	updateRequests            []gsmsg.GraphSyncRequest
	p                         peer.ID
	peristenceOptions         *persistenceoptions.PersistenceOptions
	requestQueuedHooks        *hooks.IncomingRequestQueuedHooks
	requestHooks              *hooks.IncomingRequestHooks
	blockHooks                *hooks.OutgoingBlockHooks
	updateHooks               *hooks.RequestUpdatedHooks
	completedListeners        *listeners.CompletedResponseListeners
	cancelledListeners        *listeners.RequestorCancelledListeners
	blockSentListeners        *listeners.BlockSentListeners
	networkErrorListeners     *listeners.NetworkErrorListeners
	notifeePublisher          *testutil.MockPublisher
	blockSends                chan graphsync.BlockData
	completedResponseStatuses chan graphsync.ResponseStatusCode
	networkErrorChan          chan error
	allBlocks                 []blocks.Block
	connManager               *testutil.TestConnManager
	transactionLk             *sync.Mutex
	taskqueue                 *taskqueue.WorkerTaskQueue
	collectTracing            func(t *testing.T) *testutil.Collector
}

func newTestData(t *testing.T) testData {
	t.Helper()
	ctx := context.Background()
	td := testData{}
	td.t = t
	ctx, td.collectTracing = testutil.SetupTracing(ctx)
	td.ctx, td.cancel = context.WithTimeout(ctx, 10*time.Second)

	td.blockStore = make(map[ipld.Link][]byte)
	td.persistence = testutil.NewTestStore(td.blockStore)
	td.blockChainLength = 5
	td.blockChain = testutil.SetupBlockChain(ctx, t, td.persistence, 100, td.blockChainLength)

	td.completedRequestChan = make(chan completedRequest, 1)
	td.sentResponses = make(chan sentResponse, td.blockChainLength*2)
	td.sentExtensions = make(chan sentExtension, td.blockChainLength*2)
	td.pausedRequests = make(chan pausedRequest, 1)
	td.clearedRequests = make(chan clearedRequest, 1)
	td.ignoredLinks = make(chan []ipld.Link, 1)
	td.skippedFirstBlocks = make(chan int64, 1)
	td.dedupKeys = make(chan string, 1)
	td.blockSends = make(chan graphsync.BlockData, td.blockChainLength*2)
	td.completedResponseStatuses = make(chan graphsync.ResponseStatusCode, 1)
	td.networkErrorChan = make(chan error, td.blockChainLength*2)
	td.notifeePublisher = testutil.NewMockPublisher()
	td.transactionLk = &sync.Mutex{}
	td.completedNotifications = make(map[graphsync.RequestID]graphsync.ResponseStatusCode)
	td.blkNotifications = make(map[graphsync.RequestID][]graphsync.BlockData)
	td.responseAssembler = &fakeResponseAssembler{
		transactionLk:          td.transactionLk,
		lastCompletedRequest:   td.completedRequestChan,
		sentResponses:          td.sentResponses,
		sentExtensions:         td.sentExtensions,
		pausedRequests:         td.pausedRequests,
		clearedRequests:        td.clearedRequests,
		ignoredLinks:           td.ignoredLinks,
		skippedFirstBlocks:     td.skippedFirstBlocks,
		dedupKeys:              td.dedupKeys,
		notifeePublisher:       td.notifeePublisher,
		blkNotifications:       td.blkNotifications,
		completedNotifications: td.completedNotifications,
	}

	td.extensionData = basicnode.NewBytes(testutil.RandomBytes(100))
	td.extensionName = graphsync.ExtensionName("AppleSauce/McGee")
	td.extension = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionData,
	}
	td.extensionResponseData = basicnode.NewBytes(testutil.RandomBytes(100))
	td.extensionResponse = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionResponseData,
	}
	td.extensionUpdateData = basicnode.NewBytes(testutil.RandomBytes(100))
	td.extensionUpdate = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionUpdateData,
	}
	td.requestID = graphsync.NewRequestID()
	td.requests = []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(td.requestID, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0), td.extension),
	}
	td.updateRequests = []gsmsg.GraphSyncRequest{
		gsmsg.NewUpdateRequest(td.requestID, td.extensionUpdate),
	}
	td.p = testutil.GeneratePeers(1)[0]
	td.peristenceOptions = persistenceoptions.New()
	td.requestQueuedHooks = hooks.NewRequestQueuedHooks()
	td.requestHooks = hooks.NewRequestHooks(td.peristenceOptions)
	td.blockHooks = hooks.NewBlockHooks()
	td.updateHooks = hooks.NewUpdateHooks()
	td.completedListeners = listeners.NewCompletedResponseListeners()
	td.cancelledListeners = listeners.NewRequestorCancelledListeners()
	td.blockSentListeners = listeners.NewBlockSentListeners()
	td.networkErrorListeners = listeners.NewNetworkErrorListeners()
	td.taskqueue = taskqueue.NewTaskQueue(ctx)
	td.completedListeners.Register(func(p peer.ID, requestID graphsync.RequestData, status graphsync.ResponseStatusCode) {
		select {
		case td.completedResponseStatuses <- status:
		default:
		}
	})
	td.blockSentListeners.Register(func(p peer.ID, requestID graphsync.RequestData, blockData graphsync.BlockData) {
		select {
		case td.blockSends <- blockData:
		default:
		}
	})
	td.networkErrorListeners.Register(func(p peer.ID, requestID graphsync.RequestData, err error) {
		select {
		case td.networkErrorChan <- err:
		default:
		}
	})
	td.connManager = testutil.NewTestConnManager()
	return td
}

func (td *testData) newResponseManager() *ResponseManager {
	rm := New(td.ctx, td.persistence, td.responseAssembler, td.requestQueuedHooks, td.requestHooks, td.updateHooks, td.completedListeners, td.cancelledListeners, td.blockSentListeners, td.networkErrorListeners, td.connManager, 0, nil, td.taskqueue)
	queryExecutor := td.newQueryExecutor(rm)
	td.taskqueue.Startup(6, queryExecutor)
	return rm
}

func (td *testData) nullTaskQueueResponseManager() *ResponseManager {
	ntq := nullTaskQueue{tasksQueued: make(map[peer.ID][]peertask.Topic)}
	rm := New(td.ctx, td.persistence, td.responseAssembler, td.requestQueuedHooks, td.requestHooks, td.updateHooks, td.completedListeners, td.cancelledListeners, td.blockSentListeners, td.networkErrorListeners, td.connManager, 0, nil, ntq)
	return rm
}

func (td *testData) alternateLoaderResponseManager() *ResponseManager {
	obs := make(map[ipld.Link][]byte)
	persistence := testutil.NewTestStore(obs)
	rm := New(td.ctx, persistence, td.responseAssembler, td.requestQueuedHooks, td.requestHooks, td.updateHooks, td.completedListeners, td.cancelledListeners, td.blockSentListeners, td.networkErrorListeners, td.connManager, 0, nil, td.taskqueue)
	queryExecutor := td.newQueryExecutor(rm)
	td.taskqueue.Startup(6, queryExecutor)
	return rm
}

func (td *testData) newQueryExecutor(manager queryexecutor.Manager) *queryexecutor.QueryExecutor {
	return queryexecutor.New(td.ctx, manager, td.blockHooks, td.updateHooks)
}

func (td *testData) assertPausedRequest() {
	var pausedRequest pausedRequest
	testutil.AssertReceive(td.ctx, td.t, td.pausedRequests, &pausedRequest, "should pause request")
	td.taskqueue.WaitForNoActiveTasks()
}

func (td *testData) getAllBlocks() []blocks.Block {
	if td.allBlocks == nil {
		td.allBlocks = td.blockChain.AllBlocks()
	}
	return td.allBlocks
}

func (td *testData) verifyResponse(sentResponse sentResponse) {
	k := sentResponse.link.(cidlink.Link)
	blks := td.getAllBlocks()
	blockIndex := testutil.IndexOf(blks, k.Cid)
	require.NotEqual(td.t, blockIndex, -1, "sent incorrect link")
	require.Equal(td.t, blks[blockIndex].RawData(), sentResponse.data, "sent incorrect data")
	require.Equal(td.t, td.requestID, sentResponse.requestID, "has incorrect response id")
}

func (td *testData) assertSendBlock() {
	var sentResponse sentResponse
	testutil.AssertReceive(td.ctx, td.t, td.sentResponses, &sentResponse, "did not send response")
	td.verifyResponse(sentResponse)
}

func (td *testData) assertNoResponses() {
	timer := time.NewTimer(200 * time.Millisecond)
	// verify no responses processed
	testutil.AssertDoesReceiveFirst(td.t, timer.C, "should not process more responses", td.sentResponses, td.completedRequestChan)
}

func (td *testData) assertCompleteRequestWith(expectedCode graphsync.ResponseStatusCode) {
	td.assertOnlyCompleteProcessingWith(expectedCode)
	td.notifyStatusMessagesSent()
	var status graphsync.ResponseStatusCode
	testutil.AssertReceive(td.ctx, td.t, td.completedResponseStatuses, &status, "should receive status")
	require.Equal(td.t, expectedCode, status)
	td.taskqueue.WaitForNoActiveTasks()
}

func (td *testData) assertOnlyCompleteProcessingWith(expectedCode graphsync.ResponseStatusCode) {
	var lastRequest completedRequest
	testutil.AssertReceive(td.ctx, td.t, td.completedRequestChan, &lastRequest, "should complete request")
	require.Equal(td.t, expectedCode, lastRequest.result)
}

func (td *testData) assertRequestCleared() {
	testutil.AssertDoesReceive(td.ctx, td.t, td.clearedRequests, "should clear request")
}

func (td *testData) assertRequestDoesNotCompleteWhilePaused() {
	timer := time.NewTimer(100 * time.Millisecond)
	testutil.AssertDoesReceiveFirst(td.t, timer.C, "should not complete request while paused", td.completedRequestChan)
}

func (td *testData) assertReceiveExtensionResponse() {
	var receivedExtension sentExtension
	testutil.AssertReceive(td.ctx, td.t, td.sentExtensions, &receivedExtension, "should send extension response")
	require.Equal(td.t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")
}

func (td *testData) verifyNResponsesOnlyProcessing(blockCount int) {
	for i := 0; i < blockCount; i++ {
		testutil.AssertDoesReceive(td.ctx, td.t, td.sentResponses, "should sent block")
	}
	testutil.AssertChannelEmpty(td.t, td.sentResponses, "should not send more blocks")
}

func (td *testData) verifyNResponses(blockCount int) {
	td.verifyNResponsesOnlyProcessing(blockCount)
	td.notifyBlockSendsSent()
	for i := 0; i < blockCount; i++ {
		testutil.AssertDoesReceive(td.ctx, td.t, td.blockSends, "should sent block")
	}
	testutil.AssertChannelEmpty(td.t, td.blockSends, "should not send more blocks")
}

func (td *testData) assertDedupKey(key string) {
	var dedupKey string
	testutil.AssertReceive(td.ctx, td.t, td.dedupKeys, &dedupKey, "should dedup by key")
	require.Equal(td.t, key, dedupKey)
}

func (td *testData) assertIgnoredCids(set *cid.Set) {
	var lastLinks []ipld.Link
	testutil.AssertReceive(td.ctx, td.t, td.ignoredLinks, &lastLinks, "should send ignored links")
	require.Len(td.t, lastLinks, set.Len())
	for _, link := range lastLinks {
		require.True(td.t, set.Has(link.(cidlink.Link).Cid))
	}
}

func (td *testData) assertSkippedFirstBlocks(expectedSkipCount int64) {
	var skippedFirstBlocks int64
	testutil.AssertReceive(td.ctx, td.t, td.skippedFirstBlocks, &skippedFirstBlocks, "should skip blocks")
	require.Equal(td.t, expectedSkipCount, skippedFirstBlocks)
}

func (td *testData) notifyStatusMessagesSent() {
	td.transactionLk.Lock()
	td.notifeePublisher.PublishEvents(notifications.Topic(rand.Int31), []notifications.Event{
		messagequeue.Event{Name: messagequeue.Sent, Metadata: messagequeue.Metadata{ResponseCodes: td.completedNotifications}},
	})
	td.completedNotifications = make(map[graphsync.RequestID]graphsync.ResponseStatusCode)
	td.responseAssembler.completedNotifications = td.completedNotifications
	td.transactionLk.Unlock()
}

func (td *testData) notifyBlockSendsSent() {
	td.transactionLk.Lock()
	td.notifeePublisher.PublishEvents(notifications.Topic(graphsync.NewRequestID), []notifications.Event{
		messagequeue.Event{Name: messagequeue.Sent, Metadata: messagequeue.Metadata{BlockData: td.blkNotifications}},
	})
	td.blkNotifications = make(map[graphsync.RequestID][]graphsync.BlockData)
	td.responseAssembler.blkNotifications = td.blkNotifications
	td.transactionLk.Unlock()
}

func (td *testData) notifyStatusMessagesNetworkError(err error) {
	td.transactionLk.Lock()
	td.notifeePublisher.PublishEvents(notifications.Topic(rand.Int31), []notifications.Event{
		messagequeue.Event{Name: messagequeue.Error, Err: err, Metadata: messagequeue.Metadata{ResponseCodes: td.completedNotifications}},
	})
	td.completedNotifications = make(map[graphsync.RequestID]graphsync.ResponseStatusCode)
	td.responseAssembler.completedNotifications = td.completedNotifications
	td.transactionLk.Unlock()
}

func (td *testData) notifyBlockSendsNetworkError(err error) {
	td.transactionLk.Lock()
	td.notifeePublisher.PublishEvents(notifications.Topic(rand.Int31), []notifications.Event{
		messagequeue.Event{Name: messagequeue.Error, Err: err, Metadata: messagequeue.Metadata{BlockData: td.blkNotifications}},
	})
	td.blkNotifications = make(map[graphsync.RequestID][]graphsync.BlockData)
	td.responseAssembler.blkNotifications = td.blkNotifications
	td.transactionLk.Unlock()
}

func (td *testData) assertNoCompletedResponseStatuses() {
	testutil.AssertChannelEmpty(td.t, td.completedResponseStatuses, "should not send a complete notification")
}

func (td *testData) assertNetworkErrors(err error, count int) {
	for i := 0; i < count; i++ {
		td.assertHasNetworkErrors(err)
	}
	testutil.AssertChannelEmpty(td.t, td.networkErrorChan, "should not send more blocks")
}

func (td *testData) assertHasNetworkErrors(err error) {
	var receivedErr error
	testutil.AssertReceive(td.ctx, td.t, td.networkErrorChan, &receivedErr, "should sent block")
	require.EqualError(td.t, receivedErr, err.Error())
}

type nullTaskQueue struct {
	tasksQueued map[peer.ID][]peertask.Topic
}

func (ntq nullTaskQueue) PushTask(p peer.ID, task peertask.Task) {
	ntq.tasksQueued[p] = append(ntq.tasksQueued[p], task.Topic)
}

func (ntq nullTaskQueue) TaskDone(p peer.ID, task *peertask.Task) {}
func (ntq nullTaskQueue) Remove(t peertask.Topic, p peer.ID)      {}
func (ntq nullTaskQueue) Stats() graphsync.RequestStats           { return graphsync.RequestStats{} }
func (ntq nullTaskQueue) WithPeerTopics(p peer.ID, f func(*peertracker.PeerTrackerTopics)) {
	f(&peertracker.PeerTrackerTopics{Pending: ntq.tasksQueued[p]})
}

var _ taskqueue.TaskQueue = nullTaskQueue{}
