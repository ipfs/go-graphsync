package responsemanager

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-peertaskqueue/peertask"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/dedupkey"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	"github.com/ipfs/go-graphsync/responsemanager/persistenceoptions"
	"github.com/ipfs/go-graphsync/selectorvalidator"
	"github.com/ipfs/go-graphsync/testutil"
)

type fakeQueryQueue struct {
	popWait   sync.WaitGroup
	queriesLk sync.RWMutex
	queries   []*peertask.QueueTask
}

func (fqq *fakeQueryQueue) PushTasks(to peer.ID, tasks ...peertask.Task) {
	fqq.queriesLk.Lock()

	// This isn't quite right as the queue should deduplicate requests, but
	// it's good enough.
	for _, task := range tasks {
		fqq.queries = append(fqq.queries, peertask.NewQueueTask(task, to, time.Now()))
	}
	fqq.queriesLk.Unlock()
}

func (fqq *fakeQueryQueue) PopTasks(targetWork int) (peer.ID, []*peertask.Task, int) {
	fqq.popWait.Wait()
	fqq.queriesLk.Lock()
	defer fqq.queriesLk.Unlock()
	if len(fqq.queries) == 0 {
		return "", nil, -1
	}
	// We're not bothering to implement "work"
	task := fqq.queries[0]
	fqq.queries = fqq.queries[1:]
	return task.Target, []*peertask.Task{&task.Task}, 0
}

func (fqq *fakeQueryQueue) Remove(topic peertask.Topic, p peer.ID) {
	fqq.queriesLk.Lock()
	defer fqq.queriesLk.Unlock()
	for i, query := range fqq.queries {
		if query.Target == p && query.Topic == topic {
			fqq.queries = append(fqq.queries[:i], fqq.queries[i+1:]...)
		}
	}
}

func (fqq *fakeQueryQueue) TasksDone(to peer.ID, tasks ...*peertask.Task) {
	// We don't track active tasks so this is a no-op
}

func (fqq *fakeQueryQueue) ThawRound() {

}

type fakePeerManager struct {
	lastPeer           peer.ID
	peerResponseSender peerresponsemanager.PeerResponseSender
}

func (fpm *fakePeerManager) SenderForPeer(p peer.ID) peerresponsemanager.PeerResponseSender {
	fpm.lastPeer = p
	return fpm.peerResponseSender
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

type cancelledRequest struct {
	requestID graphsync.RequestID
}

type fakePeerResponseSender struct {
	sentResponses        chan sentResponse
	sentExtensions       chan sentExtension
	lastCompletedRequest chan completedRequest
	pausedRequests       chan pausedRequest
	cancelledRequests    chan cancelledRequest
	ignoredLinks         chan []ipld.Link
	dedupKeys            chan string
}

func (fprs *fakePeerResponseSender) Startup()  {}
func (fprs *fakePeerResponseSender) Shutdown() {}

type fakeBlkData struct {
	link ipld.Link
	size uint64
}

func (fprs *fakePeerResponseSender) IgnoreBlocks(requestID graphsync.RequestID, links []ipld.Link) {
	fprs.ignoredLinks <- links
}

func (fprs *fakePeerResponseSender) DedupKey(requestID graphsync.RequestID, key string) {
	fprs.dedupKeys <- key
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

func (fprs *fakePeerResponseSender) SendResponse(
	requestID graphsync.RequestID,
	link ipld.Link,
	data []byte,
) graphsync.BlockData {
	fprs.sentResponses <- sentResponse{requestID, link, data}
	return fakeBlkData{link, uint64(len(data))}
}

func (fprs *fakePeerResponseSender) SendExtensionData(
	requestID graphsync.RequestID,
	extension graphsync.ExtensionData,
) {
	fprs.sentExtensions <- sentExtension{requestID, extension}
}

func (fprs *fakePeerResponseSender) FinishRequest(requestID graphsync.RequestID) graphsync.ResponseStatusCode {
	fprs.lastCompletedRequest <- completedRequest{requestID, graphsync.RequestCompletedFull}
	return graphsync.RequestCompletedFull
}

func (fprs *fakePeerResponseSender) FinishWithError(requestID graphsync.RequestID, status graphsync.ResponseStatusCode) {
	fprs.lastCompletedRequest <- completedRequest{requestID, status}
}

func (fprs *fakePeerResponseSender) PauseRequest(requestID graphsync.RequestID) {
	fprs.pausedRequests <- pausedRequest{requestID}
}

func (fprs *fakePeerResponseSender) FinishWithCancel(requestID graphsync.RequestID) {
	fprs.cancelledRequests <- cancelledRequest{requestID}
}

func (fprs *fakePeerResponseSender) Transaction(requestID graphsync.RequestID, transaction peerresponsemanager.Transaction) error {
	fprts := &fakePeerResponseTransactionSender{requestID, fprs}
	return transaction(fprts)
}

type fakePeerResponseTransactionSender struct {
	requestID graphsync.RequestID
	prs       peerresponsemanager.PeerResponseSender
}

func (fprts *fakePeerResponseTransactionSender) SendResponse(link ipld.Link, data []byte) graphsync.BlockData {
	return fprts.prs.SendResponse(fprts.requestID, link, data)
}

func (fprts *fakePeerResponseTransactionSender) SendExtensionData(extension graphsync.ExtensionData) {
	fprts.prs.SendExtensionData(fprts.requestID, extension)
}

func (fprts *fakePeerResponseTransactionSender) FinishRequest() graphsync.ResponseStatusCode {
	return fprts.prs.FinishRequest(fprts.requestID)
}

func (fprts *fakePeerResponseTransactionSender) FinishWithError(status graphsync.ResponseStatusCode) {
	fprts.prs.FinishWithError(fprts.requestID, status)
}

func (fprts *fakePeerResponseTransactionSender) PauseRequest() {
	fprts.prs.PauseRequest(fprts.requestID)
}

func (fprts *fakePeerResponseTransactionSender) FinishWithCancel() {
	fprts.prs.FinishWithCancel(fprts.requestID)
}
func TestIncomingQuery(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	blks := td.blockChain.AllBlocks()

	responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	responseManager.Startup()

	responseManager.ProcessRequests(td.ctx, td.p, td.requests)
	testutil.AssertDoesReceive(td.ctx, t, td.completedRequestChan, "Should have completed request but didn't")
	for i := 0; i < len(blks); i++ {
		var sentResponse sentResponse
		testutil.AssertReceive(td.ctx, t, td.sentResponses, &sentResponse, "did not send responses")
		k := sentResponse.link.(cidlink.Link)
		blockIndex := testutil.IndexOf(blks, k.Cid)
		require.NotEqual(t, blockIndex, -1, "sent incorrect link")
		require.Equal(t, blks[blockIndex].RawData(), sentResponse.data, "sent incorrect data")
		require.Equal(t, td.requestID, sentResponse.requestID, "has incorrect response id")
	}
}

func TestCancellationQueryInProgress(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	blks := td.blockChain.AllBlocks()
	responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	cancelledListenerCalled := make(chan struct{}, 1)
	td.cancelledListeners.Register(func(p peer.ID, request graphsync.RequestData) {
		cancelledListenerCalled <- struct{}{}
	})
	responseManager.Startup()
	responseManager.ProcessRequests(td.ctx, td.p, td.requests)

	// read one block
	var sentResponse sentResponse
	testutil.AssertReceive(td.ctx, t, td.sentResponses, &sentResponse, "did not send response")
	k := sentResponse.link.(cidlink.Link)
	blockIndex := testutil.IndexOf(blks, k.Cid)
	require.NotEqual(t, blockIndex, -1, "sent incorrect link")
	require.Equal(t, blks[blockIndex].RawData(), sentResponse.data, "sent incorrect data")
	require.Equal(t, td.requestID, sentResponse.requestID, "has incorrect response id")

	// send a cancellation
	cancelRequests := []gsmsg.GraphSyncRequest{
		gsmsg.CancelRequest(td.requestID),
	}
	responseManager.ProcessRequests(td.ctx, td.p, cancelRequests)

	responseManager.synchronize()

	testutil.AssertDoesReceive(td.ctx, t, cancelledListenerCalled, "should call cancelled listener")

	// at this point we should receive at most one more block, then traversal
	// should complete
	additionalBlocks := 0
	for {
		select {
		case <-td.ctx.Done():
			t.Fatal("should complete request before context closes")
		case sentResponse = <-td.sentResponses:
			k = sentResponse.link.(cidlink.Link)
			blockIndex = testutil.IndexOf(blks, k.Cid)
			require.NotEqual(t, blockIndex, -1, "did not send correct link")
			require.Equal(t, blks[blockIndex].RawData(), sentResponse.data, "sent incorrect data")
			require.Equal(t, td.requestID, sentResponse.requestID, "incorrect response id")
			additionalBlocks++
		case <-td.cancelledRequests:
			require.LessOrEqual(t, additionalBlocks, 1, "should send at most 1 additional block")
			return
		}
	}
}

func TestCancellationViaCommand(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	blks := td.blockChain.AllBlocks()
	responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	responseManager.Startup()
	responseManager.ProcessRequests(td.ctx, td.p, td.requests)

	// read one block
	var sentResponse sentResponse
	testutil.AssertReceive(td.ctx, t, td.sentResponses, &sentResponse, "did not send response")
	k := sentResponse.link.(cidlink.Link)
	blockIndex := testutil.IndexOf(blks, k.Cid)
	require.NotEqual(t, blockIndex, -1, "sent incorrect link")
	require.Equal(t, blks[blockIndex].RawData(), sentResponse.data, "sent incorrect data")
	require.Equal(t, td.requestID, sentResponse.requestID, "has incorrect response id")

	// send a cancellation
	err := responseManager.CancelResponse(td.p, td.requestID)
	require.NoError(t, err)

	// at this point we should receive at most one more block, then traversal
	// should complete
	additionalBlocks := 0
	for {
		select {
		case <-td.ctx.Done():
			t.Fatal("should complete request before context closes")
		case sentResponse = <-td.sentResponses:
			k = sentResponse.link.(cidlink.Link)
			blockIndex = testutil.IndexOf(blks, k.Cid)
			require.NotEqual(t, blockIndex, -1, "did not send correct link")
			require.Equal(t, blks[blockIndex].RawData(), sentResponse.data, "sent incorrect data")
			require.Equal(t, td.requestID, sentResponse.requestID, "incorrect response id")
			additionalBlocks++
		case completed := <-td.completedRequestChan:
			require.Equal(t, completed.result, graphsync.RequestCancelled)
			require.LessOrEqual(t, additionalBlocks, 1, "should send at most 1 additional block")
			return
		}
	}
}

func TestEarlyCancellation(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	td.queryQueue.popWait.Add(1)
	responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
	responseManager.Startup()
	responseManager.ProcessRequests(td.ctx, td.p, td.requests)

	// send a cancellation
	cancelRequests := []gsmsg.GraphSyncRequest{
		gsmsg.CancelRequest(td.requestID),
	}
	responseManager.ProcessRequests(td.ctx, td.p, cancelRequests)

	responseManager.synchronize()

	// unblock popping from queue
	td.queryQueue.popWait.Done()

	timer := time.NewTimer(200 * time.Millisecond)
	// verify no responses processed
	testutil.AssertDoesReceiveFirst(t, timer.C, "should not process more responses", td.sentResponses, td.completedRequestChan)
}

func TestValidationAndExtensions(t *testing.T) {
	t.Run("on its own, should fail validation", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
		responseManager.Startup()
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		var lastRequest completedRequest
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "should terminate with failure")
	})

	t.Run("if non validating hook succeeds, does not pass validation", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.SendExtensionData(td.extensionResponse)
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		var lastRequest completedRequest
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "should terminate with failure")
		var receivedExtension sentExtension
		testutil.AssertReceive(td.ctx, t, td.sentExtensions, &receivedExtension, "should send extension response")
		require.Equal(t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")
	})

	t.Run("if validating hook succeeds, should pass validation", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
			hookActions.SendExtensionData(td.extensionResponse)
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		var lastRequest completedRequest
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		var receivedExtension sentExtension
		testutil.AssertReceive(td.ctx, t, td.sentExtensions, &receivedExtension, "should send extension response")
		require.Equal(t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")
	})

	t.Run("if any hook fails, should fail", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.SendExtensionData(td.extensionResponse)
			hookActions.TerminateWithError(errors.New("everything went to crap"))
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		var lastRequest completedRequest
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "should terminate with failure")
		var receivedExtension sentExtension
		testutil.AssertReceive(td.ctx, t, td.sentExtensions, &receivedExtension, "should send extension response")
		require.Equal(t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")
	})

	t.Run("hooks can be unregistered", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
		responseManager.Startup()
		unregister := td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
			hookActions.SendExtensionData(td.extensionResponse)
		})

		// hook validates request
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		var lastRequest completedRequest
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		var receivedExtension sentExtension
		testutil.AssertReceive(td.ctx, t, td.sentExtensions, &receivedExtension, "should send extension response")
		require.Equal(t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")

		// unregister
		unregister()

		// no same request should fail
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "should terminate with failure")
	})

	t.Run("hooks can alter the loader", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		obs := make(map[ipld.Link][]byte)
		oloader, _ := testutil.NewTestStore(obs)
		responseManager := New(td.ctx, oloader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
		responseManager.Startup()
		// add validating hook -- so the request SHOULD succeed
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})

		// request fails with base loader reading from block store that's missing data
		var lastRequest completedRequest
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "should terminate with failure")

		err := td.peristenceOptions.Register("chainstore", td.loader)
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
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		var receivedExtension sentExtension
		testutil.AssertReceive(td.ctx, t, td.sentExtensions, &receivedExtension, "should send extension response")
		require.Equal(t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")
	})

	t.Run("hooks can alter the node builder chooser", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
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
		var lastRequest completedRequest
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
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
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		var receivedExtension sentExtension
		testutil.AssertReceive(td.ctx, t, td.sentExtensions, &receivedExtension, "should send extension response")
		require.Equal(t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")
		require.Equal(t, 5, customChooserCallCount)
	})

	t.Run("do-not-send-cids extension", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		set := cid.NewSet()
		blks := td.blockChain.Blocks(0, 5)
		for _, blk := range blks {
			set.Add(blk.Cid())
		}
		data, err := cidset.EncodeCidSet(set)
		require.NoError(t, err)
		requests := []gsmsg.GraphSyncRequest{
			gsmsg.NewRequest(td.requestID, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0),
				graphsync.ExtensionData{
					Name: graphsync.ExtensionDoNotSendCIDs,
					Data: data,
				}),
		}
		responseManager.ProcessRequests(td.ctx, td.p, requests)
		var lastRequest completedRequest
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		var lastLinks []ipld.Link
		testutil.AssertReceive(td.ctx, t, td.ignoredLinks, &lastLinks, "should send ignored links")
		require.Len(t, lastLinks, set.Len())
		for _, link := range lastLinks {
			require.True(t, set.Has(link.(cidlink.Link).Cid))
		}
	})
	t.Run("dedup-by-key extension", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
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
		var lastRequest completedRequest
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		var dedupKey string
		testutil.AssertReceive(td.ctx, t, td.dedupKeys, &dedupKey, "should dedup by key")
		require.Equal(t, dedupKey, "applesauce")
	})
	t.Run("test pause/resume", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
			hookActions.PauseResponse()
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		var pauseRequest pausedRequest
		testutil.AssertReceive(td.ctx, t, td.pausedRequests, &pauseRequest, "should pause immediately")
		timer := time.NewTimer(100 * time.Millisecond)
		testutil.AssertDoesReceiveFirst(t, timer.C, "should not complete request while paused", td.completedRequestChan)
		testutil.AssertChannelEmpty(t, td.sentResponses, "should not send more blocks")
		err := responseManager.UnpauseResponse(td.p, td.requestID)
		require.NoError(t, err)
		var lastRequest completedRequest
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
	})
	t.Run("test block hook processing", func(t *testing.T) {
		t.Run("can send extension data", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				hookActions.SendExtensionData(td.extensionResponse)
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			var lastRequest completedRequest
			testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
			require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
			for i := 0; i < td.blockChainLength; i++ {
				var receivedExtension sentExtension
				testutil.AssertReceive(td.ctx, t, td.sentExtensions, &receivedExtension, "should send extension response")
				require.Equal(t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")
			}
		})

		t.Run("can send errors", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				hookActions.TerminateWithError(errors.New("failed"))
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			var lastRequest completedRequest
			testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
			require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "request should fail")
		})

		t.Run("can pause/unpause", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
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
			timer := time.NewTimer(100 * time.Millisecond)
			testutil.AssertDoesReceiveFirst(t, timer.C, "should not complete request while paused", td.completedRequestChan)
			for i := 0; i < blockCount; i++ {
				testutil.AssertDoesReceive(td.ctx, t, td.sentResponses, "should sent block")
			}
			testutil.AssertChannelEmpty(t, td.sentResponses, "should not send more blocks")
			var pausedRequest pausedRequest
			testutil.AssertReceive(td.ctx, t, td.pausedRequests, &pausedRequest, "should pause request")
			err := responseManager.UnpauseResponse(td.p, td.requestID, td.extensionResponse)
			require.NoError(t, err)
			var sentExtension sentExtension
			testutil.AssertReceive(td.ctx, t, td.sentExtensions, &sentExtension, "should send additional response")
			require.Equal(t, td.extensionResponse, sentExtension.extension)
			var lastRequest completedRequest
			testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
			require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		})

		t.Run("can pause/unpause externally", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			blkIndex := 0
			blockCount := 3
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				blkIndex++
				if blkIndex == blockCount {
					err := responseManager.PauseResponse(p, requestData.ID())
					require.NoError(t, err)
				}
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			timer := time.NewTimer(100 * time.Millisecond)
			testutil.AssertDoesReceiveFirst(t, timer.C, "should not complete request while paused", td.completedRequestChan)
			for i := 0; i < blockCount+1; i++ {
				testutil.AssertDoesReceive(td.ctx, t, td.sentResponses, "should sent block")
			}
			testutil.AssertChannelEmpty(t, td.sentResponses, "should not send more blocks")
			var pausedRequest pausedRequest
			testutil.AssertReceive(td.ctx, t, td.pausedRequests, &pausedRequest, "should pause request")
			err := responseManager.UnpauseResponse(td.p, td.requestID)
			require.NoError(t, err)
			for i := blockCount + 1; i < td.blockChainLength; i++ {
				testutil.AssertDoesReceive(td.ctx, t, td.sentResponses, "should send block")
			}
			var lastRequest completedRequest
			testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
			require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		})
	})

	t.Run("test update hook processing", func(t *testing.T) {

		t.Run("can pause/unpause", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
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
			timer := time.NewTimer(100 * time.Millisecond)
			testutil.AssertDoesReceiveFirst(t, timer.C, "should not complete request while paused", td.completedRequestChan)
			var sentResponses []sentResponse
			for i := 0; i < blockCount; i++ {
				testutil.AssertDoesReceive(td.ctx, t, td.sentResponses, "should sent block")
			}
			testutil.AssertChannelEmpty(t, td.sentResponses, "should not send more blocks")
			var pausedRequest pausedRequest
			testutil.AssertReceive(td.ctx, t, td.pausedRequests, &pausedRequest, "should pause request")
			require.LessOrEqual(t, len(sentResponses), blockCount)
			responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)
			var lastRequest completedRequest
			testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
			require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		})

		t.Run("can send extension data", func(t *testing.T) {
			t.Run("when unpaused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
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
				var lastRequest completedRequest
				testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
				require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
				var receivedExtension sentExtension
				testutil.AssertReceive(td.ctx, t, td.sentExtensions, &receivedExtension, "should send extension response")
				require.Equal(t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")
			})

			t.Run("when paused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
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
				var sentResponses []sentResponse
				for i := 0; i < blockCount; i++ {
					testutil.AssertDoesReceive(td.ctx, t, td.sentResponses, "should sent block")
				}
				testutil.AssertChannelEmpty(t, td.sentResponses, "should not send more blocks")
				var pausedRequest pausedRequest
				testutil.AssertReceive(td.ctx, t, td.pausedRequests, &pausedRequest, "should pause request")
				require.LessOrEqual(t, len(sentResponses), blockCount)

				// send update
				responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)

				// receive data
				var receivedExtension sentExtension
				testutil.AssertReceive(td.ctx, t, td.sentExtensions, &receivedExtension, "should send extension response")

				// should still be paused
				timer := time.NewTimer(100 * time.Millisecond)
				testutil.AssertDoesReceiveFirst(t, timer.C, "should not complete request while paused", td.completedRequestChan)
			})
		})

		t.Run("can send errors", func(t *testing.T) {
			t.Run("when unpaused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
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
				var lastRequest completedRequest
				testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
				require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "request should fail")
			})

			t.Run("when paused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
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
				var sentResponses []sentResponse
				for i := 0; i < blockCount; i++ {
					testutil.AssertDoesReceive(td.ctx, t, td.sentResponses, "should sent block")
				}
				testutil.AssertChannelEmpty(t, td.sentResponses, "should not send more blocks")
				var pausedRequest pausedRequest
				testutil.AssertReceive(td.ctx, t, td.pausedRequests, &pausedRequest, "should pause request")
				require.LessOrEqual(t, len(sentResponses), blockCount)

				// send update
				responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)

				// should terminate
				var lastRequest completedRequest
				testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
				require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "request should fail")

				// cannot unpause
				err := responseManager.UnpauseResponse(td.p, td.requestID)
				require.Error(t, err)
			})
		})

	})
	t.Run("final response status listeners", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners)
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		statusChan := make(chan graphsync.ResponseStatusCode, 1)
		td.completedListeners.Register(func(p peer.ID, requestData graphsync.RequestData, status graphsync.ResponseStatusCode) {
			select {
			case statusChan <- status:
			default:
			}
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		var lastRequest completedRequest
		testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		var status graphsync.ResponseStatusCode
		testutil.AssertReceive(td.ctx, t, statusChan, &status, "should receive status")
		require.True(t, gsmsg.IsTerminalSuccessCode(status), "request should succeed")
	})
}

type testData struct {
	ctx                   context.Context
	cancel                func()
	blockStore            map[ipld.Link][]byte
	loader                ipld.Loader
	storer                ipld.Storer
	blockChainLength      int
	blockChain            *testutil.TestBlockChain
	completedRequestChan  chan completedRequest
	sentResponses         chan sentResponse
	sentExtensions        chan sentExtension
	pausedRequests        chan pausedRequest
	cancelledRequests     chan cancelledRequest
	ignoredLinks          chan []ipld.Link
	dedupKeys             chan string
	peerManager           *fakePeerManager
	queryQueue            *fakeQueryQueue
	extensionData         []byte
	extensionName         graphsync.ExtensionName
	extension             graphsync.ExtensionData
	extensionResponseData []byte
	extensionResponse     graphsync.ExtensionData
	extensionUpdateData   []byte
	extensionUpdate       graphsync.ExtensionData
	requestID             graphsync.RequestID
	requests              []gsmsg.GraphSyncRequest
	updateRequests        []gsmsg.GraphSyncRequest
	p                     peer.ID
	peristenceOptions     *persistenceoptions.PersistenceOptions
	requestHooks          *hooks.IncomingRequestHooks
	blockHooks            *hooks.OutgoingBlockHooks
	updateHooks           *hooks.RequestUpdatedHooks
	completedListeners    *hooks.CompletedResponseListeners
	cancelledListeners    *hooks.RequestorCancelledListeners
}

func newTestData(t *testing.T) testData {
	ctx := context.Background()
	td := testData{}
	td.ctx, td.cancel = context.WithTimeout(ctx, 10*time.Second)

	td.blockStore = make(map[ipld.Link][]byte)
	td.loader, td.storer = testutil.NewTestStore(td.blockStore)
	td.blockChainLength = 5
	td.blockChain = testutil.SetupBlockChain(ctx, t, td.loader, td.storer, 100, td.blockChainLength)

	td.completedRequestChan = make(chan completedRequest, 1)
	td.sentResponses = make(chan sentResponse, td.blockChainLength*2)
	td.sentExtensions = make(chan sentExtension, td.blockChainLength*2)
	td.pausedRequests = make(chan pausedRequest, 1)
	td.cancelledRequests = make(chan cancelledRequest, 1)
	td.ignoredLinks = make(chan []ipld.Link, 1)
	td.dedupKeys = make(chan string, 1)
	fprs := &fakePeerResponseSender{
		lastCompletedRequest: td.completedRequestChan,
		sentResponses:        td.sentResponses,
		sentExtensions:       td.sentExtensions,
		pausedRequests:       td.pausedRequests,
		cancelledRequests:    td.cancelledRequests,
		ignoredLinks:         td.ignoredLinks,
		dedupKeys:            td.dedupKeys,
	}
	td.peerManager = &fakePeerManager{peerResponseSender: fprs}
	td.queryQueue = &fakeQueryQueue{}

	td.extensionData = testutil.RandomBytes(100)
	td.extensionName = graphsync.ExtensionName("AppleSauce/McGee")
	td.extension = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionData,
	}
	td.extensionResponseData = testutil.RandomBytes(100)
	td.extensionResponse = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionResponseData,
	}
	td.extensionUpdateData = testutil.RandomBytes(100)
	td.extensionUpdate = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionUpdateData,
	}
	td.requestID = graphsync.RequestID(rand.Int31())
	td.requests = []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(td.requestID, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0), td.extension),
	}
	td.updateRequests = []gsmsg.GraphSyncRequest{
		gsmsg.UpdateRequest(td.requestID, td.extensionUpdate),
	}
	td.p = testutil.GeneratePeers(1)[0]
	td.peristenceOptions = persistenceoptions.New()
	td.requestHooks = hooks.NewRequestHooks(td.peristenceOptions)
	td.blockHooks = hooks.NewBlockHooks()
	td.updateHooks = hooks.NewUpdateHooks()
	td.completedListeners = hooks.NewCompletedResponseListeners()
	td.cancelledListeners = hooks.NewRequestorCancelledListeners()
	return td
}
