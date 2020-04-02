package responsemanager

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	"github.com/ipfs/go-graphsync/selectorvalidator"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipfs/go-peertaskqueue/peertask"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"
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
type fakePeerResponseSender struct {
	sentResponses        chan sentResponse
	sentExtensions       chan sentExtension
	lastCompletedRequest chan completedRequest
}

func (fprs *fakePeerResponseSender) Startup()  {}
func (fprs *fakePeerResponseSender) Shutdown() {}

func (fprs *fakePeerResponseSender) SendResponse(
	requestID graphsync.RequestID,
	link ipld.Link,
	data []byte,
) {
	fprs.sentResponses <- sentResponse{requestID, link, data}
}

func (fprs *fakePeerResponseSender) SendExtensionData(
	requestID graphsync.RequestID,
	extension graphsync.ExtensionData,
) {
	fprs.sentExtensions <- sentExtension{requestID, extension}
}

func (fprs *fakePeerResponseSender) FinishRequest(requestID graphsync.RequestID) {
	fprs.lastCompletedRequest <- completedRequest{requestID, graphsync.RequestCompletedFull}
}

func (fprs *fakePeerResponseSender) FinishWithError(requestID graphsync.RequestID, status graphsync.ResponseStatusCode) {
	fprs.lastCompletedRequest <- completedRequest{requestID, status}
}

func TestIncomingQuery(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)
	blks := blockChain.AllBlocks()

	requestIDChan := make(chan completedRequest, 1)
	sentResponses := make(chan sentResponse, len(blks))
	sentExtensions := make(chan sentExtension, 1)
	fprs := &fakePeerResponseSender{lastCompletedRequest: requestIDChan, sentResponses: sentResponses, sentExtensions: sentExtensions}
	peerManager := &fakePeerManager{peerResponseSender: fprs}
	queryQueue := &fakeQueryQueue{}
	responseManager := New(ctx, loader, peerManager, queryQueue)
	responseManager.RegisterHook(selectorvalidator.SelectorValidator(100))
	responseManager.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	requests := []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(requestID, blockChain.TipLink.(cidlink.Link).Cid, blockChain.Selector(), graphsync.Priority(math.MaxInt32)),
	}
	p := testutil.GeneratePeers(1)[0]
	responseManager.ProcessRequests(ctx, p, requests)
	testutil.AssertDoesReceive(ctx, t, requestIDChan, "Should have completed request but didn't")
	for i := 0; i < len(blks); i++ {
		var sentResponse sentResponse
		testutil.AssertReceive(ctx, t, sentResponses, &sentResponse, "did not send responses")
		k := sentResponse.link.(cidlink.Link)
		blockIndex := testutil.IndexOf(blks, k.Cid)
		require.NotEqual(t, blockIndex, -1, "sent incorrect link")
		require.Equal(t, blks[blockIndex].RawData(), sentResponse.data, "sent incorrect data")
		require.Equal(t, requestID, sentResponse.requestID, "has incorrect response id")
	}
}

func TestCancellationQueryInProgress(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)
	blks := blockChain.AllBlocks()

	requestIDChan := make(chan completedRequest)
	sentResponses := make(chan sentResponse)
	sentExtensions := make(chan sentExtension, 1)
	fprs := &fakePeerResponseSender{lastCompletedRequest: requestIDChan, sentResponses: sentResponses, sentExtensions: sentExtensions}
	peerManager := &fakePeerManager{peerResponseSender: fprs}
	queryQueue := &fakeQueryQueue{}
	responseManager := New(ctx, loader, peerManager, queryQueue)
	responseManager.RegisterHook(selectorvalidator.SelectorValidator(100))
	responseManager.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	requests := []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(requestID, blockChain.TipLink.(cidlink.Link).Cid, blockChain.Selector(), graphsync.Priority(math.MaxInt32)),
	}
	p := testutil.GeneratePeers(1)[0]
	responseManager.ProcessRequests(ctx, p, requests)

	// read one block
	var sentResponse sentResponse
	testutil.AssertReceive(ctx, t, sentResponses, &sentResponse, "did not send response")
	k := sentResponse.link.(cidlink.Link)
	blockIndex := testutil.IndexOf(blks, k.Cid)
	require.NotEqual(t, blockIndex, -1, "sent incorrect link")
	require.Equal(t, blks[blockIndex].RawData(), sentResponse.data, "sent incorrect data")
	require.Equal(t, requestID, sentResponse.requestID, "has incorrect response id")

	// send a cancellation
	requests = []gsmsg.GraphSyncRequest{
		gsmsg.CancelRequest(requestID),
	}
	responseManager.ProcessRequests(ctx, p, requests)

	responseManager.synchronize()

	// at this point we should receive at most one more block, then traversal
	// should complete
	testutil.AssertReceiveFirst(t, sentResponses, &sentResponse, "should send one additional response", ctx.Done(), requestIDChan)
	k = sentResponse.link.(cidlink.Link)
	blockIndex = testutil.IndexOf(blks, k.Cid)
	require.NotEqual(t, blockIndex, -1, "did not send correct link")
	require.Equal(t, blks[blockIndex].RawData(), sentResponse.data, "sent incorrect data")
	require.Equal(t, requestID, sentResponse.requestID, "incorrect response id")

	// We should now be done
	testutil.AssertDoesReceiveFirst(t, requestIDChan, "should complete request", ctx.Done(), sentResponses)
}

func TestEarlyCancellation(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)

	requestIDChan := make(chan completedRequest)
	sentResponses := make(chan sentResponse)
	sentExtensions := make(chan sentExtension, 1)
	fprs := &fakePeerResponseSender{lastCompletedRequest: requestIDChan, sentResponses: sentResponses, sentExtensions: sentExtensions}
	peerManager := &fakePeerManager{peerResponseSender: fprs}
	queryQueue := &fakeQueryQueue{}
	queryQueue.popWait.Add(1)
	responseManager := New(ctx, loader, peerManager, queryQueue)
	responseManager.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	requests := []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(requestID, blockChain.TipLink.(cidlink.Link).Cid, blockChain.Selector(), graphsync.Priority(math.MaxInt32)),
	}
	p := testutil.GeneratePeers(1)[0]
	responseManager.ProcessRequests(ctx, p, requests)

	// send a cancellation
	requests = []gsmsg.GraphSyncRequest{
		gsmsg.CancelRequest(requestID),
	}
	responseManager.ProcessRequests(ctx, p, requests)

	responseManager.synchronize()

	// unblock popping from queue
	queryQueue.popWait.Done()

	// verify no responses processed
	testutil.AssertDoesReceiveFirst(t, ctx.Done(), "should not process more responses", sentResponses, requestIDChan)
}

func TestValidationAndExtensions(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)

	completedRequestChan := make(chan completedRequest, 1)
	sentResponses := make(chan sentResponse, 100)
	sentExtensions := make(chan sentExtension, 1)
	fprs := &fakePeerResponseSender{lastCompletedRequest: completedRequestChan, sentResponses: sentResponses, sentExtensions: sentExtensions}
	peerManager := &fakePeerManager{peerResponseSender: fprs}
	queryQueue := &fakeQueryQueue{}

	extensionData := testutil.RandomBytes(100)
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionData,
	}
	extensionResponseData := testutil.RandomBytes(100)
	extensionResponse := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionResponseData,
	}

	requestID := graphsync.RequestID(rand.Int31())
	requests := []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(requestID, blockChain.TipLink.(cidlink.Link).Cid, blockChain.Selector(), graphsync.Priority(math.MaxInt32), extension),
	}
	p := testutil.GeneratePeers(1)[0]

	t.Run("on its own, should fail validation", func(t *testing.T) {
		responseManager := New(ctx, loader, peerManager, queryQueue)
		responseManager.Startup()
		responseManager.ProcessRequests(ctx, p, requests)
		var lastRequest completedRequest
		testutil.AssertReceive(ctx, t, completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "should terminate with failure")
	})

	t.Run("if non validating hook succeeds, does not pass validation", func(t *testing.T) {
		responseManager := New(ctx, loader, peerManager, queryQueue)
		responseManager.Startup()
		responseManager.RegisterHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.RequestReceivedHookActions) {
			hookActions.SendExtensionData(extensionResponse)
		})
		responseManager.ProcessRequests(ctx, p, requests)
		var lastRequest completedRequest
		testutil.AssertReceive(ctx, t, completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "should terminate with failure")
		var receivedExtension sentExtension
		testutil.AssertReceive(ctx, t, sentExtensions, &receivedExtension, "should send extension response")
		require.Equal(t, extensionResponse, receivedExtension.extension, "incorrect extension response sent")
	})

	t.Run("if validating hook succeeds, should pass validation", func(t *testing.T) {
		responseManager := New(ctx, loader, peerManager, queryQueue)
		responseManager.Startup()
		responseManager.RegisterHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.RequestReceivedHookActions) {
			hookActions.ValidateRequest()
			hookActions.SendExtensionData(extensionResponse)
		})
		responseManager.ProcessRequests(ctx, p, requests)
		var lastRequest completedRequest
		testutil.AssertReceive(ctx, t, completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		var receivedExtension sentExtension
		testutil.AssertReceive(ctx, t, sentExtensions, &receivedExtension, "should send extension response")
		require.Equal(t, extensionResponse, receivedExtension.extension, "incorrect extension response sent")
	})

	t.Run("if any hook fails, should fail", func(t *testing.T) {
		responseManager := New(ctx, loader, peerManager, queryQueue)
		responseManager.Startup()
		responseManager.RegisterHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.RequestReceivedHookActions) {
			hookActions.ValidateRequest()
		})
		responseManager.RegisterHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.RequestReceivedHookActions) {
			hookActions.SendExtensionData(extensionResponse)
			hookActions.TerminateWithError(errors.New("everything went to crap"))
		})
		responseManager.ProcessRequests(ctx, p, requests)
		var lastRequest completedRequest
		testutil.AssertReceive(ctx, t, completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "should terminate with failure")
		var receivedExtension sentExtension
		testutil.AssertReceive(ctx, t, sentExtensions, &receivedExtension, "should send extension response")
		require.Equal(t, extensionResponse, receivedExtension.extension, "incorrect extension response sent")
	})

	t.Run("hooks can be unregistered", func(t *testing.T) {
		responseManager := New(ctx, loader, peerManager, queryQueue)
		responseManager.Startup()
		unregister := responseManager.RegisterHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.RequestReceivedHookActions) {
			hookActions.ValidateRequest()
			hookActions.SendExtensionData(extensionResponse)
		})

		// hook validates request
		responseManager.ProcessRequests(ctx, p, requests)
		var lastRequest completedRequest
		testutil.AssertReceive(ctx, t, completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		var receivedExtension sentExtension
		testutil.AssertReceive(ctx, t, sentExtensions, &receivedExtension, "should send extension response")
		require.Equal(t, extensionResponse, receivedExtension.extension, "incorrect extension response sent")

		// unregister
		unregister()

		// no same request should fail
		responseManager.ProcessRequests(ctx, p, requests)
		testutil.AssertReceive(ctx, t, completedRequestChan, &lastRequest, "should complete request")
		require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "should terminate with failure")
	})
}
