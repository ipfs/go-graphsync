package responsemanager

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipfs/go-peertaskqueue/peertask"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
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
	ctx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testbridge.NewMockStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)
	blks := blockChain.AllBlocks()

	ipldBridge := testbridge.NewMockIPLDBridge()
	requestIDChan := make(chan completedRequest, 1)
	sentResponses := make(chan sentResponse, len(blks))
	sentExtensions := make(chan sentExtension, 1)
	fprs := &fakePeerResponseSender{lastCompletedRequest: requestIDChan, sentResponses: sentResponses, sentExtensions: sentExtensions}
	peerManager := &fakePeerManager{peerResponseSender: fprs}
	queryQueue := &fakeQueryQueue{}
	responseManager := New(ctx, loader, ipldBridge, peerManager, queryQueue)
	responseManager.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	requests := []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(requestID, blockChain.TipLink.(cidlink.Link).Cid, blockChain.Selector(), graphsync.Priority(math.MaxInt32)),
	}
	p := testutil.GeneratePeers(1)[0]
	responseManager.ProcessRequests(ctx, p, requests)
	select {
	case <-ctx.Done():
		t.Fatal("Should have completed request but didn't")
	case <-requestIDChan:
	}
	for i := 0; i < len(blks); i++ {
		select {
		case sentResponse := <-sentResponses:
			k := sentResponse.link.(cidlink.Link)
			blockIndex := testutil.IndexOf(blks, k.Cid)
			if blockIndex == -1 {
				t.Fatal("sent incorrect link")
			}
			if !reflect.DeepEqual(sentResponse.data, blks[blockIndex].RawData()) {
				t.Fatal("sent incorrect data")
			}
			if sentResponse.requestID != requestID {
				t.Fatal("incorrect response id")
			}
		case <-ctx.Done():
			t.Fatal("did not send enough responses")
		}
	}
}

func TestCancellationQueryInProgress(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testbridge.NewMockStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)
	blks := blockChain.AllBlocks()

	ipldBridge := testbridge.NewMockIPLDBridge()
	requestIDChan := make(chan completedRequest)
	sentResponses := make(chan sentResponse)
	sentExtensions := make(chan sentExtension, 1)
	fprs := &fakePeerResponseSender{lastCompletedRequest: requestIDChan, sentResponses: sentResponses, sentExtensions: sentExtensions}
	peerManager := &fakePeerManager{peerResponseSender: fprs}
	queryQueue := &fakeQueryQueue{}
	responseManager := New(ctx, loader, ipldBridge, peerManager, queryQueue)
	responseManager.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	requests := []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(requestID, blockChain.TipLink.(cidlink.Link).Cid, blockChain.Selector(), graphsync.Priority(math.MaxInt32)),
	}
	p := testutil.GeneratePeers(1)[0]
	responseManager.ProcessRequests(ctx, p, requests)

	// read one block
	select {
	case sentResponse := <-sentResponses:
		k := sentResponse.link.(cidlink.Link)
		blockIndex := testutil.IndexOf(blks, k.Cid)
		if blockIndex == -1 {
			t.Fatal("sent incorrect link")
		}
		if !reflect.DeepEqual(sentResponse.data, blks[blockIndex].RawData()) {
			t.Fatal("sent incorrect data")
		}
		if sentResponse.requestID != requestID {
			t.Fatal("incorrect response id")
		}
	case <-ctx.Done():
		t.Fatal("did not send responses")
	}

	// send a cancellation
	requests = []gsmsg.GraphSyncRequest{
		gsmsg.CancelRequest(requestID),
	}
	responseManager.ProcessRequests(ctx, p, requests)

	responseManager.synchronize()

	// at this point we should receive at most one more block, then traversal
	// should complete
	additionalMessageCount := 0
drainqueue:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("Should have completed request but didn't")
		case sentResponse := <-sentResponses:
			if additionalMessageCount > 0 {
				t.Fatal("should not send any more responses")
			}
			k := sentResponse.link.(cidlink.Link)
			blockIndex := testutil.IndexOf(blks, k.Cid)
			if blockIndex == -1 {
				t.Fatal("sent incorrect link")
			}
			if !reflect.DeepEqual(sentResponse.data, blks[blockIndex].RawData()) {
				t.Fatal("sent incorrect data")
			}
			if sentResponse.requestID != requestID {
				t.Fatal("incorrect response id")
			}
			additionalMessageCount++
		case <-requestIDChan:
			break drainqueue
		}
	}
}

func TestEarlyCancellation(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testbridge.NewMockStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)

	ipldBridge := testbridge.NewMockIPLDBridge()
	requestIDChan := make(chan completedRequest)
	sentResponses := make(chan sentResponse)
	sentExtensions := make(chan sentExtension, 1)
	fprs := &fakePeerResponseSender{lastCompletedRequest: requestIDChan, sentResponses: sentResponses, sentExtensions: sentExtensions}
	peerManager := &fakePeerManager{peerResponseSender: fprs}
	queryQueue := &fakeQueryQueue{}
	queryQueue.popWait.Add(1)
	responseManager := New(ctx, loader, ipldBridge, peerManager, queryQueue)
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
	select {
	case <-ctx.Done():
	case <-sentResponses:
		t.Fatal("should not send any more responses")
	case <-requestIDChan:
		t.Fatal("should not send have completed response")
	}
}

func TestValidationAndExtensions(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testbridge.NewMockStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)

	ipldBridge := testbridge.NewMockIPLDBridge()
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

	t.Run("with invalid selector", func(t *testing.T) {
		selectorSpec := testbridge.NewInvalidSelectorSpec()
		requestID := graphsync.RequestID(rand.Int31())
		requests := []gsmsg.GraphSyncRequest{
			gsmsg.NewRequest(requestID, blockChain.TipLink.(cidlink.Link).Cid, selectorSpec, graphsync.Priority(math.MaxInt32), extension),
		}
		p := testutil.GeneratePeers(1)[0]

		t.Run("on its own, should fail validation", func(t *testing.T) {
			responseManager := New(ctx, loader, ipldBridge, peerManager, queryQueue)
			responseManager.Startup()
			responseManager.ProcessRequests(ctx, p, requests)
			select {
			case <-ctx.Done():
				t.Fatal("Should have completed request but didn't")
			case lastRequest := <-completedRequestChan:
				if !gsmsg.IsTerminalFailureCode(lastRequest.result) {
					t.Fatal("Request should have failed but didn't")
				}
			}
		})

		t.Run("if non validating hook succeeds, does not pass validation", func(t *testing.T) {
			responseManager := New(ctx, loader, ipldBridge, peerManager, queryQueue)
			responseManager.Startup()
			responseManager.RegisterHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.RequestReceivedHookActions) {
				hookActions.SendExtensionData(extensionResponse)
			})
			responseManager.ProcessRequests(ctx, p, requests)
			select {
			case <-ctx.Done():
				t.Fatal("Should have completed request but didn't")
			case lastRequest := <-completedRequestChan:
				if !gsmsg.IsTerminalFailureCode(lastRequest.result) {
					t.Fatal("Request should have succeeded but didn't")
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal("Should have sent extension response but didn't")
			case receivedExtension := <-sentExtensions:
				if !reflect.DeepEqual(receivedExtension.extension, extensionResponse) {
					t.Fatal("Proper Extension response should have been sent but wasn't")
				}
			}
		})

		t.Run("if validating hook succeeds, should pass validation", func(t *testing.T) {
			responseManager := New(ctx, loader, ipldBridge, peerManager, queryQueue)
			responseManager.Startup()
			responseManager.RegisterHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.RequestReceivedHookActions) {
				hookActions.ValidateRequest()
				hookActions.SendExtensionData(extensionResponse)
			})
			responseManager.ProcessRequests(ctx, p, requests)
			select {
			case <-ctx.Done():
				t.Fatal("Should have completed request but didn't")
			case lastRequest := <-completedRequestChan:
				if !gsmsg.IsTerminalSuccessCode(lastRequest.result) {
					t.Fatal("Request should have succeeded but didn't")
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal("Should have sent extension response but didn't")
			case receivedExtension := <-sentExtensions:
				if !reflect.DeepEqual(receivedExtension.extension, extensionResponse) {
					t.Fatal("Proper Extension response should have been sent but wasn't")
				}
			}
		})
	})

	t.Run("with valid selector", func(t *testing.T) {
		requestID := graphsync.RequestID(rand.Int31())
		requests := []gsmsg.GraphSyncRequest{
			gsmsg.NewRequest(requestID, blockChain.TipLink.(cidlink.Link).Cid, blockChain.Selector(), graphsync.Priority(math.MaxInt32), extension),
		}
		p := testutil.GeneratePeers(1)[0]

		t.Run("on its own, should pass validation", func(t *testing.T) {
			responseManager := New(ctx, loader, ipldBridge, peerManager, queryQueue)
			responseManager.Startup()
			responseManager.ProcessRequests(ctx, p, requests)
			select {
			case <-ctx.Done():
				t.Fatal("Should have completed request but didn't")
			case lastRequest := <-completedRequestChan:
				if !gsmsg.IsTerminalSuccessCode(lastRequest.result) {
					t.Fatal("Request should have failed but didn't")
				}
			}
		})

		t.Run("if any hook fails, should fail", func(t *testing.T) {
			responseManager := New(ctx, loader, ipldBridge, peerManager, queryQueue)
			responseManager.Startup()
			responseManager.RegisterHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.RequestReceivedHookActions) {
				hookActions.SendExtensionData(extensionResponse)
				hookActions.TerminateWithError(errors.New("everything went to crap"))
			})
			responseManager.ProcessRequests(ctx, p, requests)
			select {
			case <-ctx.Done():
				t.Fatal("Should have completed request but didn't")
			case lastRequest := <-completedRequestChan:
				if !gsmsg.IsTerminalFailureCode(lastRequest.result) {
					t.Fatal("Request should have succeeded but didn't")
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal("Should have sent extension response but didn't")
			case receivedExtension := <-sentExtensions:
				if !reflect.DeepEqual(receivedExtension.extension, extensionResponse) {
					t.Fatal("Proper Extension response should have been sent but wasn't")
				}
			}
		})
	})
}
