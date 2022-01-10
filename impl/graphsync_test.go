package graphsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/donotsendfirstblocks"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/storeutil"
	"github.com/ipfs/go-graphsync/taskqueue"
	"github.com/ipfs/go-graphsync/testutil"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	unixfsbuilder "github.com/ipfs/go-unixfsnode/data/builder"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestMakeRequestToNetwork(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	td.gsnet2.SetDelegate(r)
	graphSync := td.GraphSyncHost1()

	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence1, 100, blockChainLength)

	requestCtx, requestCancel := context.WithCancel(ctx)
	defer requestCancel()
	graphSync.Request(requestCtx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	var message receivedMessage
	testutil.AssertReceive(ctx, t, r.messageReceived, &message, "did not receive message sent")

	sender := message.sender
	require.Equal(t, td.host1.ID(), sender, "received message from wrong node")

	received := message.message
	receivedRequests := received.Requests()
	require.Len(t, receivedRequests, 1, "Did not add request to received message")
	receivedRequest := receivedRequests[0]
	receivedSpec := receivedRequest.Selector()
	require.Equal(t, blockChain.Selector(), receivedSpec, "did not transmit selector spec correctly")
	_, err := selector.ParseSelector(receivedSpec)
	require.NoError(t, err, "did not receive parsible selector on other side")

	returnedData, found := receivedRequest.Extension(td.extensionName)
	require.True(t, found)
	require.Equal(t, td.extensionData, returnedData, "Failed to encode extension")

	drain(graphSync)

	tracing := collectTracing(t)
	require.ElementsMatch(t, []string{
		"request(0)->newRequest(0)",
		"request(0)->executeTask(0)",
		"request(0)->terminateRequest(0)",
		"message(0)->sendMessage(0)",
	}, tracing.TracesToStrings())

	// make sure the attributes are what we expect
	requestSpans := tracing.FindSpans("request")
	require.Equal(t, td.host2.ID().Pretty(), testutil.AttributeValueInTraceSpan(t, requestSpans[0], "peerID").AsString())
	require.Equal(t, blockChain.TipLink.String(), testutil.AttributeValueInTraceSpan(t, requestSpans[0], "root").AsString())
	require.Equal(t, []string{string(td.extensionName)}, testutil.AttributeValueInTraceSpan(t, requestSpans[0], "extensions").AsStringSlice())
	require.Equal(t, int64(0), testutil.AttributeValueInTraceSpan(t, requestSpans[0], "requestID").AsInt64())
}

func TestSendResponseToIncomingRequest(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	td.gsnet1.SetDelegate(r)

	var receivedRequestData []byte
	// initialize graphsync on second node to response to requests
	gsnet := td.GraphSyncHost2()
	gsnet.RegisterIncomingRequestHook(
		func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			var has bool
			receivedRequestData, has = requestData.Extension(td.extensionName)
			require.True(t, has, "did not have expected extension")
			hookActions.SendExtensionData(td.extensionResponse)
		},
	)

	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	requestID := graphsync.RequestID(rand.Int31())

	builder := gsmsg.NewBuilder()
	builder.AddRequest(gsmsg.NewRequest(requestID, blockChain.TipLink.(cidlink.Link).Cid, blockChain.Selector(), graphsync.Priority(math.MaxInt32), td.extension))
	message, err := builder.Build()
	require.NoError(t, err)
	// send request across network
	err = td.gsnet1.SendMessage(ctx, td.host2.ID(), message)
	require.NoError(t, err)
	// read the values sent back to requestor
	var received gsmsg.GraphSyncMessage
	var receivedBlocks []blocks.Block
	var receivedExtensions [][]byte
	for {
		var message receivedMessage
		testutil.AssertReceive(ctx, t, r.messageReceived, &message, "did not receive complete response")

		sender := message.sender
		require.Equal(t, td.host2.ID(), sender, "received message from wrong node")

		received = message.message
		receivedBlocks = append(receivedBlocks, received.Blocks()...)
		receivedResponses := received.Responses()
		receivedExtension, found := receivedResponses[0].Extension(td.extensionName)
		if found {
			receivedExtensions = append(receivedExtensions, receivedExtension)
		}
		require.Len(t, receivedResponses, 1, "Did not receive response")
		require.Equal(t, requestID, receivedResponses[0].RequestID(), "Sent response for incorrect request id")
		if receivedResponses[0].Status() != graphsync.PartialResponse {
			break
		}
	}

	require.Len(t, receivedBlocks, blockChainLength, "Send incorrect number of blocks or there were duplicate blocks")
	require.Equal(t, td.extensionData, receivedRequestData, "did not receive correct request extension data")
	require.Len(t, receivedExtensions, 1, "should have sent extension responses but didn't")
	require.Equal(t, td.extensionResponseData, receivedExtensions[0], "did not return correct extension data")
}

func TestRejectRequestsByDefault(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	requestor := td.GraphSyncHost1()
	// setup responder to disable default validation, meaning all requests are rejected
	responder := td.GraphSyncHost2(RejectAllRequestsByDefault())
	assertComplete := assertCompletionFunction(responder, 1)
	blockChainLength := 5
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 5, blockChainLength)

	// send request across network
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	testutil.VerifyEmptyResponse(ctx, t, progressChan)
	testutil.VerifySingleTerminalError(ctx, t, errChan)

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)

	tracing := collectTracing(t)
	require.ElementsMatch(t, []string{
		"request(0)->newRequest(0)",
		"request(0)->executeTask(0)",
		"request(0)->terminateRequest(0)",
		"processResponses(0)->loaderProcess(0)->cacheProcess(0)",
		"processRequests(0)->transaction(0)->execute(0)->buildMessage(0)",
		"message(0)->sendMessage(0)",
		"message(1)->sendMessage(0)",
		"response(0)",
	}, tracing.TracesToStrings())
	// has ContextCancelError exception recorded in the right place
	tracing.SingleExceptionEvent(t, "request(0)->executeTask(0)", "ContextCancelError", ipldutil.ContextCancelError{}.Error(), false)
	// RejectAllRequestsByDefault() causes no request validator to be set, so they are all invalid
	tracing.SingleExceptionEvent(t, "response(0)", "github.com/ipfs/go-graphsync/responsemanager.errorString", "request not valid", true)
}

func TestGraphsyncRoundTripRequestBudgetRequestor(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	var linksToTraverse uint64 = 5
	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1(MaxLinksPerOutgoingRequests(linksToTraverse))

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	assertCancelOrComplete := assertCancelOrCompleteFunction(responder, 1)
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	// response budgets don't include the root block, so total links traverse with be one more than expected
	blockChain.VerifyResponseRange(ctx, progressChan, 0, int(linksToTraverse))
	testutil.VerifySingleTerminalError(ctx, t, errChan)
	require.Len(t, td.blockStore1, int(linksToTraverse), "did not store all blocks")

	drain(requestor)
	drain(responder)
	wasCancelled := assertCancelOrComplete(ctx, t)

	tracing := collectTracing(t)

	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	if wasCancelled {
		require.Contains(t, traceStrings, "response(0)->abortRequest(0)")
	}
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "processResponses(0)->loaderProcess(0)->cacheProcess(0)") // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)")                             // should have one of these per block

	// has ErrBudgetExceeded exception recorded in the right place
	tracing.SingleExceptionEvent(t, "request(0)->executeTask(0)", "ErrBudgetExceeded", "traversal budget exceeded", true)
	if wasCancelled {
		tracing.SingleExceptionEvent(t, "response(0)->executeTask(0)", "ContextCancelError", ipldutil.ContextCancelError{}.Error(), true)
	}
}

func TestGraphsyncRoundTripRequestBudgetResponder(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	var linksToTraverse uint64 = 5
	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2(MaxLinksPerIncomingRequests(linksToTraverse))
	assertComplete := assertCompletionFunction(responder, 1)

	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	// response budgets don't include the root block, so total links traverse with be one more than expected
	blockChain.VerifyResponseRange(ctx, progressChan, 0, int(linksToTraverse))
	testutil.VerifySingleTerminalError(ctx, t, errChan)
	require.Len(t, td.blockStore1, int(linksToTraverse), "did not store all blocks")

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)

	tracing := collectTracing(t)

	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "processResponses(0)->loaderProcess(0)->cacheProcess(0)") // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)")                             // should have one of these per block

	// has ContextCancelError exception recorded in the right place
	// the requester gets a cancel, the responder gets a ErrBudgetExceeded
	tracing.SingleExceptionEvent(t, "request(0)->executeTask(0)", "ContextCancelError", ipldutil.ContextCancelError{}.Error(), false)
}

func TestGraphsyncRoundTrip(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	assertComplete := assertCompletionFunction(responder, 1)

	var receivedResponseData []byte
	var receivedRequestData []byte

	requestor.RegisterIncomingResponseHook(
		func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
			data, has := responseData.Extension(td.extensionName)
			if has {
				receivedResponseData = data
			}
		})

	responder.RegisterIncomingRequestHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		var has bool
		receivedRequestData, has = requestData.Extension(td.extensionName)
		if !has {
			hookActions.TerminateWithError(errors.New("Missing extension"))
		} else {
			hookActions.SendExtensionData(td.extensionResponse)
		}
	})

	finalResponseStatusChan := make(chan graphsync.ResponseStatusCode, 1)
	responder.RegisterCompletedResponseListener(func(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
		select {
		case finalResponseStatusChan <- status:
		default:
		}
	})
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	blockChain.VerifyWholeChain(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	// verify extension roundtrip
	require.Equal(t, td.extensionData, receivedRequestData, "did not receive correct extension request data")
	require.Equal(t, td.extensionResponseData, receivedResponseData, "did not receive correct extension response data")

	// verify listener
	var finalResponseStatus graphsync.ResponseStatusCode
	testutil.AssertReceive(ctx, t, finalResponseStatusChan, &finalResponseStatus, "should receive status")
	require.Equal(t, graphsync.RequestCompletedFull, finalResponseStatus)

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)

	tracing := collectTracing(t)

	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "processResponses(0)->loaderProcess(0)->cacheProcess(0)") // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)")                             // should have one of these per block

	processUpdateSpan := tracing.FindSpanByTraceString("response(0)")
	require.Equal(t, int64(0), testutil.AttributeValueInTraceSpan(t, *processUpdateSpan, "priority").AsInt64())
	require.Equal(t, []string{string(td.extensionName)}, testutil.AttributeValueInTraceSpan(t, *processUpdateSpan, "extensions").AsStringSlice())

	// each verifyBlock span should link to a cacheProcess span that stored it

	cacheProcessSpans := tracing.FindSpans("cacheProcess")
	cacheProcessLinks := make(map[string]int64)
	verifyBlockSpans := tracing.FindSpans("verifyBlock")

	for _, verifyBlockSpan := range verifyBlockSpans {
		require.Len(t, verifyBlockSpan.Links, 1, "verifyBlock span should have one link")
		found := false
		for _, cacheProcessSpan := range cacheProcessSpans {
			sid := cacheProcessSpan.SpanContext.SpanID().String()
			if verifyBlockSpan.Links[0].SpanContext.SpanID().String() == sid {
				found = true
				cacheProcessLinks[sid] = cacheProcessLinks[sid] + 1
				break
			}
		}
		require.True(t, found, "verifyBlock should link to a known cacheProcess span")
	}

	// each cacheProcess span should be linked to one verifyBlock span per block it stored

	for _, cacheProcessSpan := range cacheProcessSpans {
		blockCount := testutil.AttributeValueInTraceSpan(t, cacheProcessSpan, "blockCount").AsInt64()
		require.Equal(t, cacheProcessLinks[cacheProcessSpan.SpanContext.SpanID().String()], blockCount, "cacheProcess span should be linked to one verifyBlock span per block it processed")
	}
}

func TestGraphsyncRoundTripPartial(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup an IPLD tree and put all but 1 node into the second nodes block store
	tree := testutil.NewTestIPLDTree()
	td.blockStore2[tree.LeafAlphaLnk] = tree.LeafAlphaBlock.RawData()
	td.blockStore2[tree.MiddleMapNodeLnk] = tree.MiddleMapBlock.RawData()
	td.blockStore2[tree.MiddleListNodeLnk] = tree.MiddleListBlock.RawData()
	td.blockStore2[tree.RootNodeLnk] = tree.RootBlock.RawData()

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	assertComplete := assertCompletionFunction(responder, 1)

	finalResponseStatusChan := make(chan graphsync.ResponseStatusCode, 1)
	responder.RegisterCompletedResponseListener(func(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
		select {
		case finalResponseStatusChan <- status:
		default:
		}
	})
	// create a selector to traverse the whole tree
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	allSelector := ssb.ExploreRecursive(selector.RecursionLimitDepth(10),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	_, errChan := requestor.Request(ctx, td.host2.ID(), tree.RootNodeLnk, allSelector)

	for err := range errChan {
		// verify the error is received for leaf beta node being missing
		require.EqualError(t, err, fmt.Sprintf("remote peer is missing block: %s", tree.LeafBetaLnk.String()))
	}
	require.Equal(t, tree.LeafAlphaBlock.RawData(), td.blockStore1[tree.LeafAlphaLnk])
	require.Equal(t, tree.MiddleListBlock.RawData(), td.blockStore1[tree.MiddleListNodeLnk])
	require.Equal(t, tree.MiddleMapBlock.RawData(), td.blockStore1[tree.MiddleMapNodeLnk])
	require.Equal(t, tree.RootBlock.RawData(), td.blockStore1[tree.RootNodeLnk])

	// verify listener
	var finalResponseStatus graphsync.ResponseStatusCode
	testutil.AssertReceive(ctx, t, finalResponseStatusChan, &finalResponseStatus, "should receive status")
	require.Equal(t, graphsync.RequestCompletedPartial, finalResponseStatus)

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)

	tracing := collectTracing(t)
	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "processResponses(0)->loaderProcess(0)->cacheProcess(0)") // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)")                             // should have one of these per block
}

func TestGraphsyncRoundTripIgnoreCids(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	assertAllResponsesReceived := assertAllResponsesReceivedFunction(requestor)

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	firstHalf := blockChain.Blocks(0, 50)
	set := cid.NewSet()
	for _, blk := range firstHalf {
		td.blockStore1[cidlink.Link{Cid: blk.Cid()}] = blk.RawData()
		set.Add(blk.Cid())
	}
	encodedCidSet, err := cidset.EncodeCidSet(set)
	require.NoError(t, err)
	extension := graphsync.ExtensionData{
		Name: graphsync.ExtensionDoNotSendCIDs,
		Data: encodedCidSet,
	}

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	assertComplete := assertCompletionFunction(responder, 1)

	totalSent := 0
	totalSentOnWire := 0
	responder.RegisterOutgoingBlockHook(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		totalSent = int(blockData.Index())
		if blockData.BlockSizeOnWire() > 0 {
			totalSentOnWire++
		}
	})

	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), extension)

	blockChain.VerifyWholeChain(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	require.Equal(t, blockChainLength, totalSent)
	require.Equal(t, blockChainLength-set.Len(), totalSentOnWire)

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)
	responseCount := assertAllResponsesReceived(ctx, t)

	tracing := collectTracing(t)
	require.ElementsMatch(t, append(append(append(append(append(append([]string{
		"request(0)->newRequest(0)",
		"request(0)->executeTask(0)",
		"request(0)->terminateRequest(0)",
	},
		processResponsesTraces(t, tracing, responseCount)...),
		testutil.RepeatTraceStrings("message({})->sendMessage(0)", responseCount+1)...),
		testutil.RepeatTraceStrings("request(0)->verifyBlock({})", 50)...), // half of the full chain
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->loadBlock(0)", blockChainLength)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->sendBlock(0)->processBlockHooks(0)", blockChainLength)...),
		testutil.RepeatTraceStrings("processRequests(0)->transaction({})->execute(0)->buildMessage(0)", blockChainLength+2)...,
	), tracing.TracesToStrings())
}

func TestGraphsyncRoundTripIgnoreNBlocks(t *testing.T) {

	// create network
	ctx, collectTracing := testutil.SetupTracing(context.Background())
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	assertAllResponsesReceived := assertAllResponsesReceivedFunction(requestor)

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	// store blocks locally
	firstHalf := blockChain.Blocks(0, 50)
	for _, blk := range firstHalf {
		td.blockStore1[cidlink.Link{Cid: blk.Cid()}] = blk.RawData()
	}

	doNotSendFirstBlocksData, err := donotsendfirstblocks.EncodeDoNotSendFirstBlocks(50)
	require.NoError(t, err)
	extension := graphsync.ExtensionData{
		Name: graphsync.ExtensionsDoNotSendFirstBlocks,
		Data: doNotSendFirstBlocksData,
	}

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	assertComplete := assertCompletionFunction(responder, 1)
	totalSent := 0
	totalSentOnWire := 0
	responder.RegisterOutgoingBlockHook(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		totalSent = int(blockData.Index())
		if blockData.Index() <= 50 {
			require.True(t, blockData.BlockSizeOnWire() == 0)
		} else {
			require.True(t, blockData.BlockSizeOnWire() > 0)
			totalSentOnWire++
		}
	})

	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), extension)

	blockChain.VerifyWholeChain(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	require.Equal(t, blockChainLength, totalSent)
	require.Equal(t, blockChainLength-50, totalSentOnWire)

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)
	responseCount := assertAllResponsesReceived(ctx, t)

	tracing := collectTracing(t)
	require.ElementsMatch(t, append(append(append(append(append(append([]string{
		"request(0)->newRequest(0)",
		"request(0)->executeTask(0)",
		"request(0)->terminateRequest(0)",
	},
		processResponsesTraces(t, tracing, responseCount)...),
		testutil.RepeatTraceStrings("message({})->sendMessage(0)", responseCount+1)...),
		testutil.RepeatTraceStrings("request(0)->verifyBlock({})", 50)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->loadBlock(0)", blockChainLength)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->sendBlock(0)->processBlockHooks(0)", blockChainLength)...),
		testutil.RepeatTraceStrings("processRequests(0)->transaction({})->execute(0)->buildMessage(0)", blockChainLength+2)...,
	), tracing.TracesToStrings())
}

func TestPauseResume(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()

	stopPoint := 50
	blocksSent := 0
	requestIDChan := make(chan graphsync.RequestID, 1)
	responder.RegisterOutgoingBlockHook(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		_, has := requestData.Extension(td.extensionName)
		if has {
			select {
			case requestIDChan <- requestData.ID():
			default:
			}
			blocksSent++
			if blocksSent == stopPoint {
				hookActions.PauseResponse()
			}
		} else {
			hookActions.TerminateWithError(errors.New("should have sent extension"))
		}
	})
	assertOneRequestCompletes := assertCompletionFunction(responder, 1)
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	blockChain.VerifyResponseRange(ctx, progressChan, 0, stopPoint)
	timer := time.NewTimer(100 * time.Millisecond)
	testutil.AssertDoesReceiveFirst(t, timer.C, "should pause request", progressChan)

	requestorPeerState := requestor.(*GraphSync).PeerState(td.host2.ID())
	require.Len(t, requestorPeerState.OutgoingState.RequestStates, 1)
	require.Len(t, requestorPeerState.IncomingState.RequestStates, 0)
	require.Len(t, requestorPeerState.OutgoingState.Active, 1)
	require.Contains(t, requestorPeerState.OutgoingState.RequestStates, requestorPeerState.OutgoingState.Active[0])
	require.Len(t, requestorPeerState.OutgoingState.Pending, 0)
	require.Len(t, requestorPeerState.IncomingState.Active, 0)
	require.Len(t, requestorPeerState.IncomingState.Pending, 0)
	require.Len(t, requestorPeerState.OutgoingState.Diagnostics(), 0)
	responderPeerState := responder.(*GraphSync).PeerState(td.host1.ID())
	require.Len(t, responderPeerState.IncomingState.RequestStates, 1)
	require.Len(t, responderPeerState.OutgoingState.RequestStates, 0)
	// no tasks as response is paused by responder
	require.Len(t, responderPeerState.IncomingState.Active, 0)
	require.Len(t, responderPeerState.IncomingState.Pending, 0)
	require.Len(t, responderPeerState.OutgoingState.Active, 0)
	require.Len(t, responderPeerState.OutgoingState.Pending, 0)
	require.Len(t, responderPeerState.IncomingState.Diagnostics(), 0)

	requestID := <-requestIDChan
	err := responder.UnpauseResponse(td.host1.ID(), requestID)
	require.NoError(t, err)

	blockChain.VerifyRemainder(ctx, progressChan, stopPoint)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	drain(requestor)
	drain(responder)
	assertOneRequestCompletes(ctx, t)

	tracing := collectTracing(t)

	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(1)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(1)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "processResponses(0)->loaderProcess(0)->cacheProcess(0)") // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)")                             // should have one of these per block

	// pause recorded
	tracing.SingleExceptionEvent(t, "response(0)->executeTask(0)", "github.com/ipfs/go-graphsync/responsemanager/hooks.ErrPaused", hooks.ErrPaused{}.Error(), false)
}

func TestPauseResumeRequest(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockSize := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, uint64(blockSize), blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	assertCancelOrComplete := assertCancelOrCompleteFunction(responder, 2)

	stopPoint := 50
	blocksReceived := 0
	requestIDChan := make(chan graphsync.RequestID, 1)
	requestor.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		select {
		case requestIDChan <- responseData.RequestID():
		default:
		}
		blocksReceived++
		if blocksReceived == stopPoint {
			hookActions.PauseRequest()
		}
	})

	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	blockChain.VerifyResponseRange(ctx, progressChan, 0, stopPoint)
	timer := time.NewTimer(100 * time.Millisecond)
	testutil.AssertDoesReceiveFirst(t, timer.C, "should pause request", progressChan)

	requestID := <-requestIDChan
	err := requestor.UnpauseRequest(requestID, td.extensionUpdate)
	require.NoError(t, err)

	blockChain.VerifyRemainder(ctx, progressChan, stopPoint)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	drain(requestor)
	drain(responder)
	// the request may actually only get sent onces -- it depends
	// on whether the responder completes its send before getting the cancel
	// signal. even if the request pauses before the request is over,
	// it may not make another graphsync request if it
	// ingested the blocks into the temporary cache
	wasCancelled := assertCancelOrComplete(ctx, t)
	if wasCancelled {
		// should get max 1 cancel
		require.False(t, assertCancelOrComplete(ctx, t))
	}

	tracing := collectTracing(t)

	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	if wasCancelled {
		require.Contains(t, traceStrings, "response(0)->abortRequest(0)")
		require.Contains(t, traceStrings, "response(1)->executeTask(0)->processBlock(0)->loadBlock(0)")
		require.Contains(t, traceStrings, "response(1)->executeTask(0)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	}
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(1)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "processResponses(0)->loaderProcess(0)->cacheProcess(0)") // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)")                             // should have one of these per block

	// has ErrPaused exception recorded in the right place
	tracing.SingleExceptionEvent(t, "request(0)->executeTask(0)", "ErrPaused", hooks.ErrPaused{}.Error(), false)
}

func TestPauseResumeViaUpdate(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	var receivedReponseData []byte
	var receivedUpdateData []byte
	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()
	assertAllResponsesReceived := assertAllResponsesReceivedFunction(requestor)

	requestor.RegisterIncomingResponseHook(func(p peer.ID, response graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
		if response.Status() == graphsync.RequestPaused {
			var has bool
			receivedReponseData, has = response.Extension(td.extensionName)
			if has {
				hookActions.UpdateRequestWithExtensions(td.extensionUpdate)
			}
		}
	})

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	stopPoint := 50
	blocksSent := 0
	responder.RegisterOutgoingBlockHook(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		_, has := requestData.Extension(td.extensionName)
		if has {
			blocksSent++
			if blocksSent == stopPoint {
				hookActions.SendExtensionData(td.extensionResponse)
				hookActions.PauseResponse()
			}
		} else {
			hookActions.TerminateWithError(errors.New("should have sent extension"))
		}
	})
	responder.RegisterRequestUpdatedHook(func(p peer.ID, request graphsync.RequestData, update graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
		var has bool
		receivedUpdateData, has = update.Extension(td.extensionName)
		if has {
			hookActions.UnpauseResponse()
		}
	})
	assertComplete := assertCompletionFunction(responder, 1)
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	blockChain.VerifyWholeChain(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	require.Equal(t, td.extensionResponseData, receivedReponseData, "did not receive correct extension response data")
	require.Equal(t, td.extensionUpdateData, receivedUpdateData, "did not receive correct extension update data")

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)
	responseCount := assertAllResponsesReceived(ctx, t)

	tracing := collectTracing(t)
	require.ElementsMatch(t, append(append(append(append(append(append(append(append([]string{
		"response(0)->processUpdate(0)",
		"request(0)->newRequest(0)",
		"request(0)->executeTask(0)",
		"request(0)->terminateRequest(0)",
		"processRequests(1)",
	},
		processResponsesTraces(t, tracing, responseCount)...),
		testutil.RepeatTraceStrings("message({})->sendMessage(0)", responseCount+2)...),
		testutil.RepeatTraceStrings("request(0)->verifyBlock({})", blockChainLength)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->loadBlock(0)", 50)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->sendBlock(0)->processBlockHooks(0)", 50)...), // half of the full chain
		testutil.RepeatTraceStrings("response(0)->executeTask(1)->processBlock({})->loadBlock(0)", 50)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(1)->processBlock({})->sendBlock(0)->processBlockHooks(0)", 50)...), // half of the full chain
		testutil.RepeatTraceStrings("processRequests(0)->transaction({})->execute(0)->buildMessage(0)", blockChainLength+3)...,
	), tracing.TracesToStrings())
	// make sure the attributes are what we expect
	processUpdateSpan := tracing.FindSpanByTraceString("response(0)->processUpdate(0)")
	require.Equal(t, []string{string(td.extensionName)}, testutil.AttributeValueInTraceSpan(t, *processUpdateSpan, "extensions").AsStringSlice())
	// pause recorded
	tracing.SingleExceptionEvent(t, "response(0)->executeTask(0)", "github.com/ipfs/go-graphsync/responsemanager/hooks.ErrPaused", hooks.ErrPaused{}.Error(), false)

	message0Span := tracing.FindSpanByTraceString("processRequests(0)")
	message1Span := tracing.FindSpanByTraceString("processRequests(1)")
	responseSpan := tracing.FindSpanByTraceString("response(0)")
	// response(0) originates in processRequests(0)
	require.Len(t, responseSpan.Links, 1)
	require.Equal(t, responseSpan.Links[0].SpanContext.SpanID(), message0Span.SpanContext.SpanID())
	// response(0)->processUpdate(0) occurs thanks to processRequests(1)
	require.Len(t, processUpdateSpan.Links, 1)
	require.Equal(t, processUpdateSpan.Links[0].SpanContext.SpanID(), message1Span.SpanContext.SpanID())
}

func TestPauseResumeViaUpdateOnBlockHook(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	var receivedReponseData []byte
	var receivedUpdateData []byte
	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	assertAllResponsesReceived := assertAllResponsesReceivedFunction(requestor)

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	stopPoint := 50
	blocksReceived := 0
	requestor.RegisterIncomingBlockHook(func(p peer.ID, response graphsync.ResponseData, block graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		blocksReceived++
		if response.Status() == graphsync.RequestPaused && blocksReceived == stopPoint {
			var has bool
			receivedReponseData, has = response.Extension(td.extensionName)
			if has {
				hookActions.UpdateRequestWithExtensions(td.extensionUpdate)
			}
		}
	})

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	blocksSent := 0
	responder.RegisterOutgoingBlockHook(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		_, has := requestData.Extension(td.extensionName)
		if has {
			blocksSent++
			if blocksSent == stopPoint {
				hookActions.SendExtensionData(td.extensionResponse)
				hookActions.PauseResponse()
			}
		} else {
			hookActions.TerminateWithError(errors.New("should have sent extension"))
		}
	})
	responder.RegisterRequestUpdatedHook(func(p peer.ID, request graphsync.RequestData, update graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
		var has bool
		receivedUpdateData, has = update.Extension(td.extensionName)
		if has {
			hookActions.UnpauseResponse()
		}
	})
	assertComplete := assertCompletionFunction(responder, 1)
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	blockChain.VerifyWholeChain(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	require.Equal(t, td.extensionResponseData, receivedReponseData, "did not receive correct extension response data")
	require.Equal(t, td.extensionUpdateData, receivedUpdateData, "did not receive correct extension update data")

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)
	responseCount := assertAllResponsesReceived(ctx, t)

	tracing := collectTracing(t)
	require.ElementsMatch(t, append(append(append(append(append(append(append(append([]string{
		"response(0)->processUpdate(0)",
		"request(0)->newRequest(0)",
		"request(0)->executeTask(0)",
		"request(0)->terminateRequest(0)",
		"processRequests(1)",
	},
		processResponsesTraces(t, tracing, responseCount)...),
		testutil.RepeatTraceStrings("message({})->sendMessage(0)", responseCount+2)...),
		testutil.RepeatTraceStrings("request(0)->verifyBlock({})", blockChainLength)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->loadBlock(0)", 50)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->sendBlock(0)->processBlockHooks(0)", 50)...), // half of the full chain
		testutil.RepeatTraceStrings("response(0)->executeTask(1)->processBlock({})->loadBlock(0)", 50)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(1)->processBlock({})->sendBlock(0)->processBlockHooks(0)", 50)...), // half of the full chain
		testutil.RepeatTraceStrings("processRequests(0)->transaction({})->execute(0)->buildMessage(0)", blockChainLength+3)...,
	), tracing.TracesToStrings())
	// make sure the attributes are what we expect
	processUpdateSpan := tracing.FindSpanByTraceString("response(0)->processUpdate(0)")
	require.Equal(t, []string{string(td.extensionName)}, testutil.AttributeValueInTraceSpan(t, *processUpdateSpan, "extensions").AsStringSlice())
	// pause recorded
	tracing.SingleExceptionEvent(t, "response(0)->executeTask(0)", "github.com/ipfs/go-graphsync/responsemanager/hooks.ErrPaused", hooks.ErrPaused{}.Error(), false)
}

func TestNetworkDisconnect(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()

	stopPoint := 50
	blocksSent := 0
	requestIDChan := make(chan graphsync.RequestID, 1)
	responder.RegisterOutgoingBlockHook(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		_, has := requestData.Extension(td.extensionName)
		if has {
			select {
			case requestIDChan <- requestData.ID():
			default:
			}
			blocksSent++
			if blocksSent == stopPoint {
				hookActions.PauseResponse()
			}
		} else {
			hookActions.TerminateWithError(errors.New("should have sent extension"))
		}
	})
	networkError := make(chan error, 1)
	responder.RegisterNetworkErrorListener(func(p peer.ID, request graphsync.RequestData, err error) {
		select {
		case networkError <- err:
		default:
		}
	})
	receiverError := make(chan error, 1)
	requestor.RegisterReceiverNetworkErrorListener(func(p peer.ID, err error) {
		select {
		case receiverError <- err:
		default:
		}
	})
	requestCtx, requestCancel := context.WithTimeout(ctx, 1*time.Second)
	defer requestCancel()
	progressChan, errChan := requestor.Request(requestCtx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	blockChain.VerifyResponseRange(ctx, progressChan, 0, stopPoint)
	timer := time.NewTimer(100 * time.Millisecond)
	testutil.AssertDoesReceiveFirst(t, timer.C, "should pause request", progressChan)
	testutil.AssertChannelEmpty(t, networkError, "no network errors so far")

	// unlink peers so they cannot communicate
	require.NoError(t, td.mn.DisconnectPeers(td.host1.ID(), td.host2.ID()))
	require.NoError(t, td.mn.UnlinkPeers(td.host1.ID(), td.host2.ID()))
	requestID := <-requestIDChan
	err := responder.UnpauseResponse(td.host1.ID(), requestID)
	require.NoError(t, err)

	testutil.AssertReceive(ctx, t, networkError, &err, "should receive network error")
	testutil.AssertReceive(ctx, t, errChan, &err, "should receive an error")
	require.EqualError(t, err, graphsync.RequestClientCancelledErr{}.Error())
	testutil.AssertReceive(ctx, t, receiverError, &err, "should receive an error on receiver side")

	drain(requestor)
	drain(responder)

	tracing := collectTracing(t)

	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "processRequests(0)->transaction(0)->execute(0)->buildMessage(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	require.Contains(t, traceStrings, "response(0)->abortRequest(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(1)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(1)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "processResponses(0)->loaderProcess(0)->cacheProcess(0)") // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)")                             // should have one of these per block

	// has ContextCancelError exception recorded in the right place
	tracing.SingleExceptionEvent(t, "request(0)->executeTask(0)", "ContextCancelError", ipldutil.ContextCancelError{}.Error(), false)
}

func TestConnectFail(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	requestCtx, requestCancel := context.WithTimeout(ctx, 1*time.Second)
	defer requestCancel()

	// unlink peers so they cannot communicate
	require.NoError(t, td.mn.DisconnectPeers(td.host1.ID(), td.host2.ID()))
	require.NoError(t, td.mn.UnlinkPeers(td.host1.ID(), td.host2.ID()))

	reqNetworkError := make(chan error, 1)
	requestor.RegisterNetworkErrorListener(func(p peer.ID, request graphsync.RequestData, err error) {
		select {
		case reqNetworkError <- err:
		default:
		}
	})
	_, errChan := requestor.Request(requestCtx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	var err error
	testutil.AssertReceive(ctx, t, reqNetworkError, &err, "should receive network error")
	testutil.AssertReceive(ctx, t, errChan, &err, "should receive an error")
	require.EqualError(t, err, graphsync.RequestClientCancelledErr{}.Error())

	drain(requestor)

	tracing := collectTracing(t)
	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "message(0)->sendMessage(0)")
	// has ContextCancelError exception recorded in the right place
	tracing.SingleExceptionEvent(t, "request(0)->executeTask(0)", "ContextCancelError", ipldutil.ContextCancelError{}.Error(), false)
}

func TestGraphsyncRoundTripAlternatePersistenceAndNodes(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	assertCancelOrComplete := assertCancelOrCompleteFunction(responder, 1)

	// alternate storing location for responder
	altStore1 := make(map[ipld.Link][]byte)
	altPersistence1 := testutil.NewTestStore(altStore1)

	// alternate storing location for requestor
	altStore2 := make(map[ipld.Link][]byte)
	altPersistence2 := testutil.NewTestStore(altStore2)

	err := requestor.RegisterPersistenceOption("chainstore", altPersistence1)
	require.NoError(t, err)

	err = responder.RegisterPersistenceOption("chainstore", altPersistence2)
	require.NoError(t, err)

	blockChainLength := 100
	blockChainPersistence := altPersistence1
	blockChainPersistence.StorageWriteOpener = altPersistence2.StorageWriteOpener
	blockChain := testutil.SetupBlockChain(ctx, t, blockChainPersistence, 100, blockChainLength)

	extensionName := graphsync.ExtensionName("blockchain")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: nil,
	}

	requestor.RegisterOutgoingRequestHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
		_, has := requestData.Extension(extensionName)
		if has {
			hookActions.UseLinkTargetNodePrototypeChooser(blockChain.Chooser)
			hookActions.UsePersistenceOption("chainstore")
		}
	})
	responder.RegisterIncomingRequestHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		_, has := requestData.Extension(extensionName)
		if has {
			hookActions.UseLinkTargetNodePrototypeChooser(blockChain.Chooser)
			hookActions.UsePersistenceOption("chainstore")
		}
	})

	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector())
	testutil.VerifyEmptyResponse(ctx, t, progressChan)
	testutil.VerifyHasErrors(ctx, t, errChan)

	progressChan, errChan = requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), extension)

	blockChain.VerifyWholeChainWithTypes(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, 0, "should store no blocks in normal store")
	require.Len(t, altStore1, blockChainLength, "did not store all blocks in alternate store")

	drain(requestor)
	drain(responder)
	wasCancelled := assertCancelOrComplete(ctx, t)

	tracing := collectTracing(t)

	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->sendBlock(0)")
	// may or may not contain a second response trace: "response(1)->executeTask(0)""
	if wasCancelled {
		require.Contains(t, traceStrings, "response(0)->abortRequest(0)")
	}
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "request(1)->newRequest(0)")
	require.Contains(t, traceStrings, "request(1)->executeTask(0)")
	require.Contains(t, traceStrings, "request(1)->terminateRequest(0)")
	require.Contains(t, traceStrings, "processResponses(0)->loaderProcess(0)->cacheProcess(0)") // should have one of these per response
	require.Contains(t, traceStrings, "request(1)->verifyBlock(0)")                             // should have one of these per block (TODO: why request(1) and not (0)?)

	// TODO(rvagg): this is randomly either a SkipMe or a ipldutil.ContextCancelError; confirm this is sane
	// tracing.SingleExceptionEvent(t, "request(0)->newRequest(0)","request(0)->executeTask(0)", "SkipMe", traversal.SkipMe{}.Error(), true)
}

func TestGraphsyncRoundTripMultipleAlternatePersistence(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	assertComplete := assertCompletionFunction(responder, 2)

	// alternate storing location for responder
	altStore1 := make(map[ipld.Link][]byte)
	altPersistence1 := testutil.NewTestStore(altStore1)

	// alternate storing location for requestor
	altStore2 := make(map[ipld.Link][]byte)
	altPersistence2 := testutil.NewTestStore(altStore2)

	err := requestor.RegisterPersistenceOption("chainstore1", altPersistence1)
	require.NoError(t, err)

	err = requestor.RegisterPersistenceOption("chainstore2", altPersistence2)
	require.NoError(t, err)

	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	extensionName1 := graphsync.ExtensionName("blockchain1")
	extension1 := graphsync.ExtensionData{
		Name: extensionName1,
		Data: nil,
	}

	extensionName2 := graphsync.ExtensionName("blockchain2")
	extension2 := graphsync.ExtensionData{
		Name: extensionName2,
		Data: nil,
	}

	requestor.RegisterOutgoingRequestHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
		_, has := requestData.Extension(extensionName1)
		if has {
			hookActions.UsePersistenceOption("chainstore1")
		}
		_, has = requestData.Extension(extensionName2)
		if has {
			hookActions.UsePersistenceOption("chainstore2")
		}
	})

	progressChan1, errChan1 := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), extension1)
	progressChan2, errChan2 := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), extension2)

	blockChain.VerifyWholeChain(ctx, progressChan1)
	testutil.VerifyEmptyErrors(ctx, t, errChan1)
	require.Len(t, altStore1, blockChainLength, "did not store all blocks in alternate store 1")
	blockChain.VerifyWholeChain(ctx, progressChan2)
	testutil.VerifyEmptyErrors(ctx, t, errChan2)
	require.Len(t, altStore1, blockChainLength, "did not store all blocks in alternate store 2")

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)

	tracing := collectTracing(t)
	// two complete request traces expected
	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	// may or may not contain a second response "response(1)->executeTask(0)"
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "request(1)->newRequest(0)")
	require.Contains(t, traceStrings, "request(1)->executeTask(0)")
	require.Contains(t, traceStrings, "request(1)->terminateRequest(0)")
	require.Contains(t, traceStrings, "processResponses(0)->loaderProcess(0)->cacheProcess(0)") // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)")                             // should have one of these per block
}

// TestRoundTripLargeBlocksSlowNetwork test verifies graphsync continues to work
// under a specific of adverse conditions:
// -- large blocks being returned by a query
// -- slow network connection
// It verifies that Graphsync will properly break up network message packets
// so they can still be decoded on the client side, instead of building up a huge
// backlog of blocks and then sending them in one giant network packet that can't
// be decoded on the client side
func TestRoundTripLargeBlocksSlowNetwork(t *testing.T) {

	// create network
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)
	td.mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 100 * time.Millisecond, Bandwidth: 3000000})

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()
	assertAllResponsesReceived := assertAllResponsesReceivedFunction(requestor)
	// setup receiving peer to just record message coming in
	blockChainLength := 40
	blockChainPersistence := td.persistence1
	blockChainPersistence.StorageWriteOpener = td.persistence2.StorageWriteOpener
	blockChain := testutil.SetupBlockChain(ctx, t, blockChainPersistence, 200000, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	assertComplete := assertCompletionFunction(responder, 1)
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector())

	blockChain.VerifyWholeChain(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)
	responseCount := assertAllResponsesReceived(ctx, t)

	tracing := collectTracing(t)
	require.ElementsMatch(t, append(append(append(append(append(append([]string{
		"request(0)->newRequest(0)",
		"request(0)->executeTask(0)",
		"request(0)->terminateRequest(0)",
	},
		processResponsesTraces(t, tracing, responseCount)...),
		testutil.RepeatTraceStrings("message({})->sendMessage(0)", responseCount+1)...),
		testutil.RepeatTraceStrings("request(0)->verifyBlock({})", blockChainLength)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->loadBlock(0)", blockChainLength)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->sendBlock(0)->processBlockHooks(0)", blockChainLength)...),
		testutil.RepeatTraceStrings("processRequests(0)->transaction({})->execute(0)->buildMessage(0)", blockChainLength+2)...,
	), tracing.TracesToStrings())
}

// What this test does:
// - Import a directory via UnixFSV1
// - setup a graphsync request from one node to the other
// for a file inside of the a subdirectory, using a UnixFS ADL
// - Load the file from the new block store on the other node
// using the existing UnixFS v1 file reader
// - Verify the bytes match the original
func TestUnixFSADLFetch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// make a blockstore and dag service
	bs1 := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))

	// make a second blockstore
	bs2 := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))

	// setup an IPLD loader/storer for blockstore 1
	persistence1 := storeutil.LinkSystemForBlockstore(bs1)

	// setup an IPLD loader/storer for blockstore 2
	persistence2 := storeutil.LinkSystemForBlockstore(bs2)

	path, err := filepath.Abs(filepath.Join("fixtures", "loremfolder"))
	require.NoError(t, err, "unable to create path for fixture file")

	loremFilePath, err := filepath.Abs(filepath.Join("fixtures", "loremfolder", "subfolder", "lorem.txt"))
	require.NoError(t, err)
	loremFile, err := os.Open(loremFilePath)
	require.NoError(t, err)
	origBytes, err := ioutil.ReadAll(loremFile)
	require.NoError(t, err)
	err = loremFile.Close()
	require.NoError(t, err)

	link, _, err := unixfsbuilder.BuildUnixFSRecursive(path, &persistence2)
	require.NoError(t, err)

	td := newGsTestData(ctx, t)
	requestor := New(ctx, td.gsnet1, persistence1)
	responder := New(ctx, td.gsnet2, persistence2)

	responder.RegisterIncomingRequestHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		hookActions.ValidateRequest()
	})

	// create a selector for the whole UnixFS dag
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	selector := ssb.ExploreInterpretAs("unixfs",
		ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("subfolder",
				ssb.ExploreInterpretAs("unixfs", ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
					efsb.Insert("lorem.txt", ssb.ExploreInterpretAs("unixfs", ssb.Matcher()))
				})),
			)
		}),
	).Node()

	// execute the traversal
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), link, selector)

	var sawCorrectPath bool
	for response := range progressChan {
		if response.Path.String() == "subfolder/lorem.txt" {
			sawCorrectPath = true
			finalBytes, err := response.Node.AsBytes()
			require.NoError(t, err)
			require.Equal(t, origBytes, finalBytes)
		}
	}
	require.True(t, sawCorrectPath)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
}

func TestUnixFSFetch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	const unixfsChunkSize uint64 = 1 << 10
	const unixfsLinksPerLevel = 1024

	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// make a blockstore and dag service
	bs1 := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))

	// make a second blockstore
	bs2 := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))
	dagService2 := merkledag.NewDAGService(blockservice.New(bs2, offline.Exchange(bs2)))

	// read in a fixture file
	path, err := filepath.Abs(filepath.Join("fixtures", "lorem.txt"))
	require.NoError(t, err, "unable to create path for fixture file")

	f, err := os.Open(path)
	require.NoError(t, err, "unable to open fixture file")

	var buf bytes.Buffer
	tr := io.TeeReader(f, &buf)
	file := files.NewReaderFile(tr)

	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dagService2)

	params := ihelper.DagBuilderParams{
		Maxlinks:   unixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	db, err := params.New(chunker.NewSizeSplitter(file, int64(unixfsChunkSize)))
	require.NoError(t, err, "unable to setup dag builder")

	nd, err := balanced.Layout(db)
	require.NoError(t, err, "unable to create unix fs node")

	err = bufferedDS.Commit()
	require.NoError(t, err, "unable to commit unix fs node")

	// save the original files bytes
	origBytes := buf.Bytes()

	// setup an IPLD loader/storer for blockstore 1
	persistence1 := storeutil.LinkSystemForBlockstore(bs1)

	// setup an IPLD loader/storer for blockstore 2
	persistence2 := storeutil.LinkSystemForBlockstore(bs2)

	td := newGsTestData(ctx, t)
	requestor := New(ctx, td.gsnet1, persistence1)
	responder := New(ctx, td.gsnet2, persistence2)
	assertComplete := assertCompletionFunction(responder, 1)
	extensionName := graphsync.ExtensionName("Free for all")
	responder.RegisterIncomingRequestHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		hookActions.ValidateRequest()
		hookActions.SendExtensionData(graphsync.ExtensionData{
			Name: extensionName,
			Data: nil,
		})
	})

	// make a go-ipld-prime link for the root UnixFS node
	clink := cidlink.Link{Cid: nd.Cid()}

	// create a selector for the whole UnixFS dag
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	allSelector := ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	// execute the traversal
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), clink, allSelector,
		graphsync.ExtensionData{
			Name: extensionName,
			Data: nil,
		})

	_ = testutil.CollectResponses(ctx, t, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)

	// setup a DagService for the second block store
	dagService1 := merkledag.NewDAGService(blockservice.New(bs1, offline.Exchange(bs1)))

	// load the root of the UnixFS DAG from the new blockstore
	otherNode, err := dagService1.Get(ctx, nd.Cid())
	require.NoError(t, err, "should have been able to read received root node but didn't")

	// Setup a UnixFS file reader
	n, err := unixfile.NewUnixfsFile(ctx, dagService1, otherNode)
	require.NoError(t, err, "should have been able to setup UnixFS file but wasn't")

	fn, ok := n.(files.File)
	require.True(t, ok, "file should be a regular file, but wasn't")

	// Read the bytes for the UnixFS File
	finalBytes, err := ioutil.ReadAll(fn)
	require.NoError(t, err, "should have been able to read all of unix FS file but wasn't")

	// verify original bytes match final bytes!
	require.Equal(t, origBytes, finalBytes, "should have gotten same bytes written as read but didn't")

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)

	tracing := collectTracing(t)
	traceStrings := tracing.TracesToStrings()
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->loadBlock(0)")
	require.Contains(t, traceStrings, "response(0)->executeTask(0)->processBlock(0)->sendBlock(0)->processBlockHooks(0)")
	require.Contains(t, traceStrings, "request(0)->newRequest(0)")
	require.Contains(t, traceStrings, "request(0)->executeTask(0)")
	require.Contains(t, traceStrings, "request(0)->terminateRequest(0)")
	require.Contains(t, traceStrings, "processResponses(0)->loaderProcess(0)->cacheProcess(0)") // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)")                             // should have one of these per block
}

func TestGraphsyncBlockListeners(t *testing.T) {

	// create network
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()
	assertComplete := assertCompletionFunction(responder, 1)

	// register hooks to count blocks in various stages
	blocksSent := 0
	blocksOutgoing := 0
	blocksIncoming := 0
	responder.RegisterBlockSentListener(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData) {
		blocksSent++
	})
	requestor.RegisterIncomingBlockHook(func(p peer.ID, r graphsync.ResponseData, b graphsync.BlockData, h graphsync.IncomingBlockHookActions) {
		blocksIncoming++
	})
	responder.RegisterOutgoingBlockHook(func(p peer.ID, r graphsync.RequestData, b graphsync.BlockData, h graphsync.OutgoingBlockHookActions) {
		blocksOutgoing++
	})

	var receivedResponseData []byte
	var receivedRequestData []byte

	requestor.RegisterIncomingResponseHook(
		func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
			data, has := responseData.Extension(td.extensionName)
			if has {
				receivedResponseData = data
			}
		})
	assertAllResponsesReceived := assertAllResponsesReceivedFunction(requestor)

	responder.RegisterIncomingRequestHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		var has bool
		receivedRequestData, has = requestData.Extension(td.extensionName)
		if !has {
			hookActions.TerminateWithError(errors.New("Missing extension"))
		} else {
			hookActions.SendExtensionData(td.extensionResponse)
		}
	})

	finalResponseStatusChan := make(chan graphsync.ResponseStatusCode, 1)
	responder.RegisterCompletedResponseListener(func(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
		select {
		case finalResponseStatusChan <- status:
		default:
		}
	})
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	blockChain.VerifyWholeChain(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	// verify extension round trip
	require.Equal(t, td.extensionData, receivedRequestData, "did not receive correct extension request data")
	require.Equal(t, td.extensionResponseData, receivedResponseData, "did not receive correct extension response data")

	// verify listener
	var finalResponseStatus graphsync.ResponseStatusCode
	testutil.AssertReceive(ctx, t, finalResponseStatusChan, &finalResponseStatus, "should receive status")
	require.Equal(t, graphsync.RequestCompletedFull, finalResponseStatus)

	// assert we get notified for all the blocks
	require.Equal(t, blockChainLength, blocksOutgoing)
	require.Equal(t, blockChainLength, blocksIncoming)
	require.Equal(t, blockChainLength, blocksSent)

	drain(requestor)
	drain(responder)
	assertComplete(ctx, t)
	responseCount := assertAllResponsesReceived(ctx, t)

	tracing := collectTracing(t)
	require.ElementsMatch(t, append(append(append(append(append(append(
		[]string{
			"request(0)->newRequest(0)",
			"request(0)->executeTask(0)",
			"request(0)->terminateRequest(0)",
		},
		processResponsesTraces(t, tracing, responseCount)...),
		testutil.RepeatTraceStrings("message({})->sendMessage(0)", responseCount+1)...),
		testutil.RepeatTraceStrings("request(0)->verifyBlock({})", blockChainLength)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->loadBlock(0)", blockChainLength)...),
		testutil.RepeatTraceStrings("response(0)->executeTask(0)->processBlock({})->sendBlock(0)->processBlockHooks(0)", blockChainLength)...),
		testutil.RepeatTraceStrings("processRequests(0)->transaction({})->execute(0)->buildMessage(0)", blockChainLength+2)...,
	), tracing.TracesToStrings())
}

type gsTestData struct {
	mn                         mocknet.Mocknet
	ctx                        context.Context
	host1                      host.Host
	host2                      host.Host
	gsnet1                     gsnet.GraphSyncNetwork
	gsnet2                     gsnet.GraphSyncNetwork
	blockStore1, blockStore2   map[ipld.Link][]byte
	persistence1, persistence2 ipld.LinkSystem
	extensionData              []byte
	extensionName              graphsync.ExtensionName
	extension                  graphsync.ExtensionData
	extensionResponseData      []byte
	extensionResponse          graphsync.ExtensionData
	extensionUpdateData        []byte
	extensionUpdate            graphsync.ExtensionData
}

func drain(gs graphsync.GraphExchange) {
	gs.(*GraphSync).requestQueue.(*taskqueue.WorkerTaskQueue).WaitForNoActiveTasks()
	gs.(*GraphSync).responseQueue.(*taskqueue.WorkerTaskQueue).WaitForNoActiveTasks()
}

func assertAllResponsesReceivedFunction(gs graphsync.GraphExchange) func(context.Context, *testing.T) int {
	var responseCount int
	finalResponseStatusChanRequestor := make(chan graphsync.ResponseStatusCode, 1)
	gs.RegisterIncomingResponseHook(func(p peer.ID, response graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
		responseCount = responseCount + 1
		if response.Status().IsTerminal() {
			select {
			case finalResponseStatusChanRequestor <- response.Status():
			default:
			}
		}
	})
	return func(ctx context.Context, t *testing.T) int {
		testutil.AssertDoesReceive(ctx, t, finalResponseStatusChanRequestor, "final response never received")
		return responseCount
	}
}

func assertCompletionFunction(gs graphsync.GraphExchange, completedRequestCount int) func(context.Context, *testing.T) {
	completedResponse := make(chan struct{}, completedRequestCount)
	gs.RegisterCompletedResponseListener(func(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
		completedResponse <- struct{}{}
	})
	return func(ctx context.Context, t *testing.T) {
		testutil.AssertDoesReceive(ctx, t, completedResponse, "request never completed")
	}
}

func assertCancelOrCompleteFunction(gs graphsync.GraphExchange, requestCount int) func(context.Context, *testing.T) bool {
	completedResponse := make(chan struct{}, requestCount)
	gs.RegisterCompletedResponseListener(func(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
		completedResponse <- struct{}{}
	})
	cancelledResponse := make(chan struct{}, requestCount)
	gs.RegisterRequestorCancelledListener(func(p peer.ID, request graphsync.RequestData) {
		cancelledResponse <- struct{}{}
	})
	return func(ctx context.Context, t *testing.T) bool {
		select {
		case <-ctx.Done():
			require.FailNow(t, "request did not cancel or complete")
			return false
		case <-completedResponse:
			return false
		case <-cancelledResponse:
			return true
		}
	}
}

func newGsTestData(ctx context.Context, t *testing.T) *gsTestData {
	t.Helper()
	td := &gsTestData{ctx: ctx}
	td.mn = mocknet.New(ctx)
	var err error
	// setup network
	td.host1, err = td.mn.GenPeer()
	require.NoError(t, err, "error generating host")
	td.host2, err = td.mn.GenPeer()
	require.NoError(t, err, "error generating host")
	err = td.mn.LinkAll()
	require.NoError(t, err, "error linking hosts")

	td.gsnet1 = gsnet.NewFromLibp2pHost(td.host1)
	td.gsnet2 = gsnet.NewFromLibp2pHost(td.host2)
	td.blockStore1 = make(map[ipld.Link][]byte)
	td.persistence1 = testutil.NewTestStore(td.blockStore1)
	td.blockStore2 = make(map[ipld.Link][]byte)
	td.persistence2 = testutil.NewTestStore(td.blockStore2)
	// setup extension handlers
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

	return td
}

func (td *gsTestData) GraphSyncHost1(options ...Option) graphsync.GraphExchange {
	return New(td.ctx, td.gsnet1, td.persistence1, options...)
}

func (td *gsTestData) GraphSyncHost2(options ...Option) graphsync.GraphExchange {
	return New(td.ctx, td.gsnet2, td.persistence2, options...)
}

type receivedMessage struct {
	message gsmsg.GraphSyncMessage
	sender  peer.ID
}

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type receiver struct {
	messageReceived chan receivedMessage
}

func (r *receiver) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming gsmsg.GraphSyncMessage) {

	select {
	case <-ctx.Done():
	case r.messageReceived <- receivedMessage{incoming, sender}:
	}
}

func (r *receiver) ReceiveError(_ peer.ID, err error) {
	fmt.Println("got receive err")
}

func (r *receiver) Connected(p peer.ID) {
}

func (r *receiver) Disconnected(p peer.ID) {
}

func processResponsesTraces(t *testing.T, tracing *testutil.Collector, responseCount int) []string {
	traces := testutil.RepeatTraceStrings("processResponses({})->loaderProcess(0)->cacheProcess(0)", responseCount-1)
	finalStub := tracing.FindSpanByTraceString(fmt.Sprintf("processResponses(%d)->loaderProcess(0)", responseCount-1))
	require.NotNil(t, finalStub)
	if len(testutil.AttributeValueInTraceSpan(t, *finalStub, "requestIDs").AsInt64Slice()) == 0 {
		return append(traces, fmt.Sprintf("processResponses(%d)->loaderProcess(0)", responseCount-1))
	}
	return append(traces, fmt.Sprintf("processResponses(%d)->loaderProcess(0)->cacheProcess(0)", responseCount-1))
}
