package graphsync

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipfs/go-unixfsnode"
	unixfsbuilder "github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/donotsendfirstblocks"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/storeutil"
	"github.com/ipfs/go-graphsync/taskqueue"
	"github.com/ipfs/go-graphsync/testutil"
)

// nil means use the default protocols
// tests data transfer for the following protocol combinations:
// default protocol -> default protocols
// old protocol -> default protocols
// default protocols -> old protocol
// old protocol -> old protocol
var protocolsForTest = map[string]struct {
	host1Protocols []protocol.ID
	host2Protocols []protocol.ID
}{
	"(v2.0 -> v2.0)": {nil, nil},
	"(v1.0 -> v2.0)": {[]protocol.ID{gsnet.ProtocolGraphsync_1_0_0}, nil},
	"(v2.0 -> v1.0)": {nil, []protocol.ID{gsnet.ProtocolGraphsync_1_0_0}},
	"(v1.0 -> v1.0)": {[]protocol.ID{gsnet.ProtocolGraphsync_1_0_0}, []protocol.ID{gsnet.ProtocolGraphsync_1_0_0}},
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
		"processResponses(0)",
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
	require.Contains(t, traceStrings, "processResponses(0)")        // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)") // should have one of these per block

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
	require.Contains(t, traceStrings, "processResponses(0)")        // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)") // should have one of these per block

	// has ContextCancelError exception recorded in the right place
	// the requester gets a cancel, the responder gets a ErrBudgetExceeded
	tracing.SingleExceptionEvent(t, "request(0)->executeTask(0)", "ContextCancelError", ipldutil.ContextCancelError{}.Error(), false)
}

func TestGraphsyncRoundTrip(t *testing.T) {
	for pname, ps := range protocolsForTest {
		t.Run(pname, func(t *testing.T) {
			// create network
			ctx := context.Background()
			ctx, collectTracing := testutil.SetupTracing(ctx)
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			td := newOptionalGsTestData(ctx, t, ps.host1Protocols, ps.host2Protocols)

			// initialize graphsync on first node to make requests
			requestor := td.GraphSyncHost1()

			// setup receiving peer to just record message coming in
			blockChainLength := 100
			blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

			// initialize graphsync on second node to response to requests
			responder := td.GraphSyncHost2()
			assertComplete := assertCompletionFunction(responder, 1)

			var receivedResponseData datamodel.Node
			var receivedRequestData datamodel.Node

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
			require.Contains(t, traceStrings, "processResponses(0)")        // should have one of these per response
			require.Contains(t, traceStrings, "request(0)->verifyBlock(0)") // should have one of these per block

			processUpdateSpan := tracing.FindSpanByTraceString("response(0)")
			require.Equal(t, int64(0), testutil.AttributeValueInTraceSpan(t, *processUpdateSpan, "priority").AsInt64())
			require.Equal(t, []string{string(td.extensionName)}, testutil.AttributeValueInTraceSpan(t, *processUpdateSpan, "extensions").AsStringSlice())

			// each verifyBlock span should link to a cacheProcess span that stored it

			processResponsesSpans := tracing.FindSpans("processResponses")
			processResponsesLinks := make(map[string]int64)
			verifyBlockSpans := tracing.FindSpans("verifyBlock")

			for _, verifyBlockSpan := range verifyBlockSpans {
				require.Len(t, verifyBlockSpan.Links, 1, "verifyBlock span should have one link")
				found := false
				for _, prcessResponseSpan := range processResponsesSpans {
					sid := prcessResponseSpan.SpanContext.SpanID().String()
					if verifyBlockSpan.Links[0].SpanContext.SpanID().String() == sid {
						found = true
						processResponsesLinks[sid] = processResponsesLinks[sid] + 1
						break
					}
				}
				require.True(t, found, "verifyBlock should link to a known cacheProcess span")
			}

			// each cacheProcess span should be linked to one verifyBlock span per block it stored

			for _, processResponseSpan := range processResponsesSpans {
				blockCount := testutil.AttributeValueInTraceSpan(t, processResponseSpan, "blockCount").AsInt64()
				require.Equal(t, processResponsesLinks[processResponseSpan.SpanContext.SpanID().String()], blockCount, "cacheProcess span should be linked to one verifyBlock span per block it processed")
			}
		})
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
		require.EqualError(t, err, fmt.Sprintf("remote peer is missing block (%s) at path linkedList/2", tree.LeafBetaLnk.String()))
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
	require.Contains(t, traceStrings, "processResponses(0)")        // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)") // should have one of these per block
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
	encodedCidSet := cidset.EncodeCidSet(set)
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

	doNotSendFirstBlocksData := donotsendfirstblocks.EncodeDoNotSendFirstBlocks(50)
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
	err := responder.Unpause(ctx, requestID)
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
	require.Contains(t, traceStrings, "processResponses(0)")        // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)") // should have one of these per block

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
	err := requestor.Unpause(ctx, requestID, td.extensionUpdate)
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
	require.Contains(t, traceStrings, "processResponses(0)")        // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)") // should have one of these per block

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

	var receivedReponseData datamodel.Node
	var receivedUpdateData datamodel.Node
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

	var receivedReponseData datamodel.Node
	var receivedUpdateData datamodel.Node
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
	err := responder.Unpause(ctx, requestID)
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
	require.Contains(t, traceStrings, "processResponses(0)")        // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)") // should have one of these per block

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
	require.Contains(t, traceStrings, "processResponses(0)")        // should have one of these per response
	require.Contains(t, traceStrings, "request(1)->verifyBlock(0)") // should have one of these per block (TODO: why request(1) and not (0)?)

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
	require.Contains(t, traceStrings, "processResponses(0)")        // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)") // should have one of these per block
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
	origBytes, err := io.ReadAll(loremFile)
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
	selector := unixfsnode.UnixFSPathSelector("subfolder/lorem.txt")

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

func loadRandomUnixFxFile(ctx context.Context, t *testing.T, lsys *ipld.LinkSystem, size uint64, blockSize uint64) (datamodel.Link, uint64) {

	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	buf := bytes.NewReader(data)
	fileRoot, finalSize, err := unixfsbuilder.BuildUnixFSFile(buf, fmt.Sprintf("size-%d", blockSize), lsys)
	require.NoError(t, err)
	return fileRoot, finalSize
}

func TestUnixFSADLFetchMultiBlocks(t *testing.T) {
	ctx := context.Background()
	//ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	//defer cancel()

	// make a blockstore and dag service
	bs1 := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))

	// make a second blockstore
	bs2 := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))

	// setup an IPLD loader/storer for blockstore 1
	persistence1 := storeutil.LinkSystemForBlockstore(bs1)

	// setup an IPLD loader/storer for blockstore 2
	persistence2 := storeutil.LinkSystemForBlockstore(bs2)

	lnks := make([]dagpb.PBLink, 0, 2)
	fileRoot1, fileSize1 := loadRandomUnixFxFile(ctx, t, &persistence2, 50*1024, 1<<10)
	fileRoot2, fileSize2 := loadRandomUnixFxFile(ctx, t, &persistence2, 20*1024, 1<<10)

	entry1, err := unixfsbuilder.BuildUnixFSDirectoryEntry("file-1", int64(fileSize1), fileRoot1)
	require.NoError(t, err)
	lnks = append(lnks, entry1)
	entry2, err := unixfsbuilder.BuildUnixFSDirectoryEntry("file-2", int64(fileSize2), fileRoot2)
	require.NoError(t, err)
	lnks = append(lnks, entry2)

	link, _, err := unixfsbuilder.BuildUnixFSDirectory(lnks, &persistence2)
	require.NoError(t, err)

	td := newGsTestData(ctx, t)
	requestor := New(ctx, td.gsnet1, persistence1)
	responder := New(ctx, td.gsnet2, persistence2)

	responder.RegisterIncomingRequestHook(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		hookActions.ValidateRequest()
	})

	// create a selector for the whole UnixFS dag
	selector := unixfsnode.UnixFSPathSelector("file-1")

	// execute the traversal
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), link, selector)

	var sawCorrectPath bool
	for response := range progressChan {
		if response.Path.String() == "file-1" {
			sawCorrectPath = true
		}
	}
	require.True(t, sawCorrectPath)
	testutil.VerifyEmptyErrors(ctx, t, errChan)

	chooser := dagpb.AddSupportToChooser(basicnode.Chooser)

	proto, err := chooser(fileRoot1, ipld.LinkContext{})
	require.NoError(t, err)

	ind, err := persistence1.Load(ipld.LinkContext{}, fileRoot1, proto)
	require.NoError(t, err)

	nd, err := unixfsnode.Reify(ipld.LinkContext{}, ind, &persistence1)
	require.NoError(t, err)

	lbn, ok := nd.(datamodel.LargeBytesNode)
	require.True(t, ok)
	reader, err := lbn.AsLargeBytes()
	require.NoError(t, err)

	buf := make([]byte, 50*1024)
	_, err = reader.Read(buf)
	require.NoError(t, err)
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
	finalBytes, err := io.ReadAll(fn)
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
	require.Contains(t, traceStrings, "processResponses(0)")        // should have one of these per response
	require.Contains(t, traceStrings, "request(0)->verifyBlock(0)") // should have one of these per block
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

	var receivedResponseData datamodel.Node
	var receivedRequestData datamodel.Node

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

func TestSendUpdates(t *testing.T) {
	// create network
	ctx := context.Background()
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

	// set up pause point
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

	requestID := <-requestIDChan

	// set up extensions and listen for updates on the responder
	responderExt1 := graphsync.ExtensionData{Name: graphsync.ExtensionName("grip grop"), Data: basicnode.NewString("flim flam, blim blam")}
	responderExt2 := graphsync.ExtensionData{Name: graphsync.ExtensionName("Humpty/Dumpty"), Data: basicnode.NewInt(101)}

	var responderReceivedExt1 int
	var responderReceivedExt2 int
	updateRequests := make(chan struct{}, 1)
	unreg := responder.RegisterRequestUpdatedHook(func(p peer.ID, request graphsync.RequestData, update graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
		ext, found := update.Extension(responderExt1.Name)
		if found {
			responderReceivedExt1++
			require.Equal(t, responderExt1.Data, ext)
		}

		ext, found = update.Extension(responderExt2.Name)
		if found {
			responderReceivedExt2++
			require.Equal(t, responderExt2.Data, ext)
		}

		updateRequests <- struct{}{}
	})

	// send updates
	requestor.SendUpdate(ctx, requestID, responderExt1, responderExt2)

	// check we received what we expected
	testutil.AssertDoesReceive(ctx, t, updateRequests, "request never completed")
	require.Equal(t, 1, responderReceivedExt1, "got extension 1 in update")
	require.Equal(t, 1, responderReceivedExt2, "got extension 2 in update")
	unreg()

	// set up extensions and listen for updates on the requestor
	requestorExt1 := graphsync.ExtensionData{Name: graphsync.ExtensionName("PING"), Data: basicnode.NewBytes(testutil.RandomBytes(100))}
	requestorExt2 := graphsync.ExtensionData{Name: graphsync.ExtensionName("PONG"), Data: basicnode.NewBytes(testutil.RandomBytes(100))}

	updateResponses := make(chan struct{}, 1)

	var requestorReceivedExt1 int
	var requestorReceivedExt2 int

	unreg = requestor.RegisterIncomingResponseHook(func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
		ext, found := responseData.Extension(requestorExt1.Name)
		if found {
			requestorReceivedExt1++
			require.Equal(t, requestorExt1.Data, ext)
		}

		ext, found = responseData.Extension(requestorExt2.Name)
		if found {
			requestorReceivedExt2++
			require.Equal(t, requestorExt2.Data, ext)
		}

		updateResponses <- struct{}{}
	})

	// send updates the other way
	responder.SendUpdate(ctx, requestID, requestorExt1, requestorExt2)

	// check we received what we expected
	testutil.AssertDoesReceive(ctx, t, updateResponses, "request never completed")
	require.Equal(t, 1, requestorReceivedExt1, "got extension 1 in update")
	require.Equal(t, 1, requestorReceivedExt2, "got extension 2 in update")
	unreg()

	// finish up
	err := responder.Unpause(ctx, requestID)
	require.NoError(t, err)

	blockChain.VerifyRemainder(ctx, progressChan, stopPoint)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	drain(requestor)
	drain(responder)
	assertOneRequestCompletes(ctx, t)
}

func TestPanicHandlingInTraversal(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// panic locally to the requestor (persistence1) in the NodeReifier to simulate something dodgy
	// going on in a codec, or a reifier, or even a selector
	stopPoint := 5
	var count int
	td.persistence1.NodeReifier = func(lc linking.LinkContext, n datamodel.Node, ls *linking.LinkSystem) (datamodel.Node, error) {
		if count == stopPoint {
			panic("whoa up there boi")
		}
		count++
		return n, nil
	}

	// initialize graphsync on first node to make requests and set a panic callback
	var panicObj interface{}
	requestor := td.GraphSyncHost1(PanicCallback(func(recoverObj interface{}, debugStackTrace string) {
		panicObj = recoverObj
	}))

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.persistence2, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()

	// standard request for the whole chain
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	// we should only have received a small portion
	blockChain.VerifyResponseRange(ctx, progressChan, 0, stopPoint)
	// the panic should have been converted to a standard error
	var err error
	testutil.AssertReceive(ctx, t, errChan, &err, "should receive an error")
	require.Regexp(t, regexp.MustCompile("^recovered from panic: whoa up there boi, stack trace:"), err.Error())
	require.Len(t, td.blockStore1, int(stopPoint+1), "did not store all blocks")
	// and out panic callback should have provided us with the original panic data
	require.Equal(t, "whoa up there boi", panicObj)

	drain(requestor)
	drain(responder)
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
	extensionData              datamodel.Node
	extensionName              graphsync.ExtensionName
	extension                  graphsync.ExtensionData
	extensionResponseData      datamodel.Node
	extensionResponse          graphsync.ExtensionData
	extensionUpdateData        datamodel.Node
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
	return newOptionalGsTestData(ctx, t, nil, nil)
}

func newOptionalGsTestData(ctx context.Context, t *testing.T, network1Protocols []protocol.ID, network2Protocols []protocol.ID) *gsTestData {
	t.Helper()
	td := &gsTestData{ctx: ctx}
	td.mn = mocknet.New()
	var err error
	// setup network
	td.host1, err = td.mn.GenPeer()
	require.NoError(t, err, "error generating host")
	td.host2, err = td.mn.GenPeer()
	require.NoError(t, err, "error generating host")
	err = td.mn.LinkAll()
	require.NoError(t, err, "error linking hosts")

	opts := make([]gsnet.Option, 0)
	if network1Protocols != nil {
		opts = append(opts, gsnet.GraphsyncProtocols(network1Protocols))
	}
	td.gsnet1 = gsnet.NewFromLibp2pHost(td.host1, opts...)
	opts = make([]gsnet.Option, 0)
	if network2Protocols != nil {
		opts = append(opts, gsnet.GraphsyncProtocols(network2Protocols))
	}
	td.gsnet2 = gsnet.NewFromLibp2pHost(td.host2, opts...)
	td.blockStore1 = make(map[ipld.Link][]byte)
	td.persistence1 = testutil.NewTestStore(td.blockStore1)
	td.blockStore2 = make(map[ipld.Link][]byte)
	td.persistence2 = testutil.NewTestStore(td.blockStore2)
	// setup extension handlers
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

	return td
}

func (td *gsTestData) GraphSyncHost1(options ...Option) graphsync.GraphExchange {
	return New(td.ctx, td.gsnet1, td.persistence1, options...)
}

func (td *gsTestData) GraphSyncHost2(options ...Option) graphsync.GraphExchange {
	return New(td.ctx, td.gsnet2, td.persistence2, options...)
}

func processResponsesTraces(t *testing.T, tracing *testutil.Collector, responseCount int) []string {
	return testutil.RepeatTraceStrings("processResponses({})", responseCount)
}
