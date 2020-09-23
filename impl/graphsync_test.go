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
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestMakeRequestToNetwork(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	td.gsnet2.SetDelegate(r)
	graphSync := td.GraphSyncHost1()

	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader1, td.storer1, 100, blockChainLength)

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
	_, err := ipldutil.ParseSelector(receivedSpec)
	require.NoError(t, err, "did not receive parsible selector on other side")

	returnedData, found := receivedRequest.Extension(td.extensionName)
	require.True(t, found)
	require.Equal(t, td.extensionData, returnedData, "Failed to encode extension")
}

func TestSendResponseToIncomingRequest(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
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
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader2, td.storer2, 100, blockChainLength)

	requestID := graphsync.RequestID(rand.Int31())

	message := gsmsg.New()
	message.AddRequest(gsmsg.NewRequest(requestID, blockChain.TipLink.(cidlink.Link).Cid, blockChain.Selector(), graphsync.Priority(math.MaxInt32), td.extension))
	// send request across network
	err := td.gsnet1.SendMessage(ctx, td.host2.ID(), message)
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
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	requestor := td.GraphSyncHost1()
	// setup responder to disable default validation, meaning all requests are rejected
	_ = td.GraphSyncHost2(RejectAllRequestsByDefault())

	blockChainLength := 5
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader2, td.storer2, 5, blockChainLength)

	// send request across network
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	testutil.VerifyEmptyResponse(ctx, t, progressChan)
	testutil.VerifySingleTerminalError(ctx, t, errChan)
}

func TestGraphsyncRoundTrip(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader2, td.storer2, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()

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
}

func TestGraphsyncRoundTripPartial(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
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
		require.EqualError(t, err, fmt.Sprintf("Remote Peer Is Missing Block: %s", tree.LeafBetaLnk.String()))
	}
	require.Equal(t, tree.LeafAlphaBlock.RawData(), td.blockStore1[tree.LeafAlphaLnk])
	require.Equal(t, tree.MiddleListBlock.RawData(), td.blockStore1[tree.MiddleListNodeLnk])
	require.Equal(t, tree.MiddleMapBlock.RawData(), td.blockStore1[tree.MiddleMapNodeLnk])
	require.Equal(t, tree.RootBlock.RawData(), td.blockStore1[tree.RootNodeLnk])

	// verify listener
	var finalResponseStatus graphsync.ResponseStatusCode
	testutil.AssertReceive(ctx, t, finalResponseStatusChan, &finalResponseStatus, "should receive status")
	require.Equal(t, graphsync.RequestCompletedPartial, finalResponseStatus)
}

func TestGraphsyncRoundTripIgnoreCids(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader2, td.storer2, 100, blockChainLength)

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

	totalSent := 0
	totalSentOnWire := 0
	responder.RegisterOutgoingBlockHook(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		totalSent++
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
}

func TestPauseResume(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader2, td.storer2, 100, blockChainLength)

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

	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	blockChain.VerifyResponseRange(ctx, progressChan, 0, stopPoint)
	timer := time.NewTimer(100 * time.Millisecond)
	testutil.AssertDoesReceiveFirst(t, timer.C, "should pause request", progressChan)

	requestID := <-requestIDChan
	err := responder.UnpauseResponse(td.host1.ID(), requestID)
	require.NoError(t, err)

	blockChain.VerifyRemainder(ctx, progressChan, stopPoint)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

}
func TestPauseResumeRequest(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockSize := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader2, td.storer2, uint64(blockSize), blockChainLength)

	// initialize graphsync on second node to response to requests
	_ = td.GraphSyncHost2()

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

	blockChain.VerifyResponseRange(ctx, progressChan, 0, stopPoint-1)
	timer := time.NewTimer(100 * time.Millisecond)
	testutil.AssertDoesReceiveFirst(t, timer.C, "should pause request", progressChan)

	requestID := <-requestIDChan
	err := requestor.UnpauseRequest(requestID, td.extensionUpdate)
	require.NoError(t, err)

	blockChain.VerifyRemainder(ctx, progressChan, stopPoint-1)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")
}

func TestPauseResumeViaUpdate(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	var receivedReponseData []byte
	var receivedUpdateData []byte
	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

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
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader2, td.storer2, 100, blockChainLength)

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
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	blockChain.VerifyWholeChain(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	require.Equal(t, td.extensionResponseData, receivedReponseData, "did not receive correct extension response data")
	require.Equal(t, td.extensionUpdateData, receivedUpdateData, "did not receive correct extension update data")
}

func TestPauseResumeViaUpdateOnBlockHook(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	var receivedReponseData []byte
	var receivedUpdateData []byte
	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader2, td.storer2, 100, blockChainLength)

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
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector(), td.extension)

	blockChain.VerifyWholeChain(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
	require.Len(t, td.blockStore1, blockChainLength, "did not store all blocks")

	require.Equal(t, td.extensionResponseData, receivedReponseData, "did not receive correct extension response data")
	require.Equal(t, td.extensionUpdateData, receivedUpdateData, "did not receive correct extension update data")
}

func TestGraphsyncRoundTripAlternatePersistenceAndNodes(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()

	// alternate storing location for responder
	altStore1 := make(map[ipld.Link][]byte)
	altLoader1, altStorer1 := testutil.NewTestStore(altStore1)

	// alternate storing location for requestor
	altStore2 := make(map[ipld.Link][]byte)
	altLoader2, altStorer2 := testutil.NewTestStore(altStore2)

	err := requestor.RegisterPersistenceOption("chainstore", altLoader1, altStorer1)
	require.NoError(t, err)

	err = responder.RegisterPersistenceOption("chainstore", altLoader2, altStorer2)
	require.NoError(t, err)

	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, altLoader1, altStorer2, 100, blockChainLength)

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
}

func TestGraphsyncRoundTripMultipleAlternatePersistence(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// initialize graphsync on second node to response to requests
	_ = td.GraphSyncHost2()

	// alternate storing location for responder
	altStore1 := make(map[ipld.Link][]byte)
	altLoader1, altStorer1 := testutil.NewTestStore(altStore1)

	// alternate storing location for requestor
	altStore2 := make(map[ipld.Link][]byte)
	altLoader2, altStorer2 := testutil.NewTestStore(altStore2)

	err := requestor.RegisterPersistenceOption("chainstore1", altLoader1, altStorer1)
	require.NoError(t, err)

	err = requestor.RegisterPersistenceOption("chainstore2", altLoader2, altStorer2)
	require.NoError(t, err)

	blockChainLength := 100
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader2, td.storer2, 100, blockChainLength)

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
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)
	td.mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 100 * time.Millisecond, Bandwidth: 3000000})

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 40
	blockChain := testutil.SetupBlockChain(ctx, t, td.loader1, td.storer2, 200000, blockChainLength)

	// initialize graphsync on second node to response to requests
	td.GraphSyncHost2()

	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.TipLink, blockChain.Selector())

	blockChain.VerifyWholeChain(ctx, progressChan)
	testutil.VerifyEmptyErrors(ctx, t, errChan)
}

// What this test does:
// - Construct a blockstore + dag service
// - Import a file to UnixFS v1
// - setup a graphsync request from one node to the other
// for the file
// - Load the file from the new block store on the other node
// using the
// existing UnixFS v1 file reader
// - Verify the bytes match the original
func TestUnixFSFetch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	const unixfsChunkSize uint64 = 1 << 10
	const unixfsLinksPerLevel = 1024

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	makeLoader := func(bs bstore.Blockstore) ipld.Loader {
		return func(lnk ipld.Link, lnkCtx ipld.LinkContext) (io.Reader, error) {
			c, ok := lnk.(cidlink.Link)
			if !ok {
				return nil, errors.New("Incorrect Link Type")
			}
			// read block from one store
			block, err := bs.Get(c.Cid)
			if err != nil {
				return nil, err
			}
			return bytes.NewReader(block.RawData()), nil
		}
	}

	makeStorer := func(bs bstore.Blockstore) ipld.Storer {
		return func(lnkCtx ipld.LinkContext) (io.Writer, ipld.StoreCommitter, error) {
			var buf bytes.Buffer
			var committer ipld.StoreCommitter = func(lnk ipld.Link) error {
				c, ok := lnk.(cidlink.Link)
				if !ok {
					return errors.New("Incorrect Link Type")
				}
				block, err := blocks.NewBlockWithCid(buf.Bytes(), c.Cid)
				if err != nil {
					return err
				}
				return bs.Put(block)
			}
			return &buf, committer, nil
		}
	}
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
	loader1 := makeLoader(bs1)
	storer1 := makeStorer(bs1)

	// setup an IPLD loader/storer for blockstore 2
	loader2 := makeLoader(bs2)
	storer2 := makeStorer(bs2)

	td := newGsTestData(ctx, t)
	requestor := New(ctx, td.gsnet1, loader1, storer1)
	responder := New(ctx, td.gsnet2, loader2, storer2)
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

	allSelector := ssb.ExploreRecursive(ipldselector.RecursionLimitNone(),
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
}

type gsTestData struct {
	mn                       mocknet.Mocknet
	ctx                      context.Context
	host1                    host.Host
	host2                    host.Host
	gsnet1                   gsnet.GraphSyncNetwork
	gsnet2                   gsnet.GraphSyncNetwork
	blockStore1, blockStore2 map[ipld.Link][]byte
	loader1, loader2         ipld.Loader
	storer1, storer2         ipld.Storer
	extensionData            []byte
	extensionName            graphsync.ExtensionName
	extension                graphsync.ExtensionData
	extensionResponseData    []byte
	extensionResponse        graphsync.ExtensionData
	extensionUpdateData      []byte
	extensionUpdate          graphsync.ExtensionData
}

func newGsTestData(ctx context.Context, t *testing.T) *gsTestData {
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
	td.loader1, td.storer1 = testutil.NewTestStore(td.blockStore1)
	td.blockStore2 = make(map[ipld.Link][]byte)
	td.loader2, td.storer2 = testutil.NewTestStore(td.blockStore2)
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
	return New(td.ctx, td.gsnet1, td.loader1, td.storer1, options...)
}

func (td *gsTestData) GraphSyncHost2(options ...Option) graphsync.GraphExchange {

	return New(td.ctx, td.gsnet2, td.loader2, td.storer2, options...)
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

func (r *receiver) ReceiveError(err error) {
}

func (r *receiver) Connected(p peer.ID) {
}

func (r *receiver) Disconnected(p peer.ID) {
}
