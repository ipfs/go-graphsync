package requestmanager

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/dedupkey"
	"github.com/ipfs/go-graphsync/listeners"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/requestmanager/executor"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/requestmanager/testloader"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipfs/go-graphsync/taskqueue"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestNormalSimultaneousFetch(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blockChain2 := testutil.SetupBlockChain(ctx, t, td.persistence, 100, 5)

	returnedResponseChan1, returnedErrorChan1 := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())
	returnedResponseChan2, returnedErrorChan2 := td.requestManager.NewRequest(requestCtx, peers[0], blockChain2.TipLink, blockChain2.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 2)

	td.tcm.AssertProtected(t, peers[0])
	td.tcm.AssertProtectedWithTags(t, peers[0], requestRecords[0].gsr.ID().Tag(), requestRecords[1].gsr.ID().Tag())
	require.Equal(t, peers[0], requestRecords[0].p)
	require.Equal(t, peers[0], requestRecords[1].p)
	require.False(t, requestRecords[0].gsr.IsCancel())
	require.False(t, requestRecords[1].gsr.IsCancel())
	require.Equal(t, defaultPriority, requestRecords[0].gsr.Priority())
	require.Equal(t, defaultPriority, requestRecords[1].gsr.Priority())

	require.Equal(t, td.blockChain.TipLink.String(), requestRecords[0].gsr.Root().String())
	require.Equal(t, td.blockChain.Selector(), requestRecords[0].gsr.Selector(), "did not encode selector properly")
	require.Equal(t, blockChain2.TipLink.String(), requestRecords[1].gsr.Root().String())
	require.Equal(t, blockChain2.Selector(), requestRecords[1].gsr.Selector(), "did not encode selector properly")

	firstBlocks := append(td.blockChain.AllBlocks(), blockChain2.Blocks(0, 3)...)
	firstMetadata1 := metadataForBlocks(td.blockChain.AllBlocks(), true)
	firstMetadataEncoded1, err := metadata.EncodeMetadata(firstMetadata1)
	require.NoError(t, err, "did not encode metadata")
	firstMetadata2 := metadataForBlocks(blockChain2.Blocks(0, 3), true)
	firstMetadataEncoded2, err := metadata.EncodeMetadata(firstMetadata2)
	require.NoError(t, err, "did not encode metadata")
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.RequestCompletedFull, graphsync.ExtensionData{
			Name: graphsync.ExtensionMetadata,
			Data: firstMetadataEncoded1,
		}),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.PartialResponse, graphsync.ExtensionData{
			Name: graphsync.ExtensionMetadata,
			Data: firstMetadataEncoded2,
		}),
	}

	td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	td.fal.VerifyLastProcessedBlocks(ctx, t, firstBlocks)
	td.fal.VerifyLastProcessedResponses(ctx, t, map[graphsync.RequestID]metadata.Metadata{
		requestRecords[0].gsr.ID(): firstMetadata1,
		requestRecords[1].gsr.ID(): firstMetadata2,
	})
	td.fal.SuccessResponseOn(peers[0], requestRecords[0].gsr.ID(), td.blockChain.AllBlocks())
	td.fal.SuccessResponseOn(peers[0], requestRecords[1].gsr.ID(), blockChain2.Blocks(0, 3))

	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan1)
	blockChain2.VerifyResponseRange(requestCtx, returnedResponseChan2, 0, 3)

	td.tcm.AssertProtected(t, peers[0])
	td.tcm.RefuteProtectedWithTags(t, peers[0], requestRecords[0].gsr.ID().Tag())
	td.tcm.AssertProtectedWithTags(t, peers[0], requestRecords[1].gsr.ID().Tag())

	moreBlocks := blockChain2.RemainderBlocks(3)
	moreMetadata := metadataForBlocks(moreBlocks, true)
	moreMetadataEncoded, err := metadata.EncodeMetadata(moreMetadata)
	require.NoError(t, err, "did not encode metadata")
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.RequestCompletedFull, graphsync.ExtensionData{
			Name: graphsync.ExtensionMetadata,
			Data: moreMetadataEncoded,
		}),
	}

	td.requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	td.fal.VerifyLastProcessedBlocks(ctx, t, moreBlocks)
	td.fal.VerifyLastProcessedResponses(ctx, t, map[graphsync.RequestID]metadata.Metadata{
		requestRecords[1].gsr.ID(): moreMetadata,
	})

	td.fal.SuccessResponseOn(peers[0], requestRecords[1].gsr.ID(), moreBlocks)

	blockChain2.VerifyRemainder(requestCtx, returnedResponseChan2, 3)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan2)

	td.tcm.RefuteProtected(t, peers[0])
}

func TestCancelRequestInProgress(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	requestCtx1, cancel1 := context.WithCancel(requestCtx)
	requestCtx2, cancel2 := context.WithCancel(requestCtx)
	defer cancel2()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan1, returnedErrorChan1 := td.requestManager.NewRequest(requestCtx1, peers[0], td.blockChain.TipLink, td.blockChain.Selector())
	returnedResponseChan2, returnedErrorChan2 := td.requestManager.NewRequest(requestCtx2, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 2)

	td.tcm.AssertProtected(t, peers[0])
	td.tcm.AssertProtectedWithTags(t, peers[0], requestRecords[0].gsr.ID().Tag(), requestRecords[1].gsr.ID().Tag())

	firstBlocks := td.blockChain.Blocks(0, 3)
	firstMetadata := encodedMetadataForBlocks(t, firstBlocks, true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.PartialResponse, firstMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.PartialResponse, firstMetadata),
	}

	td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)

	td.fal.SuccessResponseOn(peers[0], requestRecords[0].gsr.ID(), firstBlocks)
	td.fal.SuccessResponseOn(peers[0], requestRecords[1].gsr.ID(), firstBlocks)
	td.blockChain.VerifyResponseRange(requestCtx1, returnedResponseChan1, 0, 3)
	cancel1()
	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	require.True(t, rr.gsr.IsCancel())
	require.Equal(t, requestRecords[0].gsr.ID(), rr.gsr.ID())

	moreBlocks := td.blockChain.RemainderBlocks(3)
	moreMetadata := encodedMetadataForBlocks(t, moreBlocks, true)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
	}
	td.requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	td.fal.SuccessResponseOn(peers[0], requestRecords[0].gsr.ID(), moreBlocks)
	td.fal.SuccessResponseOn(peers[0], requestRecords[1].gsr.ID(), moreBlocks)

	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan1)
	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan2)

	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan2)

	errors := testutil.CollectErrors(requestCtx, t, returnedErrorChan1)
	require.Len(t, errors, 1)
	_, ok := errors[0].(graphsync.RequestClientCancelledErr)
	require.True(t, ok)

	td.tcm.RefuteProtected(t, peers[0])
}
func TestCancelRequestImperativeNoMoreBlocks(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	postCancel := make(chan struct{}, 1)
	loadPostCancel := make(chan struct{}, 1)
	td.fal.OnAsyncLoad(func(graphsync.RequestID, ipld.Link, <-chan types.AsyncLoadResult) {
		select {
		case <-postCancel:
			loadPostCancel <- struct{}{}
		default:
		}
	})

	_, returnedErrorChan1 := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)

	td.tcm.AssertProtected(t, peers[0])
	td.tcm.AssertProtectedWithTags(t, peers[0], requestRecords[0].gsr.ID().Tag())

	go func() {
		firstBlocks := td.blockChain.Blocks(0, 3)
		firstMetadata := encodedMetadataForBlocks(t, firstBlocks, true)
		firstResponses := []gsmsg.GraphSyncResponse{
			gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.PartialResponse, firstMetadata),
		}
		td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
		td.fal.SuccessResponseOn(peers[0], requestRecords[0].gsr.ID(), firstBlocks)
	}()

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Second)
	defer timeoutCancel()
	err := td.requestManager.CancelRequest(timeoutCtx, requestRecords[0].gsr.ID())
	require.NoError(t, err)
	postCancel <- struct{}{}

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	require.True(t, rr.gsr.IsCancel())
	require.Equal(t, requestRecords[0].gsr.ID(), rr.gsr.ID())

	td.tcm.RefuteProtected(t, peers[0])

	errors := testutil.CollectErrors(requestCtx, t, returnedErrorChan1)
	require.Len(t, errors, 1)
	_, ok := errors[0].(graphsync.RequestClientCancelledErr)
	require.True(t, ok)
	select {
	case <-loadPostCancel:
		t.Fatalf("Loaded block after cancel")
	case <-requestCtx.Done():
	}
}

func TestCancelManagerExitsGracefully(t *testing.T) {
	ctx := context.Background()
	managerCtx, managerCancel := context.WithCancel(ctx)
	td := newTestData(managerCtx, t)
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	firstBlocks := td.blockChain.Blocks(0, 3)
	firstMetadata := encodedMetadataForBlocks(t, firstBlocks, true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.PartialResponse, firstMetadata),
	}
	td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	td.fal.SuccessResponseOn(peers[0], rr.gsr.ID(), firstBlocks)
	td.blockChain.VerifyResponseRange(ctx, returnedResponseChan, 0, 3)
	managerCancel()

	moreBlocks := td.blockChain.RemainderBlocks(3)
	moreMetadata := encodedMetadataForBlocks(t, moreBlocks, true)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
	}
	td.requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	td.fal.SuccessResponseOn(peers[0], rr.gsr.ID(), moreBlocks)
	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan)
}

func TestFailedRequest(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
	td.tcm.AssertProtected(t, peers[0])
	td.tcm.AssertProtectedWithTags(t, peers[0], rr.gsr.ID().Tag())

	failedResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestFailedContentNotFound),
	}
	td.requestManager.ProcessResponses(peers[0], failedResponses, nil)

	testutil.VerifySingleTerminalError(requestCtx, t, returnedErrorChan)
	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
	td.tcm.RefuteProtected(t, peers[0])
}

func TestLocallyFulfilledFirstRequestFailsLater(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	// async loaded response responds immediately
	td.fal.SuccessResponseOn(peers[0], rr.gsr.ID(), td.blockChain.AllBlocks())

	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan)

	// failure comes in later over network
	failedResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestFailedContentNotFound),
	}

	td.requestManager.ProcessResponses(peers[0], failedResponses, nil)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan)

}

func TestLocallyFulfilledFirstRequestSucceedsLater(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	called := make(chan struct{})
	td.responseHooks.Register(func(p peer.ID, response graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
		close(called)
	})
	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	// async loaded response responds immediately
	td.fal.SuccessResponseOn(peers[0], rr.gsr.ID(), td.blockChain.AllBlocks())

	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan)

	md := encodedMetadataForBlocks(t, td.blockChain.AllBlocks(), true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, md),
	}
	td.requestManager.ProcessResponses(peers[0], firstResponses, td.blockChain.AllBlocks())

	td.fal.VerifyNoRemainingData(t, rr.gsr.ID())
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan)
	testutil.AssertDoesReceive(requestCtx, t, called, "response hooks called for response")
}

func TestRequestReturnsMissingBlocks(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	md := encodedMetadataForBlocks(t, td.blockChain.AllBlocks(), false)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedPartial, md),
	}
	td.requestManager.ProcessResponses(peers[0], firstResponses, nil)
	for _, block := range td.blockChain.AllBlocks() {
		td.fal.ResponseOn(peers[0], rr.gsr.ID(), cidlink.Link{Cid: block.Cid()}, types.AsyncLoadResult{Data: nil, Err: fmt.Errorf("Terrible Thing")})
	}
	testutil.VerifyEmptyResponse(ctx, t, returnedResponseChan)
	errs := testutil.CollectErrors(ctx, t, returnedErrorChan)
	require.NotEqual(t, len(errs), 0, "did not send errors")
}

func TestDisconnectNotification(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(2)

	// Listen for network errors
	networkErrors := make(chan peer.ID, 1)
	td.networkErrorListeners.Register(func(p peer.ID, request graphsync.RequestData, err error) {
		networkErrors <- p
	})

	// Send a request to the target peer
	targetPeer := peers[0]
	td.requestManager.NewRequest(requestCtx, targetPeer, td.blockChain.TipLink, td.blockChain.Selector())

	// Disconnect a random peer, should not fire any events
	randomPeer := peers[1]
	td.requestManager.Disconnected(randomPeer)
	select {
	case <-networkErrors:
		t.Fatal("should not fire network error when unrelated peer disconnects")
	default:
	}

	// Disconnect the target peer, should fire a network error
	td.requestManager.Disconnected(targetPeer)
	select {
	case p := <-networkErrors:
		require.Equal(t, p, targetPeer)
	default:
		t.Fatal("should fire network error when peer disconnects")
	}
}

func TestEncodingExtensions(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	expectedError := make(chan error, 2)
	receivedExtensionData := make(chan []byte, 2)
	expectedUpdateChan := make(chan []graphsync.ExtensionData, 2)
	hook := func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
		data, has := responseData.Extension(td.extensionName1)
		require.True(t, has, "did not receive extension data in response")
		receivedExtensionData <- data
		err := <-expectedError
		if err != nil {
			hookActions.TerminateWithError(err)
		}
		update := <-expectedUpdateChan
		if len(update) > 0 {
			hookActions.UpdateRequestWithExtensions(update...)
		}
	}
	td.responseHooks.Register(hook)
	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector(), td.extension1, td.extension2)

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	gsr := rr.gsr
	returnedData1, found := gsr.Extension(td.extensionName1)
	require.True(t, found)
	require.Equal(t, td.extensionData1, returnedData1, "did not encode first extension correctly")

	returnedData2, found := gsr.Extension(td.extensionName2)
	require.True(t, found)
	require.Equal(t, td.extensionData2, returnedData2, "did not encode second extension correctly")

	t.Run("responding to extensions", func(t *testing.T) {
		expectedData := testutil.RandomBytes(100)
		expectedUpdate := testutil.RandomBytes(100)
		firstResponses := []gsmsg.GraphSyncResponse{
			gsmsg.NewResponse(gsr.ID(),
				graphsync.PartialResponse, graphsync.ExtensionData{
					Name: graphsync.ExtensionMetadata,
					Data: nil,
				},
				graphsync.ExtensionData{
					Name: td.extensionName1,
					Data: expectedData,
				},
			),
		}
		expectedError <- nil
		expectedUpdateChan <- []graphsync.ExtensionData{
			{
				Name: td.extensionName1,
				Data: expectedUpdate,
			},
		}
		td.requestManager.ProcessResponses(peers[0], firstResponses, nil)
		var received []byte
		testutil.AssertReceive(ctx, t, receivedExtensionData, &received, "did not receive extension data")
		require.Equal(t, expectedData, received, "did not receive correct extension data from resposne")

		rr = readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
		receivedUpdateData, has := rr.gsr.Extension(td.extensionName1)
		require.True(t, has)
		require.Equal(t, expectedUpdate, receivedUpdateData, "should have updated with correct extension")

		nextExpectedData := testutil.RandomBytes(100)
		nextExpectedUpdate1 := testutil.RandomBytes(100)
		nextExpectedUpdate2 := testutil.RandomBytes(100)

		secondResponses := []gsmsg.GraphSyncResponse{
			gsmsg.NewResponse(gsr.ID(),
				graphsync.PartialResponse, graphsync.ExtensionData{
					Name: graphsync.ExtensionMetadata,
					Data: nil,
				},
				graphsync.ExtensionData{
					Name: td.extensionName1,
					Data: nextExpectedData,
				},
			),
		}
		expectedError <- errors.New("a terrible thing happened")
		expectedUpdateChan <- []graphsync.ExtensionData{
			{
				Name: td.extensionName1,
				Data: nextExpectedUpdate1,
			},
			{
				Name: td.extensionName2,
				Data: nextExpectedUpdate2,
			},
		}
		td.requestManager.ProcessResponses(peers[0], secondResponses, nil)
		testutil.AssertReceive(ctx, t, receivedExtensionData, &received, "did not receive extension data")
		require.Equal(t, nextExpectedData, received, "did not receive correct extension data from resposne")

		rr = readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
		receivedUpdateData, has = rr.gsr.Extension(td.extensionName1)
		require.True(t, has)
		require.Equal(t, nextExpectedUpdate1, receivedUpdateData, "should have updated with correct extension")
		receivedUpdateData, has = rr.gsr.Extension(td.extensionName2)
		require.True(t, has)
		require.Equal(t, nextExpectedUpdate2, receivedUpdateData, "should have updated with correct extension")

		testutil.VerifySingleTerminalError(requestCtx, t, returnedErrorChan)
		testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
	})
}

func TestBlockHooks(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	receivedBlocks := make(chan graphsync.BlockData, 4)
	receivedResponses := make(chan graphsync.ResponseData, 4)
	expectedError := make(chan error, 4)
	expectedUpdateChan := make(chan []graphsync.ExtensionData, 4)
	hook := func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		receivedBlocks <- blockData
		receivedResponses <- responseData
		err := <-expectedError
		if err != nil {
			hookActions.TerminateWithError(err)
		}
		update := <-expectedUpdateChan
		if len(update) > 0 {
			hookActions.UpdateRequestWithExtensions(update...)
		}
	}
	td.blockHooks.Register(hook)
	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector(), td.extension1, td.extension2)

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	gsr := rr.gsr
	returnedData1, found := gsr.Extension(td.extensionName1)
	require.True(t, found)
	require.Equal(t, td.extensionData1, returnedData1, "did not encode first extension correctly")

	returnedData2, found := gsr.Extension(td.extensionName2)
	require.True(t, found)
	require.Equal(t, td.extensionData2, returnedData2, "did not encode second extension correctly")

	t.Run("responding to extensions", func(t *testing.T) {
		expectedData := testutil.RandomBytes(100)
		expectedUpdate := testutil.RandomBytes(100)

		firstBlocks := td.blockChain.Blocks(0, 3)
		firstMetadata := metadataForBlocks(firstBlocks, true)
		firstMetadataEncoded, err := metadata.EncodeMetadata(firstMetadata)
		require.NoError(t, err, "did not encode metadata")
		firstResponses := []gsmsg.GraphSyncResponse{
			gsmsg.NewResponse(gsr.ID(),
				graphsync.PartialResponse, graphsync.ExtensionData{
					Name: graphsync.ExtensionMetadata,
					Data: firstMetadataEncoded,
				},
				graphsync.ExtensionData{
					Name: td.extensionName1,
					Data: expectedData,
				},
			),
		}
		for i := range firstBlocks {
			expectedError <- nil
			var update []graphsync.ExtensionData
			if i == len(firstBlocks)-1 {
				update = []graphsync.ExtensionData{
					{
						Name: td.extensionName1,
						Data: expectedUpdate,
					},
				}
			}
			expectedUpdateChan <- update
		}

		td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
		td.fal.VerifyLastProcessedBlocks(ctx, t, firstBlocks)
		td.fal.VerifyLastProcessedResponses(ctx, t, map[graphsync.RequestID]metadata.Metadata{
			rr.gsr.ID(): firstMetadata,
		})
		td.fal.SuccessResponseOn(peers[0], rr.gsr.ID(), firstBlocks)

		ur := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
		receivedUpdateData, has := ur.gsr.Extension(td.extensionName1)
		require.True(t, has)
		require.Equal(t, expectedUpdate, receivedUpdateData, "should have updated with correct extension")

		for _, blk := range firstBlocks {
			var receivedResponse graphsync.ResponseData
			testutil.AssertReceive(ctx, t, receivedResponses, &receivedResponse, "did not receive response data")
			require.Equal(t, firstResponses[0].RequestID(), receivedResponse.RequestID(), "did not receive correct response ID")
			require.Equal(t, firstResponses[0].Status(), receivedResponse.Status(), "did not receive correct response status")
			metadata, has := receivedResponse.Extension(graphsync.ExtensionMetadata)
			require.True(t, has)
			require.Equal(t, firstMetadataEncoded, metadata, "should receive correct metadata")
			receivedExtensionData, _ := receivedResponse.Extension(td.extensionName1)
			require.Equal(t, expectedData, receivedExtensionData, "should receive correct response extension data")
			var receivedBlock graphsync.BlockData
			testutil.AssertReceive(ctx, t, receivedBlocks, &receivedBlock, "did not receive block data")
			require.Equal(t, blk.Cid(), receivedBlock.Link().(cidlink.Link).Cid)
			require.Equal(t, uint64(len(blk.RawData())), receivedBlock.BlockSize())
		}

		nextExpectedData := testutil.RandomBytes(100)
		nextExpectedUpdate1 := testutil.RandomBytes(100)
		nextExpectedUpdate2 := testutil.RandomBytes(100)
		nextBlocks := td.blockChain.RemainderBlocks(3)
		nextMetadata := metadataForBlocks(nextBlocks, true)
		nextMetadataEncoded, err := metadata.EncodeMetadata(nextMetadata)
		require.NoError(t, err)
		secondResponses := []gsmsg.GraphSyncResponse{
			gsmsg.NewResponse(gsr.ID(),
				graphsync.RequestCompletedFull, graphsync.ExtensionData{
					Name: graphsync.ExtensionMetadata,
					Data: nextMetadataEncoded,
				},
				graphsync.ExtensionData{
					Name: td.extensionName1,
					Data: nextExpectedData,
				},
			),
		}
		for i := range nextBlocks {
			expectedError <- nil
			var update []graphsync.ExtensionData
			if i == len(nextBlocks)-1 {
				update = []graphsync.ExtensionData{
					{
						Name: td.extensionName1,
						Data: nextExpectedUpdate1,
					},
					{
						Name: td.extensionName2,
						Data: nextExpectedUpdate2,
					},
				}
			}
			expectedUpdateChan <- update
		}
		td.requestManager.ProcessResponses(peers[0], secondResponses, nextBlocks)
		td.fal.VerifyLastProcessedBlocks(ctx, t, nextBlocks)
		td.fal.VerifyLastProcessedResponses(ctx, t, map[graphsync.RequestID]metadata.Metadata{
			rr.gsr.ID(): nextMetadata,
		})
		td.fal.SuccessResponseOn(peers[0], rr.gsr.ID(), nextBlocks)

		ur = readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
		receivedUpdateData, has = ur.gsr.Extension(td.extensionName1)
		require.True(t, has)
		require.Equal(t, nextExpectedUpdate1, receivedUpdateData, "should have updated with correct extension")
		receivedUpdateData, has = ur.gsr.Extension(td.extensionName2)
		require.True(t, has)
		require.Equal(t, nextExpectedUpdate2, receivedUpdateData, "should have updated with correct extension")

		for _, blk := range nextBlocks {
			var receivedResponse graphsync.ResponseData
			testutil.AssertReceive(ctx, t, receivedResponses, &receivedResponse, "did not receive response data")
			require.Equal(t, secondResponses[0].RequestID(), receivedResponse.RequestID(), "did not receive correct response ID")
			require.Equal(t, secondResponses[0].Status(), receivedResponse.Status(), "did not receive correct response status")
			metadata, has := receivedResponse.Extension(graphsync.ExtensionMetadata)
			require.True(t, has)
			require.Equal(t, nextMetadataEncoded, metadata, "should receive correct metadata")
			receivedExtensionData, _ := receivedResponse.Extension(td.extensionName1)
			require.Equal(t, nextExpectedData, receivedExtensionData, "should receive correct response extension data")
			var receivedBlock graphsync.BlockData
			testutil.AssertReceive(ctx, t, receivedBlocks, &receivedBlock, "did not receive block data")
			require.Equal(t, blk.Cid(), receivedBlock.Link().(cidlink.Link).Cid)
			require.Equal(t, uint64(len(blk.RawData())), receivedBlock.BlockSize())
		}

		testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan)
		td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan)
	})
}

func TestOutgoingRequestHooks(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	hook := func(p peer.ID, r graphsync.RequestData, ha graphsync.OutgoingRequestHookActions) {
		_, has := r.Extension(td.extensionName1)
		if has {
			ha.UseLinkTargetNodePrototypeChooser(td.blockChain.Chooser)
			ha.UsePersistenceOption("chainstore")
		}
	}
	td.requestHooks.Register(hook)

	returnedResponseChan1, returnedErrorChan1 := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector(), td.extension1)
	returnedResponseChan2, returnedErrorChan2 := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 2)

	dedupData, has := requestRecords[0].gsr.Extension(graphsync.ExtensionDeDupByKey)
	require.True(t, has)
	key, err := dedupkey.DecodeDedupKey(dedupData)
	require.NoError(t, err)
	require.Equal(t, "chainstore", key)

	md := metadataForBlocks(td.blockChain.AllBlocks(), true)
	mdEncoded, err := metadata.EncodeMetadata(md)
	require.NoError(t, err)
	mdExt := graphsync.ExtensionData{
		Name: graphsync.ExtensionMetadata,
		Data: mdEncoded,
	}
	responses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.RequestCompletedFull, mdExt),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.RequestCompletedFull, mdExt),
	}
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.AllBlocks())
	td.fal.VerifyLastProcessedBlocks(ctx, t, td.blockChain.AllBlocks())
	td.fal.VerifyLastProcessedResponses(ctx, t, map[graphsync.RequestID]metadata.Metadata{
		requestRecords[0].gsr.ID(): md,
		requestRecords[1].gsr.ID(): md,
	})
	td.fal.SuccessResponseOn(peers[0], requestRecords[0].gsr.ID(), td.blockChain.AllBlocks())
	td.fal.SuccessResponseOn(peers[0], requestRecords[1].gsr.ID(), td.blockChain.AllBlocks())

	td.blockChain.VerifyWholeChainWithTypes(requestCtx, returnedResponseChan1)
	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan2)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan1)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan2)
	td.fal.VerifyStoreUsed(t, requestRecords[0].gsr.ID(), "chainstore")
	td.fal.VerifyStoreUsed(t, requestRecords[1].gsr.ID(), "")
}

type outgoingRequestProcessingEvent struct {
	p                      peer.ID
	request                graphsync.RequestData
	inProgressRequestCount int
}

func TestOutgoingRequestListeners(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	// Listen for outgoing request starts
	outgoingRequests := make(chan outgoingRequestProcessingEvent, 1)
	td.outgoingRequestProcessingListeners.Register(func(p peer.ID, request graphsync.RequestData, inProgressRequestCount int) {
		outgoingRequests <- outgoingRequestProcessingEvent{p, request, inProgressRequestCount}
	})

	returnedResponseChan1, returnedErrorChan1 := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector(), td.extension1)

	requestRecords := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)

	// Should have fired by now
	select {
	case or := <-outgoingRequests:
		require.Equal(t, peers[0], or.p)
		require.Equal(t, td.blockChain.Selector(), or.request.Selector())
		require.Equal(t, td.blockChain.TipLink, cidlink.Link{Cid: or.request.Root()})
		require.Equal(t, 1, or.inProgressRequestCount)
	default:
		t.Fatal("should fire outgoing request listener")
	}

	md := metadataForBlocks(td.blockChain.AllBlocks(), true)
	mdEncoded, err := metadata.EncodeMetadata(md)
	require.NoError(t, err)
	mdExt := graphsync.ExtensionData{
		Name: graphsync.ExtensionMetadata,
		Data: mdEncoded,
	}
	responses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.RequestCompletedFull, mdExt),
	}
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.AllBlocks())
	td.fal.VerifyLastProcessedBlocks(ctx, t, td.blockChain.AllBlocks())
	td.fal.VerifyLastProcessedResponses(ctx, t, map[graphsync.RequestID]metadata.Metadata{
		requestRecords[0].gsr.ID(): md,
	})
	td.fal.SuccessResponseOn(peers[0], requestRecords[0].gsr.ID(), td.blockChain.AllBlocks())

	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan1)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan1)
}

func TestPauseResume(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blocksReceived := 0
	holdForResumeAttempt := make(chan struct{})
	holdForPause := make(chan struct{})
	pauseAt := 3

	// setup hook to pause at 3rd block (and wait on second block for resume while unpaused test)
	hook := func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		blocksReceived++
		if blocksReceived == pauseAt-1 {
			<-holdForResumeAttempt
		}
		if blocksReceived == pauseAt {
			hookActions.PauseRequest()
			close(holdForPause)
		}
	}
	td.blockHooks.Register(hook)

	// Start request
	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	// Start processing responses
	md := metadataForBlocks(td.blockChain.AllBlocks(), true)
	mdEncoded, err := metadata.EncodeMetadata(md)
	require.NoError(t, err)
	responses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, graphsync.ExtensionData{
			Name: graphsync.ExtensionMetadata,
			Data: mdEncoded,
		}),
	}
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.AllBlocks())
	td.fal.SuccessResponseOn(peers[0], rr.gsr.ID(), td.blockChain.AllBlocks())

	// attempt to unpause while request is not paused (note: hook on second block will keep it from
	// reaching pause point)
	err = td.requestManager.UnpauseRequest(rr.gsr.ID())
	require.EqualError(t, err, "request is not paused")
	close(holdForResumeAttempt)
	// verify responses sent read ONLY for blocks BEFORE the pause
	td.blockChain.VerifyResponseRange(ctx, returnedResponseChan, 0, pauseAt)
	// wait for the pause to occur
	<-holdForPause

	// read the outgoing cancel request
	pauseCancel := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
	require.True(t, pauseCancel.gsr.IsCancel())

	// verify no further responses come through
	time.Sleep(100 * time.Millisecond)
	testutil.AssertChannelEmpty(t, returnedResponseChan, "no response should be sent request is paused")
	td.fal.CleanupRequest(peers[0], rr.gsr.ID())

	// unpause
	err = td.requestManager.UnpauseRequest(rr.gsr.ID(), td.extension1, td.extension2)
	require.NoError(t, err)

	// verify the correct new request with Do-no-send-cids & other extensions
	resumedRequest := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
	doNotSendCidsData, has := resumedRequest.gsr.Extension(graphsync.ExtensionDoNotSendCIDs)
	doNotSendCids, err := cidset.DecodeCidSet(doNotSendCidsData)
	require.NoError(t, err)
	require.Equal(t, pauseAt, doNotSendCids.Len())
	require.True(t, has)
	ext1Data, has := resumedRequest.gsr.Extension(td.extensionName1)
	require.True(t, has)
	require.Equal(t, td.extensionData1, ext1Data)
	ext2Data, has := resumedRequest.gsr.Extension(td.extensionName2)
	require.True(t, has)
	require.Equal(t, td.extensionData2, ext2Data)

	// process responses
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.RemainderBlocks(pauseAt))
	td.fal.SuccessResponseOn(peers[0], rr.gsr.ID(), td.blockChain.AllBlocks())

	// verify the correct results are returned, picking up after where there request was paused
	td.blockChain.VerifyRemainder(ctx, returnedResponseChan, pauseAt)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan)
}
func TestPauseResumeExternal(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blocksReceived := 0
	holdForPause := make(chan struct{})
	pauseAt := 3

	// setup hook to pause at 3rd block (and wait on second block for resume while unpaused test)
	hook := func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		blocksReceived++
		if blocksReceived == pauseAt {
			err := td.requestManager.PauseRequest(responseData.RequestID())
			require.NoError(t, err)
			close(holdForPause)
		}
	}
	td.blockHooks.Register(hook)

	// Start request
	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	// Start processing responses
	md := metadataForBlocks(td.blockChain.AllBlocks(), true)
	mdEncoded, err := metadata.EncodeMetadata(md)
	require.NoError(t, err)
	responses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, graphsync.ExtensionData{
			Name: graphsync.ExtensionMetadata,
			Data: mdEncoded,
		}),
	}
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.AllBlocks())
	td.fal.SuccessResponseOn(peers[0], rr.gsr.ID(), td.blockChain.AllBlocks())
	// verify responses sent read ONLY for blocks BEFORE the pause
	td.blockChain.VerifyResponseRange(ctx, returnedResponseChan, 0, pauseAt)
	// wait for the pause to occur
	<-holdForPause

	// read the outgoing cancel request
	pauseCancel := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
	require.True(t, pauseCancel.gsr.IsCancel())

	// verify no further responses come through
	time.Sleep(100 * time.Millisecond)
	testutil.AssertChannelEmpty(t, returnedResponseChan, "no response should be sent request is paused")
	td.fal.CleanupRequest(peers[0], rr.gsr.ID())

	// unpause
	err = td.requestManager.UnpauseRequest(rr.gsr.ID(), td.extension1, td.extension2)
	require.NoError(t, err)

	// verify the correct new request with Do-no-send-cids & other extensions
	resumedRequest := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
	doNotSendCidsData, has := resumedRequest.gsr.Extension(graphsync.ExtensionDoNotSendCIDs)
	doNotSendCids, err := cidset.DecodeCidSet(doNotSendCidsData)
	require.NoError(t, err)
	require.Equal(t, pauseAt, doNotSendCids.Len())
	require.True(t, has)
	ext1Data, has := resumedRequest.gsr.Extension(td.extensionName1)
	require.True(t, has)
	require.Equal(t, td.extensionData1, ext1Data)
	ext2Data, has := resumedRequest.gsr.Extension(td.extensionName2)
	require.True(t, has)
	require.Equal(t, td.extensionData2, ext2Data)

	// process responses
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.RemainderBlocks(pauseAt))
	td.fal.SuccessResponseOn(peers[0], rr.gsr.ID(), td.blockChain.AllBlocks())

	// verify the correct results are returned, picking up after where there request was paused
	td.blockChain.VerifyRemainder(ctx, returnedResponseChan, pauseAt)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan)
}

func TestStats(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(2)

	blockChain2 := testutil.SetupBlockChain(ctx, t, td.persistence, 100, 5)

	_, _ = td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())
	_, _ = td.requestManager.NewRequest(requestCtx, peers[0], blockChain2.TipLink, blockChain2.Selector())
	_, _ = td.requestManager.NewRequest(requestCtx, peers[1], td.blockChain.TipLink, td.blockChain.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 3)

	peerState := td.requestManager.PeerState(peers[0])
	require.Len(t, peerState.RequestStates, 2)
	require.Equal(t, peerState.RequestStates[requestRecords[0].gsr.ID()], graphsync.Running)
	require.Equal(t, peerState.RequestStates[requestRecords[1].gsr.ID()], graphsync.Running)
	require.Len(t, peerState.Active, 2)
	require.Contains(t, peerState.Active, requestRecords[0].gsr.ID())
	require.Contains(t, peerState.Active, requestRecords[1].gsr.ID())
	require.Len(t, peerState.Pending, 0)
}

type requestRecord struct {
	gsr gsmsg.GraphSyncRequest
	p   peer.ID
}

type fakePeerHandler struct {
	requestRecordChan chan requestRecord
}

func (fph *fakePeerHandler) AllocateAndBuildMessage(p peer.ID, blkSize uint64,
	requestBuilder func(b *messagequeue.Builder)) {
	builder := messagequeue.NewBuilder(context.TODO(), messagequeue.Topic(0))
	requestBuilder(builder)
	message, err := builder.Build()
	if err != nil {
		panic(err)
	}
	fph.requestRecordChan <- requestRecord{
		gsr: message.Requests()[0],
		p:   p,
	}
}

func readNNetworkRequests(ctx context.Context,
	t *testing.T,
	requestRecordChan <-chan requestRecord,
	count int) []requestRecord {
	requestRecords := make([]requestRecord, 0, count)
	for i := 0; i < count; i++ {
		var rr requestRecord
		testutil.AssertReceive(ctx, t, requestRecordChan, &rr, fmt.Sprintf("did not receive request %d", i))
		requestRecords = append(requestRecords, rr)
	}
	// because of the simultaneous request queues it's possible for the requests to go to the network layer out of order
	// if the requests are queued at a near identical time
	// TODO: howdo?
	/*
		sort.Slice(requestRecords, func(i, j int) bool {
			return requestRecords[i].gsr.ID() < requestRecords[j].gsr.ID()
		})
	*/
	return requestRecords
}

func metadataForBlocks(blks []blocks.Block, present bool) metadata.Metadata {
	md := make(metadata.Metadata, 0, len(blks))
	for _, block := range blks {
		md = append(md, metadata.Item{
			Link:         block.Cid(),
			BlockPresent: present,
		})
	}
	return md
}

func encodedMetadataForBlocks(t *testing.T, blks []blocks.Block, present bool) graphsync.ExtensionData {
	t.Helper()
	md := metadataForBlocks(blks, present)
	metadataEncoded, err := metadata.EncodeMetadata(md)
	require.NoError(t, err, "did not encode metadata")
	return graphsync.ExtensionData{
		Name: graphsync.ExtensionMetadata,
		Data: metadataEncoded,
	}
}

type testData struct {
	requestRecordChan                  chan requestRecord
	fph                                *fakePeerHandler
	fal                                *testloader.FakeAsyncLoader
	tcm                                *testutil.TestConnManager
	requestHooks                       *hooks.OutgoingRequestHooks
	responseHooks                      *hooks.IncomingResponseHooks
	blockHooks                         *hooks.IncomingBlockHooks
	requestManager                     *RequestManager
	blockStore                         map[ipld.Link][]byte
	persistence                        ipld.LinkSystem
	blockChain                         *testutil.TestBlockChain
	extensionName1                     graphsync.ExtensionName
	extensionData1                     []byte
	extension1                         graphsync.ExtensionData
	extensionName2                     graphsync.ExtensionName
	extensionData2                     []byte
	extension2                         graphsync.ExtensionData
	networkErrorListeners              *listeners.NetworkErrorListeners
	outgoingRequestProcessingListeners *listeners.OutgoingRequestProcessingListeners
	taskqueue                          *taskqueue.WorkerTaskQueue
	executor                           *executor.Executor
}

func newTestData(ctx context.Context, t *testing.T) *testData {
	t.Helper()
	td := &testData{}
	td.requestRecordChan = make(chan requestRecord, 3)
	td.fph = &fakePeerHandler{td.requestRecordChan}
	td.fal = testloader.NewFakeAsyncLoader()
	td.tcm = testutil.NewTestConnManager()
	td.requestHooks = hooks.NewRequestHooks()
	td.responseHooks = hooks.NewResponseHooks()
	td.blockHooks = hooks.NewBlockHooks()
	td.networkErrorListeners = listeners.NewNetworkErrorListeners()
	td.outgoingRequestProcessingListeners = listeners.NewOutgoingRequestProcessingListeners()
	td.taskqueue = taskqueue.NewTaskQueue(ctx)
	lsys := cidlink.DefaultLinkSystem()
	td.requestManager = New(ctx, td.fal, lsys, td.requestHooks, td.responseHooks, td.networkErrorListeners, td.outgoingRequestProcessingListeners, td.taskqueue, td.tcm, 0)
	td.executor = executor.NewExecutor(td.requestManager, td.blockHooks, td.fal.AsyncLoad)
	td.requestManager.SetDelegate(td.fph)
	td.requestManager.Startup()
	td.taskqueue.Startup(6, td.executor)
	td.blockStore = make(map[ipld.Link][]byte)
	td.persistence = testutil.NewTestStore(td.blockStore)
	td.blockChain = testutil.SetupBlockChain(ctx, t, td.persistence, 100, 5)
	td.extensionData1 = testutil.RandomBytes(100)
	td.extensionName1 = graphsync.ExtensionName("AppleSauce/McGee")
	td.extension1 = graphsync.ExtensionData{
		Name: td.extensionName1,
		Data: td.extensionData1,
	}
	td.extensionData2 = testutil.RandomBytes(100)
	td.extensionName2 = graphsync.ExtensionName("HappyLand/Happenstance")
	td.extension2 = graphsync.ExtensionData{
		Name: td.extensionName2,
		Data: td.extensionData2,
	}
	return td
}
