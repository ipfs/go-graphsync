package requestmanager

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/dedupkey"
	"github.com/ipfs/go-graphsync/listeners"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/persistenceoptions"
	"github.com/ipfs/go-graphsync/requestmanager/executor"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
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

	requestRecords := readNNetworkRequests(requestCtx, t, td, 2)

	td.tcm.AssertProtected(t, peers[0])
	td.tcm.AssertProtectedWithTags(t, peers[0], requestRecords[0].gsr.ID().Tag(), requestRecords[1].gsr.ID().Tag())
	require.Equal(t, peers[0], requestRecords[0].p)
	require.Equal(t, peers[0], requestRecords[1].p)
	require.Equal(t, requestRecords[0].gsr.Type(), graphsync.RequestTypeNew)
	require.Equal(t, defaultPriority, requestRecords[0].gsr.Priority())
	require.Equal(t, defaultPriority, requestRecords[1].gsr.Priority())

	require.Equal(t, td.blockChain.TipLink.String(), requestRecords[0].gsr.Root().String())
	require.Equal(t, td.blockChain.Selector(), requestRecords[0].gsr.Selector(), "did not encode selector properly")
	require.Equal(t, blockChain2.TipLink.String(), requestRecords[1].gsr.Root().String())
	require.Equal(t, blockChain2.Selector(), requestRecords[1].gsr.Selector(), "did not encode selector properly")

	firstBlocks := append(td.blockChain.AllBlocks(), blockChain2.Blocks(0, 3)...)
	firstMetadata1 := metadataForBlocks(td.blockChain.AllBlocks(), graphsync.LinkActionPresent)
	firstMetadata2 := metadataForBlocks(blockChain2.Blocks(0, 3), graphsync.LinkActionPresent)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.RequestCompletedFull, firstMetadata1),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.PartialResponse, firstMetadata2),
	}

	td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)

	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan1)
	blockChain2.VerifyResponseRange(requestCtx, returnedResponseChan2, 0, 3)

	td.tcm.AssertProtected(t, peers[0])
	td.tcm.RefuteProtectedWithTags(t, peers[0], requestRecords[0].gsr.ID().Tag())
	td.tcm.AssertProtectedWithTags(t, peers[0], requestRecords[1].gsr.ID().Tag())

	moreBlocks := blockChain2.RemainderBlocks(3)
	moreMetadata := metadataForBlocks(moreBlocks, graphsync.LinkActionPresent)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
	}

	td.requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
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

	requestRecords := readNNetworkRequests(requestCtx, t, td, 2)

	td.tcm.AssertProtected(t, peers[0])
	td.tcm.AssertProtectedWithTags(t, peers[0], requestRecords[0].gsr.ID().Tag(), requestRecords[1].gsr.ID().Tag())

	firstBlocks := td.blockChain.Blocks(0, 3)
	firstMetadata := metadataForBlocks(firstBlocks, graphsync.LinkActionPresent)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.PartialResponse, firstMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.PartialResponse, firstMetadata),
	}

	td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	td.blockChain.VerifyResponseRange(requestCtx1, returnedResponseChan1, 0, 3)
	cancel1()
	rr := readNNetworkRequests(requestCtx, t, td, 1)[0]

	require.Equal(t, rr.gsr.Type(), graphsync.RequestTypeCancel)
	require.Equal(t, requestRecords[0].gsr.ID(), rr.gsr.ID())

	moreBlocks := td.blockChain.RemainderBlocks(3)
	moreMetadata := metadataForBlocks(moreBlocks, graphsync.LinkActionPresent)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
	}
	td.requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)

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

	_, returnedErrorChan1 := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, td, 1)

	td.tcm.AssertProtected(t, peers[0])
	td.tcm.AssertProtectedWithTags(t, peers[0], requestRecords[0].gsr.ID().Tag())

	go func() {
		firstBlocks := td.blockChain.Blocks(0, 3)
		firstMetadata := metadataForBlocks(firstBlocks, graphsync.LinkActionPresent)
		firstResponses := []gsmsg.GraphSyncResponse{
			gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.PartialResponse, firstMetadata),
		}
		td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	}()

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Second)
	defer timeoutCancel()
	err := td.requestManager.CancelRequest(timeoutCtx, requestRecords[0].gsr.ID())
	require.NoError(t, err)

	rr := readNNetworkRequests(requestCtx, t, td, 1)[0]

	require.Equal(t, rr.gsr.Type(), graphsync.RequestTypeCancel)
	require.Equal(t, requestRecords[0].gsr.ID(), rr.gsr.ID())

	td.tcm.RefuteProtected(t, peers[0])

	errors := testutil.CollectErrors(requestCtx, t, returnedErrorChan1)
	require.Len(t, errors, 1)
	_, ok := errors[0].(graphsync.RequestClientCancelledErr)
	require.True(t, ok)
}

func TestCancelManagerExitsGracefully(t *testing.T) {
	ctx := context.Background()
	managerCtx, managerCancel := context.WithCancel(ctx)
	td := newTestData(managerCtx, t)
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td, 1)[0]

	firstBlocks := td.blockChain.Blocks(0, 3)
	firstMetadata := metadataForBlocks(firstBlocks, graphsync.LinkActionPresent)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.PartialResponse, firstMetadata),
	}
	td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	td.blockChain.VerifyResponseRange(ctx, returnedResponseChan, 0, 3)
	managerCancel()

	moreBlocks := td.blockChain.RemainderBlocks(3)
	moreMetadata := metadataForBlocks(moreBlocks, graphsync.LinkActionPresent)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
	}
	td.requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
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

	rr := readNNetworkRequests(requestCtx, t, td, 1)[0]
	td.tcm.AssertProtected(t, peers[0])
	td.tcm.AssertProtectedWithTags(t, peers[0], rr.gsr.ID().Tag())

	failedResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestFailedContentNotFound, nil),
	}
	td.requestManager.ProcessResponses(peers[0], failedResponses, nil)

	testutil.VerifySingleTerminalError(requestCtx, t, returnedErrorChan)
	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
	td.tcm.RefuteProtected(t, peers[0])
}

/*
TODO: Delete? These tests no longer seem relevant, or at minimum need a rearchitect
- the new architecture will simply never fire a graphsync request if all of the data is
preset

Perhaps we should put this back in as a mode? Or make the "wait to fire" and exprimental feature?

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
*/

func TestRequestReturnsMissingBlocks(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td, 1)[0]

	md := metadataForBlocks(td.blockChain.AllBlocks(), graphsync.LinkActionMissing)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedPartial, md),
	}
	td.requestManager.ProcessResponses(peers[0], firstResponses, nil)
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
	receivedExtensionData := make(chan datamodel.Node, 2)
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

	rr := readNNetworkRequests(requestCtx, t, td, 1)[0]

	gsr := rr.gsr
	returnedData1, found := gsr.Extension(td.extensionName1)
	require.True(t, found)
	require.Equal(t, td.extensionData1, returnedData1, "did not encode first extension correctly")

	returnedData2, found := gsr.Extension(td.extensionName2)
	require.True(t, found)
	require.Equal(t, td.extensionData2, returnedData2, "did not encode second extension correctly")

	t.Run("responding to extensions", func(t *testing.T) {
		expectedData := basicnode.NewBytes(testutil.RandomBytes(100))
		expectedUpdate := basicnode.NewBytes(testutil.RandomBytes(100))
		firstResponses := []gsmsg.GraphSyncResponse{
			gsmsg.NewResponse(gsr.ID(),
				graphsync.PartialResponse,
				nil,
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
		var received datamodel.Node
		testutil.AssertReceive(ctx, t, receivedExtensionData, &received, "did not receive extension data")
		require.Equal(t, expectedData, received, "did not receive correct extension data from resposne")

		rr = readNNetworkRequests(requestCtx, t, td, 1)[0]
		receivedUpdateData, has := rr.gsr.Extension(td.extensionName1)
		require.True(t, has)
		require.Equal(t, expectedUpdate, receivedUpdateData, "should have updated with correct extension")

		nextExpectedData := basicnode.NewBytes(testutil.RandomBytes(100))
		nextExpectedUpdate1 := basicnode.NewBytes(testutil.RandomBytes(100))
		nextExpectedUpdate2 := basicnode.NewBytes(testutil.RandomBytes(100))

		secondResponses := []gsmsg.GraphSyncResponse{
			gsmsg.NewResponse(gsr.ID(),
				graphsync.PartialResponse,
				nil,
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

		rr = readNNetworkRequests(requestCtx, t, td, 1)[0]
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

	rr := readNNetworkRequests(requestCtx, t, td, 1)[0]

	gsr := rr.gsr
	returnedData1, found := gsr.Extension(td.extensionName1)
	require.True(t, found)
	require.Equal(t, td.extensionData1, returnedData1, "did not encode first extension correctly")

	returnedData2, found := gsr.Extension(td.extensionName2)
	require.True(t, found)
	require.Equal(t, td.extensionData2, returnedData2, "did not encode second extension correctly")

	t.Run("responding to extensions", func(t *testing.T) {
		expectedData := basicnode.NewBytes(testutil.RandomBytes(100))
		expectedUpdate := basicnode.NewBytes(testutil.RandomBytes(100))

		firstBlocks := td.blockChain.Blocks(0, 3)
		firstMetadata := metadataForBlocks(firstBlocks, graphsync.LinkActionPresent)
		firstResponses := []gsmsg.GraphSyncResponse{
			gsmsg.NewResponse(gsr.ID(),
				graphsync.PartialResponse,
				firstMetadata,
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

		ur := readNNetworkRequests(requestCtx, t, td, 1)[0]
		receivedUpdateData, has := ur.gsr.Extension(td.extensionName1)
		require.True(t, has)
		require.Equal(t, expectedUpdate, receivedUpdateData, "should have updated with correct extension")

		for _, blk := range firstBlocks {
			var receivedResponse graphsync.ResponseData
			testutil.AssertReceive(ctx, t, receivedResponses, &receivedResponse, "did not receive response data")
			require.Equal(t, firstResponses[0].RequestID(), receivedResponse.RequestID(), "did not receive correct response ID")
			require.Equal(t, firstResponses[0].Status(), receivedResponse.Status(), "did not receive correct response status")
			md := make([]gsmsg.GraphSyncLinkMetadatum, 0)
			receivedResponse.Metadata().Iterate(func(c cid.Cid, la graphsync.LinkAction) {
				md = append(md, gsmsg.GraphSyncLinkMetadatum{Link: c, Action: graphsync.LinkActionPresent})
			})
			require.Greater(t, len(md), 0)
			require.Equal(t, firstMetadata, md, "should receive correct metadata")
			receivedExtensionData, _ := receivedResponse.Extension(td.extensionName1)
			require.Equal(t, expectedData, receivedExtensionData, "should receive correct response extension data")
			var receivedBlock graphsync.BlockData
			testutil.AssertReceive(ctx, t, receivedBlocks, &receivedBlock, "did not receive block data")
			require.Equal(t, blk.Cid(), receivedBlock.Link().(cidlink.Link).Cid)
			require.Equal(t, uint64(len(blk.RawData())), receivedBlock.BlockSize())
		}

		nextExpectedData := basicnode.NewBytes(testutil.RandomBytes(100))
		nextExpectedUpdate1 := basicnode.NewBytes(testutil.RandomBytes(100))
		nextExpectedUpdate2 := basicnode.NewBytes(testutil.RandomBytes(100))
		nextBlocks := td.blockChain.RemainderBlocks(3)
		nextMetadata := metadataForBlocks(nextBlocks, graphsync.LinkActionPresent)
		secondResponses := []gsmsg.GraphSyncResponse{
			gsmsg.NewResponse(gsr.ID(),
				graphsync.RequestCompletedFull,
				nextMetadata,
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

		ur = readNNetworkRequests(requestCtx, t, td, 1)[0]
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
			md := make([]gsmsg.GraphSyncLinkMetadatum, 0)
			receivedResponse.Metadata().Iterate(func(c cid.Cid, la graphsync.LinkAction) {
				md = append(md, gsmsg.GraphSyncLinkMetadatum{Link: c, Action: graphsync.LinkActionPresent})
			})
			require.Greater(t, len(md), 0)
			require.Equal(t, nextMetadata, md, "should receive correct metadata")
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

	alternateStore := testutil.NewTestStore(make(map[datamodel.Link][]byte))
	td.persistenceOptions.Register("chainstore", alternateStore)
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

	requestRecords := readNNetworkRequests(requestCtx, t, td, 2)

	dedupData, has := requestRecords[0].gsr.Extension(graphsync.ExtensionDeDupByKey)
	require.True(t, has)
	key, err := dedupkey.DecodeDedupKey(dedupData)
	require.NoError(t, err)
	require.Equal(t, "chainstore", key)

	md := metadataForBlocks(td.blockChain.AllBlocks(), graphsync.LinkActionPresent)
	responses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.RequestCompletedFull, md),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.RequestCompletedFull, md),
	}
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.AllBlocks())

	td.blockChain.VerifyWholeChainWithTypes(requestCtx, returnedResponseChan1)
	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan2)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan1)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan2)
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

	requestRecords := readNNetworkRequests(requestCtx, t, td, 1)

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

	md := metadataForBlocks(td.blockChain.AllBlocks(), graphsync.LinkActionPresent)
	responses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.RequestCompletedFull, md),
	}
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.AllBlocks())

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

	rr := readNNetworkRequests(requestCtx, t, td, 1)[0]

	// Start processing responses
	md := metadataForBlocks(td.blockChain.AllBlocks(), graphsync.LinkActionPresent)
	responses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, md),
	}
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.AllBlocks())

	// attempt to unpause while request is not paused (note: hook on second block will keep it from
	// reaching pause point)
	err := td.requestManager.UnpauseRequest(ctx, rr.gsr.ID())
	require.EqualError(t, err, "request is not paused")
	close(holdForResumeAttempt)
	// verify responses sent read ONLY for blocks BEFORE the pause
	td.blockChain.VerifyResponseRange(ctx, returnedResponseChan, 0, pauseAt)
	// wait for the pause to occur
	<-holdForPause

	// read the outgoing cancel request
	pauseCancel := readNNetworkRequests(requestCtx, t, td, 1)[0]
	require.Equal(t, pauseCancel.gsr.Type(), graphsync.RequestTypeCancel)

	// verify no further responses come through
	time.Sleep(100 * time.Millisecond)
	testutil.AssertChannelEmpty(t, returnedResponseChan, "no response should be sent request is paused")

	// unpause
	err = td.requestManager.UnpauseRequest(ctx, rr.gsr.ID(), td.extension1, td.extension2)
	require.NoError(t, err)

	/*
		TODO: these are no longer used as the old responses are consumed upon restart, to minimize
		network utilization -- does this make sense? Maybe we should throw out these responses while paused?

		// verify the correct new request with Do-no-send-cids & other extensions
		resumedRequest := readNNetworkRequests(requestCtx, t, td, 1)[0]
		doNotSendFirstBlocksData, has := resumedRequest.gsr.Extension(graphsync.ExtensionsDoNotSendFirstBlocks)
		doNotSendFirstBlocks, err := donotsendfirstblocks.DecodeDoNotSendFirstBlocks(doNotSendFirstBlocksData)
		require.NoError(t, err)
		require.Equal(t, pauseAt, int(doNotSendFirstBlocks))
		require.True(t, has)
		ext1Data, has := resumedRequest.gsr.Extension(td.extensionName1)
		require.True(t, has)
		require.Equal(t, td.extensionData1, ext1Data)
		ext2Data, has := resumedRequest.gsr.Extension(td.extensionName2)
		require.True(t, has)
		require.Equal(t, td.extensionData2, ext2Data)
	*/

	// process responses
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.RemainderBlocks(pauseAt))
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
			err := td.requestManager.PauseRequest(ctx, responseData.RequestID())
			require.NoError(t, err)
			close(holdForPause)
		}
	}
	td.blockHooks.Register(hook)

	// Start request
	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td, 1)[0]

	// Start processing responses
	md := metadataForBlocks(td.blockChain.AllBlocks(), graphsync.LinkActionPresent)
	responses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, md),
	}
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.AllBlocks())
	// verify responses sent read ONLY for blocks BEFORE the pause
	td.blockChain.VerifyResponseRange(ctx, returnedResponseChan, 0, pauseAt)
	// wait for the pause to occur
	<-holdForPause

	// read the outgoing cancel request
	pauseCancel := readNNetworkRequests(requestCtx, t, td, 1)[0]
	require.Equal(t, pauseCancel.gsr.Type(), graphsync.RequestTypeCancel)

	// verify no further responses come through
	time.Sleep(100 * time.Millisecond)
	testutil.AssertChannelEmpty(t, returnedResponseChan, "no response should be sent request is paused")

	// unpause
	err := td.requestManager.UnpauseRequest(ctx, rr.gsr.ID(), td.extension1, td.extension2)
	require.NoError(t, err)

	// verify the correct new request with Do-no-send-cids & other extensions
	/* TODO: these are no longer used as the old responses are consumed upon restart, to minimize
	network utilization -- does this make sense? Maybe we should throw out these responses while paused?

	resumedRequest := readNNetworkRequests(requestCtx, t, td, 1)[0]
	doNotSendFirstBlocksData, has := resumedRequest.gsr.Extension(graphsync.ExtensionsDoNotSendFirstBlocks)
	doNotSendFirstBlocks, err := donotsendfirstblocks.DecodeDoNotSendFirstBlocks(doNotSendFirstBlocksData)
	require.NoError(t, err)
	require.Equal(t, pauseAt, int(doNotSendFirstBlocks))
	require.True(t, has)
	ext1Data, has := resumedRequest.gsr.Extension(td.extensionName1)
	require.True(t, has)
	require.Equal(t, td.extensionData1, ext1Data)
	ext2Data, has := resumedRequest.gsr.Extension(td.extensionName2)
	require.True(t, has)
	require.Equal(t, td.extensionData2, ext2Data)*/

	// process responses
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.RemainderBlocks(pauseAt))

	// verify the correct results are returned, picking up after where there request was paused
	td.blockChain.VerifyRemainder(ctx, returnedResponseChan, pauseAt)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan)
}

func TestUpdateRequest(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	requestCtx1, cancel1 := context.WithCancel(requestCtx)

	peers := testutil.GeneratePeers(1)

	blocksReceived := 0
	holdForPause := make(chan struct{})

	// setup hook to pause when the blocks start flowing
	hook := func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		blocksReceived++
		if blocksReceived == 1 {
			err := td.requestManager.PauseRequest(ctx, responseData.RequestID())
			require.NoError(t, err)
			close(holdForPause)
		}
	}
	td.blockHooks.Register(hook)

	// Start request
	returnedResponseChan, returnedErrorChan := td.requestManager.NewRequest(requestCtx1, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td, 1)[0]

	// Start processing responses
	md := metadataForBlocks(td.blockChain.AllBlocks(), graphsync.LinkActionPresent)
	responses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, md),
	}
	td.requestManager.ProcessResponses(peers[0], responses, td.blockChain.AllBlocks())
	td.blockChain.VerifyResponseRange(ctx, returnedResponseChan, 0, 1)

	// wait for the pause to occur
	<-holdForPause

	// read the outgoing cancel request
	readNNetworkRequests(requestCtx, t, td, 1)

	// verify no further responses come through
	time.Sleep(100 * time.Millisecond)
	testutil.AssertChannelEmpty(t, returnedResponseChan, "no response should be sent request is paused")

	// send an update with some custom extensions
	ext1Name := graphsync.ExtensionName("grip grop")
	ext1Data := "flim flam, blim blam"
	ext2Name := graphsync.ExtensionName("Humpty/Dumpty")
	var ext2Data int64 = 101

	err := td.requestManager.UpdateRequest(ctx, rr.gsr.ID(),
		graphsync.ExtensionData{Name: ext1Name, Data: basicnode.NewString(ext1Data)},
		graphsync.ExtensionData{Name: ext2Name, Data: basicnode.NewInt(ext2Data)},
	)
	require.NoError(t, err)

	// verify the correct new request and its extensions
	updatedRequest := readNNetworkRequests(requestCtx, t, td, 1)[0]

	actualData, has := updatedRequest.gsr.Extension(ext1Name)
	require.True(t, has, "has expected extension1")
	actualString, err := actualData.AsString()
	require.NoError(t, err)
	require.Equal(t, ext1Data, actualString)

	actualData, has = updatedRequest.gsr.Extension(ext2Name)
	require.True(t, has, "has expected extension2")
	actualInt, err := actualData.AsInt()
	require.NoError(t, err)
	require.Equal(t, ext2Data, actualInt)

	// pack down
	cancel1()
	errors := testutil.CollectErrors(requestCtx, t, returnedErrorChan)
	require.Len(t, errors, 1)
	_, ok := errors[0].(graphsync.RequestClientCancelledErr)
	require.True(t, ok)
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

	requestRecords := readNNetworkRequests(requestCtx, t, td, 3)

	peerState := td.requestManager.PeerState(peers[0])
	require.Len(t, peerState.RequestStates, 2)
	require.Equal(t, peerState.RequestStates[requestRecords[0].gsr.ID()], graphsync.Running)
	require.Equal(t, peerState.RequestStates[requestRecords[1].gsr.ID()], graphsync.Running)
	require.Len(t, peerState.Active, 2)
	require.Contains(t, peerState.Active, requestRecords[0].gsr.ID())
	require.Contains(t, peerState.Active, requestRecords[1].gsr.ID())
	require.Len(t, peerState.Pending, 0)
}

func TestCaptureRequestIDFromContext(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	expectedID := graphsync.NewRequestID()
	requestCtx = context.WithValue(requestCtx, graphsync.RequestIDContextKey{}, expectedID)

	peers := testutil.GeneratePeers(1)

	_, _ = td.requestManager.NewRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, td, 1)
	require.Equal(t, expectedID, requestRecords[0].gsr.ID())
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

func readNNetworkRequests(ctx context.Context, t *testing.T, td *testData, count int) []requestRecord {
	requestRecords := make(map[graphsync.RequestID]requestRecord, count)
	for i := 0; i < count; i++ {
		var rr requestRecord
		testutil.AssertReceive(ctx, t, td.requestRecordChan, &rr, fmt.Sprintf("did not receive request %d", i))
		requestRecords[rr.gsr.ID()] = rr
	}
	// because of the simultaneous request queues it's possible for the requests to go to the network layer out of order
	// if the requests are queued at a near identical time
	sorted := make([]requestRecord, 0, len(requestRecords))
	for _, id := range td.requestIds {
		if rr, ok := requestRecords[id]; ok {
			sorted = append(sorted, rr)
		}
	}
	return sorted
}

func metadataForBlocks(blks []blocks.Block, action graphsync.LinkAction) []gsmsg.GraphSyncLinkMetadatum {
	md := make([]gsmsg.GraphSyncLinkMetadatum, 0, len(blks))
	for _, block := range blks {
		md = append(md, gsmsg.GraphSyncLinkMetadatum{
			Link:   block.Cid(),
			Action: action,
		})
	}
	return md
}

type testData struct {
	requestRecordChan                  chan requestRecord
	fph                                *fakePeerHandler
	persistenceOptions                 *persistenceoptions.PersistenceOptions
	tcm                                *testutil.TestConnManager
	requestHooks                       *hooks.OutgoingRequestHooks
	responseHooks                      *hooks.IncomingResponseHooks
	blockHooks                         *hooks.IncomingBlockHooks
	requestManager                     *RequestManager
	blockStore                         map[ipld.Link][]byte
	persistence                        ipld.LinkSystem
	localBlockStore                    map[ipld.Link][]byte
	localPersistence                   ipld.LinkSystem
	blockChain                         *testutil.TestBlockChain
	extensionName1                     graphsync.ExtensionName
	extensionData1                     datamodel.Node
	extension1                         graphsync.ExtensionData
	extensionName2                     graphsync.ExtensionName
	extensionData2                     datamodel.Node
	extension2                         graphsync.ExtensionData
	networkErrorListeners              *listeners.NetworkErrorListeners
	outgoingRequestProcessingListeners *listeners.RequestProcessingListeners
	taskqueue                          *taskqueue.WorkerTaskQueue
	executor                           *executor.Executor
	requestIds                         []graphsync.RequestID
}

func newTestData(ctx context.Context, t *testing.T) *testData {
	t.Helper()
	td := &testData{}
	td.requestRecordChan = make(chan requestRecord, 3)
	td.fph = &fakePeerHandler{td.requestRecordChan}
	td.persistenceOptions = persistenceoptions.New()
	td.tcm = testutil.NewTestConnManager()
	td.requestHooks = hooks.NewRequestHooks()
	td.responseHooks = hooks.NewResponseHooks()
	td.blockHooks = hooks.NewBlockHooks()
	td.networkErrorListeners = listeners.NewNetworkErrorListeners()
	td.outgoingRequestProcessingListeners = listeners.NewRequestProcessingListeners()
	td.taskqueue = taskqueue.NewTaskQueue(ctx)
	td.localBlockStore = make(map[ipld.Link][]byte)
	td.localPersistence = testutil.NewTestStore(td.localBlockStore)
	td.requestManager = New(ctx, td.persistenceOptions, td.localPersistence, td.requestHooks, td.responseHooks, td.networkErrorListeners, td.outgoingRequestProcessingListeners, td.taskqueue, td.tcm, 0, nil)
	td.executor = executor.NewExecutor(td.requestManager, td.blockHooks)
	td.requestManager.SetDelegate(td.fph)
	td.requestManager.Startup()
	td.taskqueue.Startup(6, td.executor)
	td.blockStore = make(map[ipld.Link][]byte)
	td.persistence = testutil.NewTestStore(td.blockStore)
	td.blockChain = testutil.SetupBlockChain(ctx, t, td.persistence, 100, 5)
	td.extensionData1 = basicnode.NewBytes(testutil.RandomBytes(100))
	td.extensionName1 = graphsync.ExtensionName("AppleSauce/McGee")
	td.extension1 = graphsync.ExtensionData{
		Name: td.extensionName1,
		Data: td.extensionData1,
	}
	td.extensionData2 = basicnode.NewBytes(testutil.RandomBytes(100))
	td.extensionName2 = graphsync.ExtensionName("HappyLand/Happenstance")
	td.extension2 = graphsync.ExtensionData{
		Name: td.extensionName2,
		Data: td.extensionData2,
	}
	td.requestHooks.Register(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
		td.requestIds = append(td.requestIds, request.ID())
	})
	return td
}
