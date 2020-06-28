package requestmanager

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync/requestmanager/testloader"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/metadata"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipld/go-ipld-prime"

	blocks "github.com/ipfs/go-block-format"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testutil"
)

type requestRecord struct {
	gsr gsmsg.GraphSyncRequest
	p   peer.ID
}

type fakePeerHandler struct {
	requestRecordChan chan requestRecord
}

func (fph *fakePeerHandler) SendRequest(p peer.ID,
	graphSyncRequest gsmsg.GraphSyncRequest) {
	fph.requestRecordChan <- requestRecord{
		gsr: graphSyncRequest,
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
	return requestRecords
}

func metadataForBlocks(blks []blocks.Block, present bool) metadata.Metadata {
	md := make(metadata.Metadata, 0, len(blks))
	for _, block := range blks {
		md = append(md, metadata.Item{
			Link:         cidlink.Link{Cid: block.Cid()},
			BlockPresent: present,
		})
	}
	return md
}

func encodedMetadataForBlocks(t *testing.T, blks []blocks.Block, present bool) graphsync.ExtensionData {
	md := metadataForBlocks(blks, present)
	metadataEncoded, err := metadata.EncodeMetadata(md)
	require.NoError(t, err, "did not encode metadata")
	return graphsync.ExtensionData{
		Name: graphsync.ExtensionMetadata,
		Data: metadataEncoded,
	}
}

func TestNormalSimultaneousFetch(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blockChain2 := testutil.SetupBlockChain(ctx, t, td.loader, td.storer, 100, 5)

	returnedResponseChan1, returnedErrorChan1 := td.requestManager.SendRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())
	returnedResponseChan2, returnedErrorChan2 := td.requestManager.SendRequest(requestCtx, peers[0], blockChain2.TipLink, blockChain2.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 2)

	require.Equal(t, peers[0], requestRecords[0].p)
	require.Equal(t, peers[0], requestRecords[1].p)
	require.False(t, requestRecords[0].gsr.IsCancel())
	require.False(t, requestRecords[1].gsr.IsCancel())
	require.Equal(t, defaultPriority, requestRecords[0].gsr.Priority())
	require.Equal(t, defaultPriority, requestRecords[1].gsr.Priority())

	require.Equal(t, td.blockChain.Selector(), requestRecords[0].gsr.Selector(), "did not encode selector properly")
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
	td.fal.SuccessResponseOn(requestRecords[0].gsr.ID(), td.blockChain.AllBlocks())
	td.fal.SuccessResponseOn(requestRecords[1].gsr.ID(), blockChain2.Blocks(0, 3))

	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan1)
	blockChain2.VerifyResponseRange(requestCtx, returnedResponseChan2, 0, 3)

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

	td.fal.SuccessResponseOn(requestRecords[1].gsr.ID(), moreBlocks)

	blockChain2.VerifyRemainder(requestCtx, returnedResponseChan2, 3)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan2)
}

func TestCancelRequestInProgress(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	requestCtx1, cancel1 := context.WithCancel(requestCtx)
	requestCtx2, cancel2 := context.WithCancel(requestCtx)
	defer cancel2()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan1, returnedErrorChan1 := td.requestManager.SendRequest(requestCtx1, peers[0], td.blockChain.TipLink, td.blockChain.Selector())
	returnedResponseChan2, returnedErrorChan2 := td.requestManager.SendRequest(requestCtx2, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 2)

	firstBlocks := td.blockChain.Blocks(0, 3)
	firstMetadata := encodedMetadataForBlocks(t, firstBlocks, true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.PartialResponse, firstMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.PartialResponse, firstMetadata),
	}

	td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)

	td.fal.SuccessResponseOn(requestRecords[0].gsr.ID(), firstBlocks)
	td.fal.SuccessResponseOn(requestRecords[1].gsr.ID(), firstBlocks)
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
	td.fal.SuccessResponseOn(requestRecords[0].gsr.ID(), moreBlocks)
	td.fal.SuccessResponseOn(requestRecords[1].gsr.ID(), moreBlocks)

	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan1)
	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan2)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan2)
}

func TestCancelManagerExitsGracefully(t *testing.T) {
	ctx := context.Background()
	managerCtx, managerCancel := context.WithCancel(ctx)
	td := newTestData(managerCtx, t)
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan, returnedErrorChan := td.requestManager.SendRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	firstBlocks := td.blockChain.Blocks(0, 3)
	firstMetadata := encodedMetadataForBlocks(t, firstBlocks, true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.PartialResponse, firstMetadata),
	}
	td.requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	td.fal.SuccessResponseOn(rr.gsr.ID(), firstBlocks)
	td.blockChain.VerifyResponseRange(ctx, returnedResponseChan, 0, 3)
	managerCancel()

	moreBlocks := td.blockChain.RemainderBlocks(3)
	moreMetadata := encodedMetadataForBlocks(t, moreBlocks, true)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
	}
	td.requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	td.fal.SuccessResponseOn(rr.gsr.ID(), moreBlocks)
	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan)
}

func TestFailedRequest(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan, returnedErrorChan := td.requestManager.SendRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
	failedResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestFailedContentNotFound),
	}
	td.requestManager.ProcessResponses(peers[0], failedResponses, nil)

	testutil.VerifySingleTerminalError(requestCtx, t, returnedErrorChan)
	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
}

func TestLocallyFulfilledFirstRequestFailsLater(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan, returnedErrorChan := td.requestManager.SendRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	// async loaded response responds immediately
	td.fal.SuccessResponseOn(rr.gsr.ID(), td.blockChain.AllBlocks())

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

	returnedResponseChan, returnedErrorChan := td.requestManager.SendRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	// async loaded response responds immediately
	td.fal.SuccessResponseOn(rr.gsr.ID(), td.blockChain.AllBlocks())

	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan)

	md := encodedMetadataForBlocks(t, td.blockChain.AllBlocks(), true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, md),
	}
	td.requestManager.ProcessResponses(peers[0], firstResponses, td.blockChain.AllBlocks())

	td.fal.VerifyNoRemainingData(t, rr.gsr.ID())
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan)
}

func TestRequestReturnsMissingBlocks(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	returnedResponseChan, returnedErrorChan := td.requestManager.SendRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	md := encodedMetadataForBlocks(t, td.blockChain.AllBlocks(), false)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedPartial, md),
	}
	td.requestManager.ProcessResponses(peers[0], firstResponses, nil)
	for _, block := range td.blockChain.AllBlocks() {
		td.fal.ResponseOn(rr.gsr.ID(), cidlink.Link{Cid: block.Cid()}, types.AsyncLoadResult{Data: nil, Err: fmt.Errorf("Terrible Thing")})
	}
	testutil.VerifyEmptyResponse(ctx, t, returnedResponseChan)
	errs := testutil.CollectErrors(ctx, t, returnedErrorChan)
	require.NotEqual(t, len(errs), 0, "did not send errors")
}

func TestEncodingExtensions(t *testing.T) {
	ctx := context.Background()
	td := newTestData(ctx, t)

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	extensionData1 := testutil.RandomBytes(100)
	extensionName1 := graphsync.ExtensionName("AppleSauce/McGee")
	extension1 := graphsync.ExtensionData{
		Name: extensionName1,
		Data: extensionData1,
	}
	extensionData2 := testutil.RandomBytes(100)
	extensionName2 := graphsync.ExtensionName("HappyLand/Happenstance")
	extension2 := graphsync.ExtensionData{
		Name: extensionName2,
		Data: extensionData2,
	}

	expectedError := make(chan error, 2)
	receivedExtensionData := make(chan []byte, 2)
	expectedUpdateChan := make(chan []graphsync.ExtensionData, 2)
	hook := func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
		data, has := responseData.Extension(extensionName1)
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
	returnedResponseChan, returnedErrorChan := td.requestManager.SendRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector(), extension1, extension2)

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	gsr := rr.gsr
	returnedData1, found := gsr.Extension(extensionName1)
	require.True(t, found)
	require.Equal(t, extensionData1, returnedData1, "did not encode first extension correctly")

	returnedData2, found := gsr.Extension(extensionName2)
	require.True(t, found)
	require.Equal(t, extensionData2, returnedData2, "did not encode second extension correctly")

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
					Name: extensionName1,
					Data: expectedData,
				},
			),
		}
		expectedError <- nil
		expectedUpdateChan <- []graphsync.ExtensionData{
			{
				Name: extensionName1,
				Data: expectedUpdate,
			},
		}
		td.requestManager.ProcessResponses(peers[0], firstResponses, nil)
		var received []byte
		testutil.AssertReceive(ctx, t, receivedExtensionData, &received, "did not receive extension data")
		require.Equal(t, expectedData, received, "did not receive correct extension data from resposne")

		rr = readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
		receivedUpdateData, has := rr.gsr.Extension(extensionName1)
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
					Name: extensionName1,
					Data: nextExpectedData,
				},
			),
		}
		expectedError <- errors.New("a terrible thing happened")
		expectedUpdateChan <- []graphsync.ExtensionData{
			{
				Name: extensionName1,
				Data: nextExpectedUpdate1,
			},
			{
				Name: extensionName2,
				Data: nextExpectedUpdate2,
			},
		}
		td.requestManager.ProcessResponses(peers[0], secondResponses, nil)
		testutil.AssertReceive(ctx, t, receivedExtensionData, &received, "did not receive extension data")
		require.Equal(t, nextExpectedData, received, "did not receive correct extension data from resposne")

		rr = readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
		receivedUpdateData, has = rr.gsr.Extension(extensionName1)
		require.True(t, has)
		require.Equal(t, nextExpectedUpdate1, receivedUpdateData, "should have updated with correct extension")
		receivedUpdateData, has = rr.gsr.Extension(extensionName2)
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

	extensionData1 := testutil.RandomBytes(100)
	extensionName1 := graphsync.ExtensionName("AppleSauce/McGee")
	extension1 := graphsync.ExtensionData{
		Name: extensionName1,
		Data: extensionData1,
	}
	extensionData2 := testutil.RandomBytes(100)
	extensionName2 := graphsync.ExtensionName("HappyLand/Happenstance")
	extension2 := graphsync.ExtensionData{
		Name: extensionName2,
		Data: extensionData2,
	}

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
	returnedResponseChan, returnedErrorChan := td.requestManager.SendRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector(), extension1, extension2)

	rr := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]

	gsr := rr.gsr
	returnedData1, found := gsr.Extension(extensionName1)
	require.True(t, found)
	require.Equal(t, extensionData1, returnedData1, "did not encode first extension correctly")

	returnedData2, found := gsr.Extension(extensionName2)
	require.True(t, found)
	require.Equal(t, extensionData2, returnedData2, "did not encode second extension correctly")

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
					Name: extensionName1,
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
						Name: extensionName1,
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
		td.fal.SuccessResponseOn(rr.gsr.ID(), firstBlocks)

		ur := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
		receivedUpdateData, has := ur.gsr.Extension(extensionName1)
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
			receivedExtensionData, _ := receivedResponse.Extension(extensionName1)
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
					Name: extensionName1,
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
						Name: extensionName1,
						Data: nextExpectedUpdate1,
					},
					{
						Name: extensionName2,
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
		td.fal.SuccessResponseOn(rr.gsr.ID(), nextBlocks)

		ur = readNNetworkRequests(requestCtx, t, td.requestRecordChan, 1)[0]
		receivedUpdateData, has = ur.gsr.Extension(extensionName1)
		require.True(t, has)
		require.Equal(t, nextExpectedUpdate1, receivedUpdateData, "should have updated with correct extension")
		receivedUpdateData, has = ur.gsr.Extension(extensionName2)
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
			receivedExtensionData, _ := receivedResponse.Extension(extensionName1)
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

	extensionName1 := graphsync.ExtensionName("blockchain")
	extension1 := graphsync.ExtensionData{
		Name: extensionName1,
		Data: nil,
	}

	hook := func(p peer.ID, r graphsync.RequestData, ha graphsync.OutgoingRequestHookActions) {
		_, has := r.Extension(extensionName1)
		if has {
			ha.UseLinkTargetNodeStyleChooser(td.blockChain.Chooser)
			ha.UsePersistenceOption("chainstore")
		}
	}
	td.requestHooks.Register(hook)

	returnedResponseChan1, returnedErrorChan1 := td.requestManager.SendRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector(), extension1)
	returnedResponseChan2, returnedErrorChan2 := td.requestManager.SendRequest(requestCtx, peers[0], td.blockChain.TipLink, td.blockChain.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, td.requestRecordChan, 2)

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
	td.fal.SuccessResponseOn(requestRecords[0].gsr.ID(), td.blockChain.AllBlocks())
	td.fal.SuccessResponseOn(requestRecords[1].gsr.ID(), td.blockChain.AllBlocks())

	td.blockChain.VerifyWholeChainWithTypes(requestCtx, returnedResponseChan1)
	td.blockChain.VerifyWholeChain(requestCtx, returnedResponseChan2)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan1)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan2)
	td.fal.VerifyStoreUsed(t, requestRecords[0].gsr.ID(), "chainstore")
	td.fal.VerifyStoreUsed(t, requestRecords[1].gsr.ID(), "")
}

type testData struct {
	requestRecordChan chan requestRecord
	fph               *fakePeerHandler
	fal               *testloader.FakeAsyncLoader
	requestHooks      *hooks.OutgoingRequestHooks
	responseHooks     *hooks.IncomingResponseHooks
	blockHooks        *hooks.IncomingBlockHooks
	requestManager    *RequestManager
	blockStore        map[ipld.Link][]byte
	loader            ipld.Loader
	storer            ipld.Storer
	blockChain        *testutil.TestBlockChain
}

func newTestData(ctx context.Context, t *testing.T) *testData {
	td := &testData{}
	td.requestRecordChan = make(chan requestRecord, 3)
	td.fph = &fakePeerHandler{td.requestRecordChan}
	td.fal = testloader.NewFakeAsyncLoader()
	td.requestHooks = hooks.NewRequestHooks()
	td.responseHooks = hooks.NewResponseHooks()
	td.blockHooks = hooks.NewBlockHooks()
	td.requestManager = New(ctx, td.fal, td.requestHooks, td.responseHooks, td.blockHooks)
	td.requestManager.SetDelegate(td.fph)
	td.requestManager.Startup()
	td.blockStore = make(map[ipld.Link][]byte)
	td.loader, td.storer = testutil.NewTestStore(td.blockStore)
	td.blockChain = testutil.SetupBlockChain(ctx, t, td.loader, td.storer, 100, 5)
	return td
}
