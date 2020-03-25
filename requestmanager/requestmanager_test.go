package requestmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
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

type requestKey struct {
	requestID graphsync.RequestID
	link      ipld.Link
}

type fakeAsyncLoader struct {
	responseChannelsLk sync.RWMutex
	responseChannels   map[requestKey]chan types.AsyncLoadResult
	responses          chan map[graphsync.RequestID]metadata.Metadata
	blks               chan []blocks.Block
}

func newFakeAsyncLoader() *fakeAsyncLoader {
	return &fakeAsyncLoader{
		responseChannels: make(map[requestKey]chan types.AsyncLoadResult),
		responses:        make(chan map[graphsync.RequestID]metadata.Metadata, 1),
		blks:             make(chan []blocks.Block, 1),
	}
}
func (fal *fakeAsyncLoader) StartRequest(requestID graphsync.RequestID) {
}
func (fal *fakeAsyncLoader) ProcessResponse(responses map[graphsync.RequestID]metadata.Metadata,
	blks []blocks.Block) {
	fal.responses <- responses
	fal.blks <- blks
}
func (fal *fakeAsyncLoader) verifyLastProcessedBlocks(ctx context.Context, t *testing.T, expectedBlocks []blocks.Block) {
	var processedBlocks []blocks.Block
	testutil.AssertReceive(ctx, t, fal.blks, &processedBlocks, "should process blocks")
	require.Equal(t, processedBlocks, expectedBlocks, "should process correct blocks")
}

func (fal *fakeAsyncLoader) verifyLastProcessedResponses(ctx context.Context, t *testing.T,
	expectedResponses map[graphsync.RequestID]metadata.Metadata) {
	var responses map[graphsync.RequestID]metadata.Metadata
	testutil.AssertReceive(ctx, t, fal.responses, &responses, "processes responses")
	require.Equal(t, responses, expectedResponses, "processes correct responses")
}

func (fal *fakeAsyncLoader) verifyNoRemainingData(t *testing.T, requestID graphsync.RequestID) {
	fal.responseChannelsLk.Lock()
	for key := range fal.responseChannels {
		require.NotEqual(t, key.requestID, requestID, "request properly cleaned up")
	}
	fal.responseChannelsLk.Unlock()
}

func (fal *fakeAsyncLoader) asyncLoad(requestID graphsync.RequestID, link ipld.Link) chan types.AsyncLoadResult {
	fal.responseChannelsLk.Lock()
	responseChannel, ok := fal.responseChannels[requestKey{requestID, link}]
	if !ok {
		responseChannel = make(chan types.AsyncLoadResult, 1)
		fal.responseChannels[requestKey{requestID, link}] = responseChannel
	}
	fal.responseChannelsLk.Unlock()
	return responseChannel
}

func (fal *fakeAsyncLoader) AsyncLoad(requestID graphsync.RequestID, link ipld.Link) <-chan types.AsyncLoadResult {
	return fal.asyncLoad(requestID, link)
}
func (fal *fakeAsyncLoader) CompleteResponsesFor(requestID graphsync.RequestID) {}
func (fal *fakeAsyncLoader) CleanupRequest(requestID graphsync.RequestID) {
	fal.responseChannelsLk.Lock()
	for key := range fal.responseChannels {
		if key.requestID == requestID {
			delete(fal.responseChannels, key)
		}
	}
	fal.responseChannelsLk.Unlock()
}

func (fal *fakeAsyncLoader) responseOn(requestID graphsync.RequestID, link ipld.Link, result types.AsyncLoadResult) {
	responseChannel := fal.asyncLoad(requestID, link)
	responseChannel <- result
	close(responseChannel)
}

func (fal *fakeAsyncLoader) successResponseOn(requestID graphsync.RequestID, blks []blocks.Block) {
	for _, block := range blks {
		fal.responseOn(requestID, cidlink.Link{Cid: block.Cid()}, types.AsyncLoadResult{Data: block.RawData(), Err: nil})
	}
}

func readNNetworkRequests(ctx context.Context,
	t *testing.T,
	requestRecordChan <-chan requestRecord,
	count int) []requestRecord {
	requestRecords := make([]requestRecord, 0, count)
	for i := 0; i < count; i++ {
		var rr requestRecord
		testutil.AssertReceive(ctx, t, requestRecordChan, &rr, fmt.Sprintf("receives request %d", i))
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
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain1 := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)
	blockChain2 := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)

	returnedResponseChan1, returnedErrorChan1 := requestManager.SendRequest(requestCtx, peers[0], blockChain1.TipLink, blockChain1.Selector())
	returnedResponseChan2, returnedErrorChan2 := requestManager.SendRequest(requestCtx, peers[0], blockChain2.TipLink, blockChain2.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, requestRecordChan, 2)

	require.Equal(t, requestRecords[0].p, peers[0])
	require.Equal(t, requestRecords[1].p, peers[0])
	require.False(t, requestRecords[0].gsr.IsCancel())
	require.False(t, requestRecords[1].gsr.IsCancel())
	require.Equal(t, requestRecords[0].gsr.Priority(), maxPriority)
	require.Equal(t, requestRecords[1].gsr.Priority(), maxPriority)

	require.Equal(t, blockChain1.Selector(), requestRecords[0].gsr.Selector(), "encodes selector properly")
	require.Equal(t, blockChain2.Selector(), requestRecords[1].gsr.Selector(), "encode selector properly")

	firstBlocks := append(blockChain1.AllBlocks(), blockChain2.Blocks(0, 3)...)
	firstMetadata1 := metadataForBlocks(blockChain1.AllBlocks(), true)
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

	requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	fal.verifyLastProcessedBlocks(ctx, t, firstBlocks)
	fal.verifyLastProcessedResponses(ctx, t, map[graphsync.RequestID]metadata.Metadata{
		requestRecords[0].gsr.ID(): firstMetadata1,
		requestRecords[1].gsr.ID(): firstMetadata2,
	})
	fal.successResponseOn(requestRecords[0].gsr.ID(), blockChain1.AllBlocks())
	fal.successResponseOn(requestRecords[1].gsr.ID(), blockChain2.Blocks(0, 3))

	blockChain1.VerifyWholeChain(requestCtx, returnedResponseChan1)
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

	requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	fal.verifyLastProcessedBlocks(ctx, t, moreBlocks)
	fal.verifyLastProcessedResponses(ctx, t, map[graphsync.RequestID]metadata.Metadata{
		requestRecords[1].gsr.ID(): moreMetadata,
	})

	fal.successResponseOn(requestRecords[1].gsr.ID(), moreBlocks)

	blockChain2.VerifyRemainder(requestCtx, returnedResponseChan2, 3)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan2)
}

func TestCancelRequestInProgress(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal)
	requestManager.SetDelegate(fph)
	requestManager.Startup()
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	requestCtx1, cancel1 := context.WithCancel(requestCtx)
	requestCtx2, cancel2 := context.WithCancel(requestCtx)
	defer cancel2()
	peers := testutil.GeneratePeers(1)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)

	returnedResponseChan1, returnedErrorChan1 := requestManager.SendRequest(requestCtx1, peers[0], blockChain.TipLink, blockChain.Selector())
	returnedResponseChan2, returnedErrorChan2 := requestManager.SendRequest(requestCtx2, peers[0], blockChain.TipLink, blockChain.Selector())

	requestRecords := readNNetworkRequests(requestCtx, t, requestRecordChan, 2)

	firstBlocks := blockChain.Blocks(0, 3)
	firstMetadata := encodedMetadataForBlocks(t, firstBlocks, true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.PartialResponse, firstMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.PartialResponse, firstMetadata),
	}

	requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)

	fal.successResponseOn(requestRecords[0].gsr.ID(), firstBlocks)
	fal.successResponseOn(requestRecords[1].gsr.ID(), firstBlocks)
	blockChain.VerifyResponseRange(requestCtx1, returnedResponseChan1, 0, 3)
	cancel1()
	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	require.True(t, rr.gsr.IsCancel())
	require.Equal(t, rr.gsr.ID(), requestRecords[0].gsr.ID())

	moreBlocks := blockChain.RemainderBlocks(3)
	moreMetadata := encodedMetadataForBlocks(t, moreBlocks, true)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
	}
	requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	fal.successResponseOn(requestRecords[0].gsr.ID(), moreBlocks)
	fal.successResponseOn(requestRecords[1].gsr.ID(), moreBlocks)

	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan1)
	blockChain.VerifyWholeChain(requestCtx, returnedResponseChan2)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan2)
}

func TestCancelManagerExitsGracefully(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	ctx := context.Background()
	managerCtx, managerCancel := context.WithCancel(ctx)
	fal := newFakeAsyncLoader()
	requestManager := New(managerCtx, fal)
	requestManager.SetDelegate(fph)
	requestManager.Startup()
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)

	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], blockChain.TipLink, blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	firstBlocks := blockChain.Blocks(0, 3)
	firstMetadata := encodedMetadataForBlocks(t, firstBlocks, true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.PartialResponse, firstMetadata),
	}
	requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	fal.successResponseOn(rr.gsr.ID(), firstBlocks)
	blockChain.VerifyResponseRange(ctx, returnedResponseChan, 0, 3)
	managerCancel()

	moreBlocks := blockChain.RemainderBlocks(3)
	moreMetadata := encodedMetadataForBlocks(t, moreBlocks, true)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
	}
	requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	fal.successResponseOn(rr.gsr.ID(), moreBlocks)
	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan)
}

func TestUnencodableSelector(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	s := testutil.NewUnencodableSelectorSpec()
	r := cidlink.Link{Cid: testutil.GenerateCids(1)[0]}
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], r, s)

	testutil.VerifySingleTerminalError(requestCtx, t, returnedErrorChan)
	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
}

func TestFailedRequest(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)

	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], blockChain.TipLink, blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]
	failedResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestFailedContentNotFound),
	}
	requestManager.ProcessResponses(peers[0], failedResponses, nil)

	testutil.VerifySingleTerminalError(requestCtx, t, returnedErrorChan)
	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
}

func TestLocallyFulfilledFirstRequestFailsLater(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)

	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], blockChain.TipLink, blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	// async loaded response responds immediately
	fal.successResponseOn(rr.gsr.ID(), blockChain.AllBlocks())

	blockChain.VerifyWholeChain(requestCtx, returnedResponseChan)

	// failure comes in later over network
	failedResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestFailedContentNotFound),
	}

	requestManager.ProcessResponses(peers[0], failedResponses, nil)
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan)

}

func TestLocallyFulfilledFirstRequestSucceedsLater(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], blockChain.TipLink, blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	// async loaded response responds immediately
	fal.successResponseOn(rr.gsr.ID(), blockChain.AllBlocks())

	blockChain.VerifyWholeChain(requestCtx, returnedResponseChan)

	md := encodedMetadataForBlocks(t, blockChain.AllBlocks(), true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, md),
	}
	requestManager.ProcessResponses(peers[0], firstResponses, blockChain.AllBlocks())

	fal.verifyNoRemainingData(t, rr.gsr.ID())
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan)
}

func TestRequestReturnsMissingBlocks(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], blockChain.TipLink, blockChain.Selector())

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	md := encodedMetadataForBlocks(t, blockChain.AllBlocks(), false)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedPartial, md),
	}
	requestManager.ProcessResponses(peers[0], firstResponses, nil)
	for _, block := range blockChain.AllBlocks() {
		fal.responseOn(rr.gsr.ID(), cidlink.Link{Cid: block.Cid()}, types.AsyncLoadResult{Data: nil, Err: fmt.Errorf("Terrible Thing")})
	}
	testutil.VerifyEmptyResponse(ctx, t, returnedResponseChan)
	errs := testutil.CollectErrors(ctx, t, returnedErrorChan)
	require.NotEqual(t, len(errs), 0, "sends errors")
}

func TestEncodingExtensions(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 5)

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
	hook := func(p peer.ID, responseData graphsync.ResponseData) error {
		data, has := responseData.Extension(extensionName1)
		require.True(t, has, "receives extension data in response")
		receivedExtensionData <- data
		return <-expectedError
	}
	requestManager.RegisterHook(hook)
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], blockChain.TipLink, blockChain.Selector(), extension1, extension2)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	gsr := rr.gsr
	returnedData1, found := gsr.Extension(extensionName1)
	require.True(t, found)
	require.Equal(t, extensionData1, returnedData1, "encodes first extension correctly")

	returnedData2, found := gsr.Extension(extensionName2)
	require.True(t, found)
	require.Equal(t, extensionData2, returnedData2, "encodes second extension correctly")

	t.Run("responding to extensions", func(t *testing.T) {
		expectedData := testutil.RandomBytes(100)
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
		requestManager.ProcessResponses(peers[0], firstResponses, nil)
		var received []byte
		testutil.AssertReceive(ctx, t, receivedExtensionData, &received, "receives extension data")
		require.Equal(t, received, expectedData, "receives correct extension data from resposne")
		nextExpectedData := testutil.RandomBytes(100)

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
		requestManager.ProcessResponses(peers[0], secondResponses, nil)
		testutil.AssertReceive(ctx, t, receivedExtensionData, &received, "receives extension data")
		require.Equal(t, received, nextExpectedData, "receives correct extension data from resposne")
		testutil.VerifySingleTerminalError(requestCtx, t, returnedErrorChan)
		testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
	})
}
