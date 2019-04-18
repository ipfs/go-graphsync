package requestmanager

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync/ipldbridge"

	"github.com/ipfs/go-graphsync/requestmanager/types"

	"github.com/ipfs/go-graphsync/metadata"

	"github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipld/go-ipld-prime"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/libp2p/go-libp2p-peer"
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
	requestID gsmsg.GraphSyncRequestID
	link      ipld.Link
}

type fakeAsyncLoader struct {
	responseChannelsLk sync.RWMutex
	responseChannels   map[requestKey]chan types.AsyncLoadResult
	responses          chan map[gsmsg.GraphSyncRequestID]metadata.Metadata
	blks               chan []blocks.Block
}

func newFakeAsyncLoader() *fakeAsyncLoader {
	return &fakeAsyncLoader{
		responseChannels: make(map[requestKey]chan types.AsyncLoadResult),
		responses:        make(chan map[gsmsg.GraphSyncRequestID]metadata.Metadata, 1),
		blks:             make(chan []blocks.Block, 1),
	}
}
func (fal *fakeAsyncLoader) StartRequest(requestID gsmsg.GraphSyncRequestID) {
}
func (fal *fakeAsyncLoader) ProcessResponse(responses map[gsmsg.GraphSyncRequestID]metadata.Metadata,
	blks []blocks.Block) {
	fal.responses <- responses
	fal.blks <- blks
}
func (fal *fakeAsyncLoader) verifyLastProcessedBlocks(ctx context.Context, t *testing.T, expectedBlocks []blocks.Block) {
	select {
	case <-ctx.Done():
		t.Fatal("should have processed blocks but didn't")
	case processedBlocks := <-fal.blks:
		if !reflect.DeepEqual(processedBlocks, expectedBlocks) {
			t.Fatal("Did not process correct blocks")
		}
	}
}
func (fal *fakeAsyncLoader) verifyLastProcessedResponses(ctx context.Context, t *testing.T,
	expectedResponses map[gsmsg.GraphSyncRequestID]metadata.Metadata) {
	select {
	case <-ctx.Done():
		t.Fatal("should have processed responses but didn't")
	case responses := <-fal.responses:
		if !reflect.DeepEqual(responses, expectedResponses) {
			t.Fatal("Did not send proper metadata")
		}
	}
}

func (fal *fakeAsyncLoader) verifyNoRemainingData(t *testing.T, requestID gsmsg.GraphSyncRequestID) {
	fal.responseChannelsLk.Lock()
	for key := range fal.responseChannels {
		if key.requestID == requestID {
			t.Fatal("request not properly cleaned up")
		}
	}
	fal.responseChannelsLk.Unlock()
}

func cidsForBlocks(blks []blocks.Block) []cid.Cid {
	cids := make([]cid.Cid, 0, 5)
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	return cids
}

func (fal *fakeAsyncLoader) asyncLoad(requestID gsmsg.GraphSyncRequestID, link ipld.Link) chan types.AsyncLoadResult {
	fal.responseChannelsLk.Lock()
	responseChannel, ok := fal.responseChannels[requestKey{requestID, link}]
	if !ok {
		responseChannel = make(chan types.AsyncLoadResult, 1)
		fal.responseChannels[requestKey{requestID, link}] = responseChannel
	}
	fal.responseChannelsLk.Unlock()
	return responseChannel
}

func (fal *fakeAsyncLoader) AsyncLoad(requestID gsmsg.GraphSyncRequestID, link ipld.Link) <-chan types.AsyncLoadResult {
	return fal.asyncLoad(requestID, link)
}
func (fal *fakeAsyncLoader) CompleteResponsesFor(requestID gsmsg.GraphSyncRequestID) {}
func (fal *fakeAsyncLoader) CleanupRequest(requestID gsmsg.GraphSyncRequestID) {
	fal.responseChannelsLk.Lock()
	for key := range fal.responseChannels {
		if key.requestID == requestID {
			delete(fal.responseChannels, key)
		}
	}
	fal.responseChannelsLk.Unlock()
}

func (fal *fakeAsyncLoader) responseOn(requestID gsmsg.GraphSyncRequestID, link ipld.Link, result types.AsyncLoadResult) {
	responseChannel := fal.asyncLoad(requestID, link)
	responseChannel <- result
	close(responseChannel)
}

func (fal *fakeAsyncLoader) successResponseOn(requestID gsmsg.GraphSyncRequestID, blks []blocks.Block) {
	for _, block := range blks {
		fal.responseOn(requestID, cidlink.Link{Cid: block.Cid()}, types.AsyncLoadResult{Data: block.RawData(), Err: nil})
	}
}

func collectResponses(ctx context.Context, t *testing.T, responseChan <-chan ResponseProgress) []ResponseProgress {
	var collectedBlocks []ResponseProgress
	for {
		select {
		case blk, ok := <-responseChan:
			if !ok {
				return collectedBlocks
			}
			collectedBlocks = append(collectedBlocks, blk)
		case <-ctx.Done():
			t.Fatal("response channel never closed")
		}
	}
}

func collectErrors(ctx context.Context, t *testing.T, errChan <-chan error) []error {
	var collectedErrors []error
	for {
		select {
		case err, ok := <-errChan:
			if !ok {
				return collectedErrors
			}
			collectedErrors = append(collectedErrors, err)
		case <-ctx.Done():
			t.Fatal("error channel never closed")
		}
	}
}

func readNResponses(ctx context.Context, t *testing.T, responseChan <-chan ResponseProgress, count int) []ResponseProgress {
	var returnedBlocks []ResponseProgress
	for i := 0; i < count; i++ {
		select {
		case blk := <-responseChan:
			returnedBlocks = append(returnedBlocks, blk)
		case <-ctx.Done():
			t.Fatal("Unable to read enough responses")
		}
	}
	return returnedBlocks
}

func verifySingleTerminalError(ctx context.Context, t *testing.T, errChan <-chan error) {
	select {
	case err := <-errChan:
		if err == nil {
			t.Fatal("should have sent a erminal error but did not")
		}
	case <-ctx.Done():
		t.Fatal("no errors sent")
	}
	select {
	case _, ok := <-errChan:
		if ok {
			t.Fatal("shouldn't have sent second error but did")
		}
	case <-ctx.Done():
		t.Fatal("errors not closed")
	}
}

func verifyEmptyErrors(ctx context.Context, t *testing.T, errChan <-chan error) {
	for {
		select {
		case _, ok := <-errChan:
			if !ok {
				return
			}
			t.Fatal("errors were sent but shouldn't have been")
		case <-ctx.Done():
			t.Fatal("errors channel never closed")
		}
	}
}

func verifyEmptyResponse(ctx context.Context, t *testing.T, responseChan <-chan ResponseProgress) {
	for {
		select {
		case _, ok := <-responseChan:
			if !ok {
				return
			}
			t.Fatal("response was sent but shouldn't have been")
		case <-ctx.Done():
			t.Fatal("response channel never closed")
		}
	}
}

func readNNetworkRequests(ctx context.Context,
	t *testing.T,
	requestRecordChan <-chan requestRecord,
	count int) []requestRecord {
	requestRecords := make([]requestRecord, 0, count)
	for i := 0; i < count; i++ {
		select {
		case rr := <-requestRecordChan:
			requestRecords = append(requestRecords, rr)
		case <-ctx.Done():
			t.Fatal("should have sent two requests to the network but did not")
		}
	}
	return requestRecords
}

func verifyMatchedResponses(t *testing.T, actualResponse []ResponseProgress, expectedBlocks []blocks.Block) {
	if len(actualResponse) != len(expectedBlocks) {
		t.Fatal("wrong number of responses sent")
	}
	for _, responseProgress := range actualResponse {
		data, err := responseProgress.Node.AsBytes()
		if err != nil {
			t.Fatal("Node was not a block")
		}
		blk, err := blocks.NewBlockWithCid(data, responseProgress.LastBlock.Link.(cidlink.Link).Cid)
		if err != nil {
			t.Fatal("block did not verify")
		}
		if !testutil.ContainsBlock(expectedBlocks, blk) {
			t.Fatal("wrong block sent")
		}
	}
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

func encodedMetadataForBlocks(t *testing.T, ipldBridge ipldbridge.IPLDBridge, blks []blocks.Block, present bool) []byte {
	md := metadataForBlocks(blks, present)
	metadataEncoded, err := metadata.EncodeMetadata(md, ipldBridge)
	if err != nil {
		t.Fatal("did not encode metadata")
	}
	return metadataEncoded
}

func TestNormalSimultaneousFetch(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blocks1 := testutil.GenerateBlocksOfSize(5, 100)
	blocks2 := testutil.GenerateBlocksOfSize(5, 100)
	s1 := testbridge.NewMockSelectorSpec(cidsForBlocks(blocks1))
	s2 := testbridge.NewMockSelectorSpec(cidsForBlocks(blocks2))

	returnedResponseChan1, returnedErrorChan1 := requestManager.SendRequest(requestCtx, peers[0], s1)
	returnedResponseChan2, returnedErrorChan2 := requestManager.SendRequest(requestCtx, peers[0], s2)

	requestRecords := readNNetworkRequests(requestCtx, t, requestRecordChan, 2)

	if requestRecords[0].p != peers[0] || requestRecords[1].p != peers[0] ||
		requestRecords[0].gsr.IsCancel() != false || requestRecords[1].gsr.IsCancel() != false ||
		requestRecords[0].gsr.Priority() != maxPriority ||
		requestRecords[1].gsr.Priority() != maxPriority {
		t.Fatal("did not send correct requests")
	}

	returnedS1, err := fakeIPLDBridge.DecodeNode(requestRecords[0].gsr.Selector())
	if err != nil || !reflect.DeepEqual(s1, returnedS1) {
		t.Fatal("did not encode selector properly")
	}
	returnedS2, err := fakeIPLDBridge.DecodeNode(requestRecords[1].gsr.Selector())
	if err != nil || !reflect.DeepEqual(s2, returnedS2) {
		t.Fatal("did not encode selector properly")
	}

	firstBlocks := append(blocks1, blocks2[:3]...)
	firstMetadata1 := metadataForBlocks(blocks1, true)
	firstMetadataEncoded1, err := metadata.EncodeMetadata(firstMetadata1, fakeIPLDBridge)
	if err != nil {
		t.Fatal("did not encode metadata")
	}
	firstMetadata2 := metadataForBlocks(blocks2[:3], true)
	firstMetadataEncoded2, err := metadata.EncodeMetadata(firstMetadata2, fakeIPLDBridge)
	if err != nil {
		t.Fatal("did not encode metadata")
	}
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), gsmsg.RequestCompletedFull, firstMetadataEncoded1),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), gsmsg.PartialResponse, firstMetadataEncoded2),
	}

	requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	fal.verifyLastProcessedBlocks(ctx, t, firstBlocks)
	fal.verifyLastProcessedResponses(ctx, t, map[gsmsg.GraphSyncRequestID]metadata.Metadata{
		requestRecords[0].gsr.ID(): firstMetadata1,
		requestRecords[1].gsr.ID(): firstMetadata2,
	})
	fal.successResponseOn(requestRecords[0].gsr.ID(), blocks1)
	fal.successResponseOn(requestRecords[1].gsr.ID(), blocks2[:3])

	responses1 := collectResponses(requestCtx, t, returnedResponseChan1)
	verifyMatchedResponses(t, responses1, blocks1)
	responses2 := readNResponses(requestCtx, t, returnedResponseChan2, 3)
	verifyMatchedResponses(t, responses2, blocks2[:3])

	moreBlocks := blocks2[3:]
	moreMetadata := metadataForBlocks(moreBlocks, true)
	moreMetadataEncoded, err := metadata.EncodeMetadata(moreMetadata, fakeIPLDBridge)
	if err != nil {
		t.Fatal("did not encode metadata")
	}
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), gsmsg.RequestCompletedFull, moreMetadataEncoded),
	}

	requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	fal.verifyLastProcessedBlocks(ctx, t, moreBlocks)
	fal.verifyLastProcessedResponses(ctx, t, map[gsmsg.GraphSyncRequestID]metadata.Metadata{
		requestRecords[1].gsr.ID(): moreMetadata,
	})

	fal.successResponseOn(requestRecords[1].gsr.ID(), moreBlocks)

	responses2 = collectResponses(requestCtx, t, returnedResponseChan2)
	verifyMatchedResponses(t, responses2, moreBlocks)
	verifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	verifyEmptyErrors(requestCtx, t, returnedErrorChan2)
}

func TestCancelRequestInProgress(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	requestCtx1, cancel1 := context.WithCancel(requestCtx)
	requestCtx2, cancel2 := context.WithCancel(requestCtx)
	defer cancel2()
	peers := testutil.GeneratePeers(1)

	blocks1 := testutil.GenerateBlocksOfSize(5, 100)
	s1 := testbridge.NewMockSelectorSpec(cidsForBlocks(blocks1))

	returnedResponseChan1, returnedErrorChan1 := requestManager.SendRequest(requestCtx1, peers[0], s1)
	returnedResponseChan2, returnedErrorChan2 := requestManager.SendRequest(requestCtx2, peers[0], s1)

	requestRecords := readNNetworkRequests(requestCtx, t, requestRecordChan, 2)

	firstBlocks := blocks1[:3]
	firstMetadata := encodedMetadataForBlocks(t, fakeIPLDBridge, blocks1[:3], true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), gsmsg.PartialResponse, firstMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), gsmsg.PartialResponse, firstMetadata),
	}

	requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)

	fal.successResponseOn(requestRecords[0].gsr.ID(), blocks1[:3])
	fal.successResponseOn(requestRecords[1].gsr.ID(), blocks1[:3])
	responses1 := readNResponses(requestCtx, t, returnedResponseChan1, 3)

	cancel1()
	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]
	if rr.gsr.IsCancel() != true || rr.gsr.ID() != requestRecords[0].gsr.ID() {
		t.Fatal("did not send correct cancel message over network")
	}

	moreBlocks := blocks1[3:]
	moreMetadata := encodedMetadataForBlocks(t, fakeIPLDBridge, blocks1[3:], true)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), gsmsg.RequestCompletedFull, moreMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), gsmsg.RequestCompletedFull, moreMetadata),
	}
	requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	fal.successResponseOn(requestRecords[0].gsr.ID(), blocks1[3:])
	fal.successResponseOn(requestRecords[1].gsr.ID(), blocks1[3:])

	responses1 = append(responses1, collectResponses(requestCtx, t, returnedResponseChan1)...)
	verifyMatchedResponses(t, responses1, blocks1[:3])
	responses2 := collectResponses(requestCtx, t, returnedResponseChan2)
	verifyMatchedResponses(t, responses2, blocks1)
	verifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	verifyEmptyErrors(requestCtx, t, returnedErrorChan2)
}

func TestCancelManagerExitsGracefully(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	managerCtx, managerCancel := context.WithCancel(ctx)
	fal := newFakeAsyncLoader()
	requestManager := New(managerCtx, fal, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blocks := testutil.GenerateBlocksOfSize(5, 100)
	s := testbridge.NewMockSelectorSpec(cidsForBlocks(blocks))
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	firstBlocks := blocks[:3]
	firstMetadata := encodedMetadataForBlocks(t, fakeIPLDBridge, firstBlocks, true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), gsmsg.PartialResponse, firstMetadata),
	}
	requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	fal.successResponseOn(rr.gsr.ID(), firstBlocks)
	responses := readNResponses(requestCtx, t, returnedResponseChan, 3)
	managerCancel()

	moreBlocks := blocks[3:]
	moreMetadata := encodedMetadataForBlocks(t, fakeIPLDBridge, moreBlocks, true)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), gsmsg.RequestCompletedFull, moreMetadata),
	}
	requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	fal.successResponseOn(rr.gsr.ID(), moreBlocks)
	responses = append(responses, collectResponses(requestCtx, t, returnedResponseChan)...)
	verifyMatchedResponses(t, responses, firstBlocks)
	verifyEmptyErrors(requestCtx, t, returnedErrorChan)
}

func TestInvalidSelector(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	s := testbridge.NewInvalidSelectorSpec(testutil.GenerateCids(5))
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	verifySingleTerminalError(requestCtx, t, returnedErrorChan)
	verifyEmptyResponse(requestCtx, t, returnedResponseChan)
}

func TestUnencodableSelector(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	s := testbridge.NewUnencodableSelectorSpec(testutil.GenerateCids(5))
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	verifySingleTerminalError(requestCtx, t, returnedErrorChan)
	verifyEmptyResponse(requestCtx, t, returnedResponseChan)
}

func TestFailedRequest(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blocks := testutil.GenerateBlocksOfSize(5, 100)
	s := testbridge.NewMockSelectorSpec(cidsForBlocks(blocks))
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]
	failedResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), gsmsg.RequestFailedContentNotFound, nil),
	}
	requestManager.ProcessResponses(peers[0], failedResponses, nil)

	verifySingleTerminalError(requestCtx, t, returnedErrorChan)
	verifyEmptyResponse(requestCtx, t, returnedResponseChan)
}

func TestLocallyFulfilledFirstRequestFailsLater(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blocks := testutil.GenerateBlocksOfSize(5, 100)
	s := testbridge.NewMockSelectorSpec(cidsForBlocks(blocks))
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	// async loaded response responds immediately
	fal.successResponseOn(rr.gsr.ID(), blocks)

	responses := collectResponses(requestCtx, t, returnedResponseChan)
	verifyMatchedResponses(t, responses, blocks)

	// failure comes in later over network
	failedResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), gsmsg.RequestFailedContentNotFound, nil),
	}

	requestManager.ProcessResponses(peers[0], failedResponses, nil)
	verifyEmptyErrors(ctx, t, returnedErrorChan)

}

func TestLocallyFulfilledFirstRequestSucceedsLater(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blocks := testutil.GenerateBlocksOfSize(5, 100)
	s := testbridge.NewMockSelectorSpec(cidsForBlocks(blocks))
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	// async loaded response responds immediately
	fal.successResponseOn(rr.gsr.ID(), blocks)

	responses := collectResponses(requestCtx, t, returnedResponseChan)
	verifyMatchedResponses(t, responses, blocks)

	md := encodedMetadataForBlocks(t, fakeIPLDBridge, blocks, true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), gsmsg.RequestCompletedFull, md),
	}
	requestManager.ProcessResponses(peers[0], firstResponses, blocks)

	fal.verifyNoRemainingData(t, rr.gsr.ID())
	verifyEmptyErrors(ctx, t, returnedErrorChan)
}

func TestRequestReturnsMissingBlocks(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	fal := newFakeAsyncLoader()
	requestManager := New(ctx, fal, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	blocks := testutil.GenerateBlocksOfSize(5, 100)
	s := testbridge.NewMockSelectorSpec(cidsForBlocks(blocks))
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	md := encodedMetadataForBlocks(t, fakeIPLDBridge, blocks, false)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), gsmsg.RequestCompletedPartial, md),
	}
	requestManager.ProcessResponses(peers[0], firstResponses, nil)
	for _, block := range blocks {
		fal.responseOn(rr.gsr.ID(), cidlink.Link{Cid: block.Cid()}, types.AsyncLoadResult{Data: nil, Err: fmt.Errorf("Terrible Thing")})
	}
	verifyEmptyResponse(ctx, t, returnedResponseChan)
	errs := collectErrors(ctx, t, returnedErrorChan)
	if len(errs) != len(blocks) {
		t.Fatal("did not send all errors")
	}

}
