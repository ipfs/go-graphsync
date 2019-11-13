package requestmanager

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldbridge"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync/metadata"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipld/go-ipld-prime"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testbridge"
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
	expectedResponses map[graphsync.RequestID]metadata.Metadata) {
	select {
	case <-ctx.Done():
		t.Fatal("should have processed responses but didn't")
	case responses := <-fal.responses:
		if !reflect.DeepEqual(responses, expectedResponses) {
			t.Fatal("Did not send proper metadata")
		}
	}
}

func (fal *fakeAsyncLoader) verifyNoRemainingData(t *testing.T, requestID graphsync.RequestID) {
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
		select {
		case rr := <-requestRecordChan:
			requestRecords = append(requestRecords, rr)
		case <-ctx.Done():
			t.Fatal("should have sent two requests to the network but did not")
		}
	}
	return requestRecords
}

func verifyMatchedResponses(t *testing.T, actualResponse []graphsync.ResponseProgress, expectedBlocks []blocks.Block) {
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

func encodedMetadataForBlocks(t *testing.T, ipldBridge ipldbridge.IPLDBridge, blks []blocks.Block, present bool) graphsync.ExtensionData {
	md := metadataForBlocks(blks, present)
	metadataEncoded, err := metadata.EncodeMetadata(md, ipldBridge)
	if err != nil {
		t.Fatal("did not encode metadata")
	}
	return graphsync.ExtensionData{
		Name: graphsync.ExtensionMetadata,
		Data: metadataEncoded,
	}
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
	r1 := cidlink.Link{Cid: blocks1[0].Cid()}
	r2 := cidlink.Link{Cid: blocks2[0].Cid()}
	s1 := testbridge.NewMockSelectorSpec(cidsForBlocks(blocks1))
	s2 := testbridge.NewMockSelectorSpec(cidsForBlocks(blocks2))

	returnedResponseChan1, returnedErrorChan1 := requestManager.SendRequest(requestCtx, peers[0], r1, s1)
	returnedResponseChan2, returnedErrorChan2 := requestManager.SendRequest(requestCtx, peers[0], r2, s2)

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
	fal.successResponseOn(requestRecords[0].gsr.ID(), blocks1)
	fal.successResponseOn(requestRecords[1].gsr.ID(), blocks2[:3])

	responses1 := testutil.CollectResponses(requestCtx, t, returnedResponseChan1)
	verifyMatchedResponses(t, responses1, blocks1)
	responses2 := testutil.ReadNResponses(requestCtx, t, returnedResponseChan2, 3)
	verifyMatchedResponses(t, responses2, blocks2[:3])

	moreBlocks := blocks2[3:]
	moreMetadata := metadataForBlocks(moreBlocks, true)
	moreMetadataEncoded, err := metadata.EncodeMetadata(moreMetadata, fakeIPLDBridge)
	if err != nil {
		t.Fatal("did not encode metadata")
	}
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

	responses2 = testutil.CollectResponses(requestCtx, t, returnedResponseChan2)
	verifyMatchedResponses(t, responses2, moreBlocks)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan2)
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
	r1 := cidlink.Link{Cid: blocks1[0].Cid()}

	returnedResponseChan1, returnedErrorChan1 := requestManager.SendRequest(requestCtx1, peers[0], r1, s1)
	returnedResponseChan2, returnedErrorChan2 := requestManager.SendRequest(requestCtx2, peers[0], r1, s1)

	requestRecords := readNNetworkRequests(requestCtx, t, requestRecordChan, 2)

	firstBlocks := blocks1[:3]
	firstMetadata := encodedMetadataForBlocks(t, fakeIPLDBridge, blocks1[:3], true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.PartialResponse, firstMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.PartialResponse, firstMetadata),
	}

	requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)

	fal.successResponseOn(requestRecords[0].gsr.ID(), blocks1[:3])
	fal.successResponseOn(requestRecords[1].gsr.ID(), blocks1[:3])
	responses1 := testutil.ReadNResponses(requestCtx, t, returnedResponseChan1, 3)

	cancel1()
	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]
	if rr.gsr.IsCancel() != true || rr.gsr.ID() != requestRecords[0].gsr.ID() {
		t.Fatal("did not send correct cancel message over network")
	}

	moreBlocks := blocks1[3:]
	moreMetadata := encodedMetadataForBlocks(t, fakeIPLDBridge, blocks1[3:], true)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(requestRecords[0].gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
		gsmsg.NewResponse(requestRecords[1].gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
	}
	requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	fal.successResponseOn(requestRecords[0].gsr.ID(), blocks1[3:])
	fal.successResponseOn(requestRecords[1].gsr.ID(), blocks1[3:])

	responses1 = append(responses1, testutil.CollectResponses(requestCtx, t, returnedResponseChan1)...)
	verifyMatchedResponses(t, responses1, blocks1[:3])
	responses2 := testutil.CollectResponses(requestCtx, t, returnedResponseChan2)
	verifyMatchedResponses(t, responses2, blocks1)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan2)
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
	r := cidlink.Link{Cid: blocks[0].Cid()}

	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], r, s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	firstBlocks := blocks[:3]
	firstMetadata := encodedMetadataForBlocks(t, fakeIPLDBridge, firstBlocks, true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.PartialResponse, firstMetadata),
	}
	requestManager.ProcessResponses(peers[0], firstResponses, firstBlocks)
	fal.successResponseOn(rr.gsr.ID(), firstBlocks)
	responses := testutil.ReadNResponses(requestCtx, t, returnedResponseChan, 3)
	managerCancel()

	moreBlocks := blocks[3:]
	moreMetadata := encodedMetadataForBlocks(t, fakeIPLDBridge, moreBlocks, true)
	moreResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, moreMetadata),
	}
	requestManager.ProcessResponses(peers[0], moreResponses, moreBlocks)
	fal.successResponseOn(rr.gsr.ID(), moreBlocks)
	responses = append(responses, testutil.CollectResponses(requestCtx, t, returnedResponseChan)...)
	verifyMatchedResponses(t, responses, firstBlocks)
	testutil.VerifyEmptyErrors(requestCtx, t, returnedErrorChan)
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

	cids := testutil.GenerateCids(5)
	s := testbridge.NewUnencodableSelectorSpec(cids)
	r := cidlink.Link{Cid: cids[0]}
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], r, s)

	testutil.VerifySingleTerminalError(requestCtx, t, returnedErrorChan)
	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
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

	cids := testutil.GenerateCids(5)
	s := testbridge.NewUnencodableSelectorSpec(cids)
	r := cidlink.Link{Cid: cids[0]}
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], r, s)

	testutil.VerifySingleTerminalError(requestCtx, t, returnedErrorChan)
	testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
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
	r := cidlink.Link{Cid: blocks[0].Cid()}
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], r, s)

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
	r := cidlink.Link{Cid: blocks[0].Cid()}
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], r, s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	// async loaded response responds immediately
	fal.successResponseOn(rr.gsr.ID(), blocks)

	responses := testutil.CollectResponses(requestCtx, t, returnedResponseChan)
	verifyMatchedResponses(t, responses, blocks)

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
	r := cidlink.Link{Cid: blocks[0].Cid()}
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], r, s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	// async loaded response responds immediately
	fal.successResponseOn(rr.gsr.ID(), blocks)

	responses := testutil.CollectResponses(requestCtx, t, returnedResponseChan)
	verifyMatchedResponses(t, responses, blocks)

	md := encodedMetadataForBlocks(t, fakeIPLDBridge, blocks, true)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedFull, md),
	}
	requestManager.ProcessResponses(peers[0], firstResponses, blocks)

	fal.verifyNoRemainingData(t, rr.gsr.ID())
	testutil.VerifyEmptyErrors(ctx, t, returnedErrorChan)
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
	r := cidlink.Link{Cid: blocks[0].Cid()}
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], r, s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	md := encodedMetadataForBlocks(t, fakeIPLDBridge, blocks, false)
	firstResponses := []gsmsg.GraphSyncResponse{
		gsmsg.NewResponse(rr.gsr.ID(), graphsync.RequestCompletedPartial, md),
	}
	requestManager.ProcessResponses(peers[0], firstResponses, nil)
	for _, block := range blocks {
		fal.responseOn(rr.gsr.ID(), cidlink.Link{Cid: block.Cid()}, types.AsyncLoadResult{Data: nil, Err: fmt.Errorf("Terrible Thing")})
	}
	testutil.VerifyEmptyResponse(ctx, t, returnedResponseChan)
	errs := testutil.CollectErrors(ctx, t, returnedErrorChan)
	if len(errs) != len(blocks) {
		t.Fatal("did not send all errors")
	}

}

func TestEncodingExtensions(t *testing.T) {
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

	cids := testutil.GenerateCids(1)
	root := cidlink.Link{Cid: cids[0]}
	selector := testbridge.NewMockSelectorSpec(cids)

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
	extensionHandler := func(p peer.ID, data []byte) error {
		receivedExtensionData <- data
		return <-expectedError
	}
	requestManager.RegisterExtension(extensionName1, extensionHandler)
	returnedResponseChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], root, selector, extension1, extension2)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	gsr := rr.gsr
	returnedData1, found := gsr.Extension(extensionName1)
	if !found || !reflect.DeepEqual(extensionData1, returnedData1) {
		t.Fatal("Failed to encode first extension")
	}

	returnedData2, found := gsr.Extension(extensionName2)
	if !found || !reflect.DeepEqual(extensionData2, returnedData2) {
		t.Fatal("Failed to encode first extension")
	}

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
		select {
		case <-requestCtx.Done():
			t.Fatal("Should have checked extension but didn't")
		case received := <-receivedExtensionData:
			if !reflect.DeepEqual(received, expectedData) {
				t.Fatal("Did not receive correct extension data from resposne")
			}
		}
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
		select {
		case <-requestCtx.Done():
			t.Fatal("Should have checked extension but didn't")
		case received := <-receivedExtensionData:
			if !reflect.DeepEqual(received, nextExpectedData) {
				t.Fatal("Did not receive correct extension data from resposne")
			}
		}
		testutil.VerifySingleTerminalError(requestCtx, t, returnedErrorChan)
		testutil.VerifyEmptyResponse(requestCtx, t, returnedResponseChan)
	})
}
