package requestmanager

import (
	"context"
	"reflect"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/libp2p/go-libp2p-peer"
)

type requestRecord struct {
	isCancel  bool
	requestID gsmsg.GraphSyncRequestID
	priority  gsmsg.GraphSyncPriority
	selector  []byte
	p         peer.ID
}
type fakePeerHandler struct {
	requestRecordChan chan requestRecord
}

func (fph *fakePeerHandler) SendRequest(
	p peer.ID,
	id gsmsg.GraphSyncRequestID,
	selector []byte,
	priority gsmsg.GraphSyncPriority) {
	fph.requestRecordChan <- requestRecord{
		isCancel:  false,
		requestID: id,
		selector:  selector,
		priority:  priority,
		p:         p,
	}
}

func (fph *fakePeerHandler) CancelRequest(
	p peer.ID,
	id gsmsg.GraphSyncRequestID) {
	fph.requestRecordChan <- requestRecord{
		isCancel:  true,
		requestID: id,
		p:         p,
	}
}

func collectBlocks(ctx context.Context, t *testing.T, blocksChan <-chan ResponseProgress) []ResponseProgress {
	var collectedBlocks []blocks.Block
	for {
		select {
		case blk, ok := <-blocksChan:
			if !ok {
				return collectedBlocks
			}
			collectedBlocks = append(collectedBlocks, blk)
		case <-ctx.Done():
			t.Fatal("blocks channel never closed")
		}
	}
}

func readNBlocks(ctx context.Context, t *testing.T, blocksChan <-chan ResponseProgress, count int) []ResponseProgress {
	var returnedBlocks []blocks.Block
	for i := 0; i < 5; i++ {
		select {
		case blk := <-blocksChan:
			returnedBlocks = append(returnedBlocks, blk)
		case <-ctx.Done():
			t.Fatal("First blocks channel never closed")
		}
	}
	return returnedBlocks
}

func verifySingleTerminalError(ctx context.Context, t *testing.T, errChan <-chan ResponseError) {
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

func verifyEmptyErrors(ctx context.Context, t *testing.T, errChan <-chan ResponseError) {
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

func verifyEmptyBlocks(ctx context.Context, t *testing.T, blockChan <-chan ResponseProgress) {
	for {
		select {
		case _, ok := <-blockChan:
			if !ok {
				return
			}
			t.Fatal("blocks were sent but shouldn't have been")
		case <-ctx.Done():
			t.Fatal("blocks channel never closed")
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

func verifyMatchedBlocks(t *testing.T, actualBlocks []blocks.Block, expectedBlocks []blocks.Block) {
	if len(actualBlocks) != len(expectedBlocks) {
		t.Fatal("wrong number of blocks sent")
	}
	for _, blk := range actualBlocks {
		if !testutil.ContainsBlock(expectedBlocks, blk) {
			t.Fatal("wrong block sent")
		}
	}
}

func TestNormalSimultaneousFetch(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	requestManager := New(ctx, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(2)

	s1 := testbridge.NewMockSelectorSpec(testutil.GenerateCids(5))
	s2 := testbridge.NewMockSelectorSpec(testutil.GenerateCids(5))

	returnedBlocksChan1, returnedErrorChan1 := requestManager.SendRequest(requestCtx, peers[0], s1)
	returnedBlocksChan2, returnedErrorChan2 := requestManager.SendRequest(requestCtx, peers[1], s2)

	requestRecords := readNNetworkRequests(requestCtx, t, requestRecordChan, 2)

	if requestRecords[0].p != peers[0] || requestRecords[1].p != peers[1] ||
		requestRecords[0].isCancel != false || requestRecords[1].isCancel != false ||
		requestRecords[0].priority != maxPriority ||
		requestRecords[1].priority != maxPriority {
		t.Fatal("did not send correct requests")
	}

	returnedS1, err := fakeIPLDBridge.DecodeNode(requestRecords[0].selector)
	if err != nil || !reflect.DeepEqual(s1, returnedS1) {
		t.Fatal("did not encode selector properly")
	}
	returnedS2, err := fakeIPLDBridge.DecodeNode(requestRecords[1].selector)
	if err != nil || !reflect.DeepEqual(s2, returnedS2) {
		t.Fatal("did not encode selector properly")
	}

	// for now, we are just going going to test that blocks get sent to all peers
	// whose connection is still open
	firstBlocks := testutil.GenerateBlocksOfSize(5, 100)

	msg := gsmsg.New()
	msg.AddResponse(requestRecords[0].requestID, gsmsg.RequestCompletedFull, nil)
	msg.AddResponse(requestRecords[1].requestID, gsmsg.PartialResponse, nil)
	for _, blk := range firstBlocks {
		msg.AddBlock(blk)
	}

	requestManager.ProcessResponses(msg)

	moreBlocks := testutil.GenerateBlocksOfSize(5, 100)
	msg2 := gsmsg.New()
	msg2.AddResponse(requestRecords[1].requestID, gsmsg.RequestCompletedFull, nil)
	for _, blk := range moreBlocks {
		msg2.AddBlock(blk)
	}

	requestManager.ProcessResponses(msg2)

	returnedBlocks1 := collectBlocks(requestCtx, t, returnedBlocksChan1)
	verifyMatchedBlocks(t, returnedBlocks1, firstBlocks)
	returnedBlocks2 := collectBlocks(requestCtx, t, returnedBlocksChan2)
	verifyMatchedBlocks(t, returnedBlocks2[:5], firstBlocks)
	verifyMatchedBlocks(t, returnedBlocks2[5:], moreBlocks)
	verifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	verifyEmptyErrors(requestCtx, t, returnedErrorChan2)
}

func TestCancelRequestInProgress(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	requestManager := New(ctx, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	requestCtx1, cancel1 := context.WithCancel(requestCtx)
	requestCtx2, cancel2 := context.WithCancel(requestCtx)
	defer cancel2()
	peers := testutil.GeneratePeers(2)

	s1 := testbridge.NewMockSelectorSpec(testutil.GenerateCids(5))
	s2 := testbridge.NewMockSelectorSpec(testutil.GenerateCids(5))

	returnedBlocksChan1, returnedErrorChan1 := requestManager.SendRequest(requestCtx1, peers[0], s1)
	returnedBlocksChan2, returnedErrorChan2 := requestManager.SendRequest(requestCtx2, peers[1], s2)

	requestRecords := readNNetworkRequests(requestCtx, t, requestRecordChan, 2)

	// for now, we are just going going to test that blocks get sent to all peers
	// whose connection is still open
	firstBlocks := testutil.GenerateBlocksOfSize(5, 100)

	msg := gsmsg.New()
	msg.AddResponse(requestRecords[0].requestID, gsmsg.PartialResponse, nil)
	msg.AddResponse(requestRecords[1].requestID, gsmsg.PartialResponse, nil)
	for _, blk := range firstBlocks {
		msg.AddBlock(blk)
	}

	requestManager.ProcessResponses(msg)
	returnedBlocks1 := readNBlocks(requestCtx, t, returnedBlocksChan1, 5)
	cancel1()

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]
	if rr.isCancel != true || rr.requestID != requestRecords[0].requestID {
		t.Fatal("did not send correct cancel message over network")
	}

	moreBlocks := testutil.GenerateBlocksOfSize(5, 100)
	msg2 := gsmsg.New()
	msg2.AddResponse(requestRecords[0].requestID, gsmsg.RequestCompletedFull, nil)
	msg2.AddResponse(requestRecords[1].requestID, gsmsg.RequestCompletedFull, nil)
	for _, blk := range moreBlocks {
		msg2.AddBlock(blk)
	}

	requestManager.ProcessResponses(msg2)
	returnedBlocks1 = append(returnedBlocks1, collectBlocks(requestCtx, t, returnedBlocksChan1)...)
	verifyMatchedBlocks(t, returnedBlocks1, firstBlocks)
	returnedBlocks2 := collectBlocks(requestCtx, t, returnedBlocksChan2)
	verifyMatchedBlocks(t, returnedBlocks2[:5], firstBlocks)
	verifyMatchedBlocks(t, returnedBlocks2[5:], moreBlocks)
	verifyEmptyErrors(requestCtx, t, returnedErrorChan1)
	verifyEmptyErrors(requestCtx, t, returnedErrorChan2)
}

func TestCancelManagerExitsGracefully(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	managerCtx, managerCancel := context.WithCancel(ctx)
	requestManager := New(managerCtx, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(2)

	s := testbridge.NewMockSelectorSpec(testutil.GenerateCids(5))
	returnedBlocksChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]

	// for now, we are just going going to test that blocks get sent to all peers
	// whose connection is still open
	firstBlocks := testutil.GenerateBlocksOfSize(5, 100)
	msg := gsmsg.New()
	msg.AddResponse(rr.requestID, gsmsg.PartialResponse, nil)
	for _, blk := range firstBlocks {
		msg.AddBlock(blk)
	}
	requestManager.ProcessResponses(msg)
	returnedBlocks := readNBlocks(requestCtx, t, returnedBlocksChan, 5)
	managerCancel()

	moreBlocks := testutil.GenerateBlocksOfSize(5, 100)
	msg2 := gsmsg.New()
	msg2.AddResponse(rr.requestID, gsmsg.RequestCompletedFull, nil)
	for _, blk := range moreBlocks {
		msg2.AddBlock(blk)
	}

	requestManager.ProcessResponses(msg2)
	returnedBlocks = append(returnedBlocks, collectBlocks(requestCtx, t, returnedBlocksChan)...)
	verifyMatchedBlocks(t, returnedBlocks, firstBlocks)
	verifyEmptyErrors(requestCtx, t, returnedErrorChan)
}

func TestInvalidSelector(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	requestManager := New(ctx, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	s := testbridge.NewInvalidSelectorSpec(testutil.GenerateCids(5))
	returnedBlocksChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	verifySingleTerminalError(requestCtx, t, returnedErrorChan)
	verifyEmptyBlocks(requestCtx, t, returnedBlocksChan)
}

func TestUnencodableSelector(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	requestManager := New(ctx, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(1)

	s := testbridge.NewUnencodableSelectorSpec(testutil.GenerateCids(5))
	returnedBlocksChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	verifySingleTerminalError(requestCtx, t, returnedErrorChan)
	verifyEmptyBlocks(requestCtx, t, returnedBlocksChan)
}

func TestFailedRequest(t *testing.T) {
	requestRecordChan := make(chan requestRecord, 2)
	fph := &fakePeerHandler{requestRecordChan}
	fakeIPLDBridge := testbridge.NewMockIPLDBridge()
	ctx := context.Background()
	requestManager := New(ctx, fakeIPLDBridge)
	requestManager.SetDelegate(fph)
	requestManager.Startup()

	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers := testutil.GeneratePeers(2)

	s := testbridge.NewMockSelectorSpec(testutil.GenerateCids(5))
	returnedBlocksChan, returnedErrorChan := requestManager.SendRequest(requestCtx, peers[0], s)

	rr := readNNetworkRequests(requestCtx, t, requestRecordChan, 1)[0]
	msg := gsmsg.New()
	msg.AddResponse(rr.requestID, gsmsg.RequestFailedContentNotFound, nil)
	requestManager.ProcessResponses(msg)

	verifySingleTerminalError(requestCtx, t, returnedErrorChan)
	verifyEmptyBlocks(requestCtx, t, returnedBlocksChan)
}
