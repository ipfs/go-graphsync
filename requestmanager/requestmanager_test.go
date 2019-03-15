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

	requestRecords := make([]requestRecord, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case rr := <-requestRecordChan:
			requestRecords = append(requestRecords, rr)
		case <-requestCtx.Done():
			t.Fatal("should have sent two requests to the network but did not")
		}
	}

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

	var returnedBlocks1 []blocks.Block
collectFirstBlocks:
	for {
		select {
		case blk, ok := <-returnedBlocksChan1:
			if !ok {
				break collectFirstBlocks
			}
			returnedBlocks1 = append(returnedBlocks1, blk)
		case <-requestCtx.Done():
			t.Fatal("First blocks channel never closed")
		}
	}
	if len(returnedBlocks1) != 5 {
		t.Fatal("wrong number of blocks sent")
	}
	for _, blk := range returnedBlocks1 {
		if !testutil.ContainsBlock(firstBlocks, blk) {
			t.Fatal("wrong block sent")
		}
	}
	var returnedBlocks2 []blocks.Block
collectSecondBlocks:
	for {
		select {
		case blk, ok := <-returnedBlocksChan2:
			if !ok {
				break collectSecondBlocks
			}
			returnedBlocks2 = append(returnedBlocks2, blk)
		case <-requestCtx.Done():
			t.Fatal("Second blocks channel never closed")
		}

	}
	if len(returnedBlocks2) != 10 {
		t.Fatal("wrong number of blocks sent")
	}
	for i, blk := range returnedBlocks2 {
		if i < 5 {
			if !testutil.ContainsBlock(firstBlocks, blk) {
				t.Fatal("wrong block sent")
			}
		} else {
			if !testutil.ContainsBlock(moreBlocks, blk) {
				t.Fatal("wrong block sent")
			}
		}
	}
collectFirstErrors:
	for {
		select {
		case _, ok := <-returnedErrorChan1:
			if !ok {
				break collectFirstErrors
			}
			t.Fatal("errors were sent but shouldn't have been")
		case <-requestCtx.Done():
			t.Fatal("errors channel never closed")
		}
	}
collectSecondErrors:
	for {
		select {
		case _, ok := <-returnedErrorChan2:
			if !ok {
				break collectSecondErrors
			}
			t.Fatal("errors were sent but shouldn't have been")
		case <-requestCtx.Done():
			t.Fatal("errors channel never closed")
		}
	}
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

	requestRecords := make([]requestRecord, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case rr := <-requestRecordChan:
			requestRecords = append(requestRecords, rr)
		case <-requestCtx.Done():
			t.Fatal("should have sent two requests to the network but did not")
		}
	}

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
	var returnedBlocks1 []blocks.Block
	for i := 0; i < 5; i++ {
		select {
		case blk := <-returnedBlocksChan1:
			returnedBlocks1 = append(returnedBlocks1, blk)
		case <-requestCtx.Done():
			t.Fatal("First blocks channel never closed")
		}
	}
	cancel1()

	select {
	case rr := <-requestRecordChan:
		if rr.isCancel != true || rr.requestID != requestRecords[0].requestID {
			t.Fatal("did not send correct cancel message over network")
		}
	case <-requestCtx.Done():
		t.Fatal("did not send cancel message over network")
	}

	moreBlocks := testutil.GenerateBlocksOfSize(5, 100)
	msg2 := gsmsg.New()
	msg2.AddResponse(requestRecords[0].requestID, gsmsg.RequestCompletedFull, nil)
	msg2.AddResponse(requestRecords[1].requestID, gsmsg.RequestCompletedFull, nil)
	for _, blk := range moreBlocks {
		msg2.AddBlock(blk)
	}

	requestManager.ProcessResponses(msg2)
collectFirstBlocks:
	for {
		select {
		case blk, ok := <-returnedBlocksChan1:
			if !ok {
				break collectFirstBlocks
			}
			returnedBlocks1 = append(returnedBlocks1, blk)
		case <-requestCtx.Done():
			t.Fatal("First blocks channel never closed")
		}
	}
	if len(returnedBlocks1) != 5 {
		t.Fatal("wrong number of blocks sent")
	}
	for _, blk := range returnedBlocks1 {
		if !testutil.ContainsBlock(firstBlocks, blk) {
			t.Fatal("wrong block sent")
		}
	}
	var returnedBlocks2 []blocks.Block
collectSecondBlocks:
	for {
		select {
		case blk, ok := <-returnedBlocksChan2:
			if !ok {
				break collectSecondBlocks
			}
			returnedBlocks2 = append(returnedBlocks2, blk)
		case <-requestCtx.Done():
			t.Fatal("Second blocks channel never closed")
		}

	}
	if len(returnedBlocks2) != 10 {
		t.Fatal("wrong number of blocks sent")
	}
	for i, blk := range returnedBlocks2 {
		if i < 5 {
			if !testutil.ContainsBlock(firstBlocks, blk) {
				t.Fatal("wrong block sent")
			}
		} else {
			if !testutil.ContainsBlock(moreBlocks, blk) {
				t.Fatal("wrong block sent")
			}
		}
	}
collectFirstErrors:
	for {
		select {
		case _, ok := <-returnedErrorChan1:
			if !ok {
				break collectFirstErrors
			}
			t.Fatal("errors were sent but shouldn't have been")
		case <-requestCtx.Done():
			t.Fatal("errors channel never closed")
		}
	}
collectSecondErrors:
	for {
		select {
		case _, ok := <-returnedErrorChan2:
			if !ok {
				break collectSecondErrors
			}
			t.Fatal("errors were sent but shouldn't have been")
		case <-requestCtx.Done():
			t.Fatal("errors channel never closed")
		}
	}
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

	var rr requestRecord
	select {
	case rr = <-requestRecordChan:
	case <-requestCtx.Done():
		t.Fatal("should have request to the network but did not")
	}

	// for now, we are just going going to test that blocks get sent to all peers
	// whose connection is still open
	firstBlocks := testutil.GenerateBlocksOfSize(5, 100)
	msg := gsmsg.New()
	msg.AddResponse(rr.requestID, gsmsg.PartialResponse, nil)
	for _, blk := range firstBlocks {
		msg.AddBlock(blk)
	}
	requestManager.ProcessResponses(msg)
	var returnedBlocks []blocks.Block
	for i := 0; i < 5; i++ {
		select {
		case blk := <-returnedBlocksChan:
			returnedBlocks = append(returnedBlocks, blk)
		case <-requestCtx.Done():
			t.Fatal("blocks channel never closed")
		}
	}

	managerCancel()

	moreBlocks := testutil.GenerateBlocksOfSize(5, 100)
	msg2 := gsmsg.New()
	msg2.AddResponse(rr.requestID, gsmsg.RequestCompletedFull, nil)
	for _, blk := range moreBlocks {
		msg2.AddBlock(blk)
	}

	requestManager.ProcessResponses(msg2)
collectFirstBlocks:
	for {
		select {
		case blk, ok := <-returnedBlocksChan:
			if !ok {
				break collectFirstBlocks
			}
			returnedBlocks = append(returnedBlocks, blk)
		case <-requestCtx.Done():
			t.Fatal("blocks channel never closed")
		}
	}
	if len(returnedBlocks) != 5 {
		t.Fatal("wrong number of blocks sent")
	}
	for _, blk := range returnedBlocks {
		if !testutil.ContainsBlock(firstBlocks, blk) {
			t.Fatal("wrong block sent")
		}
	}
collectFirstErrors:
	for {
		select {
		case _, ok := <-returnedErrorChan:
			if !ok {
				break collectFirstErrors
			}
			t.Fatal("errors were sent but shouldn't have been")
		case <-requestCtx.Done():
			t.Fatal("errors channel never closed")
		}
	}
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

	select {
	case err := <-returnedErrorChan:
		if err.Error == nil || err.IsTerminal != true {
			t.Fatal("should have sent a single terminal error but did not")
		}
	case <-requestCtx.Done():
		t.Fatal("no errors sent")
	}
collectFirstBlocks:
	for {
		select {
		case _, ok := <-returnedBlocksChan:
			if !ok {
				break collectFirstBlocks
			}
			t.Fatal("blocks were sent but shouldn't have been")
		case <-requestCtx.Done():
			t.Fatal("blocks channel never closed")
		}
	}
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

	select {
	case err := <-returnedErrorChan:
		if err.Error == nil || err.IsTerminal != true {
			t.Fatal("should have sent a single terminal error but did not")
		}
	case <-requestCtx.Done():
		t.Fatal("no errors sent")
	}
collectFirstBlocks:
	for {
		select {
		case _, ok := <-returnedBlocksChan:
			if !ok {
				break collectFirstBlocks
			}
			t.Fatal("blocks were sent but shouldn't have been")
		case <-requestCtx.Done():
			t.Fatal("blocks channel never closed")
		}
	}
}
