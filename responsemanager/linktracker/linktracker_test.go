package linktracker

import (
	"math/rand"
	"testing"

	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testbridge"
)

func TestShouldSendBlocks(t *testing.T) {
	linkTracker := New()
	link1 := testbridge.NewMockLink()
	link2 := testbridge.NewMockLink()
	if !linkTracker.ShouldSendBlockFor(link1) || !linkTracker.ShouldSendBlockFor(link2) {
		t.Fatal("Links not traversed should send blocks")
	}
	requestID1 := gsmsg.GraphSyncRequestID(rand.Int31())
	requestID2 := gsmsg.GraphSyncRequestID(rand.Int31())

	linkTracker.RecordLinkTraversal(requestID1, link1, true)
	linkTracker.RecordLinkTraversal(requestID1, link2, true)
	linkTracker.RecordLinkTraversal(requestID2, link1, true)

	if linkTracker.ShouldSendBlockFor(link1) || linkTracker.ShouldSendBlockFor(link2) {
		t.Fatal("Links already traversed with blocks should not send blocks again")
	}

	linkTracker.FinishRequest(requestID1)
	if linkTracker.ShouldSendBlockFor(link1) || !linkTracker.ShouldSendBlockFor(link2) {
		t.Fatal("Finishing request should resend blocks only if there are no in progress requests for that block remain")
	}
}

func TestHasAllBlocks(t *testing.T) {
	linkTracker := New()
	link1 := testbridge.NewMockLink()
	link2 := testbridge.NewMockLink()
	requestID1 := gsmsg.GraphSyncRequestID(rand.Int31())
	requestID2 := gsmsg.GraphSyncRequestID(rand.Int31())

	linkTracker.RecordLinkTraversal(requestID1, link1, true)
	linkTracker.RecordLinkTraversal(requestID1, link2, false)
	linkTracker.RecordLinkTraversal(requestID2, link1, true)

	hasAllBlocksRequest1 := linkTracker.FinishRequest(requestID1)
	hasAllBlocksRequest2 := linkTracker.FinishRequest(requestID2)
	if hasAllBlocksRequest1 || !hasAllBlocksRequest2 {
		t.Fatal("A request has all blocks if and only if all link traversals occurred with blocks present")
	}
}

func TestBlockBecomesAvailable(t *testing.T) {
	linkTracker := New()
	link1 := testbridge.NewMockLink()
	if !linkTracker.ShouldSendBlockFor(link1) {
		t.Fatal("Links not traversed should send blocks")
	}
	requestID1 := gsmsg.GraphSyncRequestID(rand.Int31())
	requestID2 := gsmsg.GraphSyncRequestID(rand.Int31())

	linkTracker.RecordLinkTraversal(requestID1, link1, false)
	linkTracker.RecordLinkTraversal(requestID2, link1, false)

	if !linkTracker.ShouldSendBlockFor(link1) {
		t.Fatal("Links traversed without blocks should still send them if they become availabe")
	}

	linkTracker.RecordLinkTraversal(requestID1, link1, true)

	if linkTracker.ShouldSendBlockFor(link1) {
		t.Fatal("Links traversed with blocks should no longer send")
	}

	hasAllBlocks := linkTracker.FinishRequest(requestID1)
	if hasAllBlocks {
		t.Fatal("Even if block becomes available, traversal may be incomplete, request still should not be considered to have all blocks")
	}

	if !linkTracker.ShouldSendBlockFor(link1) {
		t.Fatal("Block traversals should resend for requests that never traversed while block was present")
	}
}
