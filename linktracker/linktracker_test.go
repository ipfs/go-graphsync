package linktracker

import (
	"math/rand"
	"testing"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestBlockRefCount(t *testing.T) {
	linkTracker := New()
	link1 := testutil.NewTestLink()
	link2 := testutil.NewTestLink()
	if linkTracker.BlockRefCount(link1) != 0 || linkTracker.BlockRefCount(link2) != 0 {
		t.Fatal("Links not traversed should have refcount 0")
	}
	requestID1 := graphsync.RequestID(rand.Int31())
	requestID2 := graphsync.RequestID(rand.Int31())

	linkTracker.RecordLinkTraversal(requestID1, link1, true)
	linkTracker.RecordLinkTraversal(requestID1, link2, true)
	linkTracker.RecordLinkTraversal(requestID2, link1, true)

	if linkTracker.BlockRefCount(link1) == 0 || linkTracker.BlockRefCount(link2) == 0 {
		t.Fatal("Links already traversed with blocks should not have ref count 0")
	}

	linkTracker.FinishRequest(requestID1)
	if linkTracker.BlockRefCount(link1) == 0 || linkTracker.BlockRefCount(link2) != 0 {
		t.Fatal("Finishing request decrement refcount for block traversed by request")
	}
}

func TestHasAllBlocks(t *testing.T) {
	linkTracker := New()
	link1 := testutil.NewTestLink()
	link2 := testutil.NewTestLink()
	requestID1 := graphsync.RequestID(rand.Int31())
	requestID2 := graphsync.RequestID(rand.Int31())

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
	link1 := testutil.NewTestLink()
	if linkTracker.BlockRefCount(link1) != 0 {
		t.Fatal("Links not traversed should send blocks")
	}
	requestID1 := graphsync.RequestID(rand.Int31())
	requestID2 := graphsync.RequestID(rand.Int31())

	linkTracker.RecordLinkTraversal(requestID1, link1, false)
	linkTracker.RecordLinkTraversal(requestID2, link1, false)

	if linkTracker.BlockRefCount(link1) != 0 {
		t.Fatal("Links traversed without blocks should still send them if they become availabe")
	}

	linkTracker.RecordLinkTraversal(requestID1, link1, true)
	if linkTracker.BlockRefCount(link1) == 0 {
		t.Fatal("Links traversed with blocks should no longer send")
	}

	hasAllBlocks := linkTracker.FinishRequest(requestID1)
	if hasAllBlocks {
		t.Fatal("Even if block becomes available, traversal may be incomplete, request still should not be considered to have all blocks")
	}

	if linkTracker.BlockRefCount(link1) != 0 {
		t.Fatal("Block traversals should resend for requests that never traversed while block was present")
	}
}

func TestMissingLink(t *testing.T) {
	linkTracker := New()
	link1 := testutil.NewTestLink()
	link2 := testutil.NewTestLink()
	requestID1 := graphsync.RequestID(rand.Int31())
	requestID2 := graphsync.RequestID(rand.Int31())

	linkTracker.RecordLinkTraversal(requestID1, link1, true)
	linkTracker.RecordLinkTraversal(requestID1, link2, false)
	linkTracker.RecordLinkTraversal(requestID2, link1, true)

	if linkTracker.IsKnownMissingLink(requestID1, link1) ||
		!linkTracker.IsKnownMissingLink(requestID1, link2) ||
		linkTracker.IsKnownMissingLink(requestID2, link1) ||
		linkTracker.IsKnownMissingLink(requestID2, link2) {
		t.Fatal("Did not record which links are known missing correctly")
	}
}
