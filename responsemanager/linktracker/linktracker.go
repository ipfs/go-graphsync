package linktracker

import (
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipld/go-ipld-prime"
)

// LinkTracker records links being traversed to determine useful information
// in crafting responses for a peer. Specifically, if any in progress request
// has already sent a block for a given link, don't send it again.
// Second, keep track of whether links are missing blocks so you can determine
// at the end if a complete response has been transmitted.
type LinkTracker struct {
	isMissingBlocks                   map[gsmsg.GraphSyncRequestID]struct{}
	linksWithBlocksTraversedByRequest map[gsmsg.GraphSyncRequestID][]ipld.Link
	traversalsWithBlocksInProgress    map[ipld.Link]int
}

// New makes a new link tracker
func New() *LinkTracker {
	return &LinkTracker{
		isMissingBlocks:                   make(map[gsmsg.GraphSyncRequestID]struct{}),
		linksWithBlocksTraversedByRequest: make(map[gsmsg.GraphSyncRequestID][]ipld.Link),
		traversalsWithBlocksInProgress:    make(map[ipld.Link]int),
	}
}

// ShouldSendBlockFor says whether we should send a block for a given link, based
// on whether we have traversed it already in one of the in progress requests and
// sent a block already.
func (lt *LinkTracker) ShouldSendBlockFor(link ipld.Link) bool {
	return lt.traversalsWithBlocksInProgress[link] == 0
}

// RecordLinkTraversal records that we traversed a link during a request, and
// whether we had the block when we did it.
func (lt *LinkTracker) RecordLinkTraversal(requestID gsmsg.GraphSyncRequestID, link ipld.Link, hasBlock bool) {
	if hasBlock {
		lt.linksWithBlocksTraversedByRequest[requestID] = append(lt.linksWithBlocksTraversedByRequest[requestID], link)
		lt.traversalsWithBlocksInProgress[link]++
	} else {
		lt.isMissingBlocks[requestID] = struct{}{}
	}
}

// FinishRequest records that we have completed the given request, and returns
// true if all links traversed had blocks present.
func (lt *LinkTracker) FinishRequest(requestID gsmsg.GraphSyncRequestID) (hasAllBlocks bool) {
	_, ok := lt.isMissingBlocks[requestID]
	hasAllBlocks = !ok
	links, ok := lt.linksWithBlocksTraversedByRequest[requestID]
	if !ok {
		return
	}
	for _, link := range links {
		lt.traversalsWithBlocksInProgress[link]--
		if lt.traversalsWithBlocksInProgress[link] <= 0 {
			delete(lt.traversalsWithBlocksInProgress, link)
		}
	}
	delete(lt.linksWithBlocksTraversedByRequest, requestID)

	return
}
