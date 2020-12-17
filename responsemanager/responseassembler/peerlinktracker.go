package responseassembler

import (
	"sync"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/linktracker"
	"github.com/ipld/go-ipld-prime"
)

type peerLinkTracker struct {
	linkTrackerLk sync.RWMutex
	linkTracker   *linktracker.LinkTracker
	altTrackers   map[string]*linktracker.LinkTracker
	dedupKeys     map[graphsync.RequestID]string
}

func newTracker() *peerLinkTracker {
	return &peerLinkTracker{
		linkTracker: linktracker.New(),
		dedupKeys:   make(map[graphsync.RequestID]string),
		altTrackers: make(map[string]*linktracker.LinkTracker),
	}
}

func (prs *peerLinkTracker) getLinkTracker(requestID graphsync.RequestID) *linktracker.LinkTracker {
	key, ok := prs.dedupKeys[requestID]
	if ok {
		return prs.altTrackers[key]
	}
	return prs.linkTracker
}

func (prs *peerLinkTracker) DedupKey(requestID graphsync.RequestID, key string) {
	prs.linkTrackerLk.Lock()
	defer prs.linkTrackerLk.Unlock()
	prs.dedupKeys[requestID] = key
	_, ok := prs.altTrackers[key]
	if !ok {
		prs.altTrackers[key] = linktracker.New()
	}
}

func (prs *peerLinkTracker) IgnoreBlocks(requestID graphsync.RequestID, links []ipld.Link) {
	prs.linkTrackerLk.Lock()
	linkTracker := prs.getLinkTracker(requestID)
	for _, link := range links {
		linkTracker.RecordLinkTraversal(requestID, link, true)
	}
	prs.linkTrackerLk.Unlock()
}

func (prs *peerLinkTracker) FinishTracking(requestID graphsync.RequestID) bool {
	prs.linkTrackerLk.Lock()
	defer prs.linkTrackerLk.Unlock()
	linkTracker := prs.getLinkTracker(requestID)
	allBlocks := linkTracker.FinishRequest(requestID)
	key, ok := prs.dedupKeys[requestID]
	if ok {
		delete(prs.dedupKeys, requestID)
		var otherRequestsFound bool
		for _, otherKey := range prs.dedupKeys {
			if otherKey == key {
				otherRequestsFound = true
				break
			}
		}
		if !otherRequestsFound {
			delete(prs.altTrackers, key)
		}
	}
	return allBlocks
}

func (prs *peerLinkTracker) RecordLinkTraversal(requestID graphsync.RequestID,
	link ipld.Link, hasBlock bool) (isUnique bool) {
	prs.linkTrackerLk.Lock()
	linkTracker := prs.getLinkTracker(requestID)
	isUnique = linkTracker.BlockRefCount(link) == 0
	linkTracker.RecordLinkTraversal(requestID, link, hasBlock)
	prs.linkTrackerLk.Unlock()
	return
}
