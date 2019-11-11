package responsecache

import (
	"fmt"
	"sync"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/metadata"
	logging "github.com/ipfs/go-log"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-graphsync/linktracker"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var log = logging.Logger("graphsync")

type responseCacheMessage interface {
	handle(rc *ResponseCache)
}

// UnverifiedBlockStore is an interface for storing blocks
// as they come in and removing them as they are verified
type UnverifiedBlockStore interface {
	PruneBlocks(func(ipld.Link) bool)
	VerifyBlock(ipld.Link) ([]byte, error)
	AddUnverifiedBlock(ipld.Link, []byte)
}

// ResponseCache maintains a store of unverified blocks and response
// data about links for loading, and prunes blocks as needed.
type ResponseCache struct {
	responseCacheLk sync.RWMutex

	linkTracker          *linktracker.LinkTracker
	unverifiedBlockStore UnverifiedBlockStore
}

// New initializes a new ResponseCache using the given unverified block store.
func New(unverifiedBlockStore UnverifiedBlockStore) *ResponseCache {
	return &ResponseCache{
		linkTracker:          linktracker.New(),
		unverifiedBlockStore: unverifiedBlockStore,
	}
}

// FinishRequest indicate there is no more need to track blocks tied to this
// response
func (rc *ResponseCache) FinishRequest(requestID graphsync.RequestID) {
	rc.responseCacheLk.Lock()
	rc.linkTracker.FinishRequest(requestID)

	rc.unverifiedBlockStore.PruneBlocks(func(link ipld.Link) bool {
		return rc.linkTracker.BlockRefCount(link) == 0
	})
	rc.responseCacheLk.Unlock()
}

// AttemptLoad attempts to laod the given block from the cache
func (rc *ResponseCache) AttemptLoad(requestID graphsync.RequestID, link ipld.Link) ([]byte, error) {
	rc.responseCacheLk.Lock()
	defer rc.responseCacheLk.Unlock()
	if rc.linkTracker.IsKnownMissingLink(requestID, link) {
		return nil, fmt.Errorf("Remote Peer Is Missing Block: %s", link.String())
	}
	data, _ := rc.unverifiedBlockStore.VerifyBlock(link)
	return data, nil
}

// ProcessResponse processes incoming response data, adding unverified blocks,
// and tracking link metadata from a remote peer
func (rc *ResponseCache) ProcessResponse(responses map[graphsync.RequestID]metadata.Metadata,
	blks []blocks.Block) {
	rc.responseCacheLk.Lock()

	for _, block := range blks {
		log.Debugf("Received block from network: %s", block.Cid().String())
		rc.unverifiedBlockStore.AddUnverifiedBlock(cidlink.Link{Cid: block.Cid()}, block.RawData())
	}

	for requestID, md := range responses {
		for _, item := range md {
			log.Debugf("Traverse link %s on request ID %d", item.Link.String(), requestID)
			rc.linkTracker.RecordLinkTraversal(requestID, item.Link, item.BlockPresent)
		}
	}

	// prune unused blocks right away
	rc.unverifiedBlockStore.PruneBlocks(func(link ipld.Link) bool {
		return rc.linkTracker.BlockRefCount(link) == 0
	})

	rc.responseCacheLk.Unlock()
}
