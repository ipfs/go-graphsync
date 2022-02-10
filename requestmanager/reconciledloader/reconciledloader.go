/* Package reconciledloader implements a block loader that can load from two different sources:
- a local store
- a series of remote responses for a given graphsync selector query

It verifies the sequence of remote responses matches the sequence
of loads called from a local selector traversal.

The reconciled loader also tracks whether or not there is a remote request in progress.

When there is no request in progress, it loads from the local store only.

When there is a request in progress, waits for remote responses before loading, and only calls
upon the local store for duplicate blocks and when traversing paths the remote was missing.

The reconciled loader assumes:
1. A single thread is calling AsyncLoad to load blocks
2. When a request is online, a seperate thread may call IngestResponse
3. Either thread may call SetRemoteState or Cleanup
4. The remote sends metadata for all blocks it traverses in the query (per GraphSync protocol spec) - whether or not
the actual block is sent.
*/
package reconciledloader

import (
	"context"
	"errors"
	"sync"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
)

var log = logging.Logger("gs-reconciledlaoder")

type settableWriter interface {
	SetBytes([]byte) error
}

type reconiledLoaderMessage interface {
	handle(rl *ReconciledLoader)
}

type loadAttempt struct {
	link        datamodel.Link
	linkContext linking.LinkContext
}

// ReconciledLoader is an instance of the reconciled loader
type ReconciledLoader struct {
	remoteQueue           remoteQueue
	mostRecentOfflineLoad loadAttempt
	previousOfflineLoads  offlineLoadQueue
	pathTracker           pathTracker
	requestID             graphsync.RequestID
	dataSize              uint64
	lsys                  linking.LinkSystem
}

// NewReconciledLoader returns a new reconciled loader for the given requestID & localStore
func NewReconciledLoader(requestID graphsync.RequestID, localStore linking.LinkSystem) *ReconciledLoader {
	lock := &sync.Mutex{}
	return &ReconciledLoader{
		requestID: requestID,
		lsys:      localStore,
		remoteQueue: remoteQueue{
			lock:   lock,
			signal: sync.NewCond(lock),
		},
	}
}

// SetRemoteState records whether or not the request is online
func (rl *ReconciledLoader) SetRemoteState(online bool) {
	rl.remoteQueue.setOpen(online)
}

// Cleanup frees up some memory resources for this loader prior to throwing it away
func (rl *ReconciledLoader) Cleanup(ctx context.Context) {
	rl.remoteQueue.clear()
	for !rl.previousOfflineLoads.empty() {
		rl.previousOfflineLoads.consume()
	}
}

// RetryLastOfflineLoad retries the last offline load, assuming one is present
func (rl *ReconciledLoader) RetryLastOfflineLoad() types.AsyncLoadResult {
	if rl.mostRecentOfflineLoad.link == nil {
		return types.AsyncLoadResult{Err: errors.New("cannot retry offline load when non is present")}
	}
	retryLoadAttempt := rl.mostRecentOfflineLoad
	rl.mostRecentOfflineLoad = loadAttempt{}
	return rl.BlockReadOpener(retryLoadAttempt.linkContext, retryLoadAttempt.link)
}
