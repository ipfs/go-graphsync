package reconciledloader

import (
	"context"
	"io/ioutil"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// BlockReadOpener synchronously loads the next block result
// as long as the request is online, it will wait for more remote items until it can load this link definitively
// once the request is offline
func (rl *ReconciledLoader) BlockReadOpener(lctx linking.LinkContext, link datamodel.Link) types.AsyncLoadResult {
	// if there is a most recent offline load, store it in the list of older offline loads
	if rl.mostRecentOfflineLoad.link != nil {
		rl.previousOfflineLoads.store(rl.mostRecentOfflineLoad)
		rl.mostRecentOfflineLoad = loadAttempt{}
	}

	/*// if we are on a path that is missing fromt he remote just load local
	if rl.pathTracker.stillOnMissingRemotePath(lctx.LinkPath) {
		return rl.loadLocal(lctx, link)
	}*/

	// catch up the remore or determine that we are offline
	isOnline, err := rl.catchUpRemote()
	if err != nil {
		return types.AsyncLoadResult{Err: err, Local: !isOnline}
	}

	// if we're offline just load local
	if !isOnline {
		// because the request is offline, we record what was loaded
		/// from local store should request go online later -- at that point
		// we will need to retry and reconcile with older loads
		rl.mostRecentOfflineLoad = loadAttempt{link: link, linkContext: lctx}
		return rl.loadLocal(lctx, link)
	}

	// only attempt remote load if after reconciliation we're not on a missing path
	if !rl.pathTracker.stillOnMissingRemotePath(lctx.LinkPath) {
		data, err := rl.loadRemote(lctx, link)
		if data != nil {
			return types.AsyncLoadResult{Data: data, Local: false}
		}
		if err != nil {
			return types.AsyncLoadResult{Err: err, Local: false}
		}
	}
	// remote had missing or duplicate block, attempt load local
	return rl.loadLocal(lctx, link)

}

// catchUpRemote waits for remote items to come in until:
// a. the request is offline or goes offline
// b. the remote is fully caught up with previous loads
//    from when the request was offline and has at least one more item
func (rl *ReconciledLoader) catchUpRemote() (isOnline bool, err error) {
	// wait till remote item queue is non-empty or request is offline
	isOnline = rl.remoteQueue.waitItems()
	// if we have remote items queued but previous loads that
	// occurred while the request was offline,
	for !rl.previousOfflineLoads.empty() && isOnline && err == nil {
		isOnline, err = rl.reconcileStep()
	}
	return
}

func (rl *ReconciledLoader) loadLocal(lctx linking.LinkContext, link datamodel.Link) types.AsyncLoadResult {
	stream, err := rl.lsys.StorageReadOpener(lctx, link)
	if err != nil {
		return types.AsyncLoadResult{Err: graphsync.MissingBlockErr{Link: link}, Local: true}
	}
	localData, err := ioutil.ReadAll(stream)
	if err != nil {
		return types.AsyncLoadResult{Err: graphsync.MissingBlockErr{Link: link}, Local: true}
	}
	return types.AsyncLoadResult{Data: localData, Local: true}
}

func (rl *ReconciledLoader) loadRemote(lctx linking.LinkContext, link datamodel.Link) ([]byte, error) {
	head := rl.remoteQueue.first()
	rl.remoteQueue.consume()

	// verify it matches the expected next load
	if !head.link.Equals(link.(cidlink.Link).Cid) {
		return nil, graphsync.RemoteIncorrectResponseError{
			LocalLink:  link,
			RemoteLink: cidlink.Link{Cid: head.link},
		}
	}

	// update path tracking
	rl.pathTracker.recordRemoteLoadAttempt(lctx.LinkPath, head.action)

	// if block == nil, we have no remote block to load
	if head.block == nil {
		return nil, nil
	}

	// get a context
	ctx := lctx.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	// start a span
	_, span := otel.Tracer("graphsync").Start(
		ctx,
		"verifyBlock",
		trace.WithLinks(head.traceLink),
		trace.WithAttributes(attribute.String("cid", link.String())))
	defer span.End()

	// update our total data size buffered
	rl.dataSize = rl.dataSize - uint64(len(head.block))
	log.Debugw("verified block", "request_id", rl.requestID, "total_queued_bytes", rl.dataSize)

	// save the block
	buffer, committer, err := rl.lsys.StorageWriteOpener(lctx)
	if err != nil {
		return nil, err
	}
	if settable, ok := buffer.(settableWriter); ok {
		err = settable.SetBytes(head.block)
	} else {
		_, err = buffer.Write(head.block)
	}
	if err != nil {
		return nil, err
	}
	err = committer(link)
	if err != nil {
		return nil, err
	}

	// return the block
	return head.block, nil
}

// reconcile step attempts to reconcile the head of the previous
// offline load queue with the head of the remote queue
func (rl *ReconciledLoader) reconcileStep() (isStillOnline bool, err error) {
	local := rl.previousOfflineLoads.first()
	// if we're on a path that's missing from the remote
	if rl.pathTracker.stillOnMissingRemotePath(local.path) {
		rl.previousOfflineLoads.consume()
		return true, nil
	}
	// ok we're on a path that both the remote and local have
	// do they match?

	remote := rl.remoteQueue.first()
	if local.link != remote.link {
		// return reconciliation error
		return true, graphsync.RemoteIncorrectResponseError{
			LocalLink:  cidlink.Link{Cid: local.link},
			RemoteLink: cidlink.Link{Cid: remote.link},
		}
	}
	// ok both match

	// if remote is missing, set missing path from local load
	rl.pathTracker.recordRemoteLoadAttempt(local.path, remote.action)

	// consume both once and return
	rl.previousOfflineLoads.consume()
	rl.remoteQueue.consume()
	return rl.remoteQueue.waitItems(), nil
}
