package asyncloader

import (
	"context"
	"io"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestAsyncLoadInitialLoadSucceedsLocallyPresent(t *testing.T) {
	block := testutil.GenerateBlocksOfSize(1, 100)[0]
	st := newStore()
	link := st.Store(t, block)
	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		requestID := graphsync.NewRequestID()
		p := testutil.GeneratePeers(1)[0]
		resultChan := asyncLoader.AsyncLoad(p, requestID, link, ipld.LinkContext{})
		assertSuccessResponse(ctx, t, resultChan)
		st.AssertLocalLoads(t, 1)
	})
}

func TestAsyncLoadInitialLoadSucceedsResponsePresent(t *testing.T) {
	blocks := testutil.GenerateBlocksOfSize(1, 100)
	block := blocks[0]
	link := cidlink.Link{Cid: block.Cid()}

	st := newStore()
	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		requestID := graphsync.NewRequestID()
		responses := map[graphsync.RequestID]metadata.Metadata{
			requestID: {
				metadata.Item{
					Link:         link.Cid,
					BlockPresent: true,
				},
			},
		}
		p := testutil.GeneratePeers(1)[0]
		asyncLoader.ProcessResponse(context.Background(), responses, blocks)
		resultChan := asyncLoader.AsyncLoad(p, requestID, link, ipld.LinkContext{})

		assertSuccessResponse(ctx, t, resultChan)
		st.AssertLocalLoads(t, 0)
		st.AssertBlockStored(t, block)
	})
}

func TestAsyncLoadInitialLoadFails(t *testing.T) {
	st := newStore()
	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		link := testutil.NewTestLink()
		requestID := graphsync.NewRequestID()

		responses := map[graphsync.RequestID]metadata.Metadata{
			requestID: {
				metadata.Item{
					Link:         link.(cidlink.Link).Cid,
					BlockPresent: false,
				},
			},
		}
		p := testutil.GeneratePeers(1)[0]
		asyncLoader.ProcessResponse(context.Background(), responses, nil)

		resultChan := asyncLoader.AsyncLoad(p, requestID, link, ipld.LinkContext{})
		assertFailResponse(ctx, t, resultChan)
		st.AssertLocalLoads(t, 0)
	})
}

func TestAsyncLoadInitialLoadIndeterminateWhenRequestNotInProgress(t *testing.T) {
	st := newStore()
	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		link := testutil.NewTestLink()
		requestID := graphsync.NewRequestID()
		p := testutil.GeneratePeers(1)[0]
		resultChan := asyncLoader.AsyncLoad(p, requestID, link, ipld.LinkContext{})
		assertFailResponse(ctx, t, resultChan)
		st.AssertLocalLoads(t, 1)
	})
}

func TestAsyncLoadInitialLoadIndeterminateThenSucceeds(t *testing.T) {
	blocks := testutil.GenerateBlocksOfSize(1, 100)
	block := blocks[0]
	link := cidlink.Link{Cid: block.Cid()}

	st := newStore()

	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		requestID := graphsync.NewRequestID()
		err := asyncLoader.StartRequest(requestID, "")
		require.NoError(t, err)
		p := testutil.GeneratePeers(1)[0]
		resultChan := asyncLoader.AsyncLoad(p, requestID, link, ipld.LinkContext{})

		st.AssertAttemptLoadWithoutResult(ctx, t, resultChan)

		responses := map[graphsync.RequestID]metadata.Metadata{
			requestID: {
				metadata.Item{
					Link:         link.Cid,
					BlockPresent: true,
				},
			},
		}
		asyncLoader.ProcessResponse(context.Background(), responses, blocks)
		assertSuccessResponse(ctx, t, resultChan)
		st.AssertLocalLoads(t, 1)
		st.AssertBlockStored(t, block)
	})
}

func TestAsyncLoadInitialLoadIndeterminateThenFails(t *testing.T) {
	st := newStore()

	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		link := testutil.NewTestLink()
		requestID := graphsync.NewRequestID()
		err := asyncLoader.StartRequest(requestID, "")
		require.NoError(t, err)
		p := testutil.GeneratePeers(1)[0]
		resultChan := asyncLoader.AsyncLoad(p, requestID, link, ipld.LinkContext{})

		st.AssertAttemptLoadWithoutResult(ctx, t, resultChan)

		responses := map[graphsync.RequestID]metadata.Metadata{
			requestID: {
				metadata.Item{
					Link:         link.(cidlink.Link).Cid,
					BlockPresent: false,
				},
			},
		}
		asyncLoader.ProcessResponse(context.Background(), responses, nil)
		assertFailResponse(ctx, t, resultChan)
		st.AssertLocalLoads(t, 1)
	})
}

func TestAsyncLoadInitialLoadIndeterminateThenRequestFinishes(t *testing.T) {
	st := newStore()
	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		link := testutil.NewTestLink()
		requestID := graphsync.NewRequestID()
		err := asyncLoader.StartRequest(requestID, "")
		require.NoError(t, err)
		p := testutil.GeneratePeers(1)[0]
		resultChan := asyncLoader.AsyncLoad(p, requestID, link, ipld.LinkContext{})
		st.AssertAttemptLoadWithoutResult(ctx, t, resultChan)
		asyncLoader.CompleteResponsesFor(requestID)
		assertFailResponse(ctx, t, resultChan)
		st.AssertLocalLoads(t, 1)
	})
}

func TestAsyncLoadTwiceLoadsLocallySecondTime(t *testing.T) {
	blocks := testutil.GenerateBlocksOfSize(1, 100)
	block := blocks[0]
	link := cidlink.Link{Cid: block.Cid()}
	st := newStore()
	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		requestID := graphsync.NewRequestID()
		responses := map[graphsync.RequestID]metadata.Metadata{
			requestID: {
				metadata.Item{
					Link:         link.Cid,
					BlockPresent: true,
				},
			},
		}
		p := testutil.GeneratePeers(1)[0]
		asyncLoader.ProcessResponse(context.Background(), responses, blocks)
		resultChan := asyncLoader.AsyncLoad(p, requestID, link, ipld.LinkContext{})

		assertSuccessResponse(ctx, t, resultChan)
		st.AssertLocalLoads(t, 0)

		resultChan = asyncLoader.AsyncLoad(p, requestID, link, ipld.LinkContext{})
		assertSuccessResponse(ctx, t, resultChan)
		st.AssertLocalLoads(t, 1)

		st.AssertBlockStored(t, block)
	})
}

func TestRegisterUnregister(t *testing.T) {
	st := newStore()
	otherSt := newStore()
	blocks := testutil.GenerateBlocksOfSize(3, 100)
	link1 := otherSt.Store(t, blocks[0])
	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {

		requestID1 := graphsync.NewRequestID()
		err := asyncLoader.StartRequest(requestID1, "other")
		require.EqualError(t, err, "unknown persistence option")

		err = asyncLoader.RegisterPersistenceOption("other", otherSt.lsys)
		require.NoError(t, err)
		requestID2 := graphsync.NewRequestID()
		err = asyncLoader.StartRequest(requestID2, "other")
		require.NoError(t, err)
		p := testutil.GeneratePeers(1)[0]
		resultChan1 := asyncLoader.AsyncLoad(p, requestID2, link1, ipld.LinkContext{})
		assertSuccessResponse(ctx, t, resultChan1)
		err = asyncLoader.UnregisterPersistenceOption("other")
		require.EqualError(t, err, "cannot unregister while requests are in progress")
		asyncLoader.CompleteResponsesFor(requestID2)
		asyncLoader.CleanupRequest(p, requestID2)
		err = asyncLoader.UnregisterPersistenceOption("other")
		require.NoError(t, err)

		requestID3 := graphsync.NewRequestID()
		err = asyncLoader.StartRequest(requestID3, "other")
		require.EqualError(t, err, "unknown persistence option")
	})
}
func TestRequestSplittingLoadLocallyFromBlockstore(t *testing.T) {
	st := newStore()
	otherSt := newStore()
	block := testutil.GenerateBlocksOfSize(1, 100)[0]
	link := otherSt.Store(t, block)
	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		err := asyncLoader.RegisterPersistenceOption("other", otherSt.lsys)
		require.NoError(t, err)
		requestID1 := graphsync.NewRequestID()
		p := testutil.GeneratePeers(1)[0]

		resultChan1 := asyncLoader.AsyncLoad(p, requestID1, link, ipld.LinkContext{})
		requestID2 := graphsync.NewRequestID()
		err = asyncLoader.StartRequest(requestID2, "other")
		require.NoError(t, err)
		resultChan2 := asyncLoader.AsyncLoad(p, requestID2, link, ipld.LinkContext{})

		assertFailResponse(ctx, t, resultChan1)
		assertSuccessResponse(ctx, t, resultChan2)
		st.AssertLocalLoads(t, 1)
	})
}

func TestRequestSplittingSameBlockTwoStores(t *testing.T) {
	st := newStore()
	otherSt := newStore()
	blocks := testutil.GenerateBlocksOfSize(1, 100)
	block := blocks[0]
	link := cidlink.Link{Cid: block.Cid()}
	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		err := asyncLoader.RegisterPersistenceOption("other", otherSt.lsys)
		require.NoError(t, err)
		requestID1 := graphsync.NewRequestID()
		requestID2 := graphsync.NewRequestID()
		err = asyncLoader.StartRequest(requestID1, "")
		require.NoError(t, err)
		err = asyncLoader.StartRequest(requestID2, "other")
		require.NoError(t, err)
		p := testutil.GeneratePeers(1)[0]
		resultChan1 := asyncLoader.AsyncLoad(p, requestID1, link, ipld.LinkContext{})
		resultChan2 := asyncLoader.AsyncLoad(p, requestID2, link, ipld.LinkContext{})
		responses := map[graphsync.RequestID]metadata.Metadata{
			requestID1: {
				metadata.Item{
					Link:         link.Cid,
					BlockPresent: true,
				},
			},
			requestID2: {
				metadata.Item{
					Link:         link.Cid,
					BlockPresent: true,
				},
			},
		}
		asyncLoader.ProcessResponse(context.Background(), responses, blocks)

		assertSuccessResponse(ctx, t, resultChan1)
		assertSuccessResponse(ctx, t, resultChan2)
		st.AssertBlockStored(t, block)
		otherSt.AssertBlockStored(t, block)
	})
}

func TestRequestSplittingSameBlockOnlyOneResponse(t *testing.T) {
	st := newStore()
	otherSt := newStore()
	blocks := testutil.GenerateBlocksOfSize(1, 100)
	block := blocks[0]
	link := cidlink.Link{Cid: block.Cid()}
	withLoader(st, func(ctx context.Context, asyncLoader *AsyncLoader) {
		err := asyncLoader.RegisterPersistenceOption("other", otherSt.lsys)
		require.NoError(t, err)
		requestID1 := graphsync.NewRequestID()
		requestID2 := graphsync.NewRequestID()
		err = asyncLoader.StartRequest(requestID1, "")
		require.NoError(t, err)
		err = asyncLoader.StartRequest(requestID2, "other")
		require.NoError(t, err)
		p := testutil.GeneratePeers(1)[0]
		resultChan1 := asyncLoader.AsyncLoad(p, requestID1, link, ipld.LinkContext{})
		resultChan2 := asyncLoader.AsyncLoad(p, requestID2, link, ipld.LinkContext{})
		responses := map[graphsync.RequestID]metadata.Metadata{
			requestID2: {
				metadata.Item{
					Link:         link.Cid,
					BlockPresent: true,
				},
			},
		}
		asyncLoader.ProcessResponse(context.Background(), responses, blocks)
		asyncLoader.CompleteResponsesFor(requestID1)

		assertFailResponse(ctx, t, resultChan1)
		assertSuccessResponse(ctx, t, resultChan2)
		otherSt.AssertBlockStored(t, block)
	})
}

type store struct {
	internalLoader ipld.BlockReadOpener
	lsys           ipld.LinkSystem
	blockstore     map[ipld.Link][]byte
	localLoads     int
	called         chan struct{}
}

func newStore() *store {
	blockstore := make(map[ipld.Link][]byte)
	st := &store{
		lsys:       testutil.NewTestStore(blockstore),
		blockstore: blockstore,
		localLoads: 0,
		called:     make(chan struct{}),
	}
	st.internalLoader = st.lsys.StorageReadOpener
	st.lsys.StorageReadOpener = st.loader
	return st
}

func (st *store) loader(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
	select {
	case <-st.called:
	default:
		close(st.called)
	}
	st.localLoads++
	return st.internalLoader(lnkCtx, lnk)
}

func (st *store) AssertLocalLoads(t *testing.T, localLoads int) {
	require.Equalf(t, localLoads, st.localLoads, "should have loaded locally %d times", localLoads)
}

func (st *store) AssertBlockStored(t *testing.T, blk blocks.Block) {
	require.Equal(t, blk.RawData(), st.blockstore[cidlink.Link{Cid: blk.Cid()}], "should store block")
}

func (st *store) AssertAttemptLoadWithoutResult(ctx context.Context, t *testing.T, resultChan <-chan types.AsyncLoadResult) {
	testutil.AssertDoesReceiveFirst(t, st.called, "should attempt load with no result", resultChan, ctx.Done())
}

func (st *store) Store(t *testing.T, blk blocks.Block) ipld.Link {
	writer, commit, err := st.lsys.StorageWriteOpener(ipld.LinkContext{})
	require.NoError(t, err)
	_, err = writer.Write(blk.RawData())
	require.NoError(t, err, "seeds block store")
	link := cidlink.Link{Cid: blk.Cid()}
	err = commit(link)
	require.NoError(t, err, "seeds block store")
	return link
}

func withLoader(st *store, exec func(ctx context.Context, asyncLoader *AsyncLoader)) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	asyncLoader := New(ctx, st.lsys)
	exec(ctx, asyncLoader)
}

func assertSuccessResponse(ctx context.Context, t *testing.T, resultChan <-chan types.AsyncLoadResult) {
	t.Helper()
	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.NotNil(t, result.Data, "should send response")
	require.Nil(t, result.Err, "should not send error")
}

func assertFailResponse(ctx context.Context, t *testing.T, resultChan <-chan types.AsyncLoadResult) {
	t.Helper()
	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.Nil(t, result.Data, "should not send responses")
	require.NotNil(t, result.Err, "should send an error")
}
