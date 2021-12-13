package responsecache

import (
	"context"
	"fmt"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/testutil"
)

type fakeUnverifiedBlockStore struct {
	inMemoryBlocks map[ipld.Link][]byte
}

func (ubs *fakeUnverifiedBlockStore) AddUnverifiedBlock(_ trace.Link, lnk ipld.Link, data []byte) {
	ubs.inMemoryBlocks[lnk] = data
}

func (ubs *fakeUnverifiedBlockStore) PruneBlocks(shouldPrune func(ipld.Link, uint64) bool) {
	for link, data := range ubs.inMemoryBlocks {
		if shouldPrune(link, uint64(len(data))) {
			delete(ubs.inMemoryBlocks, link)
		}
	}
}

func (ubs *fakeUnverifiedBlockStore) PruneBlock(link ipld.Link) {
	delete(ubs.inMemoryBlocks, link)
}

func (ubs *fakeUnverifiedBlockStore) VerifyBlock(lnk ipld.Link, linkCtx ipld.LinkContext) ([]byte, error) {
	data, ok := ubs.inMemoryBlocks[lnk]
	if !ok {
		return nil, fmt.Errorf("Block not found")
	}
	delete(ubs.inMemoryBlocks, lnk)
	return data, nil
}

func (ubs *fakeUnverifiedBlockStore) blocks() []blocks.Block {
	blks := make([]blocks.Block, 0, len(ubs.inMemoryBlocks))
	for link, data := range ubs.inMemoryBlocks {
		blk, err := blocks.NewBlockWithCid(data, link.(cidlink.Link).Cid)
		if err == nil {
			blks = append(blks, blk)
		}
	}
	return blks
}

func TestResponseCacheManagingLinks(t *testing.T) {
	blks := testutil.GenerateBlocksOfSize(5, 100)
	requestID1 := graphsync.NewRequestID()
	requestID2 := graphsync.NewRequestID()

	request1Metadata := metadata.Metadata{
		metadata.Item{
			Link:         blks[0].Cid(),
			BlockPresent: true,
		},
		metadata.Item{
			Link:         blks[1].Cid(),
			BlockPresent: false,
		},
		metadata.Item{
			Link:         blks[3].Cid(),
			BlockPresent: true,
		},
	}

	request2Metadata := metadata.Metadata{
		metadata.Item{
			Link:         blks[1].Cid(),
			BlockPresent: true,
		},
		metadata.Item{
			Link:         blks[3].Cid(),
			BlockPresent: true,
		},
		metadata.Item{
			Link:         blks[4].Cid(),
			BlockPresent: true,
		},
	}

	responses := map[graphsync.RequestID]metadata.Metadata{
		requestID1: request1Metadata,
		requestID2: request2Metadata,
	}

	fubs := &fakeUnverifiedBlockStore{
		inMemoryBlocks: make(map[ipld.Link][]byte),
	}
	responseCache := New(fubs)

	responseCache.ProcessResponse(context.Background(), responses, blks)

	require.Len(t, fubs.blocks(), len(blks)-1, "should prune block with no references")
	testutil.RefuteContainsBlock(t, fubs.blocks(), blks[2])

	lnkCtx := ipld.LinkContext{}
	// should load block from unverified block store
	data, err := responseCache.AttemptLoad(requestID2, cidlink.Link{Cid: blks[4].Cid()}, lnkCtx)
	require.NoError(t, err)
	require.Equal(t, blks[4].RawData(), data, "did not load correct block")

	// which will remove block
	require.Len(t, fubs.blocks(), len(blks)-2, "should prune block once verified")
	testutil.RefuteContainsBlock(t, fubs.blocks(), blks[4])

	// fails as it is a known missing block
	data, err = responseCache.AttemptLoad(requestID1, cidlink.Link{Cid: blks[1].Cid()}, lnkCtx)
	require.Error(t, err)
	require.Nil(t, data, "no data should be returned for missing block")

	// should succeed for request 2 where it's not a missing block
	data, err = responseCache.AttemptLoad(requestID2, cidlink.Link{Cid: blks[1].Cid()}, lnkCtx)
	require.NoError(t, err)
	require.Equal(t, blks[1].RawData(), data)

	// which will remove block
	require.Len(t, fubs.blocks(), len(blks)-3, "should prune block once verified")
	testutil.RefuteContainsBlock(t, fubs.blocks(), blks[1])

	// should be unknown result as block is not known missing or present in block store
	data, err = responseCache.AttemptLoad(requestID1, cidlink.Link{Cid: blks[2].Cid()}, lnkCtx)
	require.NoError(t, err)
	require.Nil(t, data, "no data should be returned for unknown block")

	responseCache.FinishRequest(requestID1)
	// should remove only block 0, since it now has no refering outstanding requests
	require.Len(t, fubs.blocks(), len(blks)-4, "should prune block when it is orphaned")
	testutil.RefuteContainsBlock(t, fubs.blocks(), blks[0])

	responseCache.FinishRequest(requestID2)
	// should remove last block since are no remaining references
	require.Len(t, fubs.blocks(), 0, "should prune block when it is orphaned")
	testutil.RefuteContainsBlock(t, fubs.blocks(), blks[3])
}
