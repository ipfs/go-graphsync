package responsecache

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/ipfs/go-block-format"

	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime/linking/cid"

	ipld "github.com/ipld/go-ipld-prime"
)

type fakeUnverifiedBlockStore struct {
	inMemoryBlocks map[ipld.Link][]byte
}

func (ubs *fakeUnverifiedBlockStore) AddUnverifiedBlock(lnk ipld.Link, data []byte) {
	ubs.inMemoryBlocks[lnk] = data
}

func (ubs *fakeUnverifiedBlockStore) PruneBlocks(shouldPrune func(ipld.Link) bool) {
	for link := range ubs.inMemoryBlocks {
		if shouldPrune(link) {
			delete(ubs.inMemoryBlocks, link)
		}
	}
}

func (ubs *fakeUnverifiedBlockStore) VerifyBlock(lnk ipld.Link) ([]byte, error) {
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
	requestID1 := gsmsg.GraphSyncRequestID(rand.Int31())
	requestID2 := gsmsg.GraphSyncRequestID(rand.Int31())

	request1Metadata := metadata.Metadata{
		metadata.Item{
			Link:         cidlink.Link{Cid: blks[0].Cid()},
			BlockPresent: true,
		},
		metadata.Item{
			Link:         cidlink.Link{Cid: blks[1].Cid()},
			BlockPresent: false,
		},
		metadata.Item{
			Link:         cidlink.Link{Cid: blks[3].Cid()},
			BlockPresent: true,
		},
	}

	request2Metadata := metadata.Metadata{
		metadata.Item{
			Link:         cidlink.Link{Cid: blks[1].Cid()},
			BlockPresent: true,
		},
		metadata.Item{
			Link:         cidlink.Link{Cid: blks[3].Cid()},
			BlockPresent: true,
		},
		metadata.Item{
			Link:         cidlink.Link{Cid: blks[4].Cid()},
			BlockPresent: true,
		},
	}

	responses := map[gsmsg.GraphSyncRequestID]metadata.Metadata{
		requestID1: request1Metadata,
		requestID2: request2Metadata,
	}

	fubs := &fakeUnverifiedBlockStore{
		inMemoryBlocks: make(map[ipld.Link][]byte),
	}
	responseCache := New(fubs)

	responseCache.ProcessResponse(responses, blks)

	if len(fubs.blocks()) != len(blks)-1 || testutil.ContainsBlock(fubs.blocks(), blks[2]) {
		t.Fatal("should have prune block not referred to but didn't")
	}

	// should load block from unverified block store
	data, err := responseCache.AttemptLoad(requestID2, cidlink.Link{Cid: blks[4].Cid()})
	if err != nil || !reflect.DeepEqual(data, blks[4].RawData()) {
		t.Fatal("did not load correct block")
	}
	// which will remove block
	if len(fubs.blocks()) != len(blks)-2 || testutil.ContainsBlock(fubs.blocks(), blks[4]) {
		t.Fatal("should have removed block on verify but didn't")
	}

	// fails as it is a known missing block
	data, err = responseCache.AttemptLoad(requestID1, cidlink.Link{Cid: blks[1].Cid()})
	if err == nil || data != nil {
		t.Fatal("found block that should not have been found")
	}

	// should succeed for request 2 where it's not a missing block
	data, err = responseCache.AttemptLoad(requestID2, cidlink.Link{Cid: blks[1].Cid()})
	if err != nil || !reflect.DeepEqual(data, blks[1].RawData()) {
		t.Fatal("did not load correct block")
	}
	// which will remove block
	if len(fubs.blocks()) != len(blks)-3 || testutil.ContainsBlock(fubs.blocks(), blks[1]) {
		t.Fatal("should have removed block on verify but didn't")
	}

	// should be unknown result as block is not known missing or present in block store
	data, err = responseCache.AttemptLoad(requestID1, cidlink.Link{Cid: blks[2].Cid()})
	if err != nil || data != nil {
		t.Fatal("should have produced unknown result but didn't")
	}

	responseCache.FinishRequest(requestID1)
	// should remove only block 0, since it now has no refering outstanding requests
	if len(fubs.blocks()) != len(blks)-4 || testutil.ContainsBlock(fubs.blocks(), blks[0]) {
		t.Fatal("should have removed block on verify but didn't")
	}

	responseCache.FinishRequest(requestID2)
	// should remove last block since are no remaining references
	if len(fubs.blocks()) != 0 || testutil.ContainsBlock(fubs.blocks(), blks[3]) {
		t.Fatal("should have removed block on verify but didn't")
	}
}
