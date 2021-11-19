package testutil

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	util "github.com/ipfs/go-ipfs-util"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	random "github.com/jbenet/go-random"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
)

var blockGenerator = blocksutil.NewBlockGenerator()
var seedSeq int64

// RandomBytes returns a byte array of the given size with random values.
func RandomBytes(n int64) []byte {
	data := new(bytes.Buffer)
	_ = random.WritePseudoRandomBytes(n, data, seedSeq)
	seedSeq++
	return data.Bytes()
}

// GenerateBlocksOfSize generates a series of blocks of the given byte size
func GenerateBlocksOfSize(n int, size int64) []blocks.Block {
	generatedBlocks := make([]blocks.Block, 0, n)
	for i := 0; i < n; i++ {
		data := RandomBytes(size)
		mhash := util.Hash(data)
		c := cid.NewCidV1(cid.Raw, mhash)
		b, _ := blocks.NewBlockWithCid(data, c)
		generatedBlocks = append(generatedBlocks, b)

	}
	return generatedBlocks
}

// GenerateCids produces n content identifiers.
func GenerateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		cids = append(cids, c)
	}
	return cids
}

var peerSeq int

// GeneratePeers creates n peer ids.
func GeneratePeers(n int) []peer.ID {
	peerIds := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		peerSeq++
		p := peer.ID(fmt.Sprint(peerSeq))
		peerIds = append(peerIds, p)
	}
	return peerIds
}

// ContainsPeer returns true if a peer is found n a list of peers.
func ContainsPeer(peers []peer.ID, p peer.ID) bool {
	for _, n := range peers {
		if p == n {
			return true
		}
	}
	return false
}

// AssertContainsPeer will fail a test if the peer is not in the given peer list
func AssertContainsPeer(t testing.TB, peers []peer.ID, p peer.ID) {
	t.Helper()
	require.True(t, ContainsPeer(peers, p), "given peer should be in list")
}

// RefuteContainsPeer will fail a test if the peer is in the given peer list
func RefuteContainsPeer(t testing.TB, peers []peer.ID, p peer.ID) {
	t.Helper()
	require.False(t, ContainsPeer(peers, p), "given peer should not be in list")
}

// IndexOf returns the index of a given cid in an array of blocks
func IndexOf(blks []blocks.Block, c cid.Cid) int {
	for i, n := range blks {
		if n.Cid() == c {
			return i
		}
	}
	return -1
}

// ContainsBlock returns true if a block is found n a list of blocks
func ContainsBlock(blks []blocks.Block, block blocks.Block) bool {
	return IndexOf(blks, block.Cid()) != -1
}

// AssertContainsBlock will fail a test if the block is not in the given block list
func AssertContainsBlock(t testing.TB, blks []blocks.Block, block blocks.Block) {
	t.Helper()
	require.True(t, ContainsBlock(blks, block), "given block should be in list")
}

// RefuteContainsBlock will fail a test if the block is in the given block list
func RefuteContainsBlock(t testing.TB, blks []blocks.Block, block blocks.Block) {
	t.Helper()
	require.False(t, ContainsBlock(blks, block), "given block should not be in list")
}

// CollectResponses is just a utility to convert a graphsync response progress
// channel into an array.
func CollectResponses(ctx context.Context, t testing.TB, responseChan <-chan graphsync.ResponseProgress) []graphsync.ResponseProgress {
	t.Helper()
	var collectedBlocks []graphsync.ResponseProgress
	for {
		select {
		case blk, ok := <-responseChan:
			if !ok {
				return collectedBlocks
			}
			collectedBlocks = append(collectedBlocks, blk)
		case <-ctx.Done():
			require.FailNow(t, "response channel never closed")
		}
	}
}

// CollectErrors is just a utility to convert an error channel into an array.
func CollectErrors(ctx context.Context, t *testing.T, errChan <-chan error) []error {
	t.Helper()
	var collectedErrors []error
	for {
		select {
		case err, ok := <-errChan:
			if !ok {
				return collectedErrors
			}
			collectedErrors = append(collectedErrors, err)
		case <-ctx.Done():
			t.Fatal("error channel never closed")
		}
	}
}

// ReadNResponses does a partial read from a ResponseProgress channel -- up
// to n values
func ReadNResponses(ctx context.Context, t testing.TB, responseChan <-chan graphsync.ResponseProgress, count int) []graphsync.ResponseProgress {
	t.Helper()
	var returnedBlocks []graphsync.ResponseProgress
	for i := 0; i < count; i++ {
		select {
		case blk, ok := <-responseChan:
			if !ok {
				require.FailNowf(t, "Channel closed early", "expected %d messages, got %d", count, len(returnedBlocks))
			}
			returnedBlocks = append(returnedBlocks, blk)
		case <-ctx.Done():
			require.FailNow(t, "Unable to read enough responses")
		}
	}
	return returnedBlocks
}

// VerifySingleTerminalError verifies that exactly one error was sent over a channel
// and then the channel was closed.
func VerifySingleTerminalError(ctx context.Context, t testing.TB, errChan <-chan error) {
	t.Helper()
	var err error
	AssertReceive(ctx, t, errChan, &err, "should receive an error")
	select {
	case secondErr, ok := <-errChan:
		require.Falsef(t, ok, "shouldn't have sent second error but sent: %s, %s", err, secondErr)
	case <-ctx.Done():
		t.Fatal("errors not closed")
	}
}

// VerifyHasErrors verifies that at least one error was sent over a channel
func VerifyHasErrors(ctx context.Context, t testing.TB, errChan <-chan error) {
	t.Helper()
	errCount := 0
	for {
		select {
		case _, ok := <-errChan:
			if !ok {
				require.NotZero(t, errCount, "should have errors")
				return
			}
			errCount++
		case <-ctx.Done():
			t.Fatal("errors not closed")
		}
	}
}

// VerifyEmptyErrors verifies that no errors were sent over a channel before
// it was closed
func VerifyEmptyErrors(ctx context.Context, t testing.TB, errChan <-chan error) {
	t.Helper()
	for {
		select {
		case _, ok := <-errChan:
			if !ok {
				return
			}
			t.Fatal("errors were sent but shouldn't have been")
		case <-ctx.Done():
			t.Fatal("errors channel never closed")
		}
	}
}

// VerifyEmptyResponse verifies that no response progress happened before the
// channel was closed.
func VerifyEmptyResponse(ctx context.Context, t testing.TB, responseChan <-chan graphsync.ResponseProgress) {
	t.Helper()
	for {
		select {
		case _, ok := <-responseChan:
			if !ok {
				return
			}
			t.Fatal("response was sent but shouldn't have been")
		case <-ctx.Done():
			t.Fatal("response channel never closed")
		}
	}
}

// NewTestLink returns a randomly generated IPLD Link
func NewTestLink() ipld.Link {
	return cidlink.Link{Cid: GenerateCids(1)[0]}
}

type fakeBlkData struct {
	link  ipld.Link
	size  uint64
	index int64
}

func (fbd fakeBlkData) Link() ipld.Link {
	return fbd.link
}

func (fbd fakeBlkData) BlockSize() uint64 {
	return fbd.size
}

func (fbd fakeBlkData) BlockSizeOnWire() uint64 {
	return fbd.size
}

func (fbd fakeBlkData) Index() int64 {
	return fbd.index
}

// NewFakeBlockData returns a fake block that matches the block data interface
func NewFakeBlockData() graphsync.BlockData {
	return &fakeBlkData{
		link:  cidlink.Link{Cid: GenerateCids(1)[0]},
		size:  rand.Uint64(),
		index: rand.Int63(),
	}
}
