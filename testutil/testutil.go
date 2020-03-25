package testutil

import (
	"bytes"
	"context"
	"testing"

	"github.com/ipfs/go-bitswap/testutil"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	util "github.com/ipfs/go-ipfs-util"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"

	random "github.com/jbenet/go-random"
	"github.com/libp2p/go-libp2p-core/peer"
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
		p := peer.ID(peerSeq)
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
func AssertContainsPeer(t *testing.T, peers []peer.ID, p peer.ID) {
	require.True(t, testutil.ContainsPeer(peers, p), "given peer should be in list")
}

// RefuteContainsPeer will fail a test if the peer is in the given peer list
func RefuteContainsPeer(t *testing.T, peers []peer.ID, p peer.ID) {
	require.False(t, testutil.ContainsPeer(peers, p), "given peer should not be in list")
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
func AssertContainsBlock(t *testing.T, blks []blocks.Block, block blocks.Block) {
	require.True(t, testutil.ContainsBlock(blks, block), "given block should be in list")
}

// RefuteContainsBlock will fail a test if the block is in the given block list
func RefuteContainsBlock(t *testing.T, blks []blocks.Block, block blocks.Block) {
	require.False(t, testutil.ContainsBlock(blks, block), "given block should not be in list")
}

// CollectResponses is just a utility to convert a graphsync response progress
// channel into an array.
func CollectResponses(ctx context.Context, t *testing.T, responseChan <-chan graphsync.ResponseProgress) []graphsync.ResponseProgress {
	var collectedBlocks []graphsync.ResponseProgress
	for {
		select {
		case blk, ok := <-responseChan:
			if !ok {
				return collectedBlocks
			}
			collectedBlocks = append(collectedBlocks, blk)
		case <-ctx.Done():
			t.Fatal("response channel never closed")
		}
	}
}

// CollectErrors is just a utility to convert an error channel into an array.
func CollectErrors(ctx context.Context, t *testing.T, errChan <-chan error) []error {
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
func ReadNResponses(ctx context.Context, t *testing.T, responseChan <-chan graphsync.ResponseProgress, count int) []graphsync.ResponseProgress {
	var returnedBlocks []graphsync.ResponseProgress
	for i := 0; i < count; i++ {
		select {
		case blk := <-responseChan:
			returnedBlocks = append(returnedBlocks, blk)
		case <-ctx.Done():
			t.Fatal("Unable to read enough responses")
		}
	}
	return returnedBlocks
}

// VerifySingleTerminalError verifies that exactly one error was sent over a channel
// and then the channel was closed.
func VerifySingleTerminalError(ctx context.Context, t *testing.T, errChan <-chan error) {
	select {
	case err := <-errChan:
		require.NotNil(t, err, "should have sent a erminal error but did not")
	case <-ctx.Done():
		t.Fatal("no errors sent")
	}
	select {
	case _, ok := <-errChan:
		if ok {
			t.Fatal("shouldn't have sent second error but did")
		}
	case <-ctx.Done():
		t.Fatal("errors not closed")
	}
}

// VerifyEmptyErrors verifies that no errors were sent over a channel before
// it was closed
func VerifyEmptyErrors(ctx context.Context, t *testing.T, errChan <-chan error) {
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
func VerifyEmptyResponse(ctx context.Context, t *testing.T, responseChan <-chan graphsync.ResponseProgress) {
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
