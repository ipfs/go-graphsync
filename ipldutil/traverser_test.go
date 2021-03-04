package ipldutil

import (
	"bytes"
	"context"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestTraverser(t *testing.T) {
	ctx := context.Background()

	t.Run("traverses correctly, simple struct", func(t *testing.T) {
		testdata := testutil.NewTestIPLDTree()
		ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
		sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()
		traverser := TraversalBuilder{
			Root:     testdata.RootNodeLnk,
			Selector: sel,
		}.Start(ctx)
		checkTraverseSequence(ctx, t, traverser, []blocks.Block{
			testdata.RootBlock,
			testdata.LeafAlphaBlock,
			testdata.MiddleMapBlock,
			testdata.LeafAlphaBlock,
			testdata.MiddleListBlock,
			testdata.LeafAlphaBlock,
			testdata.LeafAlphaBlock,
			testdata.LeafBetaBlock,
			testdata.LeafAlphaBlock,
		})
	})

	t.Run("traverses correctly, blockchain", func(t *testing.T) {
		store := make(map[ipld.Link][]byte)
		loader, storer := testutil.NewTestStore(store)
		blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 10)
		inProgressChan := make(chan graphsync.ResponseProgress)
		done := make(chan struct{})
		traverser := TraversalBuilder{
			Root:     blockChain.TipLink,
			Selector: blockChain.Selector(),
			Chooser:  blockChain.Chooser,
			Visitor: func(tp traversal.Progress, node ipld.Node, r traversal.VisitReason) error {
				select {
				case <-ctx.Done():
				case inProgressChan <- graphsync.ResponseProgress{
					Node:      node,
					Path:      tp.Path,
					LastBlock: tp.LastBlock,
				}:
				}
				return nil
			},
		}.Start(ctx)
		go func() {
			blockChain.VerifyWholeChainWithTypes(ctx, inProgressChan)
			close(done)
		}()
		checkTraverseSequence(ctx, t, traverser, blockChain.AllBlocks())
		close(inProgressChan)
		testutil.AssertDoesReceive(ctx, t, done, "should have completed verification but did not")
	})
}

func checkTraverseSequence(ctx context.Context, t *testing.T, traverser Traverser, expectedBlks []blocks.Block) {
	for _, blk := range expectedBlks {
		isComplete, err := traverser.IsComplete()
		require.False(t, isComplete)
		require.NoError(t, err)
		lnk, _ := traverser.CurrentRequest()
		require.Equal(t, lnk.(cidlink.Link).Cid, blk.Cid())
		err = traverser.Advance(bytes.NewBuffer(blk.RawData()))
		require.NoError(t, err)
	}
	isComplete, err := traverser.IsComplete()
	require.True(t, isComplete)
	require.NoError(t, err)
}
