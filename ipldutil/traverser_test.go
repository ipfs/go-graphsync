package ipldutil

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-ipld-prime"
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

	t.Run("started with shutdown context, then shutdown", func(t *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		testdata := testutil.NewTestIPLDTree()
		ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
		sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()
		traverser := TraversalBuilder{
			Root:     testdata.RootNodeLnk,
			Selector: sel,
		}.Start(cancelledCtx)
		timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		traverser.Shutdown(timeoutCtx)
		require.NoError(t, timeoutCtx.Err())
	})

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
			testdata.MiddleListBlock,
			testdata.LeafAlphaBlock,
			testdata.LeafAlphaBlock,
			testdata.LeafBetaBlock,
			testdata.LeafAlphaBlock,
			testdata.MiddleMapBlock,
			testdata.LeafAlphaBlock,
			testdata.LeafAlphaBlock,
		}, nil)
	})

	t.Run("traverses correctly, blockchain", func(t *testing.T) {
		store := make(map[ipld.Link][]byte)
		persistence := testutil.NewTestStore(store)
		blockChain := testutil.SetupBlockChain(ctx, t, persistence, 100, 10)
		inProgressChan := make(chan graphsync.ResponseProgress)
		done := make(chan struct{})
		traverser := TraversalBuilder{
			Root:       blockChain.TipLink,
			Selector:   blockChain.Selector(),
			Chooser:    blockChain.Chooser,
			LinkSystem: persistence,
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
		checkTraverseSequence(ctx, t, traverser, blockChain.AllBlocks(), nil)
		close(inProgressChan)
		testutil.AssertDoesReceive(ctx, t, done, "should have completed verification but did not")
	})

	t.Run("errors correctly, data with budget", func(t *testing.T) {
		store := make(map[ipld.Link][]byte)
		persistence := testutil.NewTestStore(store)
		blockChain := testutil.SetupBlockChain(ctx, t, persistence, 100, 10)
		traverser := TraversalBuilder{
			Root:       blockChain.TipLink,
			Selector:   blockChain.Selector(),
			Chooser:    blockChain.Chooser,
			LinkSystem: persistence,
			Visitor: func(tp traversal.Progress, node ipld.Node, r traversal.VisitReason) error {
				return nil
			},
			Budget: &traversal.Budget{
				NodeBudget: math.MaxInt64,
				LinkBudget: 6,
			},
		}.Start(ctx)
		var path ipld.Path
		for i := 0; i < 6; i++ {
			path = path.AppendSegment(ipld.PathSegmentOfString("Parents"))
			path = path.AppendSegment(ipld.PathSegmentOfInt(0))
		}
		checkTraverseSequence(ctx, t, traverser, blockChain.Blocks(0, 6), &traversal.ErrBudgetExceeded{BudgetKind: "link", Path: path, Link: blockChain.LinkTipIndex(6)})
	})

	t.Run("errors correctly, with zero budget", func(t *testing.T) {
		store := make(map[ipld.Link][]byte)
		persistence := testutil.NewTestStore(store)
		blockChain := testutil.SetupBlockChain(ctx, t, persistence, 100, 10)
		traverser := TraversalBuilder{
			Root:       blockChain.TipLink,
			Selector:   blockChain.Selector(),
			Chooser:    blockChain.Chooser,
			LinkSystem: persistence,
			Visitor: func(tp traversal.Progress, node ipld.Node, r traversal.VisitReason) error {
				return nil
			},
			Budget: &traversal.Budget{
				NodeBudget: math.MaxInt64,
				LinkBudget: 0,
			},
		}.Start(ctx)
		checkTraverseSequence(ctx, t, traverser, []blocks.Block{}, &traversal.ErrBudgetExceeded{BudgetKind: "link", Link: blockChain.TipLink})
	})
}

func checkTraverseSequence(ctx context.Context, t *testing.T, traverser Traverser, expectedBlks []blocks.Block, finalErr error) {
	t.Helper()
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
	if finalErr == nil {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, finalErr.Error())
	}
}

func Test_IsContextErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "ContextCancelErrorIsMatched",
			err:  ContextCancelError{},
			want: true,
		},
		{
			name: "WrappedContextCancelErrorIsMatched",
			err:  fmt.Errorf("%w", ContextCancelError{}),
			want: true,
		},
		{
			name: "UnwrappedContextCancelErrorWithMatchingStringIsNotMatched",
			err:  fmt.Errorf("%s", ContextCancelError{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsContextCancelErr(tt.err)
			require.Equal(t, tt.want, got, "IsContextCancelErr() = %v, want %v", got, tt.want)
		})
	}
}
