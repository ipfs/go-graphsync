package testutil

import (
	"context"
	"io/ioutil"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const blockChainTraversedNodesPerBlock = 2

// TestBlockChain is a simulated data structure similar to a blockchain
// which graphsync is uniquely suited for
type TestBlockChain struct {
	t                *testing.T
	blockChainLength int
	loader           ipld.Loader
	GenisisNode      ipld.Node
	GenisisLink      ipld.Link
	MiddleNodes      []ipld.Node
	MiddleLinks      []ipld.Link
	TipNode          ipld.Node
	TipLink          ipld.Link
}

func createBlock(nb fluent.NodeBuilder, parents []ipld.Link, size uint64) ipld.Node {
	return nb.CreateMap(func(mb fluent.MapBuilder, knb fluent.NodeBuilder, vnb fluent.NodeBuilder) {
		mb.Insert(knb.CreateString("Parents"), vnb.CreateList(func(lb fluent.ListBuilder, vnb fluent.NodeBuilder) {
			for _, parent := range parents {
				lb.Append(vnb.CreateLink(parent))
			}
		}))
		mb.Insert(knb.CreateString("Messages"), vnb.CreateList(func(lb fluent.ListBuilder, vnb fluent.NodeBuilder) {
			lb.Append(vnb.CreateBytes(RandomBytes(int64(size))))
		}))
	})
}

// SetupBlockChain creates a new test block chain with the given height
func SetupBlockChain(
	ctx context.Context,
	t *testing.T,
	loader ipld.Loader,
	storer ipld.Storer,
	size uint64,
	blockChainLength int) *TestBlockChain {
	linkBuilder := cidlink.LinkBuilder{Prefix: cid.NewPrefixV1(cid.DagCBOR, mh.SHA2_256)}
	var genisisNode ipld.Node
	err := fluent.Recover(func() {
		nb := fluent.WrapNodeBuilder(ipldfree.NodeBuilder())
		genisisNode = createBlock(nb, []ipld.Link{}, size)
	})
	require.NoError(t, err, "Error creating genesis block")
	genesisLink, err := linkBuilder.Build(ctx, ipld.LinkContext{}, genisisNode, storer)
	require.NoError(t, err, "Error creating link to genesis block")
	parent := genesisLink
	middleNodes := make([]ipld.Node, 0, blockChainLength-2)
	middleLinks := make([]ipld.Link, 0, blockChainLength-2)
	for i := 0; i < blockChainLength-2; i++ {
		var node ipld.Node
		err := fluent.Recover(func() {
			nb := fluent.WrapNodeBuilder(ipldfree.NodeBuilder())
			node = createBlock(nb, []ipld.Link{parent}, size)
		})
		require.NoError(t, err, "Error creating middle block")
		middleNodes = append(middleNodes, node)
		link, err := linkBuilder.Build(ctx, ipld.LinkContext{}, node, storer)
		require.NoError(t, err, "Error creating link to middle block")
		middleLinks = append(middleLinks, link)
		parent = link
	}
	var tipNode ipld.Node
	err = fluent.Recover(func() {
		nb := fluent.WrapNodeBuilder(ipldfree.NodeBuilder())
		tipNode = createBlock(nb, []ipld.Link{parent}, size)
	})
	require.NoError(t, err, "Error creating tip block")
	tipLink, err := linkBuilder.Build(ctx, ipld.LinkContext{}, tipNode, storer)
	require.NoError(t, err, "Error creating link to tip block")
	return &TestBlockChain{t, blockChainLength, loader, genisisNode, genesisLink, middleNodes, middleLinks, tipNode, tipLink}
}

// Selector returns the selector to recursive traverse the block chain parent links
func (tbc *TestBlockChain) Selector() ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	return ssb.ExploreRecursive(selector.RecursionLimitDepth(tbc.blockChainLength),
		ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Parents", ssb.ExploreAll(
				ssb.ExploreRecursiveEdge()))
		})).Node()
}

// LinkTipIndex returns a link to the block at the given index from the tip
func (tbc *TestBlockChain) LinkTipIndex(fromTip int) ipld.Link {
	switch height := tbc.blockChainLength - 1 - fromTip; {
	case height == 0:
		return tbc.GenisisLink
	case height == tbc.blockChainLength-1:
		return tbc.TipLink
	default:
		return tbc.MiddleLinks[height-1]
	}
}

// NodeTipIndex returns the node to the block at the given index from the tip
func (tbc *TestBlockChain) NodeTipIndex(fromTip int) ipld.Node {
	switch height := tbc.blockChainLength - 1 - fromTip; {
	case height == 0:
		return tbc.GenisisNode
	case height == tbc.blockChainLength-1:
		return tbc.TipNode
	default:
		return tbc.MiddleNodes[height-1]
	}
}
func (tbc *TestBlockChain) checkResponses(responses []graphsync.ResponseProgress, start int, end int) {
	require.Len(tbc.t, responses, (end-start)*blockChainTraversedNodesPerBlock, "traverses all nodes")
	expectedPath := ""
	for i := 0; i < start; i++ {
		if expectedPath == "" {
			expectedPath = "Parents/0"
		} else {
			expectedPath = expectedPath + "/Parents/0"
		}
	}
	for i, response := range responses {
		require.Equal(tbc.t, response.Path.String(), expectedPath, "response has correct path")
		if i%2 == 0 {
			if expectedPath == "" {
				expectedPath = "Parents"
			} else {
				expectedPath = expectedPath + "/Parents"
			}
		} else {
			expectedPath = expectedPath + "/0"
		}
		if response.LastBlock.Path.String() != response.Path.String() {
			continue
		}
		if response.LastBlock.Link == nil {
			continue
		}
		expectedLink := tbc.LinkTipIndex((i / 2) + start)
		require.Equal(tbc.t, expectedLink, response.LastBlock.Link, "response has correct link")
	}
}

// VerifyWholeChain verifies the given response channel returns the expected responses for the whole chain
func (tbc *TestBlockChain) VerifyWholeChain(ctx context.Context, responseChan <-chan graphsync.ResponseProgress) {
	tbc.VerifyRemainder(ctx, responseChan, 0)
}

// VerifyRemainder verifies the given response channel returns the remainder of the chain starting at the nth block from the tip
func (tbc *TestBlockChain) VerifyRemainder(ctx context.Context, responseChan <-chan graphsync.ResponseProgress, from int) {
	responses := CollectResponses(ctx, tbc.t, responseChan)
	tbc.checkResponses(responses, from, tbc.blockChainLength)
}

// VerifyResponseRange verifies the given response channel returns the given range of respnses, indexed from the tip
// (with possibly more data left in the channel)
func (tbc *TestBlockChain) VerifyResponseRange(ctx context.Context, responseChan <-chan graphsync.ResponseProgress, from int, to int) {
	responses := ReadNResponses(ctx, tbc.t, responseChan, (to-from)*blockChainTraversedNodesPerBlock)
	tbc.checkResponses(responses, from, to)
}

// Blocks Returns the given raw blocks for the block chain for the given range, indexed from the tip
func (tbc *TestBlockChain) Blocks(from int, to int) []blocks.Block {
	var blks []blocks.Block
	for i := from; i < to; i++ {
		link := tbc.LinkTipIndex(i)
		reader, err := tbc.loader(link, ipld.LinkContext{})
		require.NoError(tbc.t, err)
		data, err := ioutil.ReadAll(reader)
		require.NoError(tbc.t, err)
		blk, err := blocks.NewBlockWithCid(data, link.(cidlink.Link).Cid)
		require.NoError(tbc.t, err)
		blks = append(blks, blk)
	}
	return blks
}

// AllBlocks returns all blocks for a blockchain
func (tbc *TestBlockChain) AllBlocks() []blocks.Block {
	return tbc.Blocks(0, tbc.blockChainLength)
}

// RemainderBlocks returns the remaining blocks for a blockchain, indexed from tip
func (tbc *TestBlockChain) RemainderBlocks(from int) []blocks.Block {
	return tbc.Blocks(from, tbc.blockChainLength)
}
