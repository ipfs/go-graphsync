package testutil

import (
	"context"
	"io/ioutil"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
)

// TestingT covers the interface methods we need from either *testing.T or
// *testing.B
type TestingT interface {
	Errorf(format string, args ...interface{})
	FailNow()
	Fatal(args ...interface{})
}

const blockChainTraversedNodesPerBlock = 2

// TestBlockChain is a simulated data structure similar to a blockchain
// which graphsync is uniquely suited for
type TestBlockChain struct {
	t                TestingT
	blockChainLength int
	loader           ipld.Loader
	GenisisNode      ipld.Node
	GenisisLink      ipld.Link
	MiddleNodes      []ipld.Node
	MiddleLinks      []ipld.Link
	TipNode          ipld.Node
	TipLink          ipld.Link
}

func createBlock(parents []ipld.Link, size uint64) (ipld.Node, error) {
	blknb := basicnode.Prototype.Map.NewBuilder()
	blknbmnb, err := blknb.BeginMap(2)
	if err != nil {
		return nil, err
	}

	entnb, err := blknbmnb.AssembleEntry("Parents")
	if err != nil {
		return nil, err
	}
	pnblnb, err := entnb.BeginList(len(parents))
	if err != nil {
		return nil, err
	}
	for _, parent := range parents {
		err := pnblnb.AssembleValue().AssignLink(parent)
		if err != nil {
			return nil, err
		}
	}
	err = pnblnb.Finish()
	if err != nil {
		return nil, err
	}

	entnb, err = blknbmnb.AssembleEntry("Messages")
	if err != nil {
		return nil, err
	}
	mnblnb, err := entnb.BeginList(1)
	if err != nil {
		return nil, err
	}
	err = mnblnb.AssembleValue().AssignBytes(RandomBytes(int64(size)))
	if err != nil {
		return nil, err
	}
	err = mnblnb.Finish()
	if err != nil {
		return nil, err
	}

	err = blknbmnb.Finish()
	if err != nil {
		return nil, err
	}
	return blknb.Build(), nil
}

// SetupBlockChain creates a new test block chain with the given height
func SetupBlockChain(
	ctx context.Context,
	t TestingT,
	loader ipld.Loader,
	storer ipld.Storer,
	size uint64,
	blockChainLength int) *TestBlockChain {
	linkBuilder := cidlink.LinkBuilder{Prefix: cid.NewPrefixV1(cid.DagCBOR, mh.SHA2_256)}
	genisisNode, err := createBlock([]ipld.Link{}, size)
	require.NoError(t, err, "Error creating genesis block")
	genesisLink, err := linkBuilder.Build(ctx, ipld.LinkContext{}, genisisNode, storer)
	require.NoError(t, err, "Error creating link to genesis block")
	parent := genesisLink
	middleNodes := make([]ipld.Node, 0, blockChainLength-2)
	middleLinks := make([]ipld.Link, 0, blockChainLength-2)
	for i := 0; i < blockChainLength-2; i++ {
		node, err := createBlock([]ipld.Link{parent}, size)
		require.NoError(t, err, "Error creating middle block")
		middleNodes = append(middleNodes, node)
		link, err := linkBuilder.Build(ctx, ipld.LinkContext{}, node, storer)
		require.NoError(t, err, "Error creating link to middle block")
		middleLinks = append(middleLinks, link)
		parent = link
	}
	tipNode, err := createBlock([]ipld.Link{parent}, size)
	require.NoError(t, err, "Error creating tip block")
	tipLink, err := linkBuilder.Build(ctx, ipld.LinkContext{}, tipNode, storer)
	require.NoError(t, err, "Error creating link to tip block")
	return &TestBlockChain{t, blockChainLength, loader, genisisNode, genesisLink, middleNodes, middleLinks, tipNode, tipLink}
}

// Selector returns the selector to recursive traverse the block chain parent links
func (tbc *TestBlockChain) Selector() ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
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
func (tbc *TestBlockChain) checkResponses(responses []graphsync.ResponseProgress, start int, end int, verifyTypes bool) {
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
		require.Equal(tbc.t, expectedPath, response.Path.String(), "response has correct path")
		if i%2 == 0 {
			// if verifyTypes {
			// 	_, ok := response.Node.(chaintypes.Block)
			// 	require.True(tbc.t, ok, "nodes in response should have correct type")
			// }
			if expectedPath == "" {
				expectedPath = "Parents"
			} else {
				expectedPath = expectedPath + "/Parents"
			}
		} else {
			// if verifyTypes {
			// 	_, ok := response.Node.(chaintypes.Parents)
			// 	require.True(tbc.t, ok, "nodes in response should have correct type")
			// }
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
	tbc.checkResponses(responses, from, tbc.blockChainLength, false)
}

// VerifyResponseRange verifies the given response channel returns the given range of respnses, indexed from the tip
// (with possibly more data left in the channel)
func (tbc *TestBlockChain) VerifyResponseRange(ctx context.Context, responseChan <-chan graphsync.ResponseProgress, from int, to int) {
	responses := ReadNResponses(ctx, tbc.t, responseChan, (to-from)*blockChainTraversedNodesPerBlock)
	tbc.checkResponses(responses, from, to, false)
}

// VerifyWholeChainSync verifies the given set of read responses are the expected responses for the whole chain
func (tbc *TestBlockChain) VerifyWholeChainSync(responses []graphsync.ResponseProgress) {
	tbc.VerifyRemainderSync(responses, 0)
}

// VerifyRemainderSync verifies the given set of read responses are the remainder of the chain starting at the nth block from the tip
func (tbc *TestBlockChain) VerifyRemainderSync(responses []graphsync.ResponseProgress, from int) {
	tbc.checkResponses(responses, from, tbc.blockChainLength, false)
}

// VerifyResponseRangeSync verifies given set of read responses match responses for the given range of the blockchain, indexed from the tip
// (with possibly more data left in the channel)
func (tbc *TestBlockChain) VerifyResponseRangeSync(responses []graphsync.ResponseProgress, from int, to int) {
	tbc.checkResponses(responses, from, to, false)
}

// VerifyWholeChainWithTypes verifies the given response channel returns the expected responses for the whole chain
// and that the types in the response are the expected types for a block chain
func (tbc *TestBlockChain) VerifyWholeChainWithTypes(ctx context.Context, responseChan <-chan graphsync.ResponseProgress) {
	tbc.VerifyRemainderWithTypes(ctx, responseChan, 0)
}

// VerifyRemainderWithTypes verifies the given response channel returns the remainder of the chain starting at the nth block from the tip
// and that the types in the response are the expected types for a block chain
func (tbc *TestBlockChain) VerifyRemainderWithTypes(ctx context.Context, responseChan <-chan graphsync.ResponseProgress, from int) {
	responses := CollectResponses(ctx, tbc.t, responseChan)
	tbc.checkResponses(responses, from, tbc.blockChainLength, true)
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

// Chooser is a NodeBuilderChooser function that always returns the block chain
func (tbc *TestBlockChain) Chooser(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
	return basicnode.Prototype.Any, nil
	//return chaintypes.Block__NodeBuilder(), nil
}
