package testutil

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	"github.com/stretchr/testify/require"

	// to register multicodec
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/fluent"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// TestPartialTree is a set of IPLD Data that forms a tree spread across some blocks
// with a serialized in memory representation
type TestPartialTree struct {
	MissingLeaf           ipld.Node
	MissingLeafLink       ipld.Link
	HiddenMissingLeaf     ipld.Node
	HiddenMissingLeafLink ipld.Link
	PresentMiddle         ipld.Node
	PresentMiddleLink     ipld.Link
	MissingMiddle         ipld.Node
	MissingMiddleLink     ipld.Link
	PresentRoot           ipld.Node
	PresentRootLink       ipld.Link
}

// NewPartialTree returns a fake tree of nodes, spread over five blocks, then
// removes some of the blocks from the store
func NewPartialTree(t *testing.T, bs blockstore.Blockstore) TestPartialTree {
	lsys := storeutil.LinkSystemForBlockstore(bs)
	encode := func(n ipld.Node) (ipld.Node, ipld.Link) {
		lnk, err := lsys.Store(linking.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.Prefix{
			Version:  1,
			Codec:    0x0129,
			MhType:   0x13,
			MhLength: 4,
		}}, n)
		require.NoError(t, err)
		return n, lnk
	}

	var (
		missingLeaf, missingLeafLink             = encode(basicnode.NewString("alpha"))
		hiddenMissingLeaf, hiddenMissingLeafLink = encode(basicnode.NewString("beta"))
		presentMiddle, presentMiddleLink         = encode(fluent.MustBuildMap(basicnode.Prototype.Map, 3, func(na fluent.MapAssembler) {
			na.AssembleEntry("foo").AssignBool(true)
			na.AssembleEntry("bar").AssignBool(false)
			na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
				na.AssembleEntry("alink").AssignLink(missingLeafLink)
				na.AssembleEntry("nonlink").AssignString("zoo")
			})
		}))
		missingMiddle, missingMiddleLink = encode(fluent.MustBuildList(basicnode.Prototype.List, 4, func(na fluent.ListAssembler) {
			na.AssembleValue().AssignLink(missingLeafLink)
			na.AssembleValue().AssignLink(missingLeafLink)
			na.AssembleValue().AssignLink(hiddenMissingLeafLink)
			na.AssembleValue().AssignLink(missingLeafLink)
		}))
		rootNode, rootNodeLnk = encode(fluent.MustBuildMap(basicnode.Prototype.Map, 4, func(na fluent.MapAssembler) {
			na.AssembleEntry("plain").AssignString("olde string")
			na.AssembleEntry("linkedString").AssignLink(missingLeafLink)
			na.AssembleEntry("linkedMap").AssignLink(presentMiddleLink)
			na.AssembleEntry("linkedList").AssignLink(missingMiddleLink)
		}))
	)

	require.NoError(t, bs.DeleteBlock(context.TODO(), missingLeafLink.(cidlink.Link).Cid))
	require.NoError(t, bs.DeleteBlock(context.TODO(), hiddenMissingLeafLink.(cidlink.Link).Cid))
	require.NoError(t, bs.DeleteBlock(context.TODO(), missingMiddleLink.(cidlink.Link).Cid))

	return TestPartialTree{
		MissingLeaf:           missingLeaf,
		MissingLeafLink:       missingLeafLink,
		HiddenMissingLeaf:     hiddenMissingLeaf,
		HiddenMissingLeafLink: hiddenMissingLeafLink,
		PresentMiddle:         presentMiddle,
		PresentMiddleLink:     presentMiddleLink,
		MissingMiddle:         missingMiddle,
		MissingMiddleLink:     missingMiddleLink,
		PresentRoot:           rootNode,
		PresentRootLink:       rootNodeLnk,
	}
}
