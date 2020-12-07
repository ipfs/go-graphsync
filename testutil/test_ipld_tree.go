package testutil

import (
	"bytes"
	"context"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"

	// to register multicodec
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

// TestIPLDTree is a set of IPLD Data that forms a tree spread across some blocks
// with a serialized in memory representation
type TestIPLDTree struct {
	Storage           map[ipld.Link][]byte
	LeafAlpha         ipld.Node
	LeafAlphaLnk      ipld.Link
	LeafAlphaBlock    blocks.Block
	LeafBeta          ipld.Node
	LeafBetaLnk       ipld.Link
	LeafBetaBlock     blocks.Block
	MiddleMapNode     ipld.Node
	MiddleMapNodeLnk  ipld.Link
	MiddleMapBlock    blocks.Block
	MiddleListNode    ipld.Node
	MiddleListNodeLnk ipld.Link
	MiddleListBlock   blocks.Block
	RootNode          ipld.Node
	RootNodeLnk       ipld.Link
	RootBlock         blocks.Block
}

// NewTestIPLDTree returns a fake tree of nodes, spread across 5 blocks
func NewTestIPLDTree() TestIPLDTree {
	var storage = make(map[ipld.Link][]byte)
	encode := func(n ipld.Node) (ipld.Node, ipld.Link) {
		lb := cidlink.LinkBuilder{Prefix: cid.Prefix{
			Version:  1,
			Codec:    0x0129,
			MhType:   0x17,
			MhLength: 4,
		}}
		lnk, err := lb.Build(context.Background(), ipld.LinkContext{}, n,
			func(ipld.LinkContext) (io.Writer, ipld.StoreCommitter, error) {
				buf := bytes.Buffer{}
				return &buf, func(lnk ipld.Link) error {
					storage[lnk] = buf.Bytes()
					return nil
				}, nil
			},
		)
		if err != nil {
			panic(err)
		}
		return n, lnk
	}

	var (
		leafAlpha, leafAlphaLnk         = encode(basicnode.NewString("alpha"))
		leafAlphaBlock, _               = blocks.NewBlockWithCid(storage[leafAlphaLnk], leafAlphaLnk.(cidlink.Link).Cid)
		leafBeta, leafBetaLnk           = encode(basicnode.NewString("beta"))
		leafBetaBlock, _                = blocks.NewBlockWithCid(storage[leafBetaLnk], leafBetaLnk.(cidlink.Link).Cid)
		middleMapNode, middleMapNodeLnk = encode(fluent.MustBuildMap(basicnode.Prototype.Map, 3, func(na fluent.MapAssembler) {
			na.AssembleEntry("foo").AssignBool(true)
			na.AssembleEntry("bar").AssignBool(false)
			na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
				na.AssembleEntry("alink").AssignLink(leafAlphaLnk)
				na.AssembleEntry("nonlink").AssignString("zoo")
			})
		}))
		middleMapBlock, _                 = blocks.NewBlockWithCid(storage[middleMapNodeLnk], middleMapNodeLnk.(cidlink.Link).Cid)
		middleListNode, middleListNodeLnk = encode(fluent.MustBuildList(basicnode.Prototype.List, 4, func(na fluent.ListAssembler) {
			na.AssembleValue().AssignLink(leafAlphaLnk)
			na.AssembleValue().AssignLink(leafAlphaLnk)
			na.AssembleValue().AssignLink(leafBetaLnk)
			na.AssembleValue().AssignLink(leafAlphaLnk)
		}))
		middleListBlock, _    = blocks.NewBlockWithCid(storage[middleListNodeLnk], middleListNodeLnk.(cidlink.Link).Cid)
		rootNode, rootNodeLnk = encode(fluent.MustBuildMap(basicnode.Prototype.Map, 4, func(na fluent.MapAssembler) {
			na.AssembleEntry("plain").AssignString("olde string")
			na.AssembleEntry("linkedString").AssignLink(leafAlphaLnk)
			na.AssembleEntry("linkedMap").AssignLink(middleMapNodeLnk)
			na.AssembleEntry("linkedList").AssignLink(middleListNodeLnk)
		}))
		rootBlock, _ = blocks.NewBlockWithCid(storage[rootNodeLnk], rootNodeLnk.(cidlink.Link).Cid)
	)
	return TestIPLDTree{
		Storage:           storage,
		LeafAlpha:         leafAlpha,
		LeafAlphaLnk:      leafAlphaLnk,
		LeafAlphaBlock:    leafAlphaBlock,
		LeafBeta:          leafBeta,
		LeafBetaLnk:       leafBetaLnk,
		LeafBetaBlock:     leafBetaBlock,
		MiddleMapNode:     middleMapNode,
		MiddleMapNodeLnk:  middleMapNodeLnk,
		MiddleMapBlock:    middleMapBlock,
		MiddleListNode:    middleListNode,
		MiddleListNodeLnk: middleListNodeLnk,
		MiddleListBlock:   middleListBlock,
		RootNode:          rootNode,
		RootNodeLnk:       rootNodeLnk,
		RootBlock:         rootBlock,
	}
}
