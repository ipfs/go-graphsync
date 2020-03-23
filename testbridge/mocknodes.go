package testbridge

import (
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// NewUnparsableSelectorSpec returns a spec that will fail when you attempt to
// validate it or decompose to a node + selector.
func NewUnparsableSelectorSpec(cidsVisited []cid.Cid) ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	return ssb.ExploreRecursiveEdge().Node()
}

// NewInvalidSelectorSpec returns a spec that will fail when you attempt to
// validate it on the responder side
func NewInvalidSelectorSpec() ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	return ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()
}

// NewUnencodableSelectorSpec returns a spec that will fail when you attempt to
// encode it.
func NewUnencodableSelectorSpec(cidsVisited []cid.Cid) ipld.Node {
	return &ipldfree.Node{}
}
