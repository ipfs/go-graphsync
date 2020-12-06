package testutil

import (
	ipld "github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// NewUnparsableSelectorSpec returns a spec that will fail when you attempt to
// validate it or decompose to a node + selector.
func NewUnparsableSelectorSpec() ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.ExploreRecursiveEdge().Node()
}

// NewInvalidSelectorSpec returns a spec that will fail when you attempt to
// validate it on the responder side
func NewInvalidSelectorSpec() ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()
}
