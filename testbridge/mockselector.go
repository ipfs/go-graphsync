package testbridge

import (
	"github.com/ipfs/go-cid"
	ipldbridge "github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
)

type mockSelector struct {
	cidsVisited []cid.Cid
}

func newMockSelector(mss *mockSelectorSpec) ipldbridge.Selector {
	return &mockSelector{mss.cidsVisited}
}

func (ms *mockSelector) Explore(ipld.Node, ipldselector.PathSegment) ipldbridge.Selector {
	return ms
}

func (ms *mockSelector) Interests() []ipldselector.PathSegment {
	return []ipldselector.PathSegment{}
}

func (ms *mockSelector) Decide(ipld.Node) bool { return false }
