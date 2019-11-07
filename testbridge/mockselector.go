package testbridge

import (
	cid "github.com/ipfs/go-cid"
	ipldbridge "github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
)

type mockSelector struct {
	cidsVisited []cid.Cid
}

func newMockSelector(mss *mockSelectorSpec) ipldbridge.Selector {
	return &mockSelector{mss.cidsVisited}
}

func (ms *mockSelector) Explore(ipld.Node, ipld.PathSegment) ipldbridge.Selector {
	return ms
}

func (ms *mockSelector) Interests() []ipld.PathSegment {
	return []ipld.PathSegment{}
}

func (ms *mockSelector) Decide(ipld.Node) bool { return false }
