package testbridge

import (
	"github.com/ipfs/go-cid"
	ipldbridge "github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
)

type mockSelector struct {
	cidsVisited []cid.Cid
}

func newMockSelector(mss *mockSelectorSpec) ipldbridge.Selector {
	return &mockSelector{mss.cidsVisited}
}

func (ms *mockSelector) Explore(ipld.Node) (ipld.MapIterator, ipld.ListIterator, ipldbridge.Selector) {
	return nil, nil, ms
}

func (ms *mockSelector) Decide(ipld.Node) bool { return false }
