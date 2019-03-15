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

func (ms *mockSelector) Explore(ipld.Node) (ipld.KeyIterator, ipldbridge.Selector) {
	return nil, ms
}
func (ms *mockSelector) Decide(ipld.Node) bool { return false }
