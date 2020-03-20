package testbridge

import (
	"fmt"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

type mockSelectorSpec struct {
	CidsVisited    []cid.Cid
	FalseParse     bool
	FailEncode     bool
	FailValidation bool
}

// NewMockSelectorSpec returns a new mock selector that will visit the given
// cids.
func NewMockSelectorSpec(cidsVisited []cid.Cid) ipld.Node {
	return &mockSelectorSpec{cidsVisited, false, false, false}
}

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

func (mss *mockSelectorSpec) ReprKind() ipld.ReprKind { return ipld.ReprKind_Null }

func (mss *mockSelectorSpec) Lookup(key ipld.Node) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}
func (mss *mockSelectorSpec) LookupString(key string) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}
func (mss *mockSelectorSpec) LookupIndex(idx int) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}
func (mss *mockSelectorSpec) LookupSegment(seg ipld.PathSegment) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}
func (mss *mockSelectorSpec) ListIterator() ipld.ListIterator { return nil }
func (mss *mockSelectorSpec) MapIterator() ipld.MapIterator   { return nil }

func (mss *mockSelectorSpec) Length() int                   { return 0 }
func (mss *mockSelectorSpec) IsUndefined() bool             { return true }
func (mss *mockSelectorSpec) IsNull() bool                  { return true }
func (mss *mockSelectorSpec) AsBool() (bool, error)         { return false, fmt.Errorf("404") }
func (mss *mockSelectorSpec) AsInt() (int, error)           { return 0, fmt.Errorf("404") }
func (mss *mockSelectorSpec) AsFloat() (float64, error)     { return 0.0, fmt.Errorf("404") }
func (mss *mockSelectorSpec) AsString() (string, error)     { return "", fmt.Errorf("404") }
func (mss *mockSelectorSpec) AsBytes() ([]byte, error)      { return nil, fmt.Errorf("404") }
func (mss *mockSelectorSpec) AsLink() (ipld.Link, error)    { return nil, fmt.Errorf("404") }
func (mss *mockSelectorSpec) NodeBuilder() ipld.NodeBuilder { return &mockBuilder{} }

type mockBlockNode struct {
	data []byte
}

// NewMockBlockNode returns a new node for a byte array
func NewMockBlockNode(data []byte) ipld.Node {
	return &mockBlockNode{data}
}

func (mbn *mockBlockNode) ReprKind() ipld.ReprKind { return ipld.ReprKind_Bytes }
func (mbn *mockBlockNode) Lookup(key ipld.Node) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}
func (mbn *mockBlockNode) LookupString(key string) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}
func (mbn *mockBlockNode) LookupIndex(idx int) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}
func (mbn *mockBlockNode) LookupSegment(seg ipld.PathSegment) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}

func (mbn *mockBlockNode) ListIterator() ipld.ListIterator { return nil }
func (mbn *mockBlockNode) MapIterator() ipld.MapIterator   { return nil }

func (mbn *mockBlockNode) Length() int                   { return 0 }
func (mbn *mockBlockNode) IsUndefined() bool             { return false }
func (mbn *mockBlockNode) IsNull() bool                  { return false }
func (mbn *mockBlockNode) AsBool() (bool, error)         { return false, fmt.Errorf("404") }
func (mbn *mockBlockNode) AsInt() (int, error)           { return 0, fmt.Errorf("404") }
func (mbn *mockBlockNode) AsFloat() (float64, error)     { return 0.0, fmt.Errorf("404") }
func (mbn *mockBlockNode) AsString() (string, error)     { return "", fmt.Errorf("404") }
func (mbn *mockBlockNode) AsBytes() ([]byte, error)      { return mbn.data, nil }
func (mbn *mockBlockNode) AsLink() (ipld.Link, error)    { return nil, fmt.Errorf("404") }
func (mbn *mockBlockNode) NodeBuilder() ipld.NodeBuilder { return &mockBuilder{} }
