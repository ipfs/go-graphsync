package testbridge

import (
	"fmt"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
)

type mockSelectorSpec struct {
	cidsVisited    []cid.Cid
	failValidation bool
	failEncode     bool
}

// NewMockSelectorSpec returns a new mock selector that will visit the given
// cids.
func NewMockSelectorSpec(cidsVisited []cid.Cid) ipld.Node {
	return &mockSelectorSpec{cidsVisited, false, false}
}

// NewInvalidSelectorSpec returns a spec that will fail when you attempt to
// validate it or decompose to a node + selector.
func NewInvalidSelectorSpec(cidsVisited []cid.Cid) ipld.Node {
	return &mockSelectorSpec{cidsVisited, true, false}
}

// NewUnencodableSelectorSpec returns a spec that will fail when you attempt to
// encode it.
func NewUnencodableSelectorSpec(cidsVisited []cid.Cid) ipld.Node {
	return &mockSelectorSpec{cidsVisited, false, true}
}

func (mss *mockSelectorSpec) ReprKind() ipld.ReprKind { return ipld.ReprKind_Null }
func (mss *mockSelectorSpec) TraverseField(key string) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}
func (mss *mockSelectorSpec) TraverseIndex(idx int) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}
func (mss *mockSelectorSpec) ListIterator() ipld.ListIterator { return nil }
func (mss *mockSelectorSpec) MapIterator() ipld.MapIterator   { return nil }

func (mss *mockSelectorSpec) Length() int                   { return 0 }
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
func (mbn *mockBlockNode) TraverseField(key string) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}
func (mbn *mockBlockNode) TraverseIndex(idx int) (ipld.Node, error) {
	return nil, fmt.Errorf("404")
}

func (mbn *mockBlockNode) ListIterator() ipld.ListIterator { return nil }
func (mbn *mockBlockNode) MapIterator() ipld.MapIterator   { return nil }

func (mbn *mockBlockNode) Length() int                   { return 0 }
func (mbn *mockBlockNode) IsNull() bool                  { return false }
func (mbn *mockBlockNode) AsBool() (bool, error)         { return false, fmt.Errorf("404") }
func (mbn *mockBlockNode) AsInt() (int, error)           { return 0, fmt.Errorf("404") }
func (mbn *mockBlockNode) AsFloat() (float64, error)     { return 0.0, fmt.Errorf("404") }
func (mbn *mockBlockNode) AsString() (string, error)     { return "", fmt.Errorf("404") }
func (mbn *mockBlockNode) AsBytes() ([]byte, error)      { return mbn.data, nil }
func (mbn *mockBlockNode) AsLink() (ipld.Link, error)    { return nil, fmt.Errorf("404") }
func (mbn *mockBlockNode) NodeBuilder() ipld.NodeBuilder { return &mockBuilder{} }
