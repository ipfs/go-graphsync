package testbridge

import (
	"context"
	"fmt"

	ipldbridge "github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
)

type mockLink struct {
	index int
}

var nextIndex int

// NewMockLink produces an object that conforms to the ipld Link interface.
func NewMockLink() ipld.Link {
	nextIndex++
	return &mockLink{index: nextIndex}
}
func (ml *mockLink) Load(context.Context, ipldbridge.LinkContext, ipld.NodeBuilder, ipldbridge.Loader) (ipld.Node, error) {
	return nil, fmt.Errorf("Cannot load mock link")
}

func (ml *mockLink) LinkBuilder() ipld.LinkBuilder {
	return nil
}

func (ml *mockLink) String() string { return "" }
