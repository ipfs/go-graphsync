package ipldbridge

import (
	"context"
	"errors"

	"github.com/ipld/go-ipld-prime/fluent"

	ipld "github.com/ipld/go-ipld-prime"
	ipldtraversal "github.com/ipld/go-ipld-prime/traversal"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
)

var errDoNotFollow = errors.New("Dont Follow Me")

// ErrDoNotFollow is just a wrapper for whatever IPLD's ErrDoNotFollow ends up looking like
func ErrDoNotFollow() error {
	return errDoNotFollow
}

// Loader is an alias from ipld, in case it's renamed/moved.
type Loader = ipld.Loader

// Storer is an alias from ipld, in case it's renamed/moved.
type Storer = ipld.Storer

// StoreCommitter is an alias from ipld, in case it's renamed/moved.
type StoreCommitter = ipld.StoreCommitter

// AdvVisitFn is an alias from ipld, in case it's renamed/moved.
type AdvVisitFn = ipldtraversal.AdvVisitFn

// Selector is an alias from ipld, in case it's renamed/moved.
type Selector = ipldselector.Selector

// SelectorSpec is alias from ipld, in case it's renamed/moved.
type SelectorSpec = ipldselector.SelectorSpec

// SelectorSpecBuilder is alias from ipld, in case it's renamed/moved.
type SelectorSpecBuilder = ipldselector.SelectorSpecBuilder

// ExploreFieldsSpecBuilder is alias from ipld, in case it's renamed/moved.
type ExploreFieldsSpecBuilder = ipldselector.ExploreFieldsSpecBuilder

// LinkContext is an alias from ipld, in case it's renamed/moved.
type LinkContext = ipld.LinkContext

// TraversalProgress is an alias from ipld, in case it's renamed/moved.
type TraversalProgress = ipldtraversal.TraversalProgress

// TraversalReason is an alias from ipld, in case it's renamed/moved.
type TraversalReason = ipldtraversal.TraversalReason

// NodeBuilder is an alias from the ipld fluent nodebuilder, in case it's moved
type NodeBuilder = fluent.NodeBuilder

// ListBuilder is an alias from ipld fluent, in case it's moved
type ListBuilder = fluent.ListBuilder

// MapBuilder is an alias from ipld fluent, in case it's moved
type MapBuilder = fluent.MapBuilder

// SimpleNode is an alias from ipld fluent, to refer to its non error based
// node struct
type SimpleNode = fluent.Node

// IPLDBridge is an interface for making calls to IPLD, which can be
// replaced with alternative implementations
type IPLDBridge interface {

	// ExtractData provides an efficient mechanism for reading nodes w/ fluent
	// interface
	ExtractData(ipld.Node, func(SimpleNode) interface{}) (interface{}, error)

	// BuildNode provides an efficient mechanism for assembling nodes w/ fluent
	// interface
	BuildNode(func(NodeBuilder) ipld.Node) (ipld.Node, error)

	// BuildSelector provides a mechanism to build selector nodes quickly with
	// ipld's SelectorSpecBuilder
	BuildSelector(func(SelectorSpecBuilder) SelectorSpec) (ipld.Node, error)

	// EncodeNode encodes an IPLD Node to bytes for network transfer.
	EncodeNode(ipld.Node) ([]byte, error)

	// DecodeNode decodes bytes crossing a network to an IPLD Node.
	DecodeNode([]byte) (ipld.Node, error)

	// ParseSelector checks if a generic IPLD node is a selector spec,
	// and if so, a go-ipld-prime Selector.
	ParseSelector(selector ipld.Node) (Selector, error)

	// Traverse performs a selector traversal, starting at a given root, using the given selector,
	// and the given link loader. The given visit function will be called for each node
	// visited.
	Traverse(ctx context.Context, loader Loader, root ipld.Link, s Selector, fn AdvVisitFn) error
}
