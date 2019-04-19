package ipldbridge

import (
	"context"

	"github.com/ipld/go-ipld-prime/fluent"

	ipld "github.com/ipld/go-ipld-prime"
	ipldtraversal "github.com/ipld/go-ipld-prime/traversal"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
)

// Loader is an alias from ipld, in case it's renamed/moved.
type Loader = ipld.Loader

// AdvVisitFn is an alias from ipld, in case it's renamed/moved.
type AdvVisitFn = ipldtraversal.AdvVisitFn

// Selector is an alias from ipld, in case it's renamed/moved.
type Selector = ipldselector.Selector

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

// IPLDBridge is an interface for making calls to IPLD, which can be
// replaced with alternative implementations
type IPLDBridge interface {
	BuildNode(func(NodeBuilder) ipld.Node) (ipld.Node, error)

	// ValidateSelectorSpec verifies if a node matches the selector spec.
	ValidateSelectorSpec(rootedSelector ipld.Node) []error

	// EncodeNode encodes an IPLD Node to bytes for network transfer.
	EncodeNode(ipld.Node) ([]byte, error)

	// DecodeNode decodes bytes crossing a network to an IPLD Node.
	DecodeNode([]byte) (ipld.Node, error)

	// DecodeSelectorSpec checks if a generic IPLD node is a selector spec,
	// and if so, converts it to a root node and a go-ipld-prime Selector.
	DecodeSelectorSpec(rootedSelector ipld.Node) (ipld.Node, Selector, error)

	// Traverse performs a selector traversal, starting at a given root, using the given selector,
	// and the given link loader. The given visit function will be called for each node
	// visited.
	Traverse(ctx context.Context, loader Loader, root ipld.Node, s Selector, fn AdvVisitFn) error
}
