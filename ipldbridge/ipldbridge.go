package ipldbridge

import (
	"context"

	ipld "github.com/ipld/go-ipld-prime"
	ipldrepose "github.com/ipld/go-ipld-prime/repose"
	ipldtraversal "github.com/ipld/go-ipld-prime/traversal"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
)

// MulticodecDecodeTable is an alias from ipld, in case it's renamed/moved.
type MulticodecDecodeTable = ipldrepose.MulticodecDecodeTable

// NodeBuilderChooser is an an alias from ipld, in case it's renamed/moved.
type NodeBuilderChooser = ipldrepose.NodeBuilderChooser

// LinkLoader is alias from ipld, in case it.s renamed/moved.
type LinkLoader = ipldtraversal.LinkLoader

// RawLoader is an alias from ipld, in case it's renamed/moved.
type RawLoader = ipldrepose.ActualLoader

// AdvVisitFn is an alias from ipld, in case it's renamed/moved.
type AdvVisitFn = ipldtraversal.AdvVisitFn

// Selector is an alias from ipld, in case it's renamed/moved.
type Selector = ipldselector.Selector

// LinkContext is an alias from ipld, in case it's renamed/moved.
type LinkContext = ipldtraversal.LinkContext

// TraversalProgress is an alias from ipld, in case it's renamed/moved.
type TraversalProgress = ipldtraversal.TraversalProgress

// TraversalReason is an alias from ipld, in case it's renamed/moved.
type TraversalReason = ipldtraversal.TraversalReason

// IPLDBridge is an interface for making calls to IPLD, which can be
// replaced with alternative implementations
type IPLDBridge interface {
	// ComposeLinkLoader converts a raw block loader into an IPLD node loader.
	ComposeLinkLoader(actualLoader RawLoader) LinkLoader

	// ValidateSelectorSpec verifies if a node matches the selector spec.
	ValidateSelectorSpec(cidRootedSelector ipld.Node) []error

	// EncodeNode encodes an IPLD Node to bytes for network transfer.
	EncodeNode(ipld.Node) ([]byte, error)

	// DecodeNode decodes bytes crossing a network to an IPLD Node.
	DecodeNode([]byte) (ipld.Node, error)

	// DecodeSelectorSpec checks if a generic IPLD node is a selector spec,
	// and if so, converts it to a root node and a go-ipld-prime Selector.
	DecodeSelectorSpec(cidRootedSelector ipld.Node) (ipld.Node, Selector, error)

	// Traverse performs a selector traversal, starting at a given root, using the given selector,
	// and the given link loader. The given visit function will be called for each node
	// visited.
	Traverse(ctx context.Context, loader LinkLoader, root ipld.Node, s Selector, fn AdvVisitFn) error
}
