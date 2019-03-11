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
	ComposeLinkLoader(actualLoader RawLoader) LinkLoader
	ValidateSelectorSpec(cidRootedSelector ipld.Node) []error
	EncodeNode(ipld.Node) ([]byte, error)
	DecodeNode([]byte) (ipld.Node, error)
	DecodeSelectorSpec(cidRootedSelector ipld.Node) (ipld.Node, Selector, error)
	Traverse(ctx context.Context, loader LinkLoader, root ipld.Node, s Selector, fn AdvVisitFn) error
}
