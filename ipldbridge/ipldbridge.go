package ipldbridge

import (
	"errors"

	"github.com/ipld/go-ipld-prime/fluent"

	ipld "github.com/ipld/go-ipld-prime"
	ipldtraversal "github.com/ipld/go-ipld-prime/traversal"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
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

// VisitFn is an alias from ipld, in case it's renamed/moved
type VisitFn = ipldtraversal.VisitFn

// AdvVisitFn is an alias from ipld, in case it's renamed/moved.
type AdvVisitFn = ipldtraversal.AdvVisitFn

// Selector is an alias from ipld, in case it's renamed/moved.
type Selector = ipldselector.Selector

// SelectorSpec is alias from ipld, in case it's renamed/moved.
type SelectorSpec = selectorbuilder.SelectorSpec

// SelectorSpecBuilder is alias from ipld, in case it's renamed/moved.
type SelectorSpecBuilder = selectorbuilder.SelectorSpecBuilder

// ExploreFieldsSpecBuilder is alias from ipld, in case it's renamed/moved.
type ExploreFieldsSpecBuilder = selectorbuilder.ExploreFieldsSpecBuilder

// LinkContext is an alias from ipld, in case it's renamed/moved.
type LinkContext = ipld.LinkContext

// TraversalProgress is an alias from ipld, in case it's renamed/moved.
type TraversalProgress = ipldtraversal.Progress

// TraversalReason is an alias from ipld, in case it's renamed/moved.
type TraversalReason = ipldtraversal.VisitReason

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
}
