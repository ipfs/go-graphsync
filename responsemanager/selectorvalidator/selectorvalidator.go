package selectorvalidator

import (
	"errors"

	"github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

var (
	// ErrInvalidLimit means this type of recursive selector limit is not supported by default
	// -- to prevent DDOS attacks
	ErrInvalidLimit = errors.New("unsupported recursive selector limit")
)

// ValidateSelector applies the default selector validation policy to a selector
// on an incoming request -- which by default is to limit recursive selectors
// to a fixed depth
func ValidateSelector(bridge ipldbridge.IPLDBridge, node ipld.Node, maxAcceptedDepth int) error {
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())

	// this selector is a selector for traversing selectors...
	// it traverses the various selector types looking for recursion limit fields
	// and matches them
	s, err := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		efsb.Insert(selector.SelectorKey_ExploreRecursive, ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert(selector.SelectorKey_Limit, ssb.Matcher())
			efsb.Insert(selector.SelectorKey_Sequence, ssb.ExploreRecursiveEdge())
		}))
		efsb.Insert(selector.SelectorKey_ExploreFields, ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert(selector.SelectorKey_Fields, ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
		}))
		efsb.Insert(selector.SelectorKey_ExploreUnion, ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
		efsb.Insert(selector.SelectorKey_ExploreAll, ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert(selector.SelectorKey_Next, ssb.ExploreRecursiveEdge())
		}))
		efsb.Insert(selector.SelectorKey_ExploreIndex, ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert(selector.SelectorKey_Next, ssb.ExploreRecursiveEdge())
		}))
		efsb.Insert(selector.SelectorKey_ExploreRange, ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert(selector.SelectorKey_Next, ssb.ExploreRecursiveEdge())
		}))
		efsb.Insert(selector.SelectorKey_ExploreConditional, ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert(selector.SelectorKey_Next, ssb.ExploreRecursiveEdge())
		}))
	})).Selector()

	if err != nil {
		return err
	}

	return bridge.WalkMatching(node, s, func(progress traversal.Progress, visited ipld.Node) error {
		if visited.ReprKind() != ipld.ReprKind_Map || visited.Length() != 1 {
			return ErrInvalidLimit
		}
		kn, v, _ := visited.MapIterator().Next()
		kstr, _ := kn.AsString()
		switch kstr {
		case selector.SelectorKey_LimitDepth:
			maxDepthValue, err := v.AsInt()
			if err != nil {
				return ErrInvalidLimit
			}
			if maxDepthValue > maxAcceptedDepth {
				return ErrInvalidLimit
			}
			return nil
		case selector.SelectorKey_LimitNone:
			return ErrInvalidLimit
		default:
			return ErrInvalidLimit
		}
	})
}
