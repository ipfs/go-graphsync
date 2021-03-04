package selectorvalidator

import (
	"testing"

	ipld "github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
)

func TestValidateMaxRecusionDepth(t *testing.T) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Map)

	successBase := ssb.ExploreRecursive(selector.RecursionLimitDepth(80), ssb.ExploreRecursiveEdge())
	failBase := ssb.ExploreRecursive(selector.RecursionLimitDepth(120), ssb.ExploreRecursiveEdge())
	failNoneBase := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreRecursiveEdge())

	verifyOutcomes := func(t *testing.T, success ipld.Node, fail ipld.Node, failNone ipld.Node) {
		err := ValidateMaxRecursionDepth(success, 100)
		require.NoError(t, err, "valid selector should validate")
		err = ValidateMaxRecursionDepth(fail, 100)
		require.Equal(t, ErrInvalidLimit, err, "selector should fail on invalid limit")
		err = ValidateMaxRecursionDepth(failNone, 100)
		require.Equal(t, ErrInvalidLimit, err, "selector should fail on no limit")
	}

	t.Run("ExploreRecursive", func(t *testing.T) {
		success := successBase.Node()
		fail := failBase.Node()
		failNone := failNoneBase.Node()
		verifyOutcomes(t, success, fail, failNone)
	})
	t.Run("ExploreAll", func(t *testing.T) {
		success := ssb.ExploreAll(successBase).Node()
		fail := ssb.ExploreAll(failBase).Node()
		failNone := ssb.ExploreAll(failNoneBase).Node()
		verifyOutcomes(t, success, fail, failNone)
	})
	t.Run("ExploreIndex", func(t *testing.T) {
		success := ssb.ExploreIndex(0, successBase).Node()
		fail := ssb.ExploreIndex(0, failBase).Node()
		failNone := ssb.ExploreIndex(0, failNoneBase).Node()
		verifyOutcomes(t, success, fail, failNone)
	})
	t.Run("ExploreRange", func(t *testing.T) {
		success := ssb.ExploreRange(0, 10, successBase).Node()
		fail := ssb.ExploreRange(0, 10, failBase).Node()
		failNone := ssb.ExploreRange(0, 10, failNoneBase).Node()
		verifyOutcomes(t, success, fail, failNone)
	})
	t.Run("ExploreUnion", func(t *testing.T) {
		success := ssb.ExploreUnion(successBase, successBase).Node()
		fail := ssb.ExploreUnion(successBase, failBase).Node()
		failNone := ssb.ExploreUnion(successBase, failNoneBase).Node()
		verifyOutcomes(t, success, fail, failNone)
	})
	t.Run("ExploreFields", func(t *testing.T) {
		success := ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("apples", successBase)
			efsb.Insert("oranges", successBase)
		}).Node()
		fail := ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("apples", successBase)
			efsb.Insert("oranges", failBase)
		}).Node()
		failNone := ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("apples", successBase)
			efsb.Insert("oranges", failNoneBase)
		}).Node()
		verifyOutcomes(t, success, fail, failNone)
	})
	t.Run("nested ExploreRecursive", func(t *testing.T) {
		success := ssb.ExploreRecursive(
			selector.RecursionLimitDepth(10),
			ssb.ExploreUnion(
				ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
				ssb.ExploreIndex(0, successBase),
			),
		).Node()

		fail := ssb.ExploreRecursive(
			selector.RecursionLimitDepth(10),
			ssb.ExploreUnion(
				ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
				ssb.ExploreIndex(0, failBase),
			),
		).Node()
		failNone := ssb.ExploreRecursive(
			selector.RecursionLimitDepth(10),
			ssb.ExploreUnion(
				ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
				ssb.ExploreIndex(0, failNoneBase),
			),
		).Node()
		verifyOutcomes(t, success, fail, failNone)
	})
}
