package traversalrecord_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"

	graphsync "github.com/filecoin-project/boost-graphsync"
	"github.com/filecoin-project/boost-graphsync/ipldutil"
	"github.com/filecoin-project/boost-graphsync/requestmanager/reconciledloader/traversalrecord"
	"github.com/filecoin-project/boost-graphsync/testutil"
)

func TestTraversalRecord(t *testing.T) {
	testTree := testutil.NewTestIPLDTree()

	traversalRecord := buildTraversalRecord(t, testTree.Storage, testTree.RootNodeLnk)

	expectedAllLinks := []cid.Cid{
		testTree.RootBlock.Cid(),
		testTree.MiddleListBlock.Cid(),
		testTree.LeafAlphaBlock.Cid(),
		testTree.LeafAlphaBlock.Cid(),
		testTree.LeafBetaBlock.Cid(),
		testTree.LeafAlphaBlock.Cid(),
		testTree.MiddleMapBlock.Cid(),
		testTree.LeafAlphaBlock.Cid(),
		testTree.LeafAlphaBlock.Cid(),
	}
	require.Equal(t, expectedAllLinks, traversalRecord.AllLinks())

	expectedListLinks := []cid.Cid{
		testTree.MiddleListBlock.Cid(),
		testTree.LeafAlphaBlock.Cid(),
		testTree.LeafAlphaBlock.Cid(),
		testTree.LeafBetaBlock.Cid(),
		testTree.LeafAlphaBlock.Cid(),
	}

	require.Equal(t, expectedListLinks, traversalRecord.GetLinks(datamodel.ParsePath("linkedList")))

	expectedMapLinks := []cid.Cid{
		testTree.MiddleMapBlock.Cid(),
		testTree.LeafAlphaBlock.Cid(),
	}

	require.Equal(t, expectedMapLinks, traversalRecord.GetLinks(datamodel.ParsePath("linkedMap")))

	require.Empty(t, traversalRecord.GetLinks(datamodel.ParsePath("apples")))
}

type linkStep struct {
	link       cid.Cid
	successful bool
}

func TestVerification(t *testing.T) {
	testTree := testutil.NewTestIPLDTree()
	// add a missing element to the original tree
	delete(testTree.Storage, testTree.LeafBetaLnk)
	traversalRecord := buildTraversalRecord(t, testTree.Storage, testTree.RootNodeLnk)

	testCases := map[string]struct {
		linkSequence  []linkStep
		expectedError error
		expectedPaths []string
	}{
		"normal successful verification": {
			linkSequence: []linkStep{
				{testTree.RootBlock.Cid(), true},
				{testTree.MiddleListBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.LeafBetaBlock.Cid(), false},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.MiddleMapBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
			},
			expectedPaths: []string{
				"",
				"linkedList",
				"linkedList/0",
				"linkedList/1",
				"linkedList/2",
				"linkedList/3",
				"linkedMap",
				"linkedMap/nested/alink",
				"linkedString",
			},
		},
		"successful verification with missing items": {
			linkSequence: []linkStep{
				{testTree.RootBlock.Cid(), true},
				{testTree.MiddleListBlock.Cid(), false},
				{testTree.MiddleMapBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
			},
			expectedPaths: []string{
				"",
				"linkedList",
				"linkedMap",
				"linkedMap/nested/alink",
				"linkedString",
			},
		},
		"mismatched verification": {
			linkSequence: []linkStep{
				{testTree.RootBlock.Cid(), true},
				{testTree.MiddleListBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.LeafBetaBlock.Cid(), false},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.MiddleMapBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
			},
			expectedError: graphsync.RemoteIncorrectResponseError{
				LocalLink:  testTree.LeafAlphaLnk,
				RemoteLink: testTree.LeafBetaLnk,
				Path:       datamodel.NewPath([]datamodel.PathSegment{datamodel.PathSegmentOfString("linkedList"), datamodel.PathSegmentOfInt(1)}),
			},
			expectedPaths: []string{
				"",
				"linkedList",
				"linkedList/0",
				"linkedList/1",
			},
		},
		"additional data on unsuccessful loads": {
			linkSequence: []linkStep{
				{testTree.RootBlock.Cid(), true},
				{testTree.MiddleListBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.LeafAlphaBlock.Cid(), true},
				{testTree.LeafBetaBlock.Cid(), true},
			},
			expectedError: errors.New("verifying against tree with additional data not possible"),
			expectedPaths: []string{
				"",
				"linkedList",
				"linkedList/0",
				"linkedList/1",
				"linkedList/2",
			},
		},
	}

	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			verifier := traversalrecord.NewVerifier(traversalRecord)
			var actualErr error
			var actualPaths []datamodel.Path
			for _, step := range data.linkSequence {
				require.False(t, verifier.Done())
				actualPaths = append(actualPaths, verifier.CurrentPath())
				actualErr = verifier.VerifyNext(step.link, step.successful)
				if actualErr != nil {
					break
				}
			}
			if data.expectedError == nil {
				require.NoError(t, actualErr)
				require.True(t, verifier.Done())
			} else {
				require.EqualError(t, actualErr, data.expectedError.Error())
				require.False(t, verifier.Done())
			}
			require.Equal(t, data.expectedPaths, toPathStrings(actualPaths))
		})
	}
}

func buildTraversalRecord(t *testing.T, storage map[datamodel.Link][]byte, root ipld.Link) *traversalrecord.TraversalRecord {
	ctx := context.Background()
	traversalRecord := traversalrecord.NewTraversalRecord()
	traverser := ipldutil.TraversalBuilder{
		Root:     root,
		Selector: selectorparse.CommonSelector_ExploreAllRecursively,
	}.Start(ctx)
	for {
		isComplete, err := traverser.IsComplete()
		require.NoError(t, err)
		if isComplete {
			break
		}
		lnk, linkCtx := traverser.CurrentRequest()
		data, successful := storage[lnk]
		traversalRecord.RecordNextStep(linkCtx.LinkPath.Segments(), lnk.(cidlink.Link).Cid, successful)
		if successful {
			traverser.Advance(bytes.NewReader(data))
		} else {
			traverser.Error(traversal.SkipMe{})
		}
	}
	return traversalRecord
}

func toPathStrings(paths []datamodel.Path) []string {
	pathStrings := make([]string, 0, len(paths))
	for _, path := range paths {
		pathStrings = append(pathStrings, path.String())
	}
	return pathStrings
}
