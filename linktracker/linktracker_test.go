package linktracker

import (
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/testutil"
)

type request struct {
	traversals []bool
	finished   bool
}

func TestBlockRefCount(t *testing.T) {
	testCases := map[string]struct {
		requests         []request
		expectedRefCount int
	}{
		"not traversed": {
			requests:         []request{},
			expectedRefCount: 0,
		},
		"traversed once, block present": {
			requests:         []request{{traversals: []bool{true}, finished: false}},
			expectedRefCount: 1,
		},
		"traversed once, block missing": {
			requests:         []request{{traversals: []bool{false}, finished: false}},
			expectedRefCount: 0,
		},
		"traversed twice, different requests": {
			requests: []request{
				{traversals: []bool{true}, finished: false},
				{traversals: []bool{true}, finished: false},
			},
			expectedRefCount: 2,
		},
		"traversed twice, same request": {
			requests:         []request{{traversals: []bool{true, true}, finished: false}},
			expectedRefCount: 2,
		},
		"traversed twice, same request, block available after missing": {
			requests:         []request{{traversals: []bool{false, true}, finished: false}},
			expectedRefCount: 1,
		},
		"traversed once, block present, request finished": {
			requests:         []request{{traversals: []bool{true}, finished: true}},
			expectedRefCount: 0,
		},
		"traversed twice, different requests, one request finished": {
			requests: []request{
				{traversals: []bool{true}, finished: true},
				{traversals: []bool{true}, finished: false},
			},
			expectedRefCount: 1,
		},
		"traversed twice, same request, request finished": {
			requests:         []request{{traversals: []bool{true, true}, finished: true}},
			expectedRefCount: 0,
		},
		"traversed twice, same request, block available after missing, request finished": {
			requests:         []request{{traversals: []bool{false, true}, finished: true}},
			expectedRefCount: 0,
		},
	}

	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			linkTracker := New()
			link := testutil.NewTestLink()
			for _, rq := range data.requests {
				requestID := graphsync.NewRequestID()
				for _, present := range rq.traversals {
					linkTracker.RecordLinkTraversal(requestID, link, present)
				}
				if rq.finished {
					linkTracker.FinishRequest(requestID)
				}
			}
			require.Equal(t, data.expectedRefCount, linkTracker.BlockRefCount(link))
		})
	}
}

type linkTraversed struct {
	link         ipld.Link
	blockPresent bool
}

func TestFinishRequest(t *testing.T) {
	link1 := testutil.NewTestLink()
	link2 := testutil.NewTestLink()
	testCases := map[string]struct {
		linksTraversed   []linkTraversed
		allBlocksPresent bool
	}{
		"when links with blocks that are missing are traversed": {
			linksTraversed:   []linkTraversed{{link1, true}, {link2, false}},
			allBlocksPresent: false,
		},
		"when all blocks present": {
			linksTraversed:   []linkTraversed{{link1, true}},
			allBlocksPresent: true,
		},
		"when block becomes availabler after being missing": {
			linksTraversed:   []linkTraversed{{link1, false}, {link1, true}},
			allBlocksPresent: false,
		},
	}

	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			linkTracker := New()
			requestID := graphsync.NewRequestID()
			for _, lt := range data.linksTraversed {
				linkTracker.RecordLinkTraversal(requestID, lt.link, lt.blockPresent)
			}
			require.Equal(t, data.allBlocksPresent, linkTracker.FinishRequest(requestID))
		})
	}
}

func TestIsKnownMissingLink(t *testing.T) {
	testCases := map[string]struct {
		traversals         []bool
		isKnownMissingLink bool
	}{
		"no traversals": {
			isKnownMissingLink: false,
		},
		"traversed once, block present": {
			traversals:         []bool{true},
			isKnownMissingLink: false,
		},
		"traversed once, block missing": {
			traversals:         []bool{false},
			isKnownMissingLink: true,
		},
		"traversed twice, missing then found": {
			traversals:         []bool{false, true},
			isKnownMissingLink: true,
		},
	}

	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			linkTracker := New()
			link := testutil.NewTestLink()
			requestID := graphsync.NewRequestID()
			for _, present := range data.traversals {
				linkTracker.RecordLinkTraversal(requestID, link, present)
			}
			require.Equal(t, data.isKnownMissingLink, linkTracker.IsKnownMissingLink(requestID, link))
		})
	}
}
