package allocator_test

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/allocator"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestAllocator(t *testing.T) {
	peers := testutil.GeneratePeers(3)
	testCases := map[string]struct {
		total      uint64
		maxPerPeer uint64
		steps      []allocStep
	}{
		"single peer against total": {
			total:      1000,
			maxPerPeer: 1000,
			steps: []allocStep{
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 300},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 600},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 900},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 900},
					expectedPending: []pendingResult{{peers[0], 300}},
				},
				{
					op:              releaseBlock{peers[0], 400},
					totals:          map[peer.ID]uint64{peers[0]: 800},
					expectedPending: []pendingResult{},
				},
			},
		},
		"single peer against self limit": {
			total:      2000,
			maxPerPeer: 1000,
			steps: []allocStep{
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 300},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 600},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 900},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 900},
					expectedPending: []pendingResult{{peers[0], 300}},
				},
				{
					op:              releaseBlock{peers[0], 400},
					totals:          map[peer.ID]uint64{peers[0]: 800},
					expectedPending: []pendingResult{},
				},
			},
		},
		"multiple peers against total": {
			total:      2000,
			maxPerPeer: 2000,
			steps: []allocStep{
				{
					op:              alloc{peers[0], 1000},
					totals:          map[peer.ID]uint64{peers[0]: 1000},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[1], 900},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 900},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[1], 400},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 900},
					expectedPending: []pendingResult{{peers[1], 400}},
				},
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 900},
					expectedPending: []pendingResult{{peers[1], 400}, {peers[0], 300}},
				},
				{
					op:              releaseBlock{peers[0], 500},
					totals:          map[peer.ID]uint64{peers[0]: 500, peers[1]: 1300},
					expectedPending: []pendingResult{{peers[0], 300}},
				},
				{
					op:              releaseBlock{peers[1], 500},
					totals:          map[peer.ID]uint64{peers[0]: 800, peers[1]: 800},
					expectedPending: []pendingResult{},
				},
			},
		},
		"multiple peers against self limit": {
			total:      5000,
			maxPerPeer: 1000,
			steps: []allocStep{
				{
					op:              alloc{peers[0], 1000},
					totals:          map[peer.ID]uint64{peers[0]: 1000},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[1], 900},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 900},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[1], 400},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 900},
					expectedPending: []pendingResult{{peers[1], 400}},
				},
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 900},
					expectedPending: []pendingResult{{peers[1], 400}, {peers[0], 300}},
				},
				{
					op:              releaseBlock{peers[0], 500},
					totals:          map[peer.ID]uint64{peers[0]: 800, peers[1]: 900},
					expectedPending: []pendingResult{{peers[1], 400}},
				},
				{
					op:              releaseBlock{peers[1], 500},
					totals:          map[peer.ID]uint64{peers[0]: 800, peers[1]: 800},
					expectedPending: []pendingResult{},
				},
			},
		},
		"multiple peers against mix of limits": {
			total:      2700,
			maxPerPeer: 1000,
			steps: []allocStep{
				{
					op:              alloc{peers[0], 800},
					totals:          map[peer.ID]uint64{peers[0]: 800},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[1], 900},
					totals:          map[peer.ID]uint64{peers[0]: 800, peers[1]: 900},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[1], 400},
					totals:          map[peer.ID]uint64{peers[0]: 800, peers[1]: 900},
					expectedPending: []pendingResult{{peers[1], 400}},
				},
				{
					op:              alloc{peers[0], 300},
					totals:          map[peer.ID]uint64{peers[0]: 800, peers[1]: 900},
					expectedPending: []pendingResult{{peers[1], 400}, {peers[0], 300}},
				},
				{
					op:              alloc{peers[2], 1000},
					totals:          map[peer.ID]uint64{peers[0]: 800, peers[1]: 900, peers[2]: 1000},
					expectedPending: []pendingResult{{peers[1], 400}, {peers[0], 300}},
				},
				{
					op:              alloc{peers[2], 300},
					totals:          map[peer.ID]uint64{peers[0]: 800, peers[1]: 900, peers[2]: 1000},
					expectedPending: []pendingResult{{peers[1], 400}, {peers[0], 300}, {peers[2], 300}},
				},
				{
					op:              releaseBlock{peers[0], 200},
					totals:          map[peer.ID]uint64{peers[0]: 600, peers[1]: 900, peers[2]: 1000},
					expectedPending: []pendingResult{{peers[1], 400}, {peers[0], 300}, {peers[2], 300}},
				},
				{
					op:              releaseBlock{peers[2], 200},
					totals:          map[peer.ID]uint64{peers[0]: 900, peers[1]: 900, peers[2]: 800},
					expectedPending: []pendingResult{{peers[1], 400}, {peers[2], 300}},
				},
				{
					op:              alloc{peers[2], 100},
					totals:          map[peer.ID]uint64{peers[0]: 900, peers[1]: 900, peers[2]: 800},
					expectedPending: []pendingResult{{peers[1], 400}, {peers[2], 300}, {peers[2], 100}},
				},
				{
					op:     releaseBlock{peers[1], 200},
					totals: map[peer.ID]uint64{peers[0]: 900, peers[1]: 700, peers[2]: 800},
					// allocations are FIFO per peer, so even though the 100 allocation for peer[2] would
					// stay in the limits, we're blocked until the 300 can go through
					expectedPending: []pendingResult{{peers[1], 400}, {peers[2], 300}, {peers[2], 100}},
				},
				{
					op:              releaseBlock{peers[2], 100},
					totals:          map[peer.ID]uint64{peers[0]: 900, peers[1]: 700, peers[2]: 1000},
					expectedPending: []pendingResult{{peers[1], 400}, {peers[2], 100}},
				},
				{
					op:              releaseBlock{peers[1], 100},
					totals:          map[peer.ID]uint64{peers[0]: 900, peers[1]: 600, peers[2]: 1000},
					expectedPending: []pendingResult{{peers[1], 400}, {peers[2], 100}},
				},
				{
					op:              releaseBlock{peers[2], 200},
					totals:          map[peer.ID]uint64{peers[0]: 900, peers[1]: 1000, peers[2]: 800},
					expectedPending: []pendingResult{{peers[2], 100}},
				},
				{
					op:              releaseBlock{peers[0], 200},
					totals:          map[peer.ID]uint64{peers[0]: 700, peers[1]: 1000, peers[2]: 900},
					expectedPending: []pendingResult{},
				},
			},
		},
		"multiple peers, peer drops off": {
			total:      2000,
			maxPerPeer: 1000,
			steps: []allocStep{
				{
					op:              alloc{peers[0], 1000},
					totals:          map[peer.ID]uint64{peers[0]: 1000},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[1], 500},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 500},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[2], 500},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 500, peers[2]: 500},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[1], 100},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 500, peers[2]: 500},
					expectedPending: []pendingResult{{peers[1], 100}},
				},
				{
					op:              alloc{peers[2], 100},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 500, peers[2]: 500},
					expectedPending: []pendingResult{{peers[1], 100}, {peers[2], 100}},
				},
				{
					op:              alloc{peers[2], 200},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 500, peers[2]: 500},
					expectedPending: []pendingResult{{peers[1], 100}, {peers[2], 100}, {peers[2], 200}},
				},
				{
					op:              alloc{peers[1], 200},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 500, peers[2]: 500},
					expectedPending: []pendingResult{{peers[1], 100}, {peers[2], 100}, {peers[2], 200}, {peers[1], 200}},
				},
				{
					op:              alloc{peers[2], 100},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 500, peers[2]: 500},
					expectedPending: []pendingResult{{peers[1], 100}, {peers[2], 100}, {peers[2], 200}, {peers[1], 200}, {peers[2], 100}},
				},
				{
					op:              alloc{peers[1], 300},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 500, peers[2]: 500},
					expectedPending: []pendingResult{{peers[1], 100}, {peers[2], 100}, {peers[2], 200}, {peers[1], 200}, {peers[2], 100}, {peers[1], 300}},
				},
				{
					op:              releasePeer{peers[0]},
					totals:          map[peer.ID]uint64{peers[0]: 0, peers[1]: 800, peers[2]: 900},
					expectedPending: []pendingResult{{peers[1], 300}},
				},
			},
		},
		"release more than currently allocated": {
			total:      2000,
			maxPerPeer: 1000,
			steps: []allocStep{
				{
					op:              alloc{peers[0], 1000},
					totals:          map[peer.ID]uint64{peers[0]: 1000},
					expectedPending: []pendingResult{},
				},
				{
					op:              releaseBlock{peers[0], 500},
					totals:          map[peer.ID]uint64{peers[0]: 500},
					expectedPending: []pendingResult{},
				},
				{
					op:              releaseBlock{peers[0], 1000},
					totals:          map[peer.ID]uint64{peers[0]: 0},
					expectedPending: []pendingResult{},
				},
			},
		},
		"release block then peer more than allocated": {
			total:      2000,
			maxPerPeer: 1000,
			steps: []allocStep{
				{
					op:              alloc{peers[0], 1000},
					totals:          map[peer.ID]uint64{peers[0]: 1000},
					expectedPending: []pendingResult{},
				},
				{
					op:              alloc{peers[1], 500},
					totals:          map[peer.ID]uint64{peers[0]: 1000, peers[1]: 500},
					expectedPending: []pendingResult{},
				},
				{
					op:              releaseBlock{peers[0], 500},
					totals:          map[peer.ID]uint64{peers[0]: 500, peers[1]: 500},
					expectedPending: []pendingResult{},
				},
				{
					op:              releaseBlock{peers[0], 1000},
					totals:          map[peer.ID]uint64{peers[0]: 0, peers[1]: 500},
					expectedPending: []pendingResult{},
				},
				{
					op:              releasePeer{peers[1]},
					totals:          map[peer.ID]uint64{peers[0]: 0, peers[1]: 0},
					expectedPending: []pendingResult{},
				},
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			allocator := allocator.NewAllocator(data.total, data.maxPerPeer)
			var pending []pendingResultWithChan
			for _, step := range data.steps {
				switch op := step.op.(type) {
				case alloc:
					allocated := allocator.AllocateBlockMemory(op.p, op.amount)
					select {
					case <-allocated:
					default:

						pending = append(pending, pendingResultWithChan{pendingResult(op), allocated}) //nolint:gosimple
					}
				case releasePeer:
					err := allocator.ReleasePeerMemory(op.p)
					assert.NoError(t, err)
					pending = readPending(t, pending)
				case releaseBlock:
					err := allocator.ReleaseBlockMemory(op.p, op.amount)
					assert.NoError(t, err)
					pending = readPending(t, pending)
				default:
					t.Fatal("unrecognized op type")
				}
				for p, expected := range step.totals {
					require.Equal(t, expected, allocator.AllocatedForPeer(p))
				}
				pendingResults := []pendingResult{}
				for _, next := range pending {
					pendingResults = append(pendingResults, next.pendingResult)
				}
				require.Equal(t, step.expectedPending, pendingResults)
				expectedTotalPending := uint64(0)
				expectedPeersPending := map[peer.ID]struct{}{}
				for _, pendingResult := range step.expectedPending {
					expectedTotalPending += pendingResult.amount
					expectedPeersPending[pendingResult.p] = struct{}{}
				}
				expectedNumPeersPending := uint64(len(expectedPeersPending))
				expendingTotalAllocated := uint64(0)
				for _, peerTotal := range step.totals {
					expendingTotalAllocated += peerTotal
				}
				stats := allocator.Stats()
				require.Equal(t, data.total, stats.MaxAllowedAllocatedTotal)
				require.Equal(t, data.maxPerPeer, stats.MaxAllowedAllocatedPerPeer)
				require.Equal(t, expectedNumPeersPending, stats.NumPeersWithPendingAllocations)
				require.Equal(t, expendingTotalAllocated, stats.TotalAllocatedAllPeers)
				require.Equal(t, expectedTotalPending, stats.TotalPendingAllocations)
			}
		})
	}
}

func readPending(t *testing.T, pending []pendingResultWithChan) []pendingResultWithChan {
	t.Helper()
	morePending := true
	for morePending && len(pending) > 0 {
		morePending = false
	doneIter:
		for i, next := range pending {
			select {
			case err := <-next.response:
				require.NoError(t, err)
				copy(pending[i:], pending[i+1:])
				pending[len(pending)-1] = pendingResultWithChan{}
				pending = pending[:len(pending)-1]
				morePending = true
				break doneIter
			default:
			}
		}
	}
	return pending
}

type alloc struct {
	p      peer.ID
	amount uint64
}

type releaseBlock struct {
	p      peer.ID
	amount uint64
}

type releasePeer struct {
	p peer.ID
}

type pendingResult struct {
	p      peer.ID //nolint:structcheck
	amount uint64  //nolint:structcheck
}

type pendingResultWithChan struct {
	pendingResult
	response <-chan error
}

type allocStep struct {
	op              interface{}
	totals          map[peer.ID]uint64
	expectedPending []pendingResult
}
