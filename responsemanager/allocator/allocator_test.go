package allocator_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-graphsync/responsemanager/allocator"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocator(t *testing.T) {
	peers := testutil.GeneratePeers(3)
	ctx := context.Background()
	testCases := map[string]struct {
		total      uint64
		maxPerPeer uint64
		allocs     []alloc
		totals     []map[peer.ID]uint64
	}{
		"single peer against total": {
			total:      1000,
			maxPerPeer: 1000,
			allocs: []alloc{
				{peers[0], 300, false},
				{peers[0], 300, false},
				{peers[0], 300, false},
				{peers[0], 300, false},
				{peers[0], 400, true},
			},
			totals: []map[peer.ID]uint64{
				{peers[0]: 300},
				{peers[0]: 600},
				{peers[0]: 900},
				{peers[0]: 500},
				{peers[0]: 800},
			},
		},
		"single peer against self limit": {
			total:      2000,
			maxPerPeer: 1000,
			allocs: []alloc{
				{peers[0], 300, false},
				{peers[0], 300, false},
				{peers[0], 300, false},
				{peers[0], 300, false},
				{peers[0], 400, true},
			},
			totals: []map[peer.ID]uint64{
				{peers[0]: 300},
				{peers[0]: 600},
				{peers[0]: 900},
				{peers[0]: 500},
				{peers[0]: 800},
			},
		},
		"multiple peers against total": {
			total:      2000,
			maxPerPeer: 2000,
			allocs: []alloc{
				{peers[0], 1000, false},
				{peers[1], 900, false},
				{peers[1], 400, false},
				{peers[0], 300, false},
				{peers[0], 500, true},
				{peers[1], 500, true},
			},
			totals: []map[peer.ID]uint64{
				{peers[0]: 1000},
				{peers[0]: 1000, peers[1]: 900},
				{peers[0]: 500, peers[1]: 900},
				{peers[0]: 500, peers[1]: 1300},
				{peers[0]: 500, peers[1]: 800},
				{peers[0]: 800, peers[1]: 800},
			},
		},
		"multiple peers against self limit": {
			total:      5000,
			maxPerPeer: 1000,
			allocs: []alloc{
				{peers[0], 1000, false},
				{peers[1], 900, false},
				{peers[1], 400, false},
				{peers[0], 300, false},
				{peers[0], 500, true},
				{peers[1], 500, true},
			},
			totals: []map[peer.ID]uint64{
				{peers[0]: 1000},
				{peers[0]: 1000, peers[1]: 900},
				{peers[0]: 500, peers[1]: 900},
				{peers[0]: 800, peers[1]: 900},
				{peers[0]: 800, peers[1]: 400},
				{peers[0]: 800, peers[1]: 800},
			},
		},
		"multiple peers against mix of limits": {
			total:      2700,
			maxPerPeer: 1000,
			allocs: []alloc{
				{peers[0], 800, false},
				{peers[1], 900, false},
				{peers[1], 400, false},
				{peers[0], 300, false},
				{peers[2], 1000, false},
				{peers[2], 300, false},
				{peers[0], 200, true},
				{peers[2], 200, true},
				{peers[2], 100, false},
				{peers[1], 200, true},
				{peers[2], 100, true},
				{peers[1], 100, true},
				{peers[2], 200, true},
				{peers[0], 200, true},
			},
			totals: []map[peer.ID]uint64{
				{peers[0]: 800},
				{peers[0]: 800, peers[1]: 900},
				{peers[0]: 800, peers[1]: 900, peers[2]: 1000},
				{peers[0]: 600, peers[1]: 900, peers[2]: 1000},
				{peers[0]: 600, peers[1]: 900, peers[2]: 800},
				{peers[0]: 900, peers[1]: 900, peers[2]: 800},
				{peers[0]: 900, peers[1]: 700, peers[2]: 800},
				{peers[0]: 900, peers[1]: 700, peers[2]: 700},
				{peers[0]: 900, peers[1]: 700, peers[2]: 1000},
				{peers[0]: 900, peers[1]: 600, peers[2]: 1000},
				{peers[0]: 900, peers[1]: 600, peers[2]: 800},
				{peers[0]: 900, peers[1]: 1000, peers[2]: 800},
				{peers[0]: 700, peers[1]: 1000, peers[2]: 800},
				{peers[0]: 700, peers[1]: 1000, peers[2]: 900},
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			//ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			//defer cancel()
			allocator := allocator.NewAllocator(ctx, data.total, data.maxPerPeer)
			allocator.Start()
			totals := map[peer.ID]uint64{}
			currentTotal := 0
			var pending []pendingResult
			for _, alloc := range data.allocs {
				var changedTotals bool
				pending, changedTotals = readPending(t, pending, totals)
				if changedTotals {
					require.Less(t, currentTotal, len(data.totals))
					require.Equal(t, data.totals[currentTotal], totals)
					currentTotal++
				}
				if alloc.isDealloc {
					err := allocator.ReleaseBlockMemory(alloc.p, alloc.amount)
					assert.NoError(t, err)
					totals[alloc.p] = totals[alloc.p] - alloc.amount
					require.Less(t, currentTotal, len(data.totals))
					require.Equal(t, data.totals[currentTotal], totals)
					currentTotal++
				} else {
					allocated := allocator.AllocateBlockMemory(alloc.p, alloc.amount)
					select {
					case <-allocated:
						totals[alloc.p] = totals[alloc.p] + alloc.amount
						require.Less(t, currentTotal, len(data.totals))
						require.Equal(t, data.totals[currentTotal], totals)
						currentTotal++
					default:
						pending = append(pending, pendingResult{alloc.p, alloc.amount, allocated})
					}
				}
			}
			var changedTotals bool
			_, changedTotals = readPending(t, pending, totals)
			if changedTotals {
				require.Less(t, currentTotal, len(data.totals))
				require.Equal(t, data.totals[currentTotal], totals)
				currentTotal++
			}
			require.Equal(t, len(data.totals), currentTotal)
		})
	}
}

func readPending(t *testing.T, pending []pendingResult, totals map[peer.ID]uint64) ([]pendingResult, bool) {
	morePending := true
	changedTotals := false
	for morePending && len(pending) > 0 {
		morePending = false
	doneIter:
		for i, next := range pending {
			select {
			case err := <-next.response:
				require.NoError(t, err)
				copy(pending[i:], pending[i+1:])
				pending[len(pending)-1] = pendingResult{}
				pending = pending[:len(pending)-1]
				totals[next.p] = totals[next.p] + next.amount
				changedTotals = true
				morePending = true
				break doneIter
			default:
			}
		}
	}
	return pending, changedTotals
}

type alloc struct {
	p         peer.ID
	amount    uint64
	isDealloc bool
}

type pendingResult struct {
	p        peer.ID
	amount   uint64
	response <-chan error
}
