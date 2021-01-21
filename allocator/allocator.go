package allocator

import (
	"errors"
	"sync"

	pq "github.com/ipfs/go-ipfs-pq"
	peer "github.com/libp2p/go-libp2p-peer"
)

type Allocator struct {
	totalMemoryMax uint64
	perPeerMax     uint64

	allocLk                sync.RWMutex
	totalAllocatedAllPeers uint64
	nextAllocIndex         uint64
	peerStatuses           map[peer.ID]*peerStatus
	peerStatusQueue        pq.PQ
}

func NewAllocator(totalMemoryMax uint64, perPeerMax uint64) *Allocator {
	return &Allocator{
		totalMemoryMax:         totalMemoryMax,
		perPeerMax:             perPeerMax,
		totalAllocatedAllPeers: 0,
		peerStatuses:           make(map[peer.ID]*peerStatus),
		peerStatusQueue:        pq.New(makePeerStatusCompare(perPeerMax)),
	}
}

func (a *Allocator) AllocatedForPeer(p peer.ID) uint64 {
	a.allocLk.RLock()
	defer a.allocLk.RUnlock()

	status, ok := a.peerStatuses[p]
	if !ok {
		return 0
	}
	return status.totalAllocated
}

func (a *Allocator) AllocateBlockMemory(p peer.ID, amount uint64) <-chan error {
	responseChan := make(chan error, 1)
	a.allocLk.Lock()
	defer a.allocLk.Unlock()

	status, ok := a.peerStatuses[p]
	if !ok {
		status = &peerStatus{
			p:              p,
			totalAllocated: 0,
		}
		a.peerStatusQueue.Push(status)
		a.peerStatuses[p] = status
	}

	if (a.totalAllocatedAllPeers+amount <= a.totalMemoryMax) && (status.totalAllocated+amount <= a.perPeerMax) && len(status.pendingAllocations) == 0 {
		a.totalAllocatedAllPeers += amount
		status.totalAllocated += amount
		responseChan <- nil
	} else {
		pendingAllocation := pendingAllocation{p, amount, responseChan, a.nextAllocIndex}
		a.nextAllocIndex++
		status.pendingAllocations = append(status.pendingAllocations, pendingAllocation)
	}
	a.peerStatusQueue.Update(status.Index())
	return responseChan
}

func (a *Allocator) ReleaseBlockMemory(p peer.ID, amount uint64) error {
	a.allocLk.Lock()
	defer a.allocLk.Unlock()

	status, ok := a.peerStatuses[p]
	if !ok {
		return errors.New("cannot deallocate from peer with no allocations")
	}
	if status.totalAllocated > amount {
		status.totalAllocated -= amount
	} else {
		status.totalAllocated = 0
	}
	if a.totalAllocatedAllPeers > amount {
		a.totalAllocatedAllPeers -= amount
	} else {
		a.totalAllocatedAllPeers = 0
	}
	a.peerStatusQueue.Update(status.Index())
	a.processPendingAllocations()
	return nil
}

func (a *Allocator) ReleasePeerMemory(p peer.ID) error {
	a.allocLk.Lock()
	defer a.allocLk.Unlock()
	status, ok := a.peerStatuses[p]
	if !ok {
		return errors.New("cannot deallocate peer with no allocations")
	}
	a.peerStatusQueue.Remove(status.Index())
	delete(a.peerStatuses, p)
	for _, pendingAllocation := range status.pendingAllocations {
		pendingAllocation.response <- errors.New("Peer has been deallocated")
	}
	a.totalAllocatedAllPeers -= status.totalAllocated
	a.processPendingAllocations()
	return nil
}

func (a *Allocator) processPendingAllocations() {
	for a.peerStatusQueue.Len() > 0 {
		nextPeer := a.peerStatusQueue.Peek().(*peerStatus)

		if len(nextPeer.pendingAllocations) > 0 {
			if !a.processNextPendingAllocationForPeer(nextPeer) {
				return
			}
			a.peerStatusQueue.Update(nextPeer.Index())
		} else {
			if nextPeer.totalAllocated > 0 {
				return
			}
			a.peerStatusQueue.Pop()
			target := nextPeer.p
			delete(a.peerStatuses, target)
		}
	}
}

func (a *Allocator) processNextPendingAllocationForPeer(nextPeer *peerStatus) bool {
	pendingAllocation := nextPeer.pendingAllocations[0]
	if a.totalAllocatedAllPeers+pendingAllocation.amount > a.totalMemoryMax {
		return false
	}
	if nextPeer.totalAllocated+pendingAllocation.amount > a.perPeerMax {
		return false
	}
	a.totalAllocatedAllPeers += pendingAllocation.amount
	nextPeer.totalAllocated += pendingAllocation.amount
	nextPeer.pendingAllocations = nextPeer.pendingAllocations[1:]
	pendingAllocation.response <- nil
	return true
}

type peerStatus struct {
	p                  peer.ID
	totalAllocated     uint64
	index              int
	pendingAllocations []pendingAllocation
}

type pendingAllocation struct {
	p          peer.ID
	amount     uint64
	response   chan error
	allocIndex uint64
}

// SetIndex stores the int index.
func (ps *peerStatus) SetIndex(index int) {
	ps.index = index
}

// Index returns the last given by SetIndex(int).
func (ps *peerStatus) Index() int {
	return ps.index
}

func makePeerStatusCompare(maxPerPeer uint64) pq.ElemComparator {
	return func(a, b pq.Elem) bool {
		pa := a.(*peerStatus)
		pb := b.(*peerStatus)
		if len(pa.pendingAllocations) == 0 {
			if len(pb.pendingAllocations) == 0 {
				return pa.totalAllocated < pb.totalAllocated
			}
			return false
		}
		if len(pb.pendingAllocations) == 0 {
			return true
		}
		if pa.totalAllocated+pa.pendingAllocations[0].amount > maxPerPeer {
			return false
		}
		if pb.totalAllocated+pb.pendingAllocations[0].amount > maxPerPeer {
			return true
		}
		if pa.pendingAllocations[0].allocIndex < pb.pendingAllocations[0].allocIndex {
			return true
		}
		return false
	}
}
