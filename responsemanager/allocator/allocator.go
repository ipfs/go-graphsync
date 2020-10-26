package allocator

import (
	"context"
	"errors"

	pq "github.com/ipfs/go-ipfs-pq"
	peer "github.com/libp2p/go-libp2p-peer"
)

type Allocator struct {
	ctx             context.Context
	totalMemoryMax  uint64
	perPeerMax      uint64
	total           uint64
	nextAllocIndex  uint64
	messages        chan allocationRequest
	peerStatuses    map[peer.ID]*peerStatus
	peerStatusQueue pq.PQ
}

func NewAllocator(ctx context.Context, totalMemoryMax uint64, perPeerMax uint64) *Allocator {
	return &Allocator{
		ctx:             ctx,
		totalMemoryMax:  totalMemoryMax,
		perPeerMax:      perPeerMax,
		total:           0,
		peerStatuses:    make(map[peer.ID]*peerStatus),
		peerStatusQueue: pq.New(makePeerStatusCompare(perPeerMax)),
		messages:        make(chan allocationRequest, 16),
	}
}

func (a *Allocator) AllocateBlockMemory(p peer.ID, amount uint64) <-chan error {
	responseChan := make(chan error, 1)
	done := make(chan struct{}, 1)
	select {
	case <-a.ctx.Done():
		responseChan <- errors.New("context closed")
	case a.messages <- allocationRequest{p, amount, allocOperation, responseChan, done}:
	}
	select {
	case <-a.ctx.Done():
	case <-done:
	}
	return responseChan
}

func (a *Allocator) ReleaseBlockMemory(p peer.ID, amount uint64) error {
	responseChan := make(chan error, 1)
	select {
	case <-a.ctx.Done():
		responseChan <- errors.New("context closed")
	case a.messages <- allocationRequest{p, amount, deallocOperation, responseChan, nil}:
	}
	select {
	case <-a.ctx.Done():
		return errors.New("context closed")
	case err := <-responseChan:
		return err
	}
}

func (a *Allocator) ReleasePeerMemory(p peer.ID) error {
	responseChan := make(chan error, 1)
	select {
	case <-a.ctx.Done():
		responseChan <- errors.New("context closed")
	case a.messages <- allocationRequest{p, 0, deallocPeerOperation, responseChan, nil}:
	}
	select {
	case <-a.ctx.Done():
		return errors.New("context closed")
	case err := <-responseChan:
		return err
	}
}

func (a *Allocator) Start() {
	go func() {
		a.run()
		a.cleanup()
	}()
}

func (a *Allocator) run() {
	for {
		select {
		case <-a.ctx.Done():
			return
		case request := <-a.messages:
			status, ok := a.peerStatuses[request.p]
			switch request.operation {
			case allocOperation:
				if !ok {
					status = &peerStatus{
						p:              request.p,
						totalAllocated: 0,
					}
					a.peerStatusQueue.Push(status)
					a.peerStatuses[request.p] = status
				}
				a.handleAllocRequest(request, status)
			case deallocOperation:
				if !ok {
					request.response <- errors.New("cannot deallocate from peer with no allocations")
					continue
				}
				a.handleDeallocRequest(request, status)
			case deallocPeerOperation:
				if !ok {
					request.response <- errors.New("cannot deallocate from peer with no allocations")
					continue
				}
				a.handleDeallocPeerRequest(request, status)
			}
		}
	}
}

func (a *Allocator) cleanup() {
	for {
		if a.peerStatusQueue.Len() == 0 {
			return
		}
		nextPeer := a.peerStatusQueue.Peek().(*peerStatus)
		if len(nextPeer.pendingAllocations) == 0 {
			return
		}
		pendingAllocation := nextPeer.pendingAllocations[0]
		nextPeer.pendingAllocations = nextPeer.pendingAllocations[1:]
		pendingAllocation.response <- errors.New("never allocated")
		a.peerStatusQueue.Update(nextPeer.Index())
	}
}

func (a *Allocator) handleAllocRequest(request allocationRequest, status *peerStatus) {
	if (a.total+request.amount <= a.totalMemoryMax) && (status.totalAllocated+request.amount <= a.perPeerMax) && len(status.pendingAllocations) == 0 {
		a.total += request.amount
		status.totalAllocated += request.amount
		request.response <- nil
	} else {
		pendingAllocation := pendingAllocation{
			allocationRequest: request,
			allocIndex:        a.nextAllocIndex,
		}
		a.nextAllocIndex++
		status.pendingAllocations = append(status.pendingAllocations, pendingAllocation)
	}
	a.peerStatusQueue.Update(status.Index())
	request.done <- struct{}{}
}

func (a *Allocator) handleDeallocRequest(request allocationRequest, status *peerStatus) {
	status.totalAllocated -= request.amount
	a.total -= request.amount
	a.peerStatusQueue.Update(status.Index())
	for a.processNextPendingAllocation() {
	}
	request.response <- nil
}

func (a *Allocator) handleDeallocPeerRequest(request allocationRequest, status *peerStatus) {
	a.peerStatusQueue.Remove(status.Index())
	for _, pendingAllocation := range status.pendingAllocations {
		pendingAllocation.response <- errors.New("Peer has been deallocated")
	}
	a.total -= status.totalAllocated
	for a.processNextPendingAllocation() {
	}
	request.response <- nil
}

func (a *Allocator) processNextPendingAllocation() bool {
	if a.peerStatusQueue.Len() == 0 {
		return false
	}
	nextPeer := a.peerStatusQueue.Peek().(*peerStatus)

	if len(nextPeer.pendingAllocations) > 0 {
		if !a.processNextPendingAllocationForPeer(nextPeer) {
			return false
		}
		a.peerStatusQueue.Update(nextPeer.Index())
	} else {
		if nextPeer.totalAllocated > 0 {
			return false
		}
		a.peerStatusQueue.Pop()
		target := nextPeer.p
		delete(a.peerStatuses, target)
	}
	return true
}

func (a *Allocator) processNextPendingAllocationForPeer(nextPeer *peerStatus) bool {
	pendingAllocation := nextPeer.pendingAllocations[0]
	if a.total+pendingAllocation.amount > a.totalMemoryMax {
		return false
	}
	if nextPeer.totalAllocated+pendingAllocation.amount > a.perPeerMax {
		return false
	}
	a.total += pendingAllocation.amount
	nextPeer.totalAllocated += pendingAllocation.amount
	nextPeer.pendingAllocations = nextPeer.pendingAllocations[1:]
	pendingAllocation.response <- nil
	return true
}

type operationType uint64

const (
	allocOperation operationType = iota
	deallocOperation
	deallocPeerOperation
)

type allocationRequest struct {
	p         peer.ID
	amount    uint64
	operation operationType
	response  chan error
	done      chan struct{}
}

type peerStatus struct {
	p                  peer.ID
	totalAllocated     uint64
	index              int
	pendingAllocations []pendingAllocation
}

type pendingAllocation struct {
	allocationRequest
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
