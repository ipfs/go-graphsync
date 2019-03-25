package peertaskqueue

import (
	"sync"

	"github.com/ipfs/go-graphsync/responsemanager/peertaskqueue/peertask"
	"github.com/ipfs/go-graphsync/responsemanager/peertaskqueue/peertracker"
	pq "github.com/ipfs/go-ipfs-pq"
	peer "github.com/libp2p/go-libp2p-peer"
)

// PeerTaskQueue is a prioritized list of tasks to be executed on peers.
// The queue puts tasks on in blocks, then alternates between peers (roughly)
// to execute the block with the highest priority, or otherwise the one added
// first if priorities are equal.
type PeerTaskQueue struct {
	lock         sync.Mutex
	pQueue       pq.PQ
	peerTrackers map[peer.ID]*peertracker.PeerTracker
	frozenPeers  map[peer.ID]struct{}
}

// New creates a new PeerTaskQueue
func New() *PeerTaskQueue {
	return &PeerTaskQueue{
		peerTrackers: make(map[peer.ID]*peertracker.PeerTracker),
		frozenPeers:  make(map[peer.ID]struct{}),
		pQueue:       pq.New(peertracker.PeerCompare),
	}
}

// PushBlock adds a new block of tasks for the given peer to the queue
func (ptq *PeerTaskQueue) PushBlock(to peer.ID, tasks ...peertask.Task) {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()
	peerTracker, ok := ptq.peerTrackers[to]
	if !ok {
		peerTracker = peertracker.New()
		ptq.pQueue.Push(peerTracker)
		ptq.peerTrackers[to] = peerTracker
	}

	peerTracker.PushBlock(to, tasks, func(e []peertask.Task) {
		ptq.lock.Lock()
		for _, task := range e {
			peerTracker.TaskDone(task.Identifier)
		}
		ptq.pQueue.Update(peerTracker.Index())
		ptq.lock.Unlock()
	})
	ptq.pQueue.Update(peerTracker.Index())
}

// PopBlock 'pops' the next block of tasks to be performed. Returns nil if no block exists.
func (ptq *PeerTaskQueue) PopBlock() *peertask.TaskBlock {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()
	if ptq.pQueue.Len() == 0 {
		return nil
	}
	peerTracker := ptq.pQueue.Pop().(*peertracker.PeerTracker)

	out := peerTracker.PopBlock()
	ptq.pQueue.Push(peerTracker)
	return out
}

// Remove removes a task from the queue.
func (ptq *PeerTaskQueue) Remove(identifier peertask.Identifier, p peer.ID) {
	ptq.lock.Lock()
	peerTracker, ok := ptq.peerTrackers[p]
	if ok {
		peerTracker.Remove(identifier)
		// we now also 'freeze' that partner. If they sent us a cancel for a
		// block we were about to send them, we should wait a short period of time
		// to make sure we receive any other in-flight cancels before sending
		// them a block they already potentially have
		if !peerTracker.IsFrozen() {
			ptq.frozenPeers[p] = struct{}{}
		}

		peerTracker.Freeze()
		ptq.pQueue.Update(peerTracker.Index())
	}
	ptq.lock.Unlock()
}

// FullThaw completely thaws all peers in the queue so they can execute tasks.
func (ptq *PeerTaskQueue) FullThaw() {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	for p := range ptq.frozenPeers {
		peerTracker, ok := ptq.peerTrackers[p]
		if ok {
			peerTracker.FullThaw()
			delete(ptq.frozenPeers, p)
			ptq.pQueue.Update(peerTracker.Index())
		}
	}
}

// ThawRound unthaws peers incrementally, so that those have been frozen the least
// become unfrozen and able to execute tasks first.
func (ptq *PeerTaskQueue) ThawRound() {
	ptq.lock.Lock()
	defer ptq.lock.Unlock()

	for p := range ptq.frozenPeers {
		peerTracker, ok := ptq.peerTrackers[p]
		if ok {
			if peerTracker.Thaw() {
				delete(ptq.frozenPeers, p)
			}
			ptq.pQueue.Update(peerTracker.Index())
		}
	}
}
