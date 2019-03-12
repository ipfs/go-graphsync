package peermanager

import (
	"context"
	"sync"
	"time"

	gsmsg "github.com/ipfs/go-graphsync/message"
	logging "github.com/ipfs/go-log"

	peer "github.com/libp2p/go-libp2p-peer"
)

const (
	defaultCleanupInterval = time.Minute
)

var log = logging.Logger("graphsync")

var (
	metricsBuckets = []float64{1 << 6, 1 << 10, 1 << 14, 1 << 18, 1<<18 + 15, 1 << 22}
)

// PeerQueue provides a queer of messages to be sent for a single peer.
type PeerQueue interface {
	AddRequest(id gsmsg.GraphSyncRequestID,
		selector []byte,
		priority gsmsg.GraphSyncPriority)
	Cancel(id gsmsg.GraphSyncRequestID)
	Startup()
	Shutdown()
}

// PeerQueueFactory provides a function that will create a PeerQueue.
type PeerQueueFactory func(ctx context.Context, p peer.ID) PeerQueue

type peerQueueInstance struct {
	refcnt int
	pq     PeerQueue
}

// PeerManager manages a pool of peers and sends messages to peers in the pool.
type PeerManager struct {
	peerQueues   map[peer.ID]*peerQueueInstance
	peerQueuesLk sync.RWMutex

	createPeerQueue PeerQueueFactory
	ctx             context.Context
}

// New creates a new PeerManager, given a context and a peerQueueFactory.
func New(ctx context.Context, createPeerQueue PeerQueueFactory) *PeerManager {
	return &PeerManager{
		peerQueues:      make(map[peer.ID]*peerQueueInstance),
		createPeerQueue: createPeerQueue,
		ctx:             ctx,
	}
}

// ConnectedPeers returns a list of peers this PeerManager is managing.
func (pm *PeerManager) ConnectedPeers() []peer.ID {
	pm.peerQueuesLk.RLock()
	defer pm.peerQueuesLk.RUnlock()
	peers := make([]peer.ID, 0, len(pm.peerQueues))
	for p := range pm.peerQueues {
		peers = append(peers, p)
	}
	return peers
}

// Connected is called to add a new peer to the pool
func (pm *PeerManager) Connected(p peer.ID) {
	pm.peerQueuesLk.Lock()
	pq := pm.getOrCreate(p)
	pq.refcnt++
	pm.peerQueuesLk.Unlock()
}

// Disconnected is called to remove a peer from the pool.
func (pm *PeerManager) Disconnected(p peer.ID) {
	pm.peerQueuesLk.Lock()
	pq, ok := pm.peerQueues[p]
	if !ok {
		pm.peerQueuesLk.Unlock()
		return
	}

	pq.refcnt--
	if pq.refcnt > 0 {
		pm.peerQueuesLk.Unlock()
		return
	}

	delete(pm.peerQueues, p)
	pm.peerQueuesLk.Unlock()

	pq.pq.Shutdown()

}

// SendRequest sends the given request to the given peer.
func (pm *PeerManager) SendRequest(
	p peer.ID,
	id gsmsg.GraphSyncRequestID,
	selector []byte,
	priority gsmsg.GraphSyncPriority) {
	pm.peerQueuesLk.Lock()
	pqi := pm.getOrCreate(p)
	pm.peerQueuesLk.Unlock()
	pqi.pq.AddRequest(id, selector, priority)
}

// CancelRequest cancels the given request id on the given peer.
func (pm *PeerManager) CancelRequest(
	p peer.ID,
	id gsmsg.GraphSyncRequestID) {
	pm.peerQueuesLk.Lock()
	pqi := pm.getOrCreate(p)
	pm.peerQueuesLk.Unlock()
	pqi.pq.Cancel(id)
}

func (pm *PeerManager) getOrCreate(p peer.ID) *peerQueueInstance {
	pqi, ok := pm.peerQueues[p]
	if !ok {
		pq := pm.createPeerQueue(pm.ctx, p)
		pq.Startup()
		pqi = &peerQueueInstance{0, pq}
		pm.peerQueues[p] = pqi
	}
	return pqi
}
