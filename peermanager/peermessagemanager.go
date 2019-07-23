package peermanager

import (
	"context"

	"github.com/ipfs/go-block-format"

	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/libp2p/go-libp2p-core/peer"
)

// PeerQueue is a process that sends messages to a peer
type PeerQueue interface {
	PeerProcess
	AddRequest(graphSyncRequest gsmsg.GraphSyncRequest)
	AddResponses(responses []gsmsg.GraphSyncResponse, blks []blocks.Block) <-chan struct{}
}

// PeerQueueFactory provides a function that will create a PeerQueue.
type PeerQueueFactory func(ctx context.Context, p peer.ID) PeerQueue

// PeerMessageManager manages message queues for peers
type PeerMessageManager struct {
	*PeerManager
}

// NewMessageManager generates a new manger for sending messages
func NewMessageManager(ctx context.Context, createPeerQueue PeerQueueFactory) *PeerMessageManager {
	return &PeerMessageManager{
		PeerManager: New(ctx, func(ctx context.Context, p peer.ID) PeerProcess {
			return createPeerQueue(ctx, p)
		}),
	}
}

// SendRequest sends the given GraphSyncRequest to the given peer
func (pmm *PeerMessageManager) SendRequest(p peer.ID, request gsmsg.GraphSyncRequest) {
	pq := pmm.GetProcess(p).(PeerQueue)
	pq.AddRequest(request)
}

// SendResponse sends the given GraphSyncResponses and blocks to the given peer.
func (pmm *PeerMessageManager) SendResponse(p peer.ID,
	responses []gsmsg.GraphSyncResponse, blks []blocks.Block) <-chan struct{} {
	pq := pmm.GetProcess(p).(PeerQueue)
	return pq.AddResponses(responses, blks)
}
