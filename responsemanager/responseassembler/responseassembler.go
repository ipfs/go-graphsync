package responseassembler

import (
	"context"

	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/peermanager"
)

// Transaction is a series of operations that should be send together in a single response
type Transaction func(PeerResponseTransactionBuilder) error

// PeerResponseTransactionBuilder is a limited interface for sending responses inside a transaction
type PeerResponseTransactionBuilder interface {
	SendResponse(
		link ipld.Link,
		data []byte,
	) graphsync.BlockData
	SendExtensionData(graphsync.ExtensionData)
	FinishWithCancel()
	FinishRequest() graphsync.ResponseStatusCode
	FinishWithError(status graphsync.ResponseStatusCode)
	PauseRequest()
	AddNotifee(notifications.Notifee)
}

// PeerMessageHandler is an interface that can send a response for a given peer across
// the network.
type PeerMessageHandler interface {
	BuildMessage(p peer.ID, blkSize uint64, buildResponseFn func(*gsmsg.Builder), notifees []notifications.Notifee)
}

// Allocator is an interface that can manage memory allocated for blocks
type Allocator interface {
	AllocateBlockMemory(p peer.ID, amount uint64) <-chan error
}

// ResponseAssembler manages message queues for peers
type ResponseAssembler struct {
	*peermanager.PeerManager
	allocator   Allocator
	peerHandler PeerMessageHandler
	ctx         context.Context
}

// New generates a new peer manager for sending responses
func New(ctx context.Context, allocator Allocator, peerHandler PeerMessageHandler) *ResponseAssembler {
	return &ResponseAssembler{
		PeerManager: peermanager.New(ctx, func(ctx context.Context, p peer.ID) peermanager.PeerHandler {
			return newTracker()
		}),
		ctx:         ctx,
		allocator:   allocator,
		peerHandler: peerHandler,
	}
}

func (prm *ResponseAssembler) DedupKey(p peer.ID, requestID graphsync.RequestID, key string) {
	prm.GetProcess(p).(*peerLinkTracker).DedupKey(requestID, key)
}

func (prm *ResponseAssembler) IgnoreBlocks(p peer.ID, requestID graphsync.RequestID, links []ipld.Link) {
	prm.GetProcess(p).(*peerLinkTracker).IgnoreBlocks(requestID, links)
}

// Transaction Build A Response
func (prm *ResponseAssembler) Transaction(p peer.ID, requestID graphsync.RequestID, transaction Transaction) error {
	prts := &transactionBuilder{
		requestID:   requestID,
		linkTracker: prm.GetProcess(p).(*peerLinkTracker),
	}
	err := transaction(prts)
	if err == nil {
		prm.execute(p, prts.operations, prts.notifees)
	}
	return err
}

func (prs *ResponseAssembler) execute(p peer.ID, operations []responseOperation, notifees []notifications.Notifee) {
	size := uint64(0)
	for _, op := range operations {
		size += op.size()
	}
	if size > 0 {
		select {
		case <-prs.allocator.AllocateBlockMemory(p, size):
		case <-prs.ctx.Done():
			return
		}
	}
	prs.peerHandler.BuildMessage(p, size, func(responseBuilder *gsmsg.Builder) {
		for _, op := range operations {
			op.build(responseBuilder)
		}
	}, notifees)
}
