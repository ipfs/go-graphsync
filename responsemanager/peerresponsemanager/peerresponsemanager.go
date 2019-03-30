package peerresponsemanager

import (
	"context"
	"sync"

	"github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipfs/go-graphsync/ipldbridge"

	logging "github.com/ipfs/go-log"
	"github.com/ipld/go-ipld-prime"

	"github.com/ipfs/go-block-format"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/linktracker"
	"github.com/ipfs/go-graphsync/responsemanager/responsebuilder"
	peer "github.com/libp2p/go-libp2p-peer"
)

var log = logging.Logger("graphsync")

// PeerHandler is an interface that can send a response for a given peer across
// the network.
type PeerHandler interface {
	SendResponse(peer.ID, []gsmsg.GraphSyncResponse, []blocks.Block) <-chan struct{}
}

// PeerResponseManager handles batching, deduping, and sending responses for
// a given peer across multiple requests.
type PeerResponseManager struct {
	p            peer.ID
	ctx          context.Context
	cancel       context.CancelFunc
	peerHandler  PeerHandler
	ipldBridge   ipldbridge.IPLDBridge
	outgoingWork chan struct{}

	linkTrackerLk     sync.RWMutex
	linkTracker       *linktracker.LinkTracker
	responseBuilderLk sync.RWMutex
	responseBuilder   *responsebuilder.ResponseBuilder
}

// New generates a new PeerResponse manager for the given context, peer ID,
// using the given peer handler and bridge to IPLD.
func New(ctx context.Context, p peer.ID, peerHandler PeerHandler, ipldBridge ipldbridge.IPLDBridge) *PeerResponseManager {
	ctx, cancel := context.WithCancel(ctx)
	return &PeerResponseManager{
		p:            p,
		ctx:          ctx,
		cancel:       cancel,
		peerHandler:  peerHandler,
		ipldBridge:   ipldBridge,
		outgoingWork: make(chan struct{}, 1),
		linkTracker:  linktracker.New(),
	}
}

// Startup initiates message sending for a peer
func (prm *PeerResponseManager) Startup() {
	go prm.run()
}

// Shutdown stops sending messages for a peer
func (prm *PeerResponseManager) Shutdown() {
	prm.cancel()
}

// SendResponse sends a given link for a given
// requestID across the wire, as well as its corresponding
// block if the block is present and has not already been sent
func (prm *PeerResponseManager) SendResponse(
	requestID gsmsg.GraphSyncRequestID,
	link ipld.Link,
	data []byte,
) {
	hasBlock := data != nil
	prm.linkTrackerLk.Lock()
	sendBlock := hasBlock && prm.linkTracker.ShouldSendBlockFor(link)
	prm.linkTracker.RecordLinkTraversal(requestID, link, hasBlock)
	prm.linkTrackerLk.Unlock()

	if prm.buildResponse(func(responseBuilder *responsebuilder.ResponseBuilder) {
		if sendBlock {
			cidLink := link.(cidlink.Link)
			block, err := blocks.NewBlockWithCid(data, cidLink.Cid)
			if err != nil {
				log.Errorf("Data did not match cid when sending link for %s", cidLink.String())
			}
			responseBuilder.AddBlock(block)
		}
		responseBuilder.AddLink(requestID, link, hasBlock)
	}) {
		prm.signalWork()
	}
}

// FinishRequest marks the given requestID as having sent all responses
func (prm *PeerResponseManager) FinishRequest(requestID gsmsg.GraphSyncRequestID) {
	prm.linkTrackerLk.Lock()
	isComplete := prm.linkTracker.FinishRequest(requestID)
	prm.linkTrackerLk.Unlock()

	if prm.buildResponse(func(responseBuilder *responsebuilder.ResponseBuilder) {
		responseBuilder.AddCompletedRequest(requestID, isComplete)
	}) {
		prm.signalWork()
	}
}

func (prm *PeerResponseManager) buildResponse(buildResponseFn func(*responsebuilder.ResponseBuilder)) bool {
	prm.responseBuilderLk.Lock()
	defer prm.responseBuilderLk.Unlock()
	if prm.responseBuilder == nil {
		prm.responseBuilder = responsebuilder.New()
	}
	buildResponseFn(prm.responseBuilder)
	return !prm.responseBuilder.Empty()
}

func (prm *PeerResponseManager) signalWork() {
	select {
	case prm.outgoingWork <- struct{}{}:
	default:
	}
}

func (prm *PeerResponseManager) run() {
	for {
		select {
		case <-prm.ctx.Done():
			return
		case <-prm.outgoingWork:
			prm.sendResponseMessage()
		}
	}
}

func (prm *PeerResponseManager) sendResponseMessage() {
	prm.responseBuilderLk.Lock()
	builder := prm.responseBuilder
	prm.responseBuilder = nil
	prm.responseBuilderLk.Unlock()

	if builder == nil || builder.Empty() {
		return
	}
	responses, blks, err := builder.Build(prm.ipldBridge)
	if err != nil {
		log.Errorf("Unable to assemble GraphSync response: %s", err.Error())
	}

	done := prm.peerHandler.SendResponse(prm.p, responses, blks)

	// wait for message to be processed
	select {
	case <-done:
	case <-prm.ctx.Done():
	}
}
