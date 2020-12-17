package peerresponsemanager

import (
	"context"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/linktracker"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/notifications"
)

var log = logging.Logger("graphsync")

// PeerMessageHandler is an interface that can send a response for a given peer across
// the network.
type PeerMessageHandler interface {
	BuildMessage(p peer.ID, blkSize uint64, buildResponseFn func(*gsmsg.Builder), notifees []notifications.Notifee)
}

// Allocator is an interface that can manage memory allocated for blocks
type Allocator interface {
	AllocateBlockMemory(p peer.ID, amount uint64) <-chan error
}

// Transaction is a series of operations that should be send together in a single response
type Transaction func(PeerResponseTransactionBuilder) error

type peerResponseBuilder struct {
	p           peer.ID
	ctx         context.Context
	peerHandler PeerMessageHandler
	allocator   Allocator

	linkTrackerLk sync.RWMutex
	linkTracker   *linktracker.LinkTracker
	altTrackers   map[string]*linktracker.LinkTracker
	dedupKeys     map[graphsync.RequestID]string
}

// PeerResponseBuilder handles batching, deduping, and sending responses for
// a given peer across multiple requests.
type PeerResponseBuilder interface {
	DedupKey(requestID graphsync.RequestID, key string)
	IgnoreBlocks(requestID graphsync.RequestID, links []ipld.Link)
	SendResponse(
		requestID graphsync.RequestID,
		link ipld.Link,
		data []byte,
		notifees ...notifications.Notifee,
	) graphsync.BlockData
	SendExtensionData(graphsync.RequestID, graphsync.ExtensionData, ...notifications.Notifee)
	FinishWithCancel(requestID graphsync.RequestID)
	FinishRequest(requestID graphsync.RequestID, notifees ...notifications.Notifee) graphsync.ResponseStatusCode
	FinishWithError(requestID graphsync.RequestID, status graphsync.ResponseStatusCode, notifees ...notifications.Notifee)
	// Transaction calls multiple operations at once so they end up in a single response
	// Note: if the transaction function errors, the results will not execute
	Transaction(requestID graphsync.RequestID, transaction Transaction) error
	PauseRequest(requestID graphsync.RequestID, notifees ...notifications.Notifee)
}

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

// NewResponseSender generates a new PeerResponseSender for the given context, peer ID,
// using the given peer message handler.
func NewResponseSender(ctx context.Context, p peer.ID, peerHandler PeerMessageHandler, allocator Allocator) PeerResponseBuilder {
	return &peerResponseBuilder{
		p:           p,
		ctx:         ctx,
		peerHandler: peerHandler,
		linkTracker: linktracker.New(),
		dedupKeys:   make(map[graphsync.RequestID]string),
		altTrackers: make(map[string]*linktracker.LinkTracker),
		allocator:   allocator,
	}
}

func (prs *peerResponseBuilder) getLinkTracker(requestID graphsync.RequestID) *linktracker.LinkTracker {
	key, ok := prs.dedupKeys[requestID]
	if ok {
		return prs.altTrackers[key]
	}
	return prs.linkTracker
}

func (prs *peerResponseBuilder) DedupKey(requestID graphsync.RequestID, key string) {
	prs.linkTrackerLk.Lock()
	defer prs.linkTrackerLk.Unlock()
	prs.dedupKeys[requestID] = key
	_, ok := prs.altTrackers[key]
	if !ok {
		prs.altTrackers[key] = linktracker.New()
	}
}

func (prs *peerResponseBuilder) IgnoreBlocks(requestID graphsync.RequestID, links []ipld.Link) {
	prs.linkTrackerLk.Lock()
	linkTracker := prs.getLinkTracker(requestID)
	for _, link := range links {
		linkTracker.RecordLinkTraversal(requestID, link, true)
	}
	prs.linkTrackerLk.Unlock()
}

type peerResponseTransactionSender struct {
	requestID  graphsync.RequestID
	operations []responseOperation
	notifees   []notifications.Notifee
	prs        *peerResponseBuilder
}

func (prts *peerResponseTransactionSender) SendResponse(link ipld.Link, data []byte) graphsync.BlockData {
	op := prts.prs.setupBlockOperation(prts.requestID, link, data)
	prts.operations = append(prts.operations, op)
	return op
}

func (prts *peerResponseTransactionSender) SendExtensionData(extension graphsync.ExtensionData) {
	prts.operations = append(prts.operations, extensionOperation{prts.requestID, extension})
}

func (prts *peerResponseTransactionSender) FinishRequest() graphsync.ResponseStatusCode {
	op := prts.prs.setupFinishOperation(prts.requestID)
	prts.operations = append(prts.operations, op)
	return op.status
}

func (prts *peerResponseTransactionSender) FinishWithError(status graphsync.ResponseStatusCode) {
	prts.operations = append(prts.operations, prts.prs.setupFinishWithErrOperation(prts.requestID, status))
}

func (prts *peerResponseTransactionSender) PauseRequest() {
	prts.operations = append(prts.operations, statusOperation{prts.requestID, graphsync.RequestPaused})
}

func (prts *peerResponseTransactionSender) FinishWithCancel() {
	_ = prts.prs.finishTracking(prts.requestID)
}

func (prts *peerResponseTransactionSender) AddNotifee(notifee notifications.Notifee) {
	prts.notifees = append(prts.notifees, notifee)
}

func (prs *peerResponseBuilder) Transaction(requestID graphsync.RequestID, transaction Transaction) error {
	prts := &peerResponseTransactionSender{
		requestID: requestID,
		prs:       prs,
	}
	err := transaction(prts)
	if err == nil {
		prs.execute(prts.operations, prts.notifees)
	}
	return err
}

type extensionOperation struct {
	requestID graphsync.RequestID
	extension graphsync.ExtensionData
}

func (eo extensionOperation) build(responseBuilder *gsmsg.Builder) {
	responseBuilder.AddExtensionData(eo.requestID, eo.extension)
}

func (eo extensionOperation) size() uint64 {
	return uint64(len(eo.extension.Data))
}

func (prs *peerResponseBuilder) SendExtensionData(requestID graphsync.RequestID, extension graphsync.ExtensionData, notifees ...notifications.Notifee) {
	prs.execute([]responseOperation{extensionOperation{requestID, extension}}, notifees)
}

type responseOperation interface {
	build(responseBuilder *gsmsg.Builder)
	size() uint64
}

type blockOperation struct {
	data      []byte
	sendBlock bool
	link      ipld.Link
	requestID graphsync.RequestID
}

func (bo blockOperation) build(responseBuilder *gsmsg.Builder) {
	if bo.sendBlock {
		cidLink := bo.link.(cidlink.Link)
		block, err := blocks.NewBlockWithCid(bo.data, cidLink.Cid)
		if err != nil {
			log.Errorf("Data did not match cid when sending link for %s", cidLink.String())
		}
		responseBuilder.AddBlock(block)
	}
	responseBuilder.AddLink(bo.requestID, bo.link, bo.data != nil)
}

func (bo blockOperation) Link() ipld.Link {
	return bo.link
}

func (bo blockOperation) BlockSize() uint64 {
	return uint64(len(bo.data))
}

func (bo blockOperation) BlockSizeOnWire() uint64 {
	if !bo.sendBlock {
		return 0
	}
	return bo.BlockSize()
}

func (bo blockOperation) size() uint64 {
	return bo.BlockSizeOnWire()
}

func (prs *peerResponseBuilder) setupBlockOperation(requestID graphsync.RequestID,
	link ipld.Link, data []byte) blockOperation {
	hasBlock := data != nil
	prs.linkTrackerLk.Lock()
	linkTracker := prs.getLinkTracker(requestID)
	sendBlock := hasBlock && linkTracker.BlockRefCount(link) == 0
	linkTracker.RecordLinkTraversal(requestID, link, hasBlock)
	prs.linkTrackerLk.Unlock()
	return blockOperation{
		data, sendBlock, link, requestID,
	}
}

// SendResponse sends a given link for a given
// requestID across the wire, as well as its corresponding
// block if the block is present and has not already been sent
// it returns the number of block bytes sent
func (prs *peerResponseBuilder) SendResponse(
	requestID graphsync.RequestID,
	link ipld.Link,
	data []byte,
	notifees ...notifications.Notifee,
) graphsync.BlockData {
	op := prs.setupBlockOperation(requestID, link, data)
	prs.execute([]responseOperation{op}, notifees)
	return op
}

type statusOperation struct {
	requestID graphsync.RequestID
	status    graphsync.ResponseStatusCode
}

func (fo statusOperation) build(responseBuilder *gsmsg.Builder) {
	responseBuilder.AddResponseCode(fo.requestID, fo.status)
}

func (fo statusOperation) size() uint64 {
	return 0
}

func (prs *peerResponseBuilder) finishTracking(requestID graphsync.RequestID) bool {
	prs.linkTrackerLk.Lock()
	defer prs.linkTrackerLk.Unlock()
	linkTracker := prs.getLinkTracker(requestID)
	allBlocks := linkTracker.FinishRequest(requestID)
	key, ok := prs.dedupKeys[requestID]
	if ok {
		delete(prs.dedupKeys, requestID)
		var otherRequestsFound bool
		for _, otherKey := range prs.dedupKeys {
			if otherKey == key {
				otherRequestsFound = true
				break
			}
		}
		if !otherRequestsFound {
			delete(prs.altTrackers, key)
		}
	}
	return allBlocks
}

func (prs *peerResponseBuilder) setupFinishOperation(requestID graphsync.RequestID) statusOperation {
	isComplete := prs.finishTracking(requestID)
	var status graphsync.ResponseStatusCode
	if isComplete {
		status = graphsync.RequestCompletedFull
	} else {
		status = graphsync.RequestCompletedPartial
	}
	return statusOperation{requestID, status}
}

// FinishRequest marks the given requestID as having sent all responses
func (prs *peerResponseBuilder) FinishRequest(requestID graphsync.RequestID, notifees ...notifications.Notifee) graphsync.ResponseStatusCode {
	op := prs.setupFinishOperation(requestID)
	prs.execute([]responseOperation{op}, notifees)
	return op.status
}

func (prs *peerResponseBuilder) setupFinishWithErrOperation(requestID graphsync.RequestID, status graphsync.ResponseStatusCode) statusOperation {
	prs.finishTracking(requestID)
	return statusOperation{requestID, status}
}

// FinishWithError marks the given requestID as having terminated with an error
func (prs *peerResponseBuilder) FinishWithError(requestID graphsync.RequestID, status graphsync.ResponseStatusCode, notifees ...notifications.Notifee) {
	op := prs.setupFinishWithErrOperation(requestID, status)
	prs.execute([]responseOperation{op}, notifees)
}

func (prs *peerResponseBuilder) PauseRequest(requestID graphsync.RequestID, notifees ...notifications.Notifee) {
	prs.execute([]responseOperation{statusOperation{requestID, graphsync.RequestPaused}}, notifees)
}

func (prs *peerResponseBuilder) FinishWithCancel(requestID graphsync.RequestID) {
	_ = prs.finishTracking(requestID)
}

func (prs *peerResponseBuilder) execute(operations []responseOperation, notifees []notifications.Notifee) {
	size := uint64(0)
	for _, op := range operations {
		size += op.size()
	}
	if size > 0 {
		select {
		case <-prs.allocator.AllocateBlockMemory(prs.p, size):
		case <-prs.ctx.Done():
			return
		}
	}
	prs.peerHandler.BuildMessage(prs.p, size, func(responseBuilder *gsmsg.Builder) {
		for _, op := range operations {
			op.build(responseBuilder)
		}
	}, notifees)
}
