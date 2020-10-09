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
	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/peermanager"
	"github.com/ipfs/go-graphsync/responsemanager/responsebuilder"
)

const (
	// max block size is the maximum size for batching blocks in a single payload
	maxBlockSize uint64 = 512 * 1024
)

var log = logging.Logger("graphsync")

// PeerMessageHandler is an interface that can send a response for a given peer across
// the network.
type PeerMessageHandler interface {
	SendResponse(peer.ID, []gsmsg.GraphSyncResponse, []blocks.Block, ...notifications.Notifee)
}

// Transaction is a series of operations that should be send together in a single response
type Transaction func(PeerResponseTransactionSender) error

type peerResponseSender struct {
	p            peer.ID
	ctx          context.Context
	cancel       context.CancelFunc
	peerHandler  PeerMessageHandler
	outgoingWork chan struct{}

	linkTrackerLk      sync.RWMutex
	linkTracker        *linktracker.LinkTracker
	altTrackers        map[string]*linktracker.LinkTracker
	dedupKeys          map[graphsync.RequestID]string
	responseBuildersLk sync.RWMutex
	responseBuilders   []*responsebuilder.ResponseBuilder
	nextBuilderTopic   responsebuilder.Topic
	queuedMessages     chan responsebuilder.Topic
	subscriber         notifications.MappableSubscriber
}

// PeerResponseSender handles batching, deduping, and sending responses for
// a given peer across multiple requests.
type PeerResponseSender interface {
	peermanager.PeerProcess
	DedupKey(requestID graphsync.RequestID, key string)
	IgnoreBlocks(requestID graphsync.RequestID, links []ipld.Link)
	SendResponse(
		requestID graphsync.RequestID,
		link ipld.Link,
		data []byte,
	) graphsync.BlockData
	SendExtensionData(graphsync.RequestID, graphsync.ExtensionData)
	FinishWithCancel(requestID graphsync.RequestID)
	FinishRequest(requestID graphsync.RequestID) graphsync.ResponseStatusCode
	FinishWithError(requestID graphsync.RequestID, status graphsync.ResponseStatusCode)
	// Transaction calls multiple operations at once so they end up in a single response
	// Note: if the transaction function errors, the results will not execute
	Transaction(requestID graphsync.RequestID, transaction Transaction) error
	PauseRequest(requestID graphsync.RequestID)
}

// PeerResponseTransactionSender is a limited interface for sending responses inside a transaction
type PeerResponseTransactionSender interface {
	SendResponse(
		link ipld.Link,
		data []byte,
	) graphsync.BlockData
	SendExtensionData(graphsync.ExtensionData)
	FinishWithCancel()
	FinishRequest() graphsync.ResponseStatusCode
	FinishWithError(status graphsync.ResponseStatusCode)
	PauseRequest()
}

// NewResponseSender generates a new PeerResponseSender for the given context, peer ID,
// using the given peer message handler.
func NewResponseSender(ctx context.Context, p peer.ID, peerHandler PeerMessageHandler) PeerResponseSender {
	ctx, cancel := context.WithCancel(ctx)
	prs := &peerResponseSender{
		p:              p,
		ctx:            ctx,
		cancel:         cancel,
		peerHandler:    peerHandler,
		outgoingWork:   make(chan struct{}, 1),
		linkTracker:    linktracker.New(),
		dedupKeys:      make(map[graphsync.RequestID]string),
		altTrackers:    make(map[string]*linktracker.LinkTracker),
		queuedMessages: make(chan responsebuilder.Topic, 1),
	}
	prs.subscriber = notifications.NewMappableSubscriber(&subscriber{prs}, notifications.IdentityTransform)
	return prs
}

// Startup initiates message sending for a peer
func (prs *peerResponseSender) Startup() {
	go prs.run()
}

func (prs *peerResponseSender) getLinkTracker(requestID graphsync.RequestID) *linktracker.LinkTracker {
	key, ok := prs.dedupKeys[requestID]
	if ok {
		return prs.altTrackers[key]
	}
	return prs.linkTracker
}

func (prs *peerResponseSender) DedupKey(requestID graphsync.RequestID, key string) {
	prs.linkTrackerLk.Lock()
	defer prs.linkTrackerLk.Unlock()
	prs.dedupKeys[requestID] = key
	_, ok := prs.altTrackers[key]
	if !ok {
		prs.altTrackers[key] = linktracker.New()
	}
}

func (prs *peerResponseSender) IgnoreBlocks(requestID graphsync.RequestID, links []ipld.Link) {
	prs.linkTrackerLk.Lock()
	linkTracker := prs.getLinkTracker(requestID)
	for _, link := range links {
		linkTracker.RecordLinkTraversal(requestID, link, true)
	}
	prs.linkTrackerLk.Unlock()
}

type responseOperation interface {
	build(responseBuilder *responsebuilder.ResponseBuilder)
	size() uint64
}

func (prs *peerResponseSender) execute(operations []responseOperation) {
	size := uint64(0)
	for _, op := range operations {
		size += op.size()
	}
	if prs.buildResponse(size, func(responseBuilder *responsebuilder.ResponseBuilder) {
		for _, op := range operations {
			op.build(responseBuilder)
		}
	}) {
		prs.signalWork()
	}
}

// Shutdown stops sending messages for a peer
func (prs *peerResponseSender) Shutdown() {
	prs.cancel()
}

type extensionOperation struct {
	requestID graphsync.RequestID
	extension graphsync.ExtensionData
}

func (eo extensionOperation) build(responseBuilder *responsebuilder.ResponseBuilder) {
	responseBuilder.AddExtensionData(eo.requestID, eo.extension)
}

func (eo extensionOperation) size() uint64 {
	return uint64(len(eo.extension.Data))
}

func (prs *peerResponseSender) SendExtensionData(requestID graphsync.RequestID, extension graphsync.ExtensionData) {
	prs.execute([]responseOperation{extensionOperation{requestID, extension}})
}

type peerResponseTransactionSender struct {
	requestID  graphsync.RequestID
	operations []responseOperation
	prs        *peerResponseSender
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

func (prs *peerResponseSender) Transaction(requestID graphsync.RequestID, transaction Transaction) error {
	prts := &peerResponseTransactionSender{
		requestID: requestID,
		prs:       prs,
	}
	err := transaction(prts)
	if err == nil {
		prs.execute(prts.operations)
	}
	return err
}

type blockOperation struct {
	data      []byte
	sendBlock bool
	link      ipld.Link
	requestID graphsync.RequestID
}

func (bo blockOperation) build(responseBuilder *responsebuilder.ResponseBuilder) {
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

func (prs *peerResponseSender) setupBlockOperation(requestID graphsync.RequestID,
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
func (prs *peerResponseSender) SendResponse(
	requestID graphsync.RequestID,
	link ipld.Link,
	data []byte,
) graphsync.BlockData {
	op := prs.setupBlockOperation(requestID, link, data)
	prs.execute([]responseOperation{op})
	return op
}

type statusOperation struct {
	requestID graphsync.RequestID
	status    graphsync.ResponseStatusCode
}

func (fo statusOperation) build(responseBuilder *responsebuilder.ResponseBuilder) {
	responseBuilder.AddResponseCode(fo.requestID, fo.status)
}

func (fo statusOperation) size() uint64 {
	return 0
}

func (prs *peerResponseSender) finishTracking(requestID graphsync.RequestID) bool {
	prs.linkTrackerLk.Lock()
	defer prs.linkTrackerLk.Unlock()
	linkTracker := prs.getLinkTracker(requestID)
	allBlocks := linkTracker.FinishRequest(requestID)
	key, ok := prs.dedupKeys[requestID]
	if ok {
		delete(prs.dedupKeys, requestID)
		if linkTracker.Empty() {
			delete(prs.altTrackers, key)
		}
	}
	return allBlocks
}

func (prs *peerResponseSender) setupFinishOperation(requestID graphsync.RequestID) statusOperation {
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
func (prs *peerResponseSender) FinishRequest(requestID graphsync.RequestID) graphsync.ResponseStatusCode {
	op := prs.setupFinishOperation(requestID)
	prs.execute([]responseOperation{op})
	return op.status
}

func (prs *peerResponseSender) setupFinishWithErrOperation(requestID graphsync.RequestID, status graphsync.ResponseStatusCode) statusOperation {
	prs.finishTracking(requestID)
	return statusOperation{requestID, status}
}

// FinishWithError marks the given requestID as having terminated with an error
func (prs *peerResponseSender) FinishWithError(requestID graphsync.RequestID, status graphsync.ResponseStatusCode) {
	op := prs.setupFinishWithErrOperation(requestID, status)
	prs.execute([]responseOperation{op})
}

func (prs *peerResponseSender) PauseRequest(requestID graphsync.RequestID) {
	prs.execute([]responseOperation{statusOperation{requestID, graphsync.RequestPaused}})
}

func (prs *peerResponseSender) FinishWithCancel(requestID graphsync.RequestID) {
	_ = prs.finishTracking(requestID)
}

func (prs *peerResponseSender) buildResponse(blkSize uint64, buildResponseFn func(*responsebuilder.ResponseBuilder)) bool {
	prs.responseBuildersLk.Lock()
	defer prs.responseBuildersLk.Unlock()
	if shouldBeginNewResponse(prs.responseBuilders, blkSize) {
		topic := prs.nextBuilderTopic
		prs.nextBuilderTopic++
		prs.responseBuilders = append(prs.responseBuilders, responsebuilder.New(topic))
	}
	responseBuilder := prs.responseBuilders[len(prs.responseBuilders)-1]
	buildResponseFn(responseBuilder)
	return !responseBuilder.Empty()
}

func shouldBeginNewResponse(responseBuilders []*responsebuilder.ResponseBuilder, blkSize uint64) bool {
	if len(responseBuilders) == 0 {
		return true
	}
	if blkSize == 0 {
		return false
	}
	return responseBuilders[len(responseBuilders)-1].BlockSize()+blkSize > maxBlockSize
}

func (prs *peerResponseSender) signalWork() {
	select {
	case prs.outgoingWork <- struct{}{}:
	default:
	}
}

func (prs *peerResponseSender) run() {
	for {
		select {
		case <-prs.ctx.Done():
			return
		case <-prs.outgoingWork:
			prs.sendResponseMessages()
		}
	}
}

func (prs *peerResponseSender) sendResponseMessages() {
	prs.responseBuildersLk.Lock()
	builders := prs.responseBuilders
	prs.responseBuilders = nil
	prs.responseBuildersLk.Unlock()

	for _, builder := range builders {
		if builder.Empty() {
			continue
		}
		responses, blks, err := builder.Build()
		if err != nil {
			log.Errorf("Unable to assemble GraphSync response: %s", err.Error())
		}

		prs.peerHandler.SendResponse(prs.p, responses, blks, notifications.Notifee{
			Topic:      builder.Topic(),
			Subscriber: prs.subscriber,
		})

		// wait for message to be processed
		prs.waitForMessageQueud(builder.Topic())
	}
}

func (prs *peerResponseSender) waitForMessageQueud(topic responsebuilder.Topic) {
	for {
		select {
		case <-prs.ctx.Done():
			return
		case queuedTopic := <-prs.queuedMessages:
			if topic == queuedTopic {
				return
			}
		}
	}
}

type subscriber struct {
	prs *peerResponseSender
}

func (s *subscriber) OnNext(topic notifications.Topic, event notifications.Event) {
	builderTopic, ok := topic.(responsebuilder.Topic)
	if !ok {
		return
	}
	msgEvent, ok := event.(messagequeue.Event)
	if !ok {
		return
	}
	if msgEvent.Name != messagequeue.Queued {
		return
	}
	select {
	case s.prs.queuedMessages <- builderTopic:
	case <-s.prs.ctx.Done():
	}
}

func (s *subscriber) OnClose(_ notifications.Topic) {
}
