package responseassembler

import (
	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/notifications"
)

var log = logging.Logger("graphsync")

type responseOperation interface {
	build(responseBuilder *gsmsg.Builder)
	size() uint64
}

type transactionBuilder struct {
	requestID   graphsync.RequestID
	operations  []responseOperation
	notifees    []notifications.Notifee
	linkTracker *peerLinkTracker
}

func (prts *transactionBuilder) SendResponse(link ipld.Link, data []byte) graphsync.BlockData {
	op := prts.setupBlockOperation(link, data)
	prts.operations = append(prts.operations, op)
	return op
}

func (prts *transactionBuilder) SendExtensionData(extension graphsync.ExtensionData) {
	prts.operations = append(prts.operations, extensionOperation{prts.requestID, extension})
}

func (prts *transactionBuilder) FinishRequest() graphsync.ResponseStatusCode {
	op := prts.setupFinishOperation()
	prts.operations = append(prts.operations, op)
	return op.status
}

func (prts *transactionBuilder) FinishWithError(status graphsync.ResponseStatusCode) {
	prts.operations = append(prts.operations, prts.setupFinishWithErrOperation(status))
}

func (prts *transactionBuilder) PauseRequest() {
	prts.operations = append(prts.operations, statusOperation{prts.requestID, graphsync.RequestPaused})
}

func (prts *transactionBuilder) FinishWithCancel() {
	_ = prts.linkTracker.FinishTracking(prts.requestID)
}

func (prts *transactionBuilder) AddNotifee(notifee notifications.Notifee) {
	prts.notifees = append(prts.notifees, notifee)
}

func (prts *transactionBuilder) setupBlockOperation(
	link ipld.Link, data []byte) blockOperation {
	hasBlock := data != nil
	isUnique := prts.linkTracker.RecordLinkTraversal(prts.requestID, link, hasBlock)
	return blockOperation{
		data, hasBlock && isUnique, link, prts.requestID,
	}
}

func (prts *transactionBuilder) setupFinishOperation() statusOperation {
	isComplete := prts.linkTracker.FinishTracking(prts.requestID)
	var status graphsync.ResponseStatusCode
	if isComplete {
		status = graphsync.RequestCompletedFull
	} else {
		status = graphsync.RequestCompletedPartial
	}
	return statusOperation{prts.requestID, status}
}

func (prts *transactionBuilder) setupFinishWithErrOperation(status graphsync.ResponseStatusCode) statusOperation {
	prts.linkTracker.FinishTracking(prts.requestID)
	return statusOperation{prts.requestID, status}
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
