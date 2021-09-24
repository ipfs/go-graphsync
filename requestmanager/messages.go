package requestmanager

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

type pauseRequestMessage struct {
	id       graphsync.RequestID
	response chan<- error
}

func (prm *pauseRequestMessage) handle(rm *RequestManager) {
	err := rm.pause(prm.id)
	select {
	case <-rm.ctx.Done():
	case prm.response <- err:
	}
}

type unpauseRequestMessage struct {
	id         graphsync.RequestID
	extensions []graphsync.ExtensionData
	response   chan<- error
}

func (urm *unpauseRequestMessage) handle(rm *RequestManager) {
	err := rm.unpause(urm.id, urm.extensions)
	select {
	case <-rm.ctx.Done():
	case urm.response <- err:
	}
}

type processResponseMessage struct {
	p         peer.ID
	responses []gsmsg.GraphSyncResponse
	blks      []blocks.Block
}

func (prm *processResponseMessage) handle(rm *RequestManager) {
	rm.processResponseMessage(prm.p, prm.responses, prm.blks)
}

type cancelRequestMessage struct {
	requestID     graphsync.RequestID
	isPause       bool
	onTerminated  chan error
	terminalError error
}

func (crm *cancelRequestMessage) handle(rm *RequestManager) {
	rm.cancelRequest(crm.requestID, crm.isPause, crm.onTerminated, crm.terminalError)
}

type terminateRequestMessage struct {
	requestID graphsync.RequestID
}

func (trm *terminateRequestMessage) handle(rm *RequestManager) {
	rm.terminateRequest(trm.requestID)
}

type newRequestMessage struct {
	p                     peer.ID
	root                  ipld.Link
	selector              ipld.Node
	extensions            []graphsync.ExtensionData
	inProgressRequestChan chan<- inProgressRequest
}

func (nrm *newRequestMessage) handle(rm *RequestManager) {
	var ipr inProgressRequest

	ipr.request, ipr.incoming, ipr.incomingError = rm.setupRequest(nrm.p, nrm.root, nrm.selector, nrm.extensions)
	ipr.requestID = ipr.request.ID()

	select {
	case nrm.inProgressRequestChan <- ipr:
	case <-rm.ctx.Done():
	}
}
