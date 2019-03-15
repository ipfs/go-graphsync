package requestmanager

import (
	"context"
	"fmt"
	"math"

	"github.com/ipfs/go-block-format"

	"github.com/ipld/go-ipld-prime"

	ipldbridge "github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	peer "github.com/libp2p/go-libp2p-peer"
)

// ResponseProgress is the fundamental unit of responses making progress in
// the RequestManager. Still not sure about this one? Nodes? Blocks? Struct w/ error? more info?
// for now, it's just a block.
type ResponseProgress = blocks.Block

const (
	// maxPriority is the max priority as defined by the bitswap protocol
	maxPriority = gsmsg.GraphSyncPriority(math.MaxInt32)
)

type inProgressRequestStatus struct {
	ctx             context.Context
	cancelFn        func()
	p               peer.ID
	responseChannel chan ResponseProgress
}

// PeerHandler is an interface that can send requests to peers
type PeerHandler interface {
	SendRequest(
		p peer.ID,
		id gsmsg.GraphSyncRequestID,
		selector []byte,
		priority gsmsg.GraphSyncPriority)
	CancelRequest(
		p peer.ID,
		id gsmsg.GraphSyncRequestID)
}

// RequestManager tracks outgoing requests and processes incoming reponses
// to them.
type RequestManager struct {
	ctx         context.Context
	cancel      func()
	messages    chan requestManagerMessage
	ipldBridge  ipldbridge.IPLDBridge
	peerHandler PeerHandler
	rc          *responseCollector

	// dont touch out side of run loop
	nextRequestID             gsmsg.GraphSyncRequestID
	inProgressRequestStatuses map[gsmsg.GraphSyncRequestID]*inProgressRequestStatus
}

type requestManagerMessage interface {
	handle(rm *RequestManager)
}

// New generates a new request manager from a context, network, and selectorQuerier
func New(ctx context.Context, ipldBridge ipldbridge.IPLDBridge) *RequestManager {
	ctx, cancel := context.WithCancel(ctx)
	return &RequestManager{
		ctx:                       ctx,
		cancel:                    cancel,
		ipldBridge:                ipldBridge,
		rc:                        newResponseCollector(ctx),
		messages:                  make(chan requestManagerMessage, 16),
		inProgressRequestStatuses: make(map[gsmsg.GraphSyncRequestID]*inProgressRequestStatus),
	}
}

// SetDelegate specifies who will send messages out to the internet.
func (rm *RequestManager) SetDelegate(peerHandler PeerHandler) {
	rm.peerHandler = peerHandler
}

type inProgressRequest struct {
	requestID gsmsg.GraphSyncRequestID
	incoming  chan ResponseProgress
}

type newRequestMessage struct {
	p                     peer.ID
	selector              ipld.Node
	inProgressRequestChan chan<- inProgressRequest
}

// SendRequest initiates a new GraphSync request to the given peer.
func (rm *RequestManager) SendRequest(ctx context.Context, p peer.ID, cidRootedSelector ipld.Node) (<-chan ResponseProgress, error) {
	if len(rm.ipldBridge.ValidateSelectorSpec(cidRootedSelector)) != 0 {
		return nil, fmt.Errorf("Invalid Selector Spec")
	}

	inProgressRequestChan := make(chan inProgressRequest)

	select {
	case rm.messages <- &newRequestMessage{p, cidRootedSelector, inProgressRequestChan}:
	case <-rm.ctx.Done():
		ch := make(chan ResponseProgress)
		close(ch)
		return ch, nil
	case <-ctx.Done():
		ch := make(chan ResponseProgress)
		close(ch)
		return ch, nil
	}
	var receivedInProgressRequest inProgressRequest
	select {
	case <-rm.ctx.Done():
		ch := make(chan ResponseProgress)
		close(ch)
		return ch, nil
	case receivedInProgressRequest = <-inProgressRequestChan:
	}

	return rm.rc.collectResponses(ctx, receivedInProgressRequest.incoming, func() {
		rm.cancelRequest(receivedInProgressRequest.requestID, receivedInProgressRequest.incoming)
	}), nil
}

type cancelRequestMessage struct {
	requestID gsmsg.GraphSyncRequestID
}

func (rm *RequestManager) cancelRequest(requestID gsmsg.GraphSyncRequestID, incomingResponses chan ResponseProgress) {
	cancelMessageChannel := rm.messages
	for {
		select {
		case cancelMessageChannel <- &cancelRequestMessage{requestID}:
			cancelMessageChannel = nil
		// clear out any remaining responses, in case and "incoming reponse"
		// messages get processed before our cancel message
		case _, ok := <-incomingResponses:
			if !ok {
				return
			}
		case <-rm.ctx.Done():
			return
		}
	}
}

type processResponseMessage struct {
	message gsmsg.GraphSyncMessage
}

// ProcessResponses ingests the given responses from the network and
// and updates the in progress requests based on those responses.
func (rm *RequestManager) ProcessResponses(message gsmsg.GraphSyncMessage) {
	select {
	case rm.messages <- &processResponseMessage{message}:
	case <-rm.ctx.Done():
	}
}

// Startup starts processing for the WantManager.
func (rm *RequestManager) Startup() {
	go rm.run()
}

// Shutdown ends processing for the want manager.
func (rm *RequestManager) Shutdown() {
	rm.cancel()
}

func (rm *RequestManager) run() {
	// NOTE: Do not open any streams or connections from anywhere in this
	// event loop. Really, just don't do anything likely to block.
	defer rm.cleanupInProcessRequests()

	for {
		select {
		case message := <-rm.messages:
			message.handle(rm)
		case <-rm.ctx.Done():
			return
		}
	}
}

func (rm *RequestManager) cleanupInProcessRequests() {
	for _, requestStatus := range rm.inProgressRequestStatuses {
		close(requestStatus.responseChannel)
		requestStatus.cancelFn()
	}
}

func (nrm *newRequestMessage) handle(rm *RequestManager) {
	inProgressChan := make(chan ResponseProgress)
	requestID := rm.nextRequestID
	rm.nextRequestID++
	selectorBytes, err := rm.ipldBridge.EncodeNode(nrm.selector)

	if err != nil {
		close(inProgressChan)
	} else {
		ctx, cancel := context.WithCancel(rm.ctx)

		rm.inProgressRequestStatuses[requestID] = &inProgressRequestStatus{
			ctx, cancel, nrm.p, inProgressChan,
		}
		rm.peerHandler.SendRequest(nrm.p, requestID, selectorBytes, maxPriority)
		// not starting a traversal atm
	}

	select {
	case nrm.inProgressRequestChan <- inProgressRequest{
		requestID: requestID,
		incoming:  inProgressChan,
	}:
	case <-rm.ctx.Done():
	}
}

func (crm *cancelRequestMessage) handle(rm *RequestManager) {
	inProgressRequestStatus, ok := rm.inProgressRequestStatuses[crm.requestID]
	if !ok {
		return
	}

	rm.peerHandler.CancelRequest(inProgressRequestStatus.p, crm.requestID)
	close(inProgressRequestStatus.responseChannel)
	delete(rm.inProgressRequestStatuses, crm.requestID)
	inProgressRequestStatus.cancelFn()
}

func (prm *processResponseMessage) handle(rm *RequestManager) {
	for _, block := range prm.message.Blocks() {
		// dispatch every received block to every in flight request
		// this is completely a temporary implementation
		// meant to demonstrate we can produce a round trip of blocks
		// the future implementation will actual have a temporary block store
		// and will only dispatch to those requests whose selection transversal
		// actually requires them
		for _, requestStatus := range rm.inProgressRequestStatuses {
			select {
			case requestStatus.responseChannel <- block:
			case <-rm.ctx.Done():
			case <-requestStatus.ctx.Done():
			}
		}
	}

	for _, response := range prm.message.Responses() {
		// we're keeping it super light for now -- basically just ignoring
		// reason for termination and closing the channel
		if gsmsg.IsTerminalResponseCode(response.Status()) {
			requestStatus, ok := rm.inProgressRequestStatuses[response.RequestID()]
			if ok {
				close(requestStatus.responseChannel)
				delete(rm.inProgressRequestStatuses, response.RequestID())
				requestStatus.cancelFn()
			}
		}
	}
}
