package graphsync

import (
	"context"
	"errors"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	logging "github.com/ipfs/go-log/v2"
	ipld "github.com/ipld/go-ipld-prime"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/transport/graphsync/extension"
)

var log = logging.Logger("dt_graphsync")

var errContextCancelled = errors.New("context cancelled")

type graphsyncKey struct {
	requestID graphsync.RequestID
	p         peer.ID
}

type responseProgress struct {
	currentSent uint64
	maximumSent uint64
}

// Transport manages graphsync hooks for data transfer, translating from
// graphsync hooks to semantic data transfer events
type Transport struct {
	events                datatransfer.EventsHandler
	gs                    graphsync.GraphExchange
	peerID                peer.ID
	dataLock              sync.RWMutex
	graphsyncRequestMap   map[graphsyncKey]datatransfer.ChannelID
	channelIDMap          map[datatransfer.ChannelID]graphsyncKey
	contextCancelMap      map[datatransfer.ChannelID]func()
	pending               map[datatransfer.ChannelID]chan struct{}
	requestorCancelledMap map[datatransfer.ChannelID]struct{}
	pendingExtensions     map[datatransfer.ChannelID][]graphsync.ExtensionData
	responseProgressMap   map[datatransfer.ChannelID]*responseProgress
	stores                map[datatransfer.ChannelID]struct{}
}

// NewTransport makes a new hooks manager with the given hook events interface
func NewTransport(peerID peer.ID, gs graphsync.GraphExchange) *Transport {
	return &Transport{
		gs:                    gs,
		peerID:                peerID,
		graphsyncRequestMap:   make(map[graphsyncKey]datatransfer.ChannelID),
		contextCancelMap:      make(map[datatransfer.ChannelID]func()),
		requestorCancelledMap: make(map[datatransfer.ChannelID]struct{}),
		pendingExtensions:     make(map[datatransfer.ChannelID][]graphsync.ExtensionData),
		channelIDMap:          make(map[datatransfer.ChannelID]graphsyncKey),
		responseProgressMap:   make(map[datatransfer.ChannelID]*responseProgress),
		pending:               make(map[datatransfer.ChannelID]chan struct{}),
		stores:                make(map[datatransfer.ChannelID]struct{}),
	}
}

// OpenChannel initiates an outgoing request for the other peer to send data
// to us on this channel
// Note: from a data transfer symantic standpoint, it doesn't matter if the
// request is push or pull -- OpenChannel is called by the party that is
// intending to receive data
func (t *Transport) OpenChannel(ctx context.Context,
	dataSender peer.ID,
	channelID datatransfer.ChannelID,
	root ipld.Link,
	stor ipld.Node,
	doNotSendCids []cid.Cid,
	msg datatransfer.Message) error {
	if t.events == nil {
		return datatransfer.ErrHandlerNotSet
	}
	ext, err := extension.ToExtensionData(msg)
	if err != nil {
		return err
	}
	internalCtx, internalCancel := context.WithCancel(ctx)

	t.dataLock.Lock()
	// if we have an existing request pending for the channelID, cancel it first.
	if cancelF, ok := t.contextCancelMap[channelID]; ok {
		cancelF()
	}
	t.pending[channelID] = make(chan struct{})
	t.contextCancelMap[channelID] = internalCancel
	t.dataLock.Unlock()

	exts := []graphsync.ExtensionData{ext}
	if len(doNotSendCids) != 0 {
		set := cid.NewSet()
		for _, c := range doNotSendCids {
			set.Add(c)
		}
		bz, err := cidset.EncodeCidSet(set)
		if err != nil {
			return xerrors.Errorf("failed to encode cid set: %w", err)
		}
		doNotSendExt := graphsync.ExtensionData{Name: graphsync.ExtensionDoNotSendCIDs,
			Data: bz}
		exts = append(exts, doNotSendExt)
	}
	_, errChan := t.gs.Request(internalCtx, dataSender, root, stor, exts...)

	go t.executeGsRequest(ctx, channelID, errChan)
	return nil
}

func (t *Transport) consumeResponses(ctx context.Context, errChan <-chan error) error {
	var lastError error
	for {
		select {
		case <-ctx.Done():
			return errContextCancelled
		case err, ok := <-errChan:
			if !ok {
				return lastError
			}
			lastError = err
		}
	}
}

func (t *Transport) executeGsRequest(ctx context.Context, channelID datatransfer.ChannelID, errChan <-chan error) {
	lastError := t.consumeResponses(ctx, errChan)

	if _, ok := lastError.(graphsync.RequestContextCancelledErr); ok {
		log.Warnf("graphsync request context cancelled, channel Id: %v", channelID)
		if err := t.events.OnRequestTimedOut(ctx, channelID); err != nil {
			log.Error(err)
		}
		return
	}

	if _, ok := lastError.(graphsync.RequestCancelledErr); ok {
		// TODO Should we do anything for RequestCancelledErr ?
		return
	}

	if lastError != nil {
		log.Warnf("graphsync error: %s", lastError.Error())
	}
	err := t.events.OnChannelCompleted(channelID, lastError == nil)
	if err != nil {
		log.Error(err)
	}
}

func (t *Transport) gsKeyFromChannelID(ctx context.Context, chid datatransfer.ChannelID) (graphsyncKey, error) {
	for {
		t.dataLock.RLock()
		gsKey, ok := t.channelIDMap[chid]
		if ok {
			t.dataLock.RUnlock()
			return gsKey, nil
		}
		pending, hasPending := t.pending[chid]
		t.dataLock.RUnlock()
		if !hasPending {
			return graphsyncKey{}, datatransfer.ErrChannelNotFound
		}
		select {
		case <-ctx.Done():
			return graphsyncKey{}, datatransfer.ErrChannelNotFound
		case <-pending:
		}
	}
}

// PauseChannel paused the given channel ID
func (t *Transport) PauseChannel(ctx context.Context,
	chid datatransfer.ChannelID,
) error {
	if t.events == nil {
		return datatransfer.ErrHandlerNotSet
	}
	gsKey, err := t.gsKeyFromChannelID(ctx, chid)
	if err != nil {
		return err
	}
	if gsKey.p == t.peerID {
		return t.gs.PauseRequest(gsKey.requestID)
	}

	t.dataLock.RLock()
	defer t.dataLock.RUnlock()
	if _, ok := t.requestorCancelledMap[chid]; ok {
		return nil
	}
	return t.gs.PauseResponse(gsKey.p, gsKey.requestID)
}

// ResumeChannel resumes the given channel
func (t *Transport) ResumeChannel(ctx context.Context,
	msg datatransfer.Message,
	chid datatransfer.ChannelID,
) error {
	if t.events == nil {
		return datatransfer.ErrHandlerNotSet
	}
	gsKey, err := t.gsKeyFromChannelID(ctx, chid)
	if err != nil {
		return err
	}
	var extensions []graphsync.ExtensionData
	if msg != nil {
		msgExt, err := extension.ToExtensionData(msg)
		if err != nil {
			return err
		}
		extensions = append(extensions, msgExt)
	}
	if gsKey.p == t.peerID {
		return t.gs.UnpauseRequest(gsKey.requestID, extensions...)
	}
	t.dataLock.Lock()
	defer t.dataLock.Unlock()

	if _, ok := t.requestorCancelledMap[chid]; ok {

		t.pendingExtensions[chid] = append(t.pendingExtensions[chid], extensions...)
		return nil
	}
	return t.gs.UnpauseResponse(gsKey.p, gsKey.requestID, extensions...)
}

// CloseChannel closes the given channel
func (t *Transport) CloseChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	if t.events == nil {
		return datatransfer.ErrHandlerNotSet
	}
	gsKey, err := t.gsKeyFromChannelID(ctx, chid)
	if err != nil {
		return err
	}
	if gsKey.p == t.peerID {
		t.dataLock.RLock()
		cancelFn, ok := t.contextCancelMap[chid]
		t.dataLock.RUnlock()
		if !ok {
			return datatransfer.ErrChannelNotFound
		}
		cancelFn()
		return nil
	}
	t.dataLock.Lock()
	if _, ok := t.requestorCancelledMap[chid]; ok {
		return nil
	}
	t.dataLock.Unlock()
	return t.gs.CancelResponse(gsKey.p, gsKey.requestID)
}

// CleanupChannel is called on the otherside of a cancel - removes any associated
// data for the channel
func (t *Transport) CleanupChannel(chid datatransfer.ChannelID) {
	t.dataLock.Lock()
	gsKey, ok := t.channelIDMap[chid]
	if ok {
		t.cleanupChannel(chid, gsKey)
	}
	t.dataLock.Unlock()
}

// SetEventHandler sets the handler for events on channels
func (t *Transport) SetEventHandler(events datatransfer.EventsHandler) error {
	if t.events != nil {
		return datatransfer.ErrHandlerAlreadySet
	}
	t.events = events
	t.gs.RegisterIncomingRequestHook(t.gsReqRecdHook)
	t.gs.RegisterCompletedResponseListener(t.gsCompletedResponseListener)
	t.gs.RegisterIncomingBlockHook(t.gsIncomingBlockHook)
	t.gs.RegisterOutgoingBlockHook(t.gsOutgoingBlockHook)
	t.gs.RegisterOutgoingRequestHook(t.gsOutgoingRequestHook)
	t.gs.RegisterIncomingResponseHook(t.gsIncomingResponseHook)
	t.gs.RegisterRequestUpdatedHook(t.gsRequestUpdatedHook)
	t.gs.RegisterRequestorCancelledListener(t.gsRequestorCancelledListener)
	return nil
}

// UseStore tells the graphsync transport to use the given loader and storer for this channelID
func (t *Transport) UseStore(channelID datatransfer.ChannelID, loader ipld.Loader, storer ipld.Storer) error {
	err := t.gs.RegisterPersistenceOption("data-transfer-"+channelID.String(), loader, storer)
	if err != nil {
		return err
	}
	t.dataLock.Lock()
	t.stores[channelID] = struct{}{}
	t.dataLock.Unlock()
	return nil
}

func (t *Transport) gsOutgoingRequestHook(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
	message, _ := extension.GetTransferData(request)

	// extension not found; probably not our request.
	if message == nil {
		return
	}

	var initiator peer.ID
	var responder peer.ID
	if message.IsRequest() {
		initiator = t.peerID
		responder = p
	} else {
		initiator = p
		responder = t.peerID
	}
	chid := datatransfer.ChannelID{Initiator: initiator, Responder: responder, ID: message.TransferID()}
	err := t.events.OnChannelOpened(chid)
	// record the outgoing graphsync request to map it to channel ID going forward
	t.dataLock.Lock()
	if err == nil {
		t.graphsyncRequestMap[graphsyncKey{request.ID(), t.peerID}] = chid
		t.channelIDMap[chid] = graphsyncKey{request.ID(), t.peerID}
	}
	pending, hasPending := t.pending[chid]
	if hasPending {
		close(pending)
		delete(t.pending, chid)
	}
	_, ok := t.stores[chid]
	if ok {
		hookActions.UsePersistenceOption("data-transfer-" + chid.String())
	}
	t.dataLock.Unlock()
}

func (t *Transport) gsIncomingBlockHook(p peer.ID, response graphsync.ResponseData, block graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
	t.dataLock.RLock()
	chid, ok := t.graphsyncRequestMap[graphsyncKey{response.RequestID(), t.peerID}]
	t.dataLock.RUnlock()

	if !ok {
		return
	}

	err := t.events.OnDataReceived(chid, block.Link(), block.BlockSize())
	if err != nil && err != datatransfer.ErrPause {
		hookActions.TerminateWithError(err)
		return
	}

	if err == datatransfer.ErrPause {
		hookActions.PauseRequest()
	}
}

func (t *Transport) gsOutgoingBlockHook(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
	t.dataLock.RLock()
	chid, ok := t.graphsyncRequestMap[graphsyncKey{request.ID(), p}]
	if !ok {
		t.dataLock.RUnlock()
		return
	}
	rp := t.responseProgressMap[chid]
	t.dataLock.RUnlock()
	rp.currentSent += block.BlockSize()
	if rp.currentSent <= rp.maximumSent {
		return
	}
	rp.maximumSent = rp.currentSent

	msg, err := t.events.OnDataSent(chid, block.Link(), block.BlockSize())
	if err != nil && err != datatransfer.ErrPause {
		hookActions.TerminateWithError(err)
		return
	}

	if err == datatransfer.ErrPause {
		hookActions.PauseResponse()
	}

	if msg != nil {
		extension, err := extension.ToExtensionData(msg)
		if err != nil {
			hookActions.TerminateWithError(err)
			return
		}
		hookActions.SendExtensionData(extension)
	}
}

// gsReqRecdHook is a graphsync.OnRequestReceivedHook hook
// if an incoming request does not match a previous push request, it returns an error.
func (t *Transport) gsReqRecdHook(p peer.ID, request graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {

	// if this is a push request the sender is us.
	msg, err := extension.GetTransferData(request)
	if err != nil {
		hookActions.TerminateWithError(err)
		return
	}

	// extension not found; probably not our request.
	if msg == nil {
		return
	}

	var chid datatransfer.ChannelID
	var responseMessage datatransfer.Message
	if msg.IsRequest() {
		// when a DT request comes in on graphsync, it's a pull
		chid = datatransfer.ChannelID{ID: msg.TransferID(), Initiator: p, Responder: t.peerID}
		request := msg.(datatransfer.Request)
		responseMessage, err = t.events.OnRequestReceived(chid, request)
	} else {
		// when a DT response comes in on graphsync, it's a push
		chid = datatransfer.ChannelID{ID: msg.TransferID(), Initiator: t.peerID, Responder: p}
		response := msg.(datatransfer.Response)
		err = t.events.OnResponseReceived(chid, response)
	}

	if responseMessage != nil {
		extension, extensionErr := extension.ToExtensionData(responseMessage)
		if extensionErr != nil {
			hookActions.TerminateWithError(err)
			return
		}
		hookActions.SendExtensionData(extension)
	}

	if err != nil && err != datatransfer.ErrPause {
		hookActions.TerminateWithError(err)
		return
	}

	if err == datatransfer.ErrPause {
		hookActions.PauseResponse()
	}

	t.dataLock.Lock()
	gsKey := graphsyncKey{request.ID(), p}
	if _, ok := t.requestorCancelledMap[chid]; ok {
		delete(t.requestorCancelledMap, chid)
		extensions := t.pendingExtensions[chid]
		delete(t.pendingExtensions, chid)
		for _, ext := range extensions {
			hookActions.SendExtensionData(ext)
		}
	}
	t.graphsyncRequestMap[gsKey] = chid
	t.channelIDMap[chid] = gsKey
	existing := t.responseProgressMap[chid]
	if existing != nil {
		existing.currentSent = 0
	} else {
		t.responseProgressMap[chid] = &responseProgress{}
	}
	_, ok := t.stores[chid]
	if ok {
		hookActions.UsePersistenceOption("data-transfer-" + chid.String())
	}
	t.dataLock.Unlock()
	hookActions.ValidateRequest()
}

// gsCompletedResponseListener is a graphsync.OnCompletedResponseListener. We use it learn when the data transfer is complete
// for the side that is responding to a graphsync request
func (t *Transport) gsCompletedResponseListener(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
	t.dataLock.RLock()
	chid, ok := t.graphsyncRequestMap[graphsyncKey{request.ID(), p}]
	t.dataLock.RUnlock()

	if !ok {
		return
	}

	if status != graphsync.RequestCancelled {
		success := status == graphsync.RequestCompletedFull
		err := t.events.OnChannelCompleted(chid, success)
		if err != nil {
			log.Error(err)
		}
	}
}

func (t *Transport) cleanupChannel(chid datatransfer.ChannelID, gsKey graphsyncKey) {
	delete(t.channelIDMap, chid)
	delete(t.contextCancelMap, chid)
	delete(t.pending, chid)
	delete(t.graphsyncRequestMap, gsKey)
	delete(t.responseProgressMap, chid)
	delete(t.pendingExtensions, chid)
	delete(t.requestorCancelledMap, chid)
	_, ok := t.stores[chid]
	if ok {
		err := t.gs.UnregisterPersistenceOption("data-transfer-" + chid.String())
		if err != nil {
			log.Error(err)
		}
	}
	delete(t.stores, chid)
}

func (t *Transport) gsRequestUpdatedHook(p peer.ID, request graphsync.RequestData, update graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
	t.dataLock.RLock()
	chid, ok := t.graphsyncRequestMap[graphsyncKey{request.ID(), p}]
	t.dataLock.RUnlock()

	if !ok {
		return
	}

	responseMessage, err := t.processExtension(chid, update, p)

	if responseMessage != nil {
		extension, extensionErr := extension.ToExtensionData(responseMessage)
		if extensionErr != nil {
			hookActions.TerminateWithError(err)
			return
		}
		hookActions.SendExtensionData(extension)
	}

	if err != nil && err != datatransfer.ErrPause {
		hookActions.TerminateWithError(err)
	}

}

// gsIncomingResponseHook is a graphsync.OnIncomingResponseHook. We use it to pass on responses
func (t *Transport) gsIncomingResponseHook(p peer.ID, response graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {

	t.dataLock.RLock()
	chid, ok := t.graphsyncRequestMap[graphsyncKey{response.RequestID(), t.peerID}]
	t.dataLock.RUnlock()

	if !ok {
		return
	}

	responseMessage, err := t.processExtension(chid, response, p)

	if responseMessage != nil {
		extension, extensionErr := extension.ToExtensionData(responseMessage)
		if extensionErr != nil {
			hookActions.TerminateWithError(err)
			return
		}
		hookActions.UpdateRequestWithExtensions(extension)
	}

	if err != nil {
		hookActions.TerminateWithError(err)
	}
}

func (t *Transport) processExtension(chid datatransfer.ChannelID, gsMsg extension.GsExtended, p peer.ID) (datatransfer.Message, error) {

	// if this is a push request the sender is us.
	msg, err := extension.GetTransferData(gsMsg)
	if err != nil {
		return nil, err
	}

	// extension not found; probably not our request.
	if msg == nil {
		return nil, nil
	}

	if msg.IsRequest() {

		// only accept request message updates when original message was also request
		if (chid != datatransfer.ChannelID{ID: msg.TransferID(), Initiator: p, Responder: t.peerID}) {
			return nil, errors.New("received request on response channel")
		}
		dtRequest := msg.(datatransfer.Request)
		return t.events.OnRequestReceived(chid, dtRequest)
	}

	// only accept response message updates when original message was also response
	if (chid != datatransfer.ChannelID{ID: msg.TransferID(), Initiator: t.peerID, Responder: p}) {
		return nil, errors.New("received response on request channel")
	}

	dtResponse := msg.(datatransfer.Response)
	return nil, t.events.OnResponseReceived(chid, dtResponse)
}

func (t *Transport) gsRequestorCancelledListener(p peer.ID, request graphsync.RequestData) {
	t.dataLock.Lock()
	defer t.dataLock.Unlock()

	chid, ok := t.graphsyncRequestMap[graphsyncKey{request.ID(), p}]
	if ok {
		t.requestorCancelledMap[chid] = struct{}{}
	}
}
