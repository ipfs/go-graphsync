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

var defaultSupportedExtensions = []graphsync.ExtensionName{extension.ExtensionDataTransfer1_1, extension.ExtensionDataTransfer1_0}

// Option is an option for setting up the graphsync transport
type Option func(*Transport)

// SupportedExtensions sets what data transfer extensions are supported
func SupportedExtensions(supportedExtensions []graphsync.ExtensionName) Option {
	return func(t *Transport) {
		t.supportedExtensions = supportedExtensions
	}
}

// RegisterCompletedRequestListener is used by the tests
func RegisterCompletedRequestListener(l func(channelID datatransfer.ChannelID)) Option {
	return func(t *Transport) {
		t.completedRequestListener = l
	}
}

// RegisterCompletedResponseListener is used by the tests
func RegisterCompletedResponseListener(l func(channelID datatransfer.ChannelID)) Option {
	return func(t *Transport) {
		t.completedResponseListener = l
	}
}

// Transport manages graphsync hooks for data transfer, translating from
// graphsync hooks to semantic data transfer events
type Transport struct {
	events                    datatransfer.EventsHandler
	gs                        graphsync.GraphExchange
	peerID                    peer.ID
	dataLock                  sync.RWMutex
	graphsyncRequestMap       map[graphsyncKey]datatransfer.ChannelID
	channelIDMap              map[datatransfer.ChannelID]graphsyncKey
	contextCancelMap          map[datatransfer.ChannelID]func()
	pending                   map[datatransfer.ChannelID]chan struct{}
	requestorCancelledMap     map[datatransfer.ChannelID]struct{}
	pendingExtensions         map[datatransfer.ChannelID][]graphsync.ExtensionData
	stores                    map[datatransfer.ChannelID]struct{}
	supportedExtensions       []graphsync.ExtensionName
	unregisterFuncs           []graphsync.UnregisterHookFunc
	completedRequestListener  func(channelID datatransfer.ChannelID)
	completedResponseListener func(channelID datatransfer.ChannelID)
}

// NewTransport makes a new hooks manager with the given hook events interface
func NewTransport(peerID peer.ID, gs graphsync.GraphExchange, options ...Option) *Transport {
	t := &Transport{
		gs:                    gs,
		peerID:                peerID,
		graphsyncRequestMap:   make(map[graphsyncKey]datatransfer.ChannelID),
		contextCancelMap:      make(map[datatransfer.ChannelID]func()),
		requestorCancelledMap: make(map[datatransfer.ChannelID]struct{}),
		pendingExtensions:     make(map[datatransfer.ChannelID][]graphsync.ExtensionData),
		channelIDMap:          make(map[datatransfer.ChannelID]graphsyncKey),
		pending:               make(map[datatransfer.ChannelID]chan struct{}),
		stores:                make(map[datatransfer.ChannelID]struct{}),
		supportedExtensions:   defaultSupportedExtensions,
	}
	for _, option := range options {
		option(t)
	}
	return t
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
	exts, err := extension.ToExtensionData(msg, t.supportedExtensions)
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
	responseChan, errChan := t.gs.Request(internalCtx, dataSender, root, stor, exts...)

	go t.executeGsRequest(internalCtx, channelID, responseChan, errChan)
	return nil
}

func (t *Transport) consumeResponses(responseChan <-chan graphsync.ResponseProgress, errChan <-chan error) error {
	var lastError error
	for range responseChan {
	}
	for err := range errChan {
		lastError = err
	}
	return lastError
}

func (t *Transport) executeGsRequest(internalCtx context.Context, channelID datatransfer.ChannelID, responseChan <-chan graphsync.ResponseProgress, errChan <-chan error) {
	lastError := t.consumeResponses(responseChan, errChan)

	if _, ok := lastError.(graphsync.RequestContextCancelledErr); ok {
		terr := xerrors.Errorf("graphsync request context cancelled")
		log.Warnf("channel id %v: %s", channelID, terr)
		if err := t.events.OnRequestTimedOut(channelID, terr); err != nil {
			log.Error(err)
		}
		return
	}

	if _, ok := lastError.(graphsync.RequestCancelledErr); ok {
		// TODO Should we do anything for RequestCancelledErr ?
		return
	}

	// TODO: There seems to be a bug in graphsync. I believe it should return
	// graphsync.RequestCancelledErr on the errChan if the request's context is
	// cancelled, but it doesn't seem to be doing that
	if internalCtx.Err() != nil {
		log.Warnf("graphsync request cancelled for channel %s", channelID)
		return
	}

	if lastError != nil {
		log.Warnf("graphsync error: %s", lastError.Error())
	}

	log.Debugf("finished executing graphsync request for channel %s", channelID)

	var completeErr error
	if lastError != nil {
		completeErr = xerrors.Errorf("graphsync request failed to complete: %w", lastError)
	}

	// Used by the tests to listen for when a request completes
	if t.completedRequestListener != nil {
		t.completedRequestListener(channelID)
	}

	err := t.events.OnChannelCompleted(channelID, completeErr)
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
		extensions, err = extension.ToExtensionData(msg, t.supportedExtensions)
		if err != nil {
			return err
		}
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

	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterIncomingRequestHook(t.gsReqRecdHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterCompletedResponseListener(t.gsCompletedResponseListener))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterIncomingBlockHook(t.gsIncomingBlockHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterOutgoingBlockHook(t.gsOutgoingBlockHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterBlockSentListener(t.gsBlockSentHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterOutgoingRequestHook(t.gsOutgoingRequestHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterIncomingResponseHook(t.gsIncomingResponseHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterRequestUpdatedHook(t.gsRequestUpdatedHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterRequestorCancelledListener(t.gsRequestorCancelledListener))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterNetworkErrorListener(t.gsNetworkSendErrorListener))
	return nil
}

// Shutdown disconnects a transport interface from graphsync
func (t *Transport) Shutdown(ctx context.Context) error {
	for _, unregisterFunc := range t.unregisterFuncs {
		unregisterFunc()
	}
	t.dataLock.RLock()
	for _, cancel := range t.contextCancelMap {
		cancel()
	}
	t.dataLock.RUnlock()
	return nil
}

// UseStore tells the graphsync transport to use the given loader and storer for this channelID
func (t *Transport) UseStore(channelID datatransfer.ChannelID, loader ipld.Loader, storer ipld.Storer) error {
	t.dataLock.Lock()
	defer t.dataLock.Unlock()
	_, ok := t.stores[channelID]
	if ok {
		return nil
	}
	err := t.gs.RegisterPersistenceOption("data-transfer-"+channelID.String(), loader, storer)
	if err != nil {
		return err
	}
	t.stores[channelID] = struct{}{}
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

func (t *Transport) gsBlockSentHook(p peer.ID, request graphsync.RequestData, block graphsync.BlockData) {
	// When a data transfer is restarted, the requester sends a list of CIDs
	// that it already has. Graphsync calls the sent hook for all blocks even
	// if they are in the list (meaning, they aren't actually sent over the
	// wire). So here we check if the block was actually sent
	// over the wire before firing the data sent event.
	if block.BlockSizeOnWire() == 0 {
		return
	}

	t.dataLock.RLock()
	chid, ok := t.graphsyncRequestMap[graphsyncKey{request.ID(), p}]
	t.dataLock.RUnlock()
	if !ok {
		return
	}

	if err := t.events.OnDataSent(chid, block.Link(), block.BlockSize()); err != nil {
		log.Errorf("failed to process data sent: %+v", err)
	}
}

func (t *Transport) gsOutgoingBlockHook(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
	// When a data transfer is restarted, the requester sends a list of CIDs
	// that it already has. Graphsync calls the outgoing block hook for all
	// blocks even if they are in the list (meaning, they aren't actually going
	// to be sent over the wire). So here we check if the block is actually
	// going to be sent over the wire before firing the data queued event.
	if block.BlockSizeOnWire() == 0 {
		return
	}

	t.dataLock.RLock()
	chid, ok := t.graphsyncRequestMap[graphsyncKey{request.ID(), p}]
	t.dataLock.RUnlock()
	if !ok {
		return
	}

	msg, err := t.events.OnDataQueued(chid, block.Link(), block.BlockSize())
	if err != nil && err != datatransfer.ErrPause {
		hookActions.TerminateWithError(err)
		return
	}

	if err == datatransfer.ErrPause {
		hookActions.PauseResponse()
	}

	if msg != nil {
		extensions, err := extension.ToExtensionData(msg, t.supportedExtensions)
		if err != nil {
			hookActions.TerminateWithError(err)
			return
		}
		for _, extension := range extensions {
			hookActions.SendExtensionData(extension)
		}
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
		extensions, extensionErr := extension.ToExtensionData(responseMessage, t.supportedExtensions)
		if extensionErr != nil {
			hookActions.TerminateWithError(err)
			return
		}
		for _, extension := range extensions {
			hookActions.SendExtensionData(extension)
		}
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

	if status == graphsync.RequestCancelled {
		return
	}

	var completeErr error
	if status != graphsync.RequestCompletedFull {
		statusStr := gsResponseStatusCodeString(status)
		completeErr = xerrors.Errorf("graphsync response to peer %s did not complete: response status code %s", p, statusStr)
	}

	// Used by the tests to listen for when a response completes
	if t.completedResponseListener != nil {
		t.completedResponseListener(chid)
	}

	err := t.events.OnChannelCompleted(chid, completeErr)
	if err != nil {
		log.Error(err)
	}
}

// Remove this map once this PR lands: https://github.com/ipfs/go-graphsync/pull/148
var gsResponseStatusCodes = map[graphsync.ResponseStatusCode]string{
	graphsync.RequestAcknowledged:          "RequestAcknowledged",
	graphsync.AdditionalPeers:              "AdditionalPeers",
	graphsync.NotEnoughGas:                 "NotEnoughGas",
	graphsync.OtherProtocol:                "OtherProtocol",
	graphsync.PartialResponse:              "PartialResponse",
	graphsync.RequestPaused:                "RequestPaused",
	graphsync.RequestCompletedFull:         "RequestCompletedFull",
	graphsync.RequestCompletedPartial:      "RequestCompletedPartial",
	graphsync.RequestRejected:              "RequestRejected",
	graphsync.RequestFailedBusy:            "RequestFailedBusy",
	graphsync.RequestFailedUnknown:         "RequestFailedUnknown",
	graphsync.RequestFailedLegal:           "RequestFailedLegal",
	graphsync.RequestFailedContentNotFound: "RequestFailedContentNotFound",
	graphsync.RequestCancelled:             "RequestCancelled",
}

func gsResponseStatusCodeString(code graphsync.ResponseStatusCode) string {
	str, ok := gsResponseStatusCodes[code]
	if ok {
		return str
	}
	return gsResponseStatusCodes[graphsync.RequestFailedUnknown]
}

func (t *Transport) cleanupChannel(chid datatransfer.ChannelID, gsKey graphsyncKey) {
	delete(t.channelIDMap, chid)
	delete(t.contextCancelMap, chid)
	delete(t.pending, chid)
	delete(t.graphsyncRequestMap, gsKey)
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
		extensions, extensionErr := extension.ToExtensionData(responseMessage, t.supportedExtensions)
		if extensionErr != nil {
			hookActions.TerminateWithError(err)
			return
		}
		for _, extension := range extensions {
			hookActions.SendExtensionData(extension)
		}
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
		extensions, extensionErr := extension.ToExtensionData(responseMessage, t.supportedExtensions)
		if extensionErr != nil {
			hookActions.TerminateWithError(err)
			return
		}
		for _, extension := range extensions {
			hookActions.UpdateRequestWithExtensions(extension)
		}
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

// Called when there is a graphsync error sending data
func (t *Transport) gsNetworkSendErrorListener(p peer.ID, request graphsync.RequestData, gserr error) {
	t.dataLock.Lock()
	defer t.dataLock.Unlock()

	chid, ok := t.graphsyncRequestMap[graphsyncKey{request.ID(), p}]
	if !ok {
		return
	}

	err := t.events.OnSendDataError(chid, gserr)
	if err != nil {
		log.Errorf("failed to fire transport send error %s: %s", gserr, err)
	}
}
