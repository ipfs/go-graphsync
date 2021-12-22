package testutil

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/transport/graphsync/extension"
)

func matchDtMessage(t *testing.T, extensions []graphsync.ExtensionData) datatransfer.Message {
	var matchedExtension *graphsync.ExtensionData
	for _, ext := range extensions {
		if ext.Name == extension.ExtensionDataTransfer1_1 {
			matchedExtension = &ext
			break
		}
	}
	require.NotNil(t, matchedExtension)
	buf := bytes.NewReader(matchedExtension.Data)
	received, err := message.FromNet(buf)
	require.NoError(t, err)
	return received
}

// ReceivedGraphSyncRequest contains data about a received graphsync request
type ReceivedGraphSyncRequest struct {
	Ctx             context.Context
	P               peer.ID
	Root            ipld.Link
	Selector        ipld.Node
	Extensions      []graphsync.ExtensionData
	ResponseChan    chan graphsync.ResponseProgress
	ResponseErrChan chan error
}

// DTMessage returns the data transfer message among the graphsync extensions sent with this request
func (gsRequest ReceivedGraphSyncRequest) DTMessage(t *testing.T) datatransfer.Message {
	return matchDtMessage(t, gsRequest.Extensions)
}

type PauseRequest struct {
	RequestID graphsync.RequestID
}

type ResumeRequest struct {
	RequestID  graphsync.RequestID
	Extensions []graphsync.ExtensionData
}

// DTMessage returns the data transfer message among the graphsync extensions sent with this request
func (resumeRequest ResumeRequest) DTMessage(t *testing.T) datatransfer.Message {
	return matchDtMessage(t, resumeRequest.Extensions)
}

type PauseResponse struct {
	P         peer.ID
	RequestID graphsync.RequestID
}

type ResumeResponse struct {
	P          peer.ID
	RequestID  graphsync.RequestID
	Extensions []graphsync.ExtensionData
}

// DTMessage returns the data transfer message among the graphsync extensions sent with this request
func (resumeResponse ResumeResponse) DTMessage(t *testing.T) datatransfer.Message {
	return matchDtMessage(t, resumeResponse.Extensions)
}

type CancelResponse struct {
	P         peer.ID
	RequestID graphsync.RequestID
}

// FakeGraphSync implements a GraphExchange but does nothing
type FakeGraphSync struct {
	requests                     chan ReceivedGraphSyncRequest // records calls to fakeGraphSync.Request
	pauseRequests                chan PauseRequest
	resumeRequests               chan ResumeRequest
	pauseResponses               chan PauseResponse
	resumeResponses              chan ResumeResponse
	cancelResponses              chan CancelResponse
	cancelRequests               chan graphsync.RequestID
	persistenceOptionsLk         sync.RWMutex
	persistenceOptions           map[string]ipld.LinkSystem
	leaveRequestsOpen            bool
	OutgoingRequestHook          graphsync.OnOutgoingRequestHook
	IncomingBlockHook            graphsync.OnIncomingBlockHook
	OutgoingBlockHook            graphsync.OnOutgoingBlockHook
	IncomingRequestQueuedHook    graphsync.OnIncomingRequestQueuedHook
	IncomingRequestHook          graphsync.OnIncomingRequestHook
	CompletedResponseListener    graphsync.OnResponseCompletedListener
	RequestUpdatedHook           graphsync.OnRequestUpdatedHook
	IncomingResponseHook         graphsync.OnIncomingResponseHook
	RequestorCancelledListener   graphsync.OnRequestorCancelledListener
	BlockSentListener            graphsync.OnBlockSentListener
	NetworkErrorListener         graphsync.OnNetworkErrorListener
	ReceiverNetworkErrorListener graphsync.OnReceiverNetworkErrorListener
}

// NewFakeGraphSync returns a new fake graphsync implementation
func NewFakeGraphSync() *FakeGraphSync {
	return &FakeGraphSync{
		requests:           make(chan ReceivedGraphSyncRequest, 2),
		pauseRequests:      make(chan PauseRequest, 1),
		resumeRequests:     make(chan ResumeRequest, 1),
		pauseResponses:     make(chan PauseResponse, 1),
		resumeResponses:    make(chan ResumeResponse, 1),
		cancelResponses:    make(chan CancelResponse, 1),
		cancelRequests:     make(chan graphsync.RequestID, 1),
		persistenceOptions: make(map[string]ipld.LinkSystem),
	}
}

func (fgs *FakeGraphSync) LeaveRequestsOpen() {
	fgs.leaveRequestsOpen = true
}

// AssertNoRequestReceived asserts that no requests should ahve been received by this graphsync implementation
func (fgs *FakeGraphSync) AssertNoRequestReceived(t *testing.T) {
	require.Empty(t, fgs.requests, "should not receive request")
}

// AssertRequestReceived asserts a request should be received before the context closes (and returns said request)
func (fgs *FakeGraphSync) AssertRequestReceived(ctx context.Context, t *testing.T) ReceivedGraphSyncRequest {
	var requestReceived ReceivedGraphSyncRequest
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case requestReceived = <-fgs.requests:
	}
	return requestReceived
}

// AssertNoPauseRequestReceived asserts that no pause requests should ahve been received by this graphsync implementation
func (fgs *FakeGraphSync) AssertNoPauseRequestReceived(t *testing.T) {
	require.Empty(t, fgs.pauseRequests, "should not receive pause request")
}

// AssertPauseRequestReceived asserts a pause request should be received before the context closes (and returns said request)
func (fgs *FakeGraphSync) AssertPauseRequestReceived(ctx context.Context, t *testing.T) PauseRequest {
	var pauseRequestReceived PauseRequest
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case pauseRequestReceived = <-fgs.pauseRequests:
	}
	return pauseRequestReceived
}

// AssertNoResumeRequestReceived asserts that no resume requests should ahve been received by this graphsync implementation
func (fgs *FakeGraphSync) AssertNoResumeRequestReceived(t *testing.T) {
	require.Empty(t, fgs.resumeRequests, "should not receive resume request")
}

// AssertResumeRequestReceived asserts a resume request should be received before the context closes (and returns said request)
func (fgs *FakeGraphSync) AssertResumeRequestReceived(ctx context.Context, t *testing.T) ResumeRequest {
	var resumeRequestReceived ResumeRequest
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case resumeRequestReceived = <-fgs.resumeRequests:
	}
	return resumeRequestReceived
}

// AssertNoPauseResponseReceived asserts that no pause requests should ahve been received by this graphsync implementation
func (fgs *FakeGraphSync) AssertNoPauseResponseReceived(t *testing.T) {
	require.Empty(t, fgs.pauseResponses, "should not receive pause request")
}

// AssertPauseResponseReceived asserts a pause request should be received before the context closes (and returns said request)
func (fgs *FakeGraphSync) AssertPauseResponseReceived(ctx context.Context, t *testing.T) PauseResponse {
	var pauseResponseReceived PauseResponse
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case pauseResponseReceived = <-fgs.pauseResponses:
	}
	return pauseResponseReceived
}

// AssertNoResumeResponseReceived asserts that no resume requests should ahve been received by this graphsync implementation
func (fgs *FakeGraphSync) AssertNoResumeResponseReceived(t *testing.T) {
	require.Empty(t, fgs.resumeResponses, "should not receive resume request")
}

// AssertResumeResponseReceived asserts a resume request should be received before the context closes (and returns said request)
func (fgs *FakeGraphSync) AssertResumeResponseReceived(ctx context.Context, t *testing.T) ResumeResponse {
	var resumeResponseReceived ResumeResponse
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case resumeResponseReceived = <-fgs.resumeResponses:
	}
	return resumeResponseReceived
}

// AssertNoCancelResponseReceived asserts that no responses were cancelled by thiss graphsync implementation
func (fgs *FakeGraphSync) AssertNoCancelResponseReceived(t *testing.T) {
	require.Empty(t, fgs.cancelResponses, "should not cancel request")
}

// AssertCancelResponseReceived asserts a response was cancelled before the context closes (and returns said response)
func (fgs *FakeGraphSync) AssertCancelResponseReceived(ctx context.Context, t *testing.T) CancelResponse {
	var cancelResponseReceived CancelResponse
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case cancelResponseReceived = <-fgs.cancelResponses:
	}
	return cancelResponseReceived
}

// AssertCancelRequestReceived asserts a request was cancelled
func (fgs *FakeGraphSync) AssertCancelRequestReceived(ctx context.Context, t *testing.T) graphsync.RequestID {
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
		return 0
	case requestID := <-fgs.cancelRequests:
		return requestID
	}
}

// AssertHasPersistenceOption verifies that a persistence option was registered
func (fgs *FakeGraphSync) AssertHasPersistenceOption(t *testing.T, name string) ipld.LinkSystem {
	fgs.persistenceOptionsLk.RLock()
	defer fgs.persistenceOptionsLk.RUnlock()
	option, ok := fgs.persistenceOptions[name]
	require.Truef(t, ok, "persistence option %s should be registered", name)
	return option
}

// AssertDoesNotHavePersistenceOption verifies that a persistence option is not registered
func (fgs *FakeGraphSync) AssertDoesNotHavePersistenceOption(t *testing.T, name string) {
	fgs.persistenceOptionsLk.RLock()
	defer fgs.persistenceOptionsLk.RUnlock()
	_, ok := fgs.persistenceOptions[name]
	require.Falsef(t, ok, "persistence option %s should be registered", name)
}

// Request initiates a new GraphSync request to the given peer using the given selector spec.
func (fgs *FakeGraphSync) Request(ctx context.Context, p peer.ID, root ipld.Link, selector ipld.Node, extensions ...graphsync.ExtensionData) (<-chan graphsync.ResponseProgress, <-chan error) {
	errors := make(chan error)
	responses := make(chan graphsync.ResponseProgress)
	fgs.requests <- ReceivedGraphSyncRequest{ctx, p, root, selector, extensions, responses, errors}
	if !fgs.leaveRequestsOpen {
		close(responses)
		close(errors)
	}
	return responses, errors
}

// RegisterPersistenceOption registers an alternate loader/storer combo that can be substituted for the default
func (fgs *FakeGraphSync) RegisterPersistenceOption(name string, lsys ipld.LinkSystem) error {
	fgs.persistenceOptionsLk.Lock()
	defer fgs.persistenceOptionsLk.Unlock()
	_, ok := fgs.persistenceOptions[name]
	if ok {
		return errors.New("already registered")
	}
	fgs.persistenceOptions[name] = lsys
	return nil
}

// UnregisterPersistenceOption unregisters an existing loader/storer combo
func (fgs *FakeGraphSync) UnregisterPersistenceOption(name string) error {
	fgs.persistenceOptionsLk.Lock()
	defer fgs.persistenceOptionsLk.Unlock()
	delete(fgs.persistenceOptions, name)
	return nil
}

// RegisterIncomingRequestHook adds a hook that runs when a request is received
func (fgs *FakeGraphSync) RegisterIncomingRequestHook(hook graphsync.OnIncomingRequestHook) graphsync.UnregisterHookFunc {
	fgs.IncomingRequestHook = hook
	return func() {
		fgs.IncomingRequestHook = nil
	}
}

// RegisterIncomingRequestQueuedHook adds a hook that runs when an incoming GS request is queued.
func (fgs *FakeGraphSync) RegisterIncomingRequestQueuedHook(hook graphsync.OnIncomingRequestQueuedHook) graphsync.UnregisterHookFunc {
	fgs.IncomingRequestQueuedHook = hook
	return func() {
		fgs.IncomingRequestQueuedHook = nil
	}
}

// RegisterIncomingResponseHook adds a hook that runs when a response is received
func (fgs *FakeGraphSync) RegisterIncomingResponseHook(hook graphsync.OnIncomingResponseHook) graphsync.UnregisterHookFunc {
	fgs.IncomingResponseHook = hook
	return func() {
		fgs.IncomingResponseHook = nil
	}
}

// RegisterOutgoingRequestHook adds a hook that runs immediately prior to sending a new request
func (fgs *FakeGraphSync) RegisterOutgoingRequestHook(hook graphsync.OnOutgoingRequestHook) graphsync.UnregisterHookFunc {
	fgs.OutgoingRequestHook = hook
	return func() {
		fgs.OutgoingRequestHook = nil
	}
}

// RegisterOutgoingBlockHook adds a hook that runs every time a block is sent from a responder
func (fgs *FakeGraphSync) RegisterOutgoingBlockHook(hook graphsync.OnOutgoingBlockHook) graphsync.UnregisterHookFunc {
	fgs.OutgoingBlockHook = hook
	return func() {
		fgs.OutgoingBlockHook = nil
	}
}

// RegisterIncomingBlockHook adds a hook that runs every time a block is received by the requestor
func (fgs *FakeGraphSync) RegisterIncomingBlockHook(hook graphsync.OnIncomingBlockHook) graphsync.UnregisterHookFunc {
	fgs.IncomingBlockHook = hook
	return func() {
		fgs.IncomingBlockHook = nil
	}
}

// RegisterRequestUpdatedHook adds a hook that runs every time an update to a request is received
func (fgs *FakeGraphSync) RegisterRequestUpdatedHook(hook graphsync.OnRequestUpdatedHook) graphsync.UnregisterHookFunc {
	fgs.RequestUpdatedHook = hook
	return func() {
		fgs.RequestUpdatedHook = nil
	}
}

// RegisterCompletedResponseListener adds a listener on the responder for completed responses
func (fgs *FakeGraphSync) RegisterCompletedResponseListener(listener graphsync.OnResponseCompletedListener) graphsync.UnregisterHookFunc {
	fgs.CompletedResponseListener = listener
	return func() {
		fgs.CompletedResponseListener = nil
	}
}

// UnpauseResponse unpauses a response that was paused in a block hook based on peer ID and request ID
func (fgs *FakeGraphSync) UnpauseResponse(p peer.ID, requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	fgs.resumeResponses <- ResumeResponse{p, requestID, extensions}
	return nil
}

// PauseResponse pauses a response based on peer ID and request ID
func (fgs *FakeGraphSync) PauseResponse(p peer.ID, requestID graphsync.RequestID) error {
	fgs.pauseResponses <- PauseResponse{p, requestID}
	return nil
}

// UnpauseRequest unpauses a request that was paused in a block hook based on request ID
func (fgs *FakeGraphSync) UnpauseRequest(requestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	fgs.resumeRequests <- ResumeRequest{requestID, extensions}
	return nil
}

// PauseRequest unpauses a response that was paused in a block hook based on peer ID and request ID
func (fgs *FakeGraphSync) PauseRequest(requestID graphsync.RequestID) error {
	fgs.pauseRequests <- PauseRequest{requestID}
	return nil
}

func (fgs *FakeGraphSync) CancelRequest(ctx context.Context, requestID graphsync.RequestID) error {
	fgs.cancelRequests <- requestID
	return nil
}

// CancelResponse cancels a response in progress
func (fgs *FakeGraphSync) CancelResponse(p peer.ID, requestID graphsync.RequestID) error {
	fgs.cancelResponses <- CancelResponse{p, requestID}
	return nil
}

// RegisterRequestorCancelledListener adds a listener on the responder for requests cancelled by the requestor
func (fgs *FakeGraphSync) RegisterRequestorCancelledListener(listener graphsync.OnRequestorCancelledListener) graphsync.UnregisterHookFunc {
	fgs.RequestorCancelledListener = listener
	return func() {
		fgs.RequestorCancelledListener = nil
	}
}

// RegisterBlockSentListener adds a listener on the responder as blocks go out
func (fgs *FakeGraphSync) RegisterBlockSentListener(listener graphsync.OnBlockSentListener) graphsync.UnregisterHookFunc {
	fgs.BlockSentListener = listener
	return func() {
		fgs.BlockSentListener = nil
	}
}

// RegisterNetworkErrorListener adds a listener on the responder as blocks go out
func (fgs *FakeGraphSync) RegisterNetworkErrorListener(listener graphsync.OnNetworkErrorListener) graphsync.UnregisterHookFunc {
	fgs.NetworkErrorListener = listener
	return func() {
		fgs.NetworkErrorListener = nil
	}
}

// RegisterNetworkErrorListener adds a listener on the responder as blocks go out
func (fgs *FakeGraphSync) RegisterReceiverNetworkErrorListener(listener graphsync.OnReceiverNetworkErrorListener) graphsync.UnregisterHookFunc {
	fgs.ReceiverNetworkErrorListener = listener
	return func() {
		fgs.ReceiverNetworkErrorListener = nil
	}
}

func (fgs *FakeGraphSync) Stats() graphsync.Stats {
	return graphsync.Stats{}
}

func (fgs *FakeGraphSync) RegisterOutgoingRequestProcessingListener(graphsync.OnOutgoingRequestProcessingListener) graphsync.UnregisterHookFunc {
	// TODO: just a stub for now, hopefully nobody needs this
	return func() {}
}

var _ graphsync.GraphExchange = &FakeGraphSync{}

type fakeBlkData struct {
	link  ipld.Link
	size  uint64
	index int64
}

func (fbd fakeBlkData) Link() ipld.Link {
	return fbd.link
}

func (fbd fakeBlkData) BlockSize() uint64 {
	return fbd.size
}

func (fbd fakeBlkData) BlockSizeOnWire() uint64 {
	return fbd.size
}

func (fbd fakeBlkData) Index() int64 {
	return fbd.index
}

// NewFakeBlockData returns a fake block that matches the block data interface
func NewFakeBlockData() graphsync.BlockData {
	return &fakeBlkData{
		link:  cidlink.Link{Cid: GenerateCids(1)[0]},
		size:  rand.Uint64(),
		index: int64(rand.Uint32()),
	}
}

type fakeRequest struct {
	id         graphsync.RequestID
	root       cid.Cid
	selector   ipld.Node
	priority   graphsync.Priority
	isCancel   bool
	extensions map[graphsync.ExtensionName][]byte
}

// ID Returns the request ID for this Request
func (fr *fakeRequest) ID() graphsync.RequestID {
	return fr.id
}

// Root returns the CID to the root block of this request
func (fr *fakeRequest) Root() cid.Cid {
	return fr.root
}

// Selector returns the byte representation of the selector for this request
func (fr *fakeRequest) Selector() ipld.Node {
	return fr.selector
}

// Priority returns the priority of this request
func (fr *fakeRequest) Priority() graphsync.Priority {
	return fr.priority
}

// Extension returns the content for an extension on a response, or errors
// if extension is not present
func (fr *fakeRequest) Extension(name graphsync.ExtensionName) ([]byte, bool) {
	data, has := fr.extensions[name]
	return data, has
}

// IsCancel returns true if this particular request is being cancelled
func (fr *fakeRequest) IsCancel() bool {
	return fr.isCancel
}

// NewFakeRequest returns a fake request that matches the request data interface
func NewFakeRequest(id graphsync.RequestID, extensions map[graphsync.ExtensionName][]byte) graphsync.RequestData {
	return &fakeRequest{
		id:         id,
		root:       GenerateCids(1)[0],
		selector:   allSelector,
		priority:   graphsync.Priority(rand.Int()),
		isCancel:   false,
		extensions: extensions,
	}
}

type fakeResponse struct {
	id         graphsync.RequestID
	status     graphsync.ResponseStatusCode
	extensions map[graphsync.ExtensionName][]byte
}

// RequestID returns the request ID for this response
func (fr *fakeResponse) RequestID() graphsync.RequestID {
	return fr.id
}

// Status returns the status for a response
func (fr *fakeResponse) Status() graphsync.ResponseStatusCode {
	return fr.status
}

// Extension returns the content for an extension on a response, or errors
// if extension is not present
func (fr *fakeResponse) Extension(name graphsync.ExtensionName) ([]byte, bool) {
	data, has := fr.extensions[name]
	return data, has
}

// NewFakeResponse returns a fake response that matches the response data interface
func NewFakeResponse(id graphsync.RequestID, extensions map[graphsync.ExtensionName][]byte, status graphsync.ResponseStatusCode) graphsync.ResponseData {
	return &fakeResponse{
		id:         id,
		status:     status,
		extensions: extensions,
	}
}

type FakeOutgoingRequestHookActions struct {
	PersistenceOption string
}

func (fa *FakeOutgoingRequestHookActions) UsePersistenceOption(name string) {
	fa.PersistenceOption = name
}
func (fa *FakeOutgoingRequestHookActions) UseLinkTargetNodePrototypeChooser(_ traversal.LinkTargetNodePrototypeChooser) {
}

var _ graphsync.OutgoingRequestHookActions = &FakeOutgoingRequestHookActions{}

type FakeIncomingBlockHookActions struct {
	TerminationError error
	SentExtensions   []graphsync.ExtensionData
	Paused           bool
}

func (fa *FakeIncomingBlockHookActions) TerminateWithError(err error) {
	fa.TerminationError = err
}

func (fa *FakeIncomingBlockHookActions) UpdateRequestWithExtensions(extensions ...graphsync.ExtensionData) {
	fa.SentExtensions = append(fa.SentExtensions, extensions...)
}

func (fa *FakeIncomingBlockHookActions) PauseRequest() {
	fa.Paused = true
}

var _ graphsync.IncomingBlockHookActions = &FakeIncomingBlockHookActions{}

type FakeOutgoingBlockHookActions struct {
	TerminationError error
	SentExtensions   []graphsync.ExtensionData
	Paused           bool
}

func (fa *FakeOutgoingBlockHookActions) SendExtensionData(extension graphsync.ExtensionData) {
	fa.SentExtensions = append(fa.SentExtensions, extension)
}

func (fa *FakeOutgoingBlockHookActions) TerminateWithError(err error) {
	fa.TerminationError = err
}

func (fa *FakeOutgoingBlockHookActions) PauseResponse() {
	fa.Paused = true
}

var _ graphsync.OutgoingBlockHookActions = &FakeOutgoingBlockHookActions{}

type FakeIncomingRequestHookActions struct {
	PersistenceOption string
	TerminationError  error
	Validated         bool
	SentExtensions    []graphsync.ExtensionData
	Paused            bool
}

func (fa *FakeIncomingRequestHookActions) SendExtensionData(ext graphsync.ExtensionData) {
	fa.SentExtensions = append(fa.SentExtensions, ext)
}

func (fa *FakeIncomingRequestHookActions) UsePersistenceOption(name string) {
	fa.PersistenceOption = name
}

func (fa *FakeIncomingRequestHookActions) UseLinkTargetNodePrototypeChooser(_ traversal.LinkTargetNodePrototypeChooser) {
}

func (fa *FakeIncomingRequestHookActions) TerminateWithError(err error) {
	fa.TerminationError = err
}

func (fa *FakeIncomingRequestHookActions) ValidateRequest() {
	fa.Validated = true
}

func (fa *FakeIncomingRequestHookActions) PauseResponse() {
	fa.Paused = true
}

var _ graphsync.IncomingRequestHookActions = &FakeIncomingRequestHookActions{}

type FakeRequestUpdatedActions struct {
	TerminationError error
	SentExtensions   []graphsync.ExtensionData
	Unpaused         bool
}

func (fa *FakeRequestUpdatedActions) SendExtensionData(extension graphsync.ExtensionData) {
	fa.SentExtensions = append(fa.SentExtensions, extension)
}

func (fa *FakeRequestUpdatedActions) TerminateWithError(err error) {
	fa.TerminationError = err
}

func (fa *FakeRequestUpdatedActions) UnpauseResponse() {
	fa.Unpaused = true
}

var _ graphsync.RequestUpdatedHookActions = &FakeRequestUpdatedActions{}

type FakeIncomingResponseHookActions struct {
	TerminationError error
	SentExtensions   []graphsync.ExtensionData
}

func (fa *FakeIncomingResponseHookActions) TerminateWithError(err error) {
	fa.TerminationError = err
}

func (fa *FakeIncomingResponseHookActions) UpdateRequestWithExtensions(extensions ...graphsync.ExtensionData) {
	fa.SentExtensions = append(fa.SentExtensions, extensions...)
}

var _ graphsync.IncomingResponseHookActions = &FakeIncomingResponseHookActions{}

type FakeRequestQueuedHookActions struct {
	ctxAugFuncs []func(context.Context) context.Context
}

func (fa *FakeRequestQueuedHookActions) AugmentContext(ctxAugFunc func(reqCtx context.Context) context.Context) {
	fa.ctxAugFuncs = append(fa.ctxAugFuncs, ctxAugFunc)
}

var _ graphsync.RequestQueuedHookActions = &FakeRequestQueuedHookActions{}
