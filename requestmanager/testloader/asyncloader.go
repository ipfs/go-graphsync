package testloader

import (
	"context"
	"sync"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipfs/go-graphsync/testutil"
)

type requestKey struct {
	p         peer.ID
	requestID graphsync.RequestID
	link      ipld.Link
}

type storeKey struct {
	requestID graphsync.RequestID
	storeName string
}

// FakeAsyncLoader simultates the requestmanager.AsyncLoader interface
// with mocked responses and can also be used to simulate a
// executor.AsycLoadFn -- all responses are stubbed and no actual processing is
// done
type FakeAsyncLoader struct {
	responseChannelsLk sync.RWMutex
	responseChannels   map[requestKey]chan types.AsyncLoadResult
	responses          chan map[graphsync.RequestID]metadata.Metadata
	blks               chan []blocks.Block
	storesRequestedLk  sync.RWMutex
	storesRequested    map[storeKey]struct{}
	cb                 func(graphsync.RequestID, ipld.Link, <-chan types.AsyncLoadResult)
}

// NewFakeAsyncLoader returns a new FakeAsyncLoader instance
func NewFakeAsyncLoader() *FakeAsyncLoader {
	return &FakeAsyncLoader{
		responseChannels: make(map[requestKey]chan types.AsyncLoadResult),
		responses:        make(chan map[graphsync.RequestID]metadata.Metadata, 10),
		blks:             make(chan []blocks.Block, 10),
		storesRequested:  make(map[storeKey]struct{}),
	}
}

// StartRequest just requests what store was requested for a given requestID
func (fal *FakeAsyncLoader) StartRequest(requestID graphsync.RequestID, name string) error {
	fal.storesRequestedLk.Lock()
	fal.storesRequested[storeKey{requestID, name}] = struct{}{}
	fal.storesRequestedLk.Unlock()
	return nil
}

// ProcessResponse just records values passed to verify expectations later
func (fal *FakeAsyncLoader) ProcessResponse(responses map[graphsync.RequestID]metadata.Metadata,
	blks []blocks.Block) {
	fal.responses <- responses
	fal.blks <- blks
}

// VerifyLastProcessedBlocks verifies the blocks passed to the last call to ProcessResponse
// match the expected ones
func (fal *FakeAsyncLoader) VerifyLastProcessedBlocks(ctx context.Context, t *testing.T, expectedBlocks []blocks.Block) {
	t.Helper()
	var processedBlocks []blocks.Block
	testutil.AssertReceive(ctx, t, fal.blks, &processedBlocks, "did not process blocks")
	require.Equal(t, expectedBlocks, processedBlocks, "did not process correct blocks")
}

// VerifyLastProcessedResponses verifies the responses passed to the last call to ProcessResponse
// match the expected ones
func (fal *FakeAsyncLoader) VerifyLastProcessedResponses(ctx context.Context, t *testing.T,
	expectedResponses map[graphsync.RequestID]metadata.Metadata) {
	t.Helper()
	var responses map[graphsync.RequestID]metadata.Metadata
	testutil.AssertReceive(ctx, t, fal.responses, &responses, "did not process responses")
	require.Equal(t, expectedResponses, responses, "did not process correct responses")
}

// VerifyNoRemainingData verifies no outstanding response channels are open for the given
// RequestID (CleanupRequest was called last)
func (fal *FakeAsyncLoader) VerifyNoRemainingData(t *testing.T, requestID graphsync.RequestID) {
	t.Helper()
	fal.responseChannelsLk.RLock()
	for key := range fal.responseChannels {
		require.NotEqual(t, key.requestID, requestID, "did not clean up request properly")
	}
	fal.responseChannelsLk.RUnlock()
}

// VerifyStoreUsed verifies the given store was used for the given request
func (fal *FakeAsyncLoader) VerifyStoreUsed(t *testing.T, requestID graphsync.RequestID, storeName string) {
	t.Helper()
	fal.storesRequestedLk.RLock()
	_, ok := fal.storesRequested[storeKey{requestID, storeName}]
	require.True(t, ok, "request should load from correct store")
	fal.storesRequestedLk.RUnlock()
}

func (fal *FakeAsyncLoader) asyncLoad(p peer.ID, requestID graphsync.RequestID, link ipld.Link, linkContext ipld.LinkContext) chan types.AsyncLoadResult {
	fal.responseChannelsLk.Lock()
	responseChannel, ok := fal.responseChannels[requestKey{p, requestID, link}]
	if !ok {
		responseChannel = make(chan types.AsyncLoadResult, 1)
		fal.responseChannels[requestKey{p, requestID, link}] = responseChannel
	}
	fal.responseChannelsLk.Unlock()
	return responseChannel
}

// OnAsyncLoad allows you to listen for load requests to the loader and perform other actions or tests
func (fal *FakeAsyncLoader) OnAsyncLoad(cb func(graphsync.RequestID, ipld.Link, <-chan types.AsyncLoadResult)) {
	fal.cb = cb
}

// AsyncLoad simulates an asynchronous load with responses stubbed by ResponseOn & SuccessResponseOn
func (fal *FakeAsyncLoader) AsyncLoad(p peer.ID, requestID graphsync.RequestID, link ipld.Link, linkContext ipld.LinkContext) <-chan types.AsyncLoadResult {
	res := fal.asyncLoad(p, requestID, link, linkContext)
	if fal.cb != nil {
		fal.cb(requestID, link, res)
	}
	return res
}

// CompleteResponsesFor in the case of the test loader does nothing
func (fal *FakeAsyncLoader) CompleteResponsesFor(requestID graphsync.RequestID) {}

// CleanupRequest simulates the effect of cleaning up the request by removing any response channels
// for the request
func (fal *FakeAsyncLoader) CleanupRequest(p peer.ID, requestID graphsync.RequestID) {
	fal.responseChannelsLk.Lock()
	for key := range fal.responseChannels {
		if key.requestID == requestID {
			delete(fal.responseChannels, key)
		}
	}
	fal.responseChannelsLk.Unlock()
}

// ResponseOn sets the value returned when the given link is loaded for the given request. Because it's an
// "asynchronous" load, this can be called AFTER the attempt to load this link -- and the client will only get
// the response at that point
func (fal *FakeAsyncLoader) ResponseOn(p peer.ID, requestID graphsync.RequestID, link ipld.Link, result types.AsyncLoadResult) {
	responseChannel := fal.asyncLoad(p, requestID, link, ipld.LinkContext{})
	responseChannel <- result
	close(responseChannel)
}

// SuccessResponseOn is convenience function for setting several asynchronous responses at once as all successes
// and returning the given blocks
func (fal *FakeAsyncLoader) SuccessResponseOn(p peer.ID, requestID graphsync.RequestID, blks []blocks.Block) {
	for _, block := range blks {
		fal.ResponseOn(p, requestID, cidlink.Link{Cid: block.Cid()}, types.AsyncLoadResult{Data: block.RawData(), Local: false, Err: nil})
	}
}
