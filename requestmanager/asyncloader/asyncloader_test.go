package asyncloader

import (
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/testutil"
	ipld "github.com/ipld/go-ipld-prime"
)

func TestAsyncLoadInitialLoadSucceedsLocallyPresent(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	block := testutil.GenerateBlocksOfSize(1, 100)[0]
	writer, commit, err := storer(ipld.LinkContext{})
	require.NoError(t, err)
	_, err = writer.Write(block.RawData())
	require.NoError(t, err, "seeds block store")
	link := cidlink.Link{Cid: block.Cid()}
	err = commit(link)
	require.NoError(t, err, "seeds block store")

	wrappedLoader := func(link ipld.Link, linkContext ipld.LinkContext) (io.Reader, error) {
		callCount++
		return loader(link, linkContext)
	}

	asyncLoader := New(ctx, wrappedLoader, storer)
	asyncLoader.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	resultChan := asyncLoader.AsyncLoad(requestID, link)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.NotNil(t, result.Data, "should send response")
	require.Nil(t, result.Err, "should not send error")

	require.NotZero(t, callCount, "should attempt to load link from local store")
}

func TestAsyncLoadInitialLoadSucceedsResponsePresent(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blocks := testutil.GenerateBlocksOfSize(1, 100)
	block := blocks[0]

	link := cidlink.Link{Cid: block.Cid()}

	wrappedLoader := func(link ipld.Link, linkContext ipld.LinkContext) (io.Reader, error) {
		callCount++
		return loader(link, linkContext)
	}

	asyncLoader := New(ctx, wrappedLoader, storer)
	asyncLoader.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	responses := map[graphsync.RequestID]metadata.Metadata{
		requestID: metadata.Metadata{
			metadata.Item{
				Link:         link,
				BlockPresent: true,
			},
		},
	}
	asyncLoader.ProcessResponse(responses, blocks)
	resultChan := asyncLoader.AsyncLoad(requestID, link)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.NotNil(t, result.Data, "should send response")
	require.Nil(t, result.Err, "should not send error")

	require.Zero(t, callCount, "should not attempt to load link from local store")
	require.Equal(t, blockStore[link], block.RawData(), "should store block")
}

func TestAsyncLoadInitialLoadFails(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)

	wrappedLoader := func(link ipld.Link, linkContext ipld.LinkContext) (io.Reader, error) {
		callCount++
		return loader(link, linkContext)
	}

	asyncLoader := New(ctx, wrappedLoader, storer)
	asyncLoader.Startup()

	link := testutil.NewTestLink()
	requestID := graphsync.RequestID(rand.Int31())

	responses := map[graphsync.RequestID]metadata.Metadata{
		requestID: metadata.Metadata{
			metadata.Item{
				Link:         link,
				BlockPresent: false,
			},
		},
	}
	asyncLoader.ProcessResponse(responses, nil)

	resultChan := asyncLoader.AsyncLoad(requestID, link)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.Nil(t, result.Data, "should not have sent responses")
	require.NotNil(t, result.Err, "should have sent an error")
	require.Zero(t, callCount, "should not attempt to load link from local store")
}

func TestAsyncLoadInitialLoadIndeterminateWhenRequestNotInProgress(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)

	wrappedLoader := func(link ipld.Link, linkContext ipld.LinkContext) (io.Reader, error) {
		callCount++
		return loader(link, linkContext)
	}

	asyncLoader := New(ctx, wrappedLoader, storer)
	asyncLoader.Startup()

	link := testutil.NewTestLink()
	requestID := graphsync.RequestID(rand.Int31())
	resultChan := asyncLoader.AsyncLoad(requestID, link)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.Nil(t, result.Data, "should not have sent responses")
	require.NotNil(t, result.Err, "should have sent an error")
	require.NotZero(t, callCount, "should attempt to load link from local store")
}

func TestAsyncLoadInitialLoadIndeterminateThenSucceeds(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blocks := testutil.GenerateBlocksOfSize(1, 100)
	block := blocks[0]

	link := cidlink.Link{Cid: block.Cid()}
	called := make(chan struct{}, 2)
	wrappedLoader := func(link ipld.Link, linkContext ipld.LinkContext) (io.Reader, error) {
		called <- struct{}{}
		callCount++
		return loader(link, linkContext)
	}

	asyncLoader := New(ctx, wrappedLoader, storer)
	asyncLoader.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	asyncLoader.StartRequest(requestID)
	resultChan := asyncLoader.AsyncLoad(requestID, link)

	testutil.AssertDoesReceiveFirst(t, called, "attemps load with no result", resultChan, ctx.Done())

	responses := map[graphsync.RequestID]metadata.Metadata{
		requestID: metadata.Metadata{
			metadata.Item{
				Link:         link,
				BlockPresent: true,
			},
		},
	}
	asyncLoader.ProcessResponse(responses, blocks)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.NotNil(t, result.Data, "should send response")
	require.Nil(t, result.Err, "should not send error")

	require.Equal(t, callCount, 1, "should attempt to load from local store exactly once")

	require.Equal(t, blockStore[link], block.RawData(), "should store block")
}

func TestAsyncLoadInitialLoadIndeterminateThenFails(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)

	link := testutil.NewTestLink()
	called := make(chan struct{}, 2)
	wrappedLoader := func(link ipld.Link, linkContext ipld.LinkContext) (io.Reader, error) {
		called <- struct{}{}
		callCount++
		return loader(link, linkContext)
	}

	asyncLoader := New(ctx, wrappedLoader, storer)
	asyncLoader.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	asyncLoader.StartRequest(requestID)
	resultChan := asyncLoader.AsyncLoad(requestID, link)

	testutil.AssertDoesReceiveFirst(t, called, "attemps load with no result", resultChan, ctx.Done())
	responses := map[graphsync.RequestID]metadata.Metadata{
		requestID: metadata.Metadata{
			metadata.Item{
				Link:         link,
				BlockPresent: false,
			},
		},
	}
	asyncLoader.ProcessResponse(responses, nil)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.Nil(t, result.Data, "should not have sent responses")
	require.NotNil(t, result.Err, "should have sent an error")
	require.Equal(t, callCount, 1, "should attempt to load from local store exactly once")
}

func TestAsyncLoadInitialLoadIndeterminateThenRequestFinishes(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)

	link := testutil.NewTestLink()
	called := make(chan struct{}, 2)
	wrappedLoader := func(link ipld.Link, linkContext ipld.LinkContext) (io.Reader, error) {
		called <- struct{}{}
		callCount++
		return loader(link, linkContext)
	}

	asyncLoader := New(ctx, wrappedLoader, storer)
	asyncLoader.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	asyncLoader.StartRequest(requestID)
	resultChan := asyncLoader.AsyncLoad(requestID, link)

	testutil.AssertDoesReceiveFirst(t, called, "attemps load with no result", resultChan, ctx.Done())
	asyncLoader.CompleteResponsesFor(requestID)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.Nil(t, result.Data, "should not have sent responses")
	require.NotNil(t, result.Err, "should have sent an error")
	require.Equal(t, callCount, 1, "should attempt to load from local store exactly once")
}

func TestAsyncLoadTwiceLoadsLocallySecondTime(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	blocks := testutil.GenerateBlocksOfSize(1, 100)
	block := blocks[0]

	link := cidlink.Link{Cid: block.Cid()}

	wrappedLoader := func(link ipld.Link, linkContext ipld.LinkContext) (io.Reader, error) {
		callCount++
		return loader(link, linkContext)
	}

	asyncLoader := New(ctx, wrappedLoader, storer)
	asyncLoader.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	responses := map[graphsync.RequestID]metadata.Metadata{
		requestID: metadata.Metadata{
			metadata.Item{
				Link:         link,
				BlockPresent: true,
			},
		},
	}
	asyncLoader.ProcessResponse(responses, blocks)
	resultChan := asyncLoader.AsyncLoad(requestID, link)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.NotNil(t, result.Data, "should send response")
	require.Nil(t, result.Err, "should not send error")

	require.Zero(t, callCount, "should not attempt to load link from local store")
	require.Equal(t, blockStore[link], block.RawData(), "should store block")

	resultChan = asyncLoader.AsyncLoad(requestID, link)

	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.NotNil(t, result.Data, "should send response")
	require.Nil(t, result.Err, "should not send error")
	require.NotZero(t, callCount, "should attempt to load link from local store")
	require.Equal(t, blockStore[link], block.RawData(), "should store block")
}
