package loader

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal"
)

type callParams struct {
	requestID graphsync.RequestID
	link      ipld.Link
}

func makeAsyncLoadFn(responseChan chan types.AsyncLoadResult, calls chan callParams) AsyncLoadFn {
	return func(requestID graphsync.RequestID, link ipld.Link) <-chan types.AsyncLoadResult {
		calls <- callParams{requestID, link}
		return responseChan
	}
}

func TestWrappedAsyncLoaderReturnsValues(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	responseChan := make(chan types.AsyncLoadResult, 1)
	calls := make(chan callParams, 1)
	asyncLoadFn := makeAsyncLoadFn(responseChan, calls)
	errChan := make(chan error)
	requestID := graphsync.RequestID(rand.Int31())
	onNewBlockFn := func(graphsync.BlockData) error { return nil }
	loader := WrapAsyncLoader(ctx, asyncLoadFn, requestID, errChan, onNewBlockFn)

	link := testutil.NewTestLink()

	data := testutil.RandomBytes(100)
	responseChan <- types.AsyncLoadResult{Data: data, Err: nil}
	stream, err := loader(link, ipld.LinkContext{})
	require.NoError(t, err, "should load")
	returnedData, err := ioutil.ReadAll(stream)
	require.NoError(t, err, "stream did not read")
	require.Equal(t, data, returnedData, "should return correct data")
}

func TestWrappedAsyncLoaderSideChannelsErrors(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	responseChan := make(chan types.AsyncLoadResult, 1)
	calls := make(chan callParams, 1)
	asyncLoadFn := makeAsyncLoadFn(responseChan, calls)
	errChan := make(chan error, 1)
	requestID := graphsync.RequestID(rand.Int31())
	onNewBlockFn := func(graphsync.BlockData) error { return nil }
	loader := WrapAsyncLoader(ctx, asyncLoadFn, requestID, errChan, onNewBlockFn)

	link := testutil.NewTestLink()
	err := errors.New("something went wrong")
	responseChan <- types.AsyncLoadResult{Data: nil, Err: err}
	stream, loadErr := loader(link, ipld.LinkContext{})
	require.Nil(t, stream, "should return nil reader")
	_, isSkipErr := loadErr.(traversal.SkipMe)
	require.True(t, isSkipErr)
	var returnedErr error
	testutil.AssertReceive(ctx, t, errChan, &returnedErr, "should return an error on side channel")
	require.EqualError(t, returnedErr, err.Error())
}

func TestWrappedAsyncLoaderContextCancels(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	subCtx, subCancel := context.WithCancel(ctx)
	responseChan := make(chan types.AsyncLoadResult, 1)
	calls := make(chan callParams, 1)
	asyncLoadFn := makeAsyncLoadFn(responseChan, calls)
	errChan := make(chan error, 1)
	requestID := graphsync.RequestID(rand.Int31())
	onNewBlockFn := func(graphsync.BlockData) error { return nil }
	loader := WrapAsyncLoader(subCtx, asyncLoadFn, requestID, errChan, onNewBlockFn)
	link := testutil.NewTestLink()
	resultsChan := make(chan struct {
		io.Reader
		error
	})
	go func() {
		stream, err := loader(link, ipld.LinkContext{})
		resultsChan <- struct {
			io.Reader
			error
		}{stream, err}
	}()
	subCancel()

	var result struct {
		io.Reader
		error
	}
	testutil.AssertReceive(ctx, t, resultsChan, &result, "should return from sub context cancelling")
	require.Nil(t, result.Reader)
	require.Error(t, result.error, "should error from sub context cancelling")
}

func TestWrappedAsyncLoaderBlockHookErrors(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	responseChan := make(chan types.AsyncLoadResult, 1)
	calls := make(chan callParams, 1)
	asyncLoadFn := makeAsyncLoadFn(responseChan, calls)
	errChan := make(chan error, 1)
	requestID := graphsync.RequestID(rand.Int31())
	blockHookErr := errors.New("Something went wrong")
	onNewBlockFn := func(graphsync.BlockData) error { return blockHookErr }
	loader := WrapAsyncLoader(ctx, asyncLoadFn, requestID, errChan, onNewBlockFn)

	link := testutil.NewTestLink()

	data := testutil.RandomBytes(100)
	responseChan <- types.AsyncLoadResult{Data: data, Err: nil}
	stream, err := loader(link, ipld.LinkContext{})
	require.Nil(t, stream, "should return nil reader")
	require.EqualError(t, err, blockHookErr.Error())
}
