package loadattemptqueue

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipfs/go-graphsync/testutil"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/stretchr/testify/require"
)

func TestAsyncLoadInitialLoadSucceeds(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	loadAttempter := func(graphsync.RequestID, ipld.Link) ([]byte, error) {
		callCount++
		return testutil.RandomBytes(100), nil
	}
	loadAttemptQueue := New(loadAttempter)

	link := testutil.NewTestLink()
	requestID := graphsync.RequestID(rand.Int31())

	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(requestID, link, resultChan)
	loadAttemptQueue.AttemptLoad(lr, false)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.NotNil(t, result.Data, "should send response")
	require.Nil(t, result.Err, "should not send error")

	require.NotZero(t, callCount, "should attempt to load link from local store")
}

func TestAsyncLoadInitialLoadFails(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	loadAttempter := func(graphsync.RequestID, ipld.Link) ([]byte, error) {
		callCount++
		return nil, fmt.Errorf("something went wrong")
	}
	loadAttemptQueue := New(loadAttempter)

	link := testutil.NewTestLink()
	requestID := graphsync.RequestID(rand.Int31())
	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(requestID, link, resultChan)
	loadAttemptQueue.AttemptLoad(lr, false)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.Nil(t, result.Data, "should not send responses")
	require.NotNil(t, result.Err, "should send an error")
	require.NotZero(t, callCount, "should attempt to load link from local store")
}

func TestAsyncLoadInitialLoadIndeterminateRetryFalse(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	loadAttempter := func(graphsync.RequestID, ipld.Link) ([]byte, error) {
		var result []byte
		if callCount > 0 {
			result = testutil.RandomBytes(100)
		}
		callCount++
		return result, nil
	}

	loadAttemptQueue := New(loadAttempter)

	link := testutil.NewTestLink()
	requestID := graphsync.RequestID(rand.Int31())
	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(requestID, link, resultChan)
	loadAttemptQueue.AttemptLoad(lr, false)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.Nil(t, result.Data, "should not send responses")
	require.NotNil(t, result.Err, "should send an error")
	require.Equal(t, callCount, 1, "should attempt to load once and then not retry")
}

func TestAsyncLoadInitialLoadIndeterminateRetryTrueThenRetriedSuccess(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	called := make(chan struct{}, 2)
	loadAttempter := func(graphsync.RequestID, ipld.Link) ([]byte, error) {
		var result []byte
		called <- struct{}{}
		if callCount > 0 {
			result = testutil.RandomBytes(100)
		}
		callCount++
		return result, nil
	}
	loadAttemptQueue := New(loadAttempter)

	link := testutil.NewTestLink()
	requestID := graphsync.RequestID(rand.Int31())
	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(requestID, link, resultChan)
	loadAttemptQueue.AttemptLoad(lr, true)

	testutil.AssertDoesReceiveFirst(t, called, "should attempt load with no result", resultChan, ctx.Done())
	loadAttemptQueue.RetryLoads()

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.NotNil(t, result.Data, "should send response")
	require.Nil(t, result.Err, "should not send error")
	require.Equal(t, callCount, 2, "should attempt to load multiple times till success")
}

func TestAsyncLoadInitialLoadIndeterminateThenRequestFinishes(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	called := make(chan struct{}, 2)
	loadAttempter := func(graphsync.RequestID, ipld.Link) ([]byte, error) {
		var result []byte
		called <- struct{}{}
		if callCount > 0 {
			result = testutil.RandomBytes(100)
		}
		callCount++
		return result, nil
	}
	loadAttemptQueue := New(loadAttempter)

	link := testutil.NewTestLink()
	requestID := graphsync.RequestID(rand.Int31())
	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(requestID, link, resultChan)
	loadAttemptQueue.AttemptLoad(lr, true)

	testutil.AssertDoesReceiveFirst(t, called, "should attempt load with no result", resultChan, ctx.Done())
	loadAttemptQueue.ClearRequest(requestID)
	loadAttemptQueue.RetryLoads()

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.Nil(t, result.Data, "should not send responses")
	require.NotNil(t, result.Err, "should send an error")
	require.Equal(t, callCount, 1, "should attempt to load only once because request is finised")
}
