package loadattemptqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestAsyncLoadInitialLoadSucceeds(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	loadAttempter := func(peer.ID, graphsync.RequestID, ipld.Link, ipld.LinkContext) types.AsyncLoadResult {
		callCount++
		return types.AsyncLoadResult{
			Data: testutil.RandomBytes(100),
		}
	}
	loadAttemptQueue := New(loadAttempter)

	link := testutil.NewTestLink()
	linkContext := ipld.LinkContext{}
	requestID := graphsync.NewRequestID()
	p := testutil.GeneratePeers(1)[0]

	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(p, requestID, link, linkContext, resultChan)
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
	loadAttempter := func(peer.ID, graphsync.RequestID, ipld.Link, ipld.LinkContext) types.AsyncLoadResult {
		callCount++
		return types.AsyncLoadResult{
			Err: fmt.Errorf("something went wrong"),
		}
	}
	loadAttemptQueue := New(loadAttempter)

	link := testutil.NewTestLink()
	linkContext := ipld.LinkContext{}
	requestID := graphsync.NewRequestID()
	resultChan := make(chan types.AsyncLoadResult, 1)
	p := testutil.GeneratePeers(1)[0]

	lr := NewLoadRequest(p, requestID, link, linkContext, resultChan)
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
	loadAttempter := func(peer.ID, graphsync.RequestID, ipld.Link, ipld.LinkContext) types.AsyncLoadResult {
		var result []byte
		if callCount > 0 {
			result = testutil.RandomBytes(100)
		}
		callCount++
		return types.AsyncLoadResult{
			Data: result,
		}
	}

	loadAttemptQueue := New(loadAttempter)

	link := testutil.NewTestLink()
	linkContext := ipld.LinkContext{}
	requestID := graphsync.NewRequestID()
	p := testutil.GeneratePeers(1)[0]

	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(p, requestID, link, linkContext, resultChan)
	loadAttemptQueue.AttemptLoad(lr, false)

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.Nil(t, result.Data, "should not send responses")
	require.NotNil(t, result.Err, "should send an error")
	require.Equal(t, 1, callCount, "should attempt to load once and then not retry")
}

func TestAsyncLoadInitialLoadIndeterminateRetryTrueThenRetriedSuccess(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	called := make(chan struct{}, 2)
	loadAttempter := func(peer.ID, graphsync.RequestID, ipld.Link, ipld.LinkContext) types.AsyncLoadResult {
		var result []byte
		called <- struct{}{}
		if callCount > 0 {
			result = testutil.RandomBytes(100)
		}
		callCount++
		return types.AsyncLoadResult{
			Data: result,
		}
	}
	loadAttemptQueue := New(loadAttempter)

	link := testutil.NewTestLink()
	linkContext := ipld.LinkContext{}
	requestID := graphsync.NewRequestID()
	resultChan := make(chan types.AsyncLoadResult, 1)
	p := testutil.GeneratePeers(1)[0]
	lr := NewLoadRequest(p, requestID, link, linkContext, resultChan)
	loadAttemptQueue.AttemptLoad(lr, true)

	testutil.AssertDoesReceiveFirst(t, called, "should attempt load with no result", resultChan, ctx.Done())
	loadAttemptQueue.RetryLoads()

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.NotNil(t, result.Data, "should send response")
	require.Nil(t, result.Err, "should not send error")
	require.Equal(t, 2, callCount, "should attempt to load multiple times till success")
}

func TestAsyncLoadInitialLoadIndeterminateThenRequestFinishes(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	callCount := 0
	called := make(chan struct{}, 2)
	loadAttempter := func(peer.ID, graphsync.RequestID, ipld.Link, ipld.LinkContext) types.AsyncLoadResult {
		var result []byte
		called <- struct{}{}
		if callCount > 0 {
			result = testutil.RandomBytes(100)
		}
		callCount++
		return types.AsyncLoadResult{
			Data: result,
		}
	}
	loadAttemptQueue := New(loadAttempter)

	link := testutil.NewTestLink()
	linkContext := ipld.LinkContext{}
	requestID := graphsync.NewRequestID()
	resultChan := make(chan types.AsyncLoadResult, 1)
	p := testutil.GeneratePeers(1)[0]
	lr := NewLoadRequest(p, requestID, link, linkContext, resultChan)
	loadAttemptQueue.AttemptLoad(lr, true)

	testutil.AssertDoesReceiveFirst(t, called, "should attempt load with no result", resultChan, ctx.Done())
	loadAttemptQueue.ClearRequest(requestID)
	loadAttemptQueue.RetryLoads()

	var result types.AsyncLoadResult
	testutil.AssertReceive(ctx, t, resultChan, &result, "should close response channel with response")
	require.Nil(t, result.Data, "should not send responses")
	require.NotNil(t, result.Err, "should send an error")
	require.Equal(t, 1, callCount, "should attempt to load only once because request is finised")
}
