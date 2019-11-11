package loadattemptqueue

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	ipld "github.com/ipld/go-ipld-prime"
)

func TestAsyncLoadInitialLoadSucceeds(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	callCount := 0
	loadAttempter := func(graphsync.RequestID, ipld.Link) ([]byte, error) {
		callCount++
		return testutil.RandomBytes(100), nil
	}
	loadAttemptQueue := New(loadAttempter)

	link := testbridge.NewMockLink()
	requestID := graphsync.RequestID(rand.Int31())

	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(requestID, link, resultChan)
	loadAttemptQueue.AttemptLoad(lr, false)

	select {
	case result := <-resultChan:
		if result.Data == nil {
			t.Fatal("should have sent a response")
		}
		if result.Err != nil {
			t.Fatal("should not have sent an error")
		}
	case <-ctx.Done():
		t.Fatal("should have closed response channel")
	}

	if callCount == 0 {
		t.Fatal("should have attempted to load link but did not")
	}
}

func TestAsyncLoadInitialLoadFails(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	callCount := 0
	loadAttempter := func(graphsync.RequestID, ipld.Link) ([]byte, error) {
		callCount++
		return nil, fmt.Errorf("something went wrong")
	}
	loadAttemptQueue := New(loadAttempter)

	link := testbridge.NewMockLink()
	requestID := graphsync.RequestID(rand.Int31())
	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(requestID, link, resultChan)
	loadAttemptQueue.AttemptLoad(lr, false)

	select {
	case result := <-resultChan:
		if result.Data != nil {
			t.Fatal("should not have sent responses")
		}
		if result.Err == nil {
			t.Fatal("should have sent an error")
		}
	case <-ctx.Done():
		t.Fatal("should have closed response channel")
	}

	if callCount == 0 {
		t.Fatal("should have attempted to load link but did not")
	}

}

func TestAsyncLoadInitialLoadIndeterminateRetryFalse(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
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

	link := testbridge.NewMockLink()
	requestID := graphsync.RequestID(rand.Int31())
	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(requestID, link, resultChan)
	loadAttemptQueue.AttemptLoad(lr, false)

	select {
	case result := <-resultChan:
		if result.Data != nil {
			t.Fatal("should not have sent responses")
		}
		if result.Err == nil {
			t.Fatal("should have sent an error")

		}
	case <-ctx.Done():
		t.Fatal("should have produced result")
	}

	if callCount > 1 {
		t.Fatal("should have failed after load with indeterminate result")
	}
}

func TestAsyncLoadInitialLoadIndeterminateRetryTrueThenRetriedSuccess(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
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

	link := testbridge.NewMockLink()
	requestID := graphsync.RequestID(rand.Int31())
	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(requestID, link, resultChan)
	loadAttemptQueue.AttemptLoad(lr, true)

	select {
	case <-called:
	case <-resultChan:
		t.Fatal("Should not have sent message on response chan")
	case <-ctx.Done():
		t.Fatal("should have attempted load once")
	}
	loadAttemptQueue.RetryLoads()

	select {
	case result := <-resultChan:
		if result.Data == nil {
			t.Fatal("should have sent a response")
		}
		if result.Err != nil {
			t.Fatal("should not have sent an error")
		}
	case <-ctx.Done():
		t.Fatal("should have closed response channel")
	}

	if callCount < 2 {
		t.Fatal("should have attempted to load multiple times till success but did not")
	}
}

func TestAsyncLoadInitialLoadIndeterminateThenRequestFinishes(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
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

	link := testbridge.NewMockLink()
	requestID := graphsync.RequestID(rand.Int31())
	resultChan := make(chan types.AsyncLoadResult, 1)
	lr := NewLoadRequest(requestID, link, resultChan)
	loadAttemptQueue.AttemptLoad(lr, true)

	select {
	case <-called:
	case <-resultChan:
		t.Fatal("Should not have sent message on response chan")
	case <-ctx.Done():
		t.Fatal("should have attempted load once")
	}
	loadAttemptQueue.ClearRequest(requestID)
	loadAttemptQueue.RetryLoads()

	select {
	case result := <-resultChan:
		if result.Data != nil {
			t.Fatal("should not have sent responses")
		}
		if result.Err == nil {
			t.Fatal("should have sent an error")
		}
	case <-ctx.Done():
		t.Fatal("should have closed response channel")
	}

	if callCount > 1 {
		t.Fatal("should only have attempted one call but attempted multiple")
	}
}
