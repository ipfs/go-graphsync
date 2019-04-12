package asyncloader

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	ipld "github.com/ipld/go-ipld-prime"
)

func TestAsyncLoadWhenRequestNotInProgress(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	callCount := 0
	loadAttempter := func(gsmsg.GraphSyncRequestID, ipld.Link) ([]byte, error) {
		callCount++
		return testutil.RandomBytes(100), nil
	}
	asyncLoader := New(ctx, loadAttempter)
	asyncLoader.Startup()

	link := testbridge.NewMockLink()
	requestID := gsmsg.GraphSyncRequestID(rand.Int31())
	responseChan, errChan := asyncLoader.AsyncLoad(requestID, link)

	select {
	case _, ok := <-responseChan:
		if ok {
			t.Fatal("should not have sent responses")
		}
	case <-ctx.Done():
		t.Fatal("should have closed response channel")
	}

	select {
	case _, ok := <-errChan:
		if !ok {
			t.Fatal("should have sent an error")
		}
	case <-ctx.Done():
		t.Fatal("should have closed error channel")
	}

	if callCount > 0 {
		t.Fatal("should not have attempted to load link but did")
	}
}

func TestAsyncLoadWhenInitialLoadSucceeds(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	callCount := 0
	loadAttempter := func(gsmsg.GraphSyncRequestID, ipld.Link) ([]byte, error) {
		callCount++
		return testutil.RandomBytes(100), nil
	}
	asyncLoader := New(ctx, loadAttempter)
	asyncLoader.Startup()

	link := testbridge.NewMockLink()
	requestID := gsmsg.GraphSyncRequestID(rand.Int31())
	asyncLoader.StartRequest(requestID)
	responseChan, errChan := asyncLoader.AsyncLoad(requestID, link)

	select {
	case _, ok := <-responseChan:
		if !ok {
			t.Fatal("should have sent a response")
		}
	case <-ctx.Done():
		t.Fatal("should have closed response channel")
	}

	select {
	case _, ok := <-errChan:
		if ok {
			t.Fatal("should not have sent an error")
		}
	case <-ctx.Done():
		t.Fatal("should have closed error channel")
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
	loadAttempter := func(gsmsg.GraphSyncRequestID, ipld.Link) ([]byte, error) {
		callCount++
		return nil, fmt.Errorf("something went wrong")
	}
	asyncLoader := New(ctx, loadAttempter)
	asyncLoader.Startup()

	link := testbridge.NewMockLink()
	requestID := gsmsg.GraphSyncRequestID(rand.Int31())
	asyncLoader.StartRequest(requestID)
	responseChan, errChan := asyncLoader.AsyncLoad(requestID, link)

	select {
	case _, ok := <-responseChan:
		if ok {
			t.Fatal("should not have sent responses")
		}
	case <-ctx.Done():
		t.Fatal("should have closed response channel")
	}

	select {
	case _, ok := <-errChan:
		if !ok {
			t.Fatal("should have sent an error")
		}
	case <-ctx.Done():
		t.Fatal("should have closed error channel")
	}

	if callCount == 0 {
		t.Fatal("should have attempted to load link but did not")
	}
}

func TestAsyncLoadInitialLoadIndeterminateThenSucceeds(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	callCount := 0
	called := make(chan struct{}, 2)
	loadAttempter := func(gsmsg.GraphSyncRequestID, ipld.Link) ([]byte, error) {
		var result []byte
		called <- struct{}{}
		if callCount > 0 {
			result = testutil.RandomBytes(100)
		}
		callCount++
		return result, nil
	}
	asyncLoader := New(ctx, loadAttempter)
	asyncLoader.Startup()

	link := testbridge.NewMockLink()
	requestID := gsmsg.GraphSyncRequestID(rand.Int31())
	asyncLoader.StartRequest(requestID)
	responseChan, errChan := asyncLoader.AsyncLoad(requestID, link)
	select {
	case <-called:
	case <-responseChan:
		t.Fatal("Should not have sent message on response chan")
	case <-errChan:
		t.Fatal("Should not have sent messages on error chan")
	case <-ctx.Done():
		t.Fatal("should have attempted load once")
	}
	asyncLoader.NewResponsesAvailable()

	select {
	case _, ok := <-responseChan:
		if !ok {
			t.Fatal("should have sent a response")
		}
	case <-ctx.Done():
		t.Fatal("should have closed response channel")
	}

	select {
	case _, ok := <-errChan:
		if ok {
			t.Fatal("should not have sent an error")
		}
	case <-ctx.Done():
		t.Fatal("should have closed error channel")
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
	loadAttempter := func(gsmsg.GraphSyncRequestID, ipld.Link) ([]byte, error) {
		var result []byte
		called <- struct{}{}
		if callCount > 0 {
			result = testutil.RandomBytes(100)
		}
		callCount++
		return result, nil
	}
	asyncLoader := New(ctx, loadAttempter)
	asyncLoader.Startup()

	link := testbridge.NewMockLink()
	requestID := gsmsg.GraphSyncRequestID(rand.Int31())
	asyncLoader.StartRequest(requestID)
	responseChan, errChan := asyncLoader.AsyncLoad(requestID, link)
	select {
	case <-called:
	case <-responseChan:
		t.Fatal("Should not have sent message on response chan")
	case <-errChan:
		t.Fatal("Should not have sent messages on error chan")
	case <-ctx.Done():
		t.Fatal("should have attempted load once")
	}
	asyncLoader.FinishRequest(requestID)
	asyncLoader.NewResponsesAvailable()

	select {
	case _, ok := <-responseChan:
		if ok {
			t.Fatal("should not have sent responses")
		}
	case <-ctx.Done():
		t.Fatal("should have closed response channel")
	}

	select {
	case _, ok := <-errChan:
		if !ok {
			t.Fatal("should have sent an error")
		}
	case <-ctx.Done():
		t.Fatal("should have closed error channel")
	}
	if callCount > 1 {
		t.Fatal("should only have attempted one call but attempted multiple")
	}
}
