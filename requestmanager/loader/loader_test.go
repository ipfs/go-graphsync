package loader

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/ipfs/go-graphsync/requestmanager/types"

	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime"
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
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	responseChan := make(chan types.AsyncLoadResult, 1)
	calls := make(chan callParams, 1)
	asyncLoadFn := makeAsyncLoadFn(responseChan, calls)
	errChan := make(chan error)
	requestID := graphsync.RequestID(rand.Int31())
	loader := WrapAsyncLoader(ctx, asyncLoadFn, requestID, errChan)

	link := testutil.NewTestLink()

	data := testutil.RandomBytes(100)
	responseChan <- types.AsyncLoadResult{Data: data, Err: nil}
	stream, err := loader(link, ipld.LinkContext{})
	if err != nil {
		t.Fatal("Should not have errored on load")
	}
	returnedData, err := ioutil.ReadAll(stream)
	if err != nil {
		t.Fatal("error in return stream")
	}
	if !reflect.DeepEqual(data, returnedData) {
		t.Fatal("returned data did not match expected")
	}
}

func TestWrappedAsyncLoaderSideChannelsErrors(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	responseChan := make(chan types.AsyncLoadResult, 1)
	calls := make(chan callParams, 1)
	asyncLoadFn := makeAsyncLoadFn(responseChan, calls)
	errChan := make(chan error, 1)
	requestID := graphsync.RequestID(rand.Int31())
	loader := WrapAsyncLoader(ctx, asyncLoadFn, requestID, errChan)

	link := testutil.NewTestLink()
	err := errors.New("something went wrong")
	responseChan <- types.AsyncLoadResult{Data: nil, Err: err}
	stream, loadErr := loader(link, ipld.LinkContext{})
	if stream != nil || loadErr != ipldutil.ErrDoNotFollow() {
		t.Fatal("Should have errored on load")
	}
	select {
	case <-ctx.Done():
		t.Fatal("should have returned an error on side channel but didn't")
	case returnedErr := <-errChan:
		if returnedErr != err {
			t.Fatal("returned wrong error on side channel")
		}
	}
}

func TestWrappedAsyncLoaderContextCancels(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	subCtx, subCancel := context.WithCancel(ctx)
	responseChan := make(chan types.AsyncLoadResult, 1)
	calls := make(chan callParams, 1)
	asyncLoadFn := makeAsyncLoadFn(responseChan, calls)
	errChan := make(chan error, 1)
	requestID := graphsync.RequestID(rand.Int31())
	loader := WrapAsyncLoader(subCtx, asyncLoadFn, requestID, errChan)
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

	select {
	case <-ctx.Done():
		t.Fatal("should have returned from context cancelling but didn't")
	case result := <-resultsChan:
		if result.Reader != nil || result.error == nil {
			t.Fatal("should have errored from context cancelling but didn't")
		}
	}
}
