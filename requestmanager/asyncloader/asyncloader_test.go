package asyncloader

import (
	"context"
	"io"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/metadata"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipfs/go-graphsync/testutil"
	ipld "github.com/ipld/go-ipld-prime"
)

func TestAsyncLoadInitialLoadSucceedsLocallyPresent(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	callCount := 0
	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blockStore)
	block := testutil.GenerateBlocksOfSize(1, 100)[0]
	writer, commit, err := storer(ipld.LinkContext{})
	_, err = writer.Write(block.RawData())
	if err != nil {
		t.Fatal("could not seed block store")
	}
	link := cidlink.Link{Cid: block.Cid()}
	err = commit(link)
	if err != nil {
		t.Fatal("could not seed block store")
	}

	wrappedLoader := func(link ipld.Link, linkContext ipld.LinkContext) (io.Reader, error) {
		callCount++
		return loader(link, linkContext)
	}

	asyncLoader := New(ctx, wrappedLoader, storer)
	asyncLoader.Startup()

	requestID := graphsync.RequestID(rand.Int31())
	resultChan := asyncLoader.AsyncLoad(requestID, link)

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
		t.Fatal("should have attempted to load link from local store but did not")
	}
}

func TestAsyncLoadInitialLoadSucceedsResponsePresent(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
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

	if callCount > 0 {
		t.Fatal("should not have attempted to load link from local store")
	}

	if !reflect.DeepEqual(blockStore[link], block.RawData()) {
		t.Fatal("should have stored block but didn't")
	}
}

func TestAsyncLoadInitialLoadFails(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
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

	if callCount > 0 {
		t.Fatal("should not have attempted to load link from local store")
	}
}

func TestAsyncLoadInitialLoadIndeterminateWhenRequestNotInProgress(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
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
		t.Fatal("should have attempted to load link from local store but did not")
	}
}

func TestAsyncLoadInitialLoadIndeterminateThenSucceeds(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
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

	select {
	case <-called:
	case <-resultChan:
		t.Fatal("Should not have sent message on response chan")
	case <-ctx.Done():
		t.Fatal("should have attempted load once")
	}

	responses := map[graphsync.RequestID]metadata.Metadata{
		requestID: metadata.Metadata{
			metadata.Item{
				Link:         link,
				BlockPresent: true,
			},
		},
	}
	asyncLoader.ProcessResponse(responses, blocks)

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

	if callCount != 1 {
		t.Fatal("should have attempted to load from local store exactly once")
	}

	if !reflect.DeepEqual(blockStore[link], block.RawData()) {
		t.Fatal("should have stored block but didn't")
	}
}

func TestAsyncLoadInitialLoadIndeterminateThenFails(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
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

	select {
	case <-called:
	case <-resultChan:
		t.Fatal("Should not have sent message on response chan")
	case <-ctx.Done():
		t.Fatal("should have attempted load once")
	}
	responses := map[graphsync.RequestID]metadata.Metadata{
		requestID: metadata.Metadata{
			metadata.Item{
				Link:         link,
				BlockPresent: false,
			},
		},
	}
	asyncLoader.ProcessResponse(responses, nil)

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

	if callCount != 1 {
		t.Fatal("should have attempted to load from local store exactly once")
	}
}

func TestAsyncLoadInitialLoadIndeterminateThenRequestFinishes(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
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

	select {
	case <-called:
	case <-resultChan:
		t.Fatal("Should not have sent message on response chan")
	case <-ctx.Done():
		t.Fatal("should have attempted load once")
	}
	asyncLoader.CompleteResponsesFor(requestID)

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

	if callCount != 1 {
		t.Fatal("should have attempted to load from local store exactly once")
	}
}

func TestAsyncLoadTwiceLoadsLocallySecondTime(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
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

	if callCount > 0 {
		t.Fatal("should not have attempted to load link from local store")
	}

	if !reflect.DeepEqual(blockStore[link], block.RawData()) {
		t.Fatal("should have stored block but didn't")
	}

	resultChan = asyncLoader.AsyncLoad(requestID, link)

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
		t.Fatal("should have attempted to load link from local store but did not")
	}

	if !reflect.DeepEqual(blockStore[link], block.RawData()) {
		t.Fatal("should have stored block but didn't")
	}
}
