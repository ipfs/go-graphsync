package loader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"reflect"
	"testing"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/testutil"

	"github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
)

type fakeResponseSender struct {
	lastRequestID graphsync.RequestID
	lastLink      ipld.Link
	lastData      []byte
}

func (frs *fakeResponseSender) SendResponse(
	requestID graphsync.RequestID,
	link ipld.Link,
	data []byte,
) {
	frs.lastRequestID = requestID
	frs.lastLink = link
	frs.lastData = data
}

func TestWrappedLoaderSendsResponses(t *testing.T) {
	ctx := context.Background()
	frs := &fakeResponseSender{}
	link1 := testutil.NewTestLink()
	link2 := testutil.NewTestLink()
	sourceBytes := testutil.RandomBytes(100)
	byteBuffer := bytes.NewReader(sourceBytes)

	loader := func(ipldLink ipld.Link, lnkCtx ipld.LinkContext) (io.Reader, error) {
		if ipldLink == link1 {
			return byteBuffer, nil
		}
		return nil, fmt.Errorf("unable to load block")
	}
	requestID := graphsync.RequestID(rand.Int31())
	wrappedLoader := WrapLoader(ctx, loader, requestID, frs)

	reader, err := wrappedLoader(link1, ipld.LinkContext{})
	if err != nil {
		t.Fatal("Should not have error if underlying loader returns valid buffer and no error")
	}
	result, err := ioutil.ReadAll(reader)
	if err != nil || !reflect.DeepEqual(result, sourceBytes) {
		t.Fatal("Should return reader that functions identical to source reader")
	}
	if frs.lastRequestID != requestID ||
		frs.lastLink != link1 ||
		!reflect.DeepEqual(frs.lastData, sourceBytes) {
		t.Fatal("Should have sent block to response sender with correct params but did not")
	}

	reader, err = wrappedLoader(link2, ipld.LinkContext{})

	if reader != nil || err == nil {
		t.Fatal("Should return an error and empty reader if underlying loader does")
	}

	if err != ipldbridge.ErrDoNotFollow() {
		t.Fatal("Should convert error to a do not follow error")
	}

	if frs.lastRequestID != requestID ||
		frs.lastLink != link2 ||
		frs.lastData != nil {
		t.Fatal("Should sent metadata for link but no block, but did not")
	}
}
