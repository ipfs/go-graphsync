package loader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err, "Should not have error if underlying loader returns valid buffer and no error")
	result, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, result, sourceBytes, "Should return reader that functions identical to source reader")
	require.Equal(t, frs.lastRequestID, requestID, "should send block to response sender with correct params")
	require.Equal(t, frs.lastLink, link1, "should send block to response sender with correct params")
	require.Equal(t, frs.lastData, sourceBytes, "should send block to response sender with correct params")

	reader, err = wrappedLoader(link2, ipld.LinkContext{})

	require.Nil(t, reader, "should return empty reader")
	require.Error(t, err, "should return an error")

	require.Equal(t, err, ipldutil.ErrDoNotFollow(), "Should convert error to a do not follow error")
	require.Equal(t, frs.lastRequestID, requestID)
	require.Equal(t, frs.lastLink, link2, "Should send metadata")
	require.Nil(t, frs.lastData, "Should not send block")
}
