package requestmanager

import (
	"context"
	"fmt"
	"testing"
	"time"

	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestBufferingResponseProgress(t *testing.T) {
	backgroundCtx := context.Background()
	ctx, cancel := context.WithTimeout(backgroundCtx, time.Second)
	defer cancel()
	rc := newResponseCollector(ctx)
	requestCtx, requestCancel := context.WithCancel(backgroundCtx)
	defer requestCancel()
	incomingResponses := make(chan graphsync.ResponseProgress)
	incomingErrors := make(chan error)
	cancelRequest := func() {}

	outgoingResponses, outgoingErrors := rc.collectResponses(
		requestCtx, incomingResponses, incomingErrors, cancelRequest, func() {})

	blockStore := make(map[ipld.Link][]byte)
	persistence := testutil.NewTestStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, persistence, 100, 10)
	blocks := blockChain.AllBlocks()

	for i, block := range blocks {
		testutil.AssertSends(ctx, t, incomingResponses, graphsync.ResponseProgress{
			Node: blockChain.NodeTipIndex(i),
			LastBlock: struct {
				Path ipld.Path
				Link ipld.Link
			}{ipld.Path{}, cidlink.Link{Cid: block.Cid()}},
		}, "did not write block to channel")
	}

	interimError := fmt.Errorf("A block was missing")
	terminalError := fmt.Errorf("Something terrible happened")
	testutil.AssertSends(ctx, t, incomingErrors, interimError, "did not write error to channel")
	testutil.AssertSends(ctx, t, incomingErrors, terminalError, "did not write error to channel")

	for _, block := range blocks {
		var testResponse graphsync.ResponseProgress
		testutil.AssertReceive(ctx, t, outgoingResponses, &testResponse, "should read from outgoing responses")
		require.Equal(t, block.Cid(), testResponse.LastBlock.Link.(cidlink.Link).Cid, "did not store block correctly")
	}

	for i := 0; i < 2; i++ {
		var testErr error
		testutil.AssertReceive(ctx, t, outgoingErrors, &testErr, "should have read from channel but couldn't")
		if i == 0 {
			require.Equal(t, interimError, testErr, "incorrect error message sent")
		} else {
			require.Equal(t, terminalError, testErr, "incorrect error message sent")
		}
	}
}
