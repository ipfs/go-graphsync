package requestmanager

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync/testutil"
)

func TestBufferingResponseProgress(t *testing.T) {
	backgroundCtx := context.Background()
	ctx, cancel := context.WithTimeout(backgroundCtx, time.Second)
	defer cancel()
	rc := newResponseCollector(ctx)
	requestCtx, requestCancel := context.WithCancel(backgroundCtx)
	defer requestCancel()
	incomingResponses := make(chan ResponseProgress)
	cancelRequest := func() {}

	outgoingResponses := rc.collectResponses(requestCtx, incomingResponses, cancelRequest)

	blocks := testutil.GenerateBlocksOfSize(10, 100)

	for _, block := range blocks {
		select {
		case <-ctx.Done():
			t.Fatal("should have written to channel but couldn't")
		case incomingResponses <- block:
		}
	}

	for _, block := range blocks {
		select {
		case <-ctx.Done():
			t.Fatal("should have read from channel but couldn't")
		case testBlock := <-outgoingResponses:
			if testBlock.Cid() != block.Cid() {
				t.Fatal("stored blocks incorrectly")
			}
		}
	}
}
