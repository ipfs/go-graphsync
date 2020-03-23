package requestmanager

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/testbridge"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

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
		requestCtx, incomingResponses, incomingErrors, cancelRequest)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testbridge.NewMockStore(blockStore)
	blockChain := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 10)
	blocks := blockChain.AllBlocks()

	for i, block := range blocks {
		select {
		case <-ctx.Done():
			t.Fatal("should have written to channel but couldn't")
		case incomingResponses <- graphsync.ResponseProgress{
			Node: blockChain.NodeTipIndex(i),
			LastBlock: struct {
				Path ipld.Path
				Link ipld.Link
			}{ipld.Path{}, cidlink.Link{Cid: block.Cid()}},
		}:
		}
	}

	interimError := fmt.Errorf("A block was missing")
	terminalError := fmt.Errorf("Something terrible happened")
	select {
	case <-ctx.Done():
		t.Fatal("should have written error to channel but didn't")
	case incomingErrors <- interimError:
	}
	select {
	case <-ctx.Done():
		t.Fatal("should have written error to channel but didn't")
	case incomingErrors <- terminalError:
	}

	for _, block := range blocks {
		select {
		case <-ctx.Done():
			t.Fatal("should have read from channel but couldn't")
		case testResponse := <-outgoingResponses:
			if testResponse.LastBlock.Link.(cidlink.Link).Cid != block.Cid() {
				t.Fatal("stored blocks incorrectly")
			}
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("should have read from channel but couldn't")
		case testErr := <-outgoingErrors:
			if i == 0 {
				if !reflect.DeepEqual(testErr, interimError) {
					t.Fatal("incorrect error message sent")
				}
			} else {
				if !reflect.DeepEqual(testErr, terminalError) {
					t.Fatal("incorrect error message sent")
				}
			}
		}
	}
}
