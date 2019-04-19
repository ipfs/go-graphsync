package requestmanager

import (
	"context"
	"fmt"
	"reflect"
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
	incomingErrors := make(chan ResponseError)
	cancelRequest := func() {}

	outgoingResponses, outgoingErrors := rc.collectResponses(
		requestCtx, incomingResponses, incomingErrors, cancelRequest)

	blocks := testutil.GenerateBlocksOfSize(10, 100)

	for _, block := range blocks {
		select {
		case <-ctx.Done():
			t.Fatal("should have written to channel but couldn't")
		case incomingResponses <- block:
		}
	}

	interimError := ResponseError{false, fmt.Errorf("A block was missing")}
	terminalError := ResponseError{true, fmt.Errorf("Something terrible happened")}
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
		case testBlock := <-outgoingResponses:
			if testBlock.Cid() != block.Cid() {
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
