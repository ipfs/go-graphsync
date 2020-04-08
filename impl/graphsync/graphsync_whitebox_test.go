package graphsyncimpl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

func TestGraphsyncImpl_SubscribeToEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1
	gs1 := testutil.NewFakeGraphSync()
	dt1 := NewGraphSyncDataTransfer(host1, gs1)

	subscribe1Calls := make(chan struct{}, 1)
	subscriber := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			subscribe1Calls <- struct{}{}
		}
	}

	subscribe2Calls := make(chan struct{}, 1)
	subscriber2 := func(event datatransfer.Event, cst datatransfer.ChannelState) {
		if event.Code != datatransfer.Error {
			subscribe2Calls <- struct{}{}
		}
	}

	unsubFunc := dt1.SubscribeToEvents(subscriber)
	impl := dt1.(*graphsyncImpl)
	assert.Equal(t, 1, len(impl.subscribers))

	unsubFunc2 := impl.SubscribeToEvents(subscriber2)
	assert.Equal(t, 2, len(impl.subscribers))

	//  ensure subsequent calls don't cause errors, and also check that the right item
	// is removed, i.e. no false positives.
	unsubFunc()
	unsubFunc()
	assert.Equal(t, 1, len(impl.subscribers))

	// ensure it can delete all elems
	unsubFunc2()
	assert.Equal(t, 0, len(impl.subscribers))
}
