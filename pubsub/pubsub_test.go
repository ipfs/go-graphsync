package pubsub_test

import (
	"testing"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestPubSub(t *testing.T) {
	ps := pubsub.New()

	subscriber1Count := 0
	subscriber := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		subscriber1Count++
	}

	subscriber2Count := 0
	subscriber2 := func(event datatransfer.Event, cst datatransfer.ChannelState) {
		subscriber2Count++
	}

	unsubFunc := ps.Subscribe(subscriber)

	testEvent1 := datatransfer.Event{}
	testState1 := datatransfer.ChannelState{}
	ps.Publish(testEvent1, testState1)
	assert.Equal(t, subscriber1Count, 1)
	assert.Equal(t, subscriber2Count, 0)

	unsubFunc2 := ps.Subscribe(subscriber2)
	testEvent2 := datatransfer.Event{}
	testState2 := datatransfer.ChannelState{}
	ps.Publish(testEvent2, testState2)
	assert.Equal(t, subscriber1Count, 2)
	assert.Equal(t, subscriber2Count, 1)

	//  ensure subsequent calls don't cause errors, and also check that the right item
	// is removed, i.e. no false positives.
	unsubFunc()
	unsubFunc()
	testEvent3 := datatransfer.Event{}
	testState3 := datatransfer.ChannelState{}
	ps.Publish(testEvent3, testState3)
	assert.Equal(t, subscriber1Count, 2)
	assert.Equal(t, subscriber2Count, 2)

	// ensure it can delete all elems
	unsubFunc2()
	testEvent4 := datatransfer.Event{}
	testState4 := datatransfer.ChannelState{}
	ps.Publish(testEvent4, testState4)
	assert.Equal(t, subscriber1Count, 2)
	assert.Equal(t, subscriber2Count, 2)
}
