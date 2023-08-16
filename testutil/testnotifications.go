package testutil

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-protocolnetwork/pkg/notifications"
)

type TestSubscriber struct {
	receivedEvents chan DispatchedEvent
	closed         chan messagequeue.Topic
}

type DispatchedEvent struct {
	Topic messagequeue.Topic
	Event messagequeue.Event
}

func NewTestSubscriber(bufferSize int) *TestSubscriber {
	return &TestSubscriber{
		receivedEvents: make(chan DispatchedEvent, bufferSize),
		closed:         make(chan messagequeue.Topic, bufferSize),
	}
}

func (ts *TestSubscriber) OnNext(topic messagequeue.Topic, ev messagequeue.Event) {
	ts.receivedEvents <- DispatchedEvent{topic, ev}
}

func (ts *TestSubscriber) OnClose(topic messagequeue.Topic) {
	ts.closed <- topic
}

func (ts *TestSubscriber) ExpectEvents(ctx context.Context, t *testing.T, events []DispatchedEvent) {
	t.Helper()
	for _, expectedEvent := range events {
		var event DispatchedEvent
		AssertReceive(ctx, t, ts.receivedEvents, &event, "should receive another event")
		require.Equal(t, expectedEvent, event)
	}
}
func (ts *TestSubscriber) ExpectEventsAllTopics(ctx context.Context, t *testing.T, events []messagequeue.Event) {
	t.Helper()
	for _, expectedEvent := range events {
		var event DispatchedEvent
		AssertReceive(ctx, t, ts.receivedEvents, &event, "should receive another event")
		require.Equal(t, expectedEvent, event.Event)
	}
}

func (ts *TestSubscriber) NoEventsReceived(t *testing.T) {
	t.Helper()
	AssertChannelEmpty(t, ts.receivedEvents, "should have received no events")
}

func (ts *TestSubscriber) ExpectClosesAnyOrder(ctx context.Context, t *testing.T, topics []messagequeue.Topic) {
	t.Helper()
	expectedTopics := make(map[messagequeue.Topic]struct{})
	receivedTopics := make(map[messagequeue.Topic]struct{})
	for _, expectedTopic := range topics {
		expectedTopics[expectedTopic] = struct{}{}
		var topic messagequeue.Topic
		AssertReceive(ctx, t, ts.closed, &topic, "should receive another event")
		receivedTopics[topic] = struct{}{}
	}
	require.Equal(t, expectedTopics, receivedTopics)
}

func (ts *TestSubscriber) ExpectNCloses(ctx context.Context, t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		AssertDoesReceive(ctx, t, ts.closed, "should receive another event")
	}
}

func (ts *TestSubscriber) ExpectCloses(ctx context.Context, t *testing.T, topics []messagequeue.Topic) {
	t.Helper()
	for _, expectedTopic := range topics {
		var topic messagequeue.Topic
		AssertReceive(ctx, t, ts.closed, &topic, "should receive another event")
		require.Equal(t, expectedTopic, topic)
	}
}

type MockPublisher struct {
	subscribersLk sync.Mutex
	subscribers   []notifications.Subscriber[messagequeue.Topic, messagequeue.Event]
}

func (mp *MockPublisher) AddSubscriber(subscriber notifications.Subscriber[messagequeue.Topic, messagequeue.Event]) {
	mp.subscribersLk.Lock()
	mp.subscribers = append(mp.subscribers, subscriber)
	mp.subscribersLk.Unlock()
}

func (mp *MockPublisher) PublishEvents(topic messagequeue.Topic, events []messagequeue.Event) {
	mp.subscribersLk.Lock()
	for _, subscriber := range mp.subscribers {

		for _, ev := range events {
			subscriber.OnNext(topic, ev)
		}
		subscriber.OnClose(topic)
	}
	mp.subscribersLk.Unlock()
}

func NewMockPublisher() *MockPublisher {
	return &MockPublisher{}
}
