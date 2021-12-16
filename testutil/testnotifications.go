package testutil

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/notifications"
)

type TestSubscriber struct {
	receivedEvents chan DispatchedEvent
	closed         chan notifications.Topic
}

type DispatchedEvent struct {
	Topic notifications.Topic
	Event notifications.Event
}

func NewTestSubscriber(bufferSize int) *TestSubscriber {
	return &TestSubscriber{
		receivedEvents: make(chan DispatchedEvent, bufferSize),
		closed:         make(chan notifications.Topic, bufferSize),
	}
}

func (ts *TestSubscriber) OnNext(topic notifications.Topic, ev notifications.Event) {
	ts.receivedEvents <- DispatchedEvent{topic, ev}
}

func (ts *TestSubscriber) OnClose(topic notifications.Topic) {
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
func (ts *TestSubscriber) ExpectEventsAllTopics(ctx context.Context, t *testing.T, events []notifications.Event) {
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

func (ts *TestSubscriber) ExpectClosesAnyOrder(ctx context.Context, t *testing.T, topics []notifications.Topic) {
	t.Helper()
	expectedTopics := make(map[notifications.Topic]struct{})
	receivedTopics := make(map[notifications.Topic]struct{})
	for _, expectedTopic := range topics {
		expectedTopics[expectedTopic] = struct{}{}
		var topic notifications.Topic
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

func (ts *TestSubscriber) ExpectCloses(ctx context.Context, t *testing.T, topics []notifications.Topic) {
	t.Helper()
	for _, expectedTopic := range topics {
		var topic notifications.Topic
		AssertReceive(ctx, t, ts.closed, &topic, "should receive another event")
		require.Equal(t, expectedTopic, topic)
	}
}

type MockPublisher struct {
	subscribersLk sync.Mutex
	subscribers   []notifications.Subscriber
}

func (mp *MockPublisher) AddSubscriber(subscriber notifications.Subscriber) {
	mp.subscribersLk.Lock()
	mp.subscribers = append(mp.subscribers, subscriber)
	mp.subscribersLk.Unlock()
}

func (mp *MockPublisher) PublishEvents(topic notifications.Topic, events []notifications.Event) {
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
