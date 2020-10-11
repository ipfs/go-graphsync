package testutil

import (
	"context"
	"sync"
	"testing"

	"github.com/ipfs/go-graphsync/notifications"
	"github.com/stretchr/testify/require"
)

type TestSubscriber struct {
	expectedTopic  notifications.Topic
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
	for _, expectedEvent := range events {
		var event DispatchedEvent
		AssertReceive(ctx, t, ts.receivedEvents, &event, "should receive another event")
		require.Equal(t, expectedEvent, event)
	}
}

func (ts *TestSubscriber) NoEventsReceived(t *testing.T) {
	AssertChannelEmpty(t, ts.receivedEvents, "should have received no events")
}

func (ts *TestSubscriber) ExpectClosesAnyOrder(ctx context.Context, t *testing.T, topics []notifications.Topic) {
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

func (ts *TestSubscriber) ExpectCloses(ctx context.Context, t *testing.T, topics []notifications.Topic) {
	for _, expectedTopic := range topics {
		var topic notifications.Topic
		AssertReceive(ctx, t, ts.closed, &topic, "should receive another event")
		require.Equal(t, expectedTopic, topic)
	}
}

type NotifeeVerifier struct {
	expectedTopic notifications.Topic
	subscriber    *TestSubscriber
}

func (nv *NotifeeVerifier) ExpectEvents(ctx context.Context, t *testing.T, events []notifications.Event) {
	dispatchedEvents := make([]DispatchedEvent, 0, len(events))
	for _, ev := range events {
		dispatchedEvents = append(dispatchedEvents, DispatchedEvent{nv.expectedTopic, ev})
	}
	nv.subscriber.ExpectEvents(ctx, t, dispatchedEvents)
}

func (nv *NotifeeVerifier) ExpectClose(ctx context.Context, t *testing.T) {
	nv.subscriber.ExpectCloses(ctx, t, []notifications.Topic{nv.expectedTopic})
}

func NewTestNotifee(topic notifications.Topic, bufferSize int) (notifications.Notifee, *NotifeeVerifier) {
	subscriber := NewTestSubscriber(bufferSize)
	return notifications.Notifee{
			Topic:      topic,
			Subscriber: notifications.NewMappableSubscriber(subscriber, notifications.IdentityTransform),
		}, &NotifeeVerifier{
			expectedTopic: topic,
			subscriber:    subscriber,
		}
}

type MockPublisher struct {
	notifeesLk sync.Mutex
	notifees   []notifications.Notifee
}

func (mp *MockPublisher) AddNotifees(notifees []notifications.Notifee) {
	mp.notifeesLk.Lock()
	mp.notifees = append(mp.notifees, notifees...)
	mp.notifeesLk.Unlock()
}

func (mp *MockPublisher) PublishMatchingEvents(shouldPublish func(notifications.Topic) bool, events []notifications.Event) {
	mp.notifeesLk.Lock()
	var newNotifees []notifications.Notifee
	for _, notifee := range mp.notifees {
		if shouldPublish(notifee.Topic) {
			for _, ev := range events {
				notifee.Subscriber.OnNext(notifee.Topic, ev)
			}
			notifee.Subscriber.OnClose(notifee.Topic)
		} else {
			newNotifees = append(newNotifees, notifee)
		}
	}
	mp.notifees = newNotifees
	mp.notifeesLk.Unlock()
}

func (mp *MockPublisher) PublishEvents(events []notifications.Event) {
	mp.PublishMatchingEvents(func(notifications.Topic) bool { return true }, events)
}

func (mp *MockPublisher) PublishEventsOnTopics(topics []notifications.Topic, events []notifications.Event) {
	shouldPublish := func(topic notifications.Topic) bool {
		for _, testTopic := range topics {
			if topic == testTopic {
				return true
			}
		}
		return false
	}
	mp.PublishMatchingEvents(shouldPublish, events)
}

func NewMockPublisher() *MockPublisher {
	return &MockPublisher{}
}
