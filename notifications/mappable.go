package notifications

import "sync"

type mappableSubscriber struct {
	idMapLk        sync.RWMutex
	idMap          map[Topic]Topic
	eventTransform EventTransform
	Subscriber
}

// NewMappableSubscriber produces a subscriber that will transform
// events and topics before passing them on to the given subscriber
func NewMappableSubscriber(sub Subscriber, eventTransform EventTransform) MappableSubscriber {
	return &mappableSubscriber{
		Subscriber:     sub,
		eventTransform: eventTransform,
		idMap:          make(map[Topic]Topic),
	}
}

func (m *mappableSubscriber) Map(id Topic, destinationID Topic) {
	m.idMapLk.Lock()
	m.idMap[id] = destinationID
	m.idMapLk.Unlock()
}

func (m *mappableSubscriber) transform(id Topic) Topic {
	m.idMapLk.RLock()
	defer m.idMapLk.RUnlock()
	destID, ok := m.idMap[id]
	if !ok {
		return id
	}
	return destID
}

func (m *mappableSubscriber) OnNext(topic Topic, ev Event) {
	newTopic := m.transform(topic)
	newEv := m.eventTransform(ev)
	m.Subscriber.OnNext(newTopic, newEv)
}

func (m *mappableSubscriber) OnClose(topic Topic) {
	newTopic := m.transform(topic)
	m.Subscriber.OnClose(newTopic)
}
