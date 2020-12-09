package notifications

import "sync"

type topicDataSubscriber struct {
	idMapLk        sync.RWMutex
	data           map[Topic]TopicData
	Subscriber
}

// NewTopicDataSubscriber produces a subscriber that will transform
// events and topics before passing them on to the given subscriber
func NewTopicDataSubscriber(sub Subscriber) TopicDataSubscriber {
	return &topicDataSubscriber{
		Subscriber:     sub,
		data:           make(map[Topic]TopicData),
	}
}

func (m *topicDataSubscriber) AddTopicData(id Topic, data TopicData) {
	m.idMapLk.Lock()
	m.data[id] = data
	m.idMapLk.Unlock()
}

func (m *topicDataSubscriber) transform(id Topic) Topic {
	m.idMapLk.RLock()
	defer m.idMapLk.RUnlock()
	destID, ok := m.data[id]
	if !ok {
		return id
	}
	return destID
}

func (m *topicDataSubscriber) OnNext(topic Topic, ev Event) {
	newTopic := m.transform(topic)
	m.Subscriber.OnNext(newTopic, ev)
}

func (m *topicDataSubscriber) OnClose(topic Topic) {
	newTopic := m.transform(topic)
	m.Subscriber.OnClose(newTopic)
}
