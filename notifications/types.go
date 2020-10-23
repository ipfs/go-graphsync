package notifications

// Topic is a topic that events appear on
type Topic interface{}

// Event is a publishable event
type Event interface{}

// Subscriber is a subscriber that can receive events
type Subscriber interface {
	OnNext(Topic, Event)
	OnClose(Topic)
}

// MappableSubscriber is a subscriber that remaps events received to other topics
// and events
type MappableSubscriber interface {
	Subscriber
	Map(sourceID Topic, destinationID Topic)
}

// Subscribable is a stream that can be subscribed to
type Subscribable interface {
	Subscribe(topic Topic, sub Subscriber) bool
	Unsubscribe(sub Subscriber) bool
}

// Publisher is an publisher of events that can be subscribed to
type Publisher interface {
	Close(Topic)
	Publish(Topic, Event)
	Shutdown()
	Startup()
	Subscribable
}

// EventTransform if a fucntion transforms one kind of event to another
type EventTransform func(Event) Event

// Notifee is a mappable suscriber where you want events to appear
// on this specified topic (used to call SubscribeOn to setup a remapping)
type Notifee struct {
	Topic      Topic
	Subscriber MappableSubscriber
}

// SubscribeOn subscribes to the given subscribe on the given topic, but
// maps to a differnt topic specified in a notifee which has a mappable
// subscriber
func SubscribeOn(p Subscribable, topic Topic, notifee Notifee) {
	notifee.Subscriber.Map(topic, notifee.Topic)
	p.Subscribe(topic, notifee.Subscriber)
}

// IdentityTransform sets up an event transform that makes no changes
func IdentityTransform(ev Event) Event { return ev }
