package responsemanager

import (
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-peertaskqueue/peertask"
)

// RequestIDFromTaskTopic extracts a request ID from a given peer task topic
func RequestIDFromTaskTopic(topic peertask.Topic) graphsync.RequestID {
	return topic.(responseKey).requestID
}
