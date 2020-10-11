package responsemanager

import (
	"context"
	"errors"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	"github.com/libp2p/go-libp2p-core/peer"
)

var errNetworkError = errors.New("network error")

type subscriber struct {
	ctx                   context.Context
	messages              chan responseManagerMessage
	blockSentListeners    BlockSentListeners
	networkErrorListeners NetworkErrorListeners
	completedListeners    CompletedListeners
}

type blockSentNotification struct {
	p         peer.ID
	requestID graphsync.RequestID
	blockData graphsync.BlockData
}

type statusNotification struct {
	p         peer.ID
	requestID graphsync.RequestID
	status    graphsync.ResponseStatusCode
}

func (s *subscriber) OnNext(topic notifications.Topic, event notifications.Event) {
	responseEvent, ok := event.(peerresponsemanager.Event)
	if !ok {
		return
	}
	bsn, isBsn := topic.(blockSentNotification)
	if isBsn {
		switch responseEvent.Name {
		case peerresponsemanager.Error:
			s.networkErrorListeners.NotifyNetworkErrorListeners(bsn.p, bsn.requestID, responseEvent.Err)
			select {
			case s.messages <- &errorRequestMessage{bsn.p, bsn.requestID, errNetworkError, make(chan error, 1)}:
			case <-s.ctx.Done():
			}
		case peerresponsemanager.Sent:
			s.blockSentListeners.NotifyBlockSentListeners(bsn.p, bsn.requestID, bsn.blockData)
		}
		return
	}
	sn, isSn := topic.(statusNotification)
	if isSn {
		switch responseEvent.Name {
		case peerresponsemanager.Error:
			s.networkErrorListeners.NotifyNetworkErrorListeners(sn.p, sn.requestID, responseEvent.Err)
			select {
			case s.messages <- &errorRequestMessage{sn.p, sn.requestID, errNetworkError, make(chan error, 1)}:
			case <-s.ctx.Done():
			}
		case peerresponsemanager.Sent:
			s.completedListeners.NotifyCompletedListeners(sn.p, sn.requestID, sn.status)
		}
	}
}

func (s *subscriber) OnClose(topic notifications.Topic) {

}
