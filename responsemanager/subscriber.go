package responsemanager

import (
	"context"
	"errors"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/responsemanager/peerresponsemanager"
	"github.com/libp2p/go-libp2p-core/peer"
)

var errNetworkError = errors.New("network error")

type subscriber struct {
	p                     peer.ID
	request               gsmsg.GraphSyncRequest
	ctx                   context.Context
	messages              chan responseManagerMessage
	blockSentListeners    BlockSentListeners
	networkErrorListeners NetworkErrorListeners
	completedListeners    CompletedListeners
}

func (s *subscriber) OnNext(topic notifications.Topic, event notifications.Event) {
	responseEvent, ok := event.(peerresponsemanager.Event)
	if !ok {
		return
	}
	blockData, isBlockData := topic.(graphsync.BlockData)
	if isBlockData {
		switch responseEvent.Name {
		case peerresponsemanager.Error:
			s.networkErrorListeners.NotifyNetworkErrorListeners(s.p, s.request, responseEvent.Err)
			select {
			case s.messages <- &errorRequestMessage{s.p, s.request.ID(), errNetworkError, make(chan error, 1)}:
			case <-s.ctx.Done():
			}
		case peerresponsemanager.Sent:
			s.blockSentListeners.NotifyBlockSentListeners(s.p, s.request, blockData)
		}
		return
	}
	status, isStatus := topic.(graphsync.ResponseStatusCode)
	if isStatus {
		switch responseEvent.Name {
		case peerresponsemanager.Error:
			s.networkErrorListeners.NotifyNetworkErrorListeners(s.p, s.request, responseEvent.Err)
			select {
			case s.messages <- &errorRequestMessage{s.p, s.request.ID(), errNetworkError, make(chan error, 1)}:
			case <-s.ctx.Done():
			}
		case peerresponsemanager.Sent:
			s.completedListeners.NotifyCompletedListeners(s.p, s.request, status)
		}
	}
}

func (s *subscriber) OnClose(topic notifications.Topic) {

}
