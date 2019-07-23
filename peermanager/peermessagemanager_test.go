package peermanager

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"

	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/libp2p/go-libp2p-core/peer"
)

type messageSent struct {
	p       peer.ID
	message gsmsg.GraphSyncMessage
}

type fakePeer struct {
	p            peer.ID
	messagesSent chan messageSent
}

func (fp *fakePeer) Startup()  {}
func (fp *fakePeer) Shutdown() {}

func (fp *fakePeer) AddRequest(graphSyncRequest gsmsg.GraphSyncRequest) {
	message := gsmsg.New()
	message.AddRequest(graphSyncRequest)
	fp.messagesSent <- messageSent{fp.p, message}
}

func (fp *fakePeer) AddResponses([]gsmsg.GraphSyncResponse, []blocks.Block) <-chan struct{} {
	return nil
}

func makePeerQueueFactory(messagesSent chan messageSent) PeerQueueFactory {
	return func(ctx context.Context, p peer.ID) PeerQueue {
		return &fakePeer{
			p:            p,
			messagesSent: messagesSent,
		}
	}
}

func TestSendingMessagesToPeers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	messagesSent := make(chan messageSent, 5)
	peerQueueFactory := makePeerQueueFactory(messagesSent)

	tp := testutil.GeneratePeers(5)

	id := gsmsg.GraphSyncRequestID(rand.Int31())
	priority := gsmsg.GraphSyncPriority(rand.Int31())
	root := testutil.GenerateCids(1)[0]
	selector := testutil.RandomBytes(100)

	peerManager := NewMessageManager(ctx, peerQueueFactory)

	request := gsmsg.NewRequest(id, root, selector, priority)
	peerManager.SendRequest(tp[0], request)
	peerManager.SendRequest(tp[1], request)
	cancelRequest := gsmsg.CancelRequest(id)
	peerManager.SendRequest(tp[0], cancelRequest)

	select {
	case <-ctx.Done():
		t.Fatal("did not send first message")
	case firstMessage := <-messagesSent:
		if firstMessage.p != tp[0] {
			t.Fatal("First message sent to wrong peer")
		}
		request := firstMessage.message.Requests()[0]
		if request.ID() != id ||
			request.IsCancel() != false ||
			request.Priority() != priority ||
			!reflect.DeepEqual(request.Selector(), selector) {
			t.Fatal("did not send correct first message")
		}
	}
	select {
	case <-ctx.Done():
		t.Fatal("did not send second message")
	case secondMessage := <-messagesSent:
		if secondMessage.p != tp[1] {
			t.Fatal("Second message sent to wrong peer")
		}
		request := secondMessage.message.Requests()[0]
		if request.ID() != id ||
			request.IsCancel() != false ||
			request.Priority() != priority ||
			!reflect.DeepEqual(request.Selector(), selector) {
			t.Fatal("did not send correct second message")
		}
	}
	select {
	case <-ctx.Done():
		t.Fatal("did not send third message")
	case thirdMessage := <-messagesSent:
		if thirdMessage.p != tp[0] {
			t.Fatal("Third message sent to wrong peer")
		}
		request := thirdMessage.message.Requests()[0]
		if request.ID() != id ||
			request.IsCancel() != true {
			t.Fatal("third message was not a cancel")
		}
	}
	connectedPeers := peerManager.ConnectedPeers()
	if len(connectedPeers) != 2 ||
		!testutil.ContainsPeer(connectedPeers, tp[0]) ||
		!testutil.ContainsPeer(connectedPeers, tp[1]) {
		t.Fatal("did not connect all peers that were sent messages")
	}
}
