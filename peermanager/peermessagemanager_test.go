package peermanager

import (
	"context"
	"math/rand"
	"testing"
	"time"

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/testutil"
)

type messageSent struct {
	p       peer.ID
	message gsmsg.GraphSyncMessage
}

var _ PeerQueue = (*fakePeer)(nil)

type fakePeer struct {
	p            peer.ID
	messagesSent chan messageSent
}

func (fp *fakePeer) AllocateAndBuildMessage(blkSize uint64, buildMessage func(b *gsmsg.Builder), notifees []notifications.Notifee) {
	builder := gsmsg.NewBuilder(gsmsg.Topic(0))
	buildMessage(builder)
	message, err := builder.Build()
	if err != nil {
		panic(err)
	}

	fp.messagesSent <- messageSent{fp.p, message}
}

func (fp *fakePeer) Startup()  {}
func (fp *fakePeer) Shutdown() {}

//func (fp *fakePeer) AddRequest(graphSyncRequest gsmsg.GraphSyncRequest, notifees ...notifications.Notifee) {
//	message := gsmsg.New()
//	message.AddRequest(graphSyncRequest)
//	fp.messagesSent <- messageSent{fp.p, message}
//}

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

	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()

	peerManager := NewMessageManager(ctx, peerQueueFactory)

	request := gsmsg.NewRequest(id, root, selector, priority)
	peerManager.AllocateAndBuildMessage(tp[0], 0, func(b *gsmsg.Builder) {
		b.AddRequest(request)
	}, []notifications.Notifee{})
	peerManager.AllocateAndBuildMessage(tp[1], 0, func(b *gsmsg.Builder) {
		b.AddRequest(request)
	}, []notifications.Notifee{})
	cancelRequest := gsmsg.CancelRequest(id)
	peerManager.AllocateAndBuildMessage(tp[0], 0, func(b *gsmsg.Builder) {
		b.AddRequest(cancelRequest)
	}, []notifications.Notifee{})

	var firstMessage messageSent
	testutil.AssertReceive(ctx, t, messagesSent, &firstMessage, "first message did not send")
	require.Equal(t, tp[0], firstMessage.p, "first message sent to incorrect peer")
	request = firstMessage.message.Requests()[0]
	require.Equal(t, id, request.ID())
	require.False(t, request.IsCancel())
	require.Equal(t, priority, request.Priority())
	require.Equal(t, selector, request.Selector())

	var secondMessage messageSent
	testutil.AssertReceive(ctx, t, messagesSent, &secondMessage, "second message did not send")
	require.Equal(t, tp[1], secondMessage.p, "second message sent to incorrect peer")
	request = secondMessage.message.Requests()[0]
	require.Equal(t, id, request.ID())
	require.False(t, request.IsCancel())
	require.Equal(t, priority, request.Priority())
	require.Equal(t, selector, request.Selector())

	var thirdMessage messageSent
	testutil.AssertReceive(ctx, t, messagesSent, &thirdMessage, "third message did not send")

	require.Equal(t, tp[0], thirdMessage.p, "third message sent to incorrect peer")
	request = thirdMessage.message.Requests()[0]
	require.Equal(t, id, request.ID())
	require.True(t, request.IsCancel())

	connectedPeers := peerManager.ConnectedPeers()
	require.Len(t, connectedPeers, 2)

	testutil.AssertContainsPeer(t, connectedPeers, tp[0])
	testutil.AssertContainsPeer(t, connectedPeers, tp[1])
}
