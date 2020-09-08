package peermanager

import (
	"context"
	"math/rand"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testutil"
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

	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()

	peerManager := NewMessageManager(ctx, peerQueueFactory)

	request := gsmsg.NewRequest(id, root, selector, priority)
	peerManager.SendRequest(tp[0], request)
	peerManager.SendRequest(tp[1], request)
	cancelRequest := gsmsg.CancelRequest(id)
	peerManager.SendRequest(tp[0], cancelRequest)

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
