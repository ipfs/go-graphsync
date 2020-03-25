package network

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testutil"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type receiver struct {
	messageReceived chan struct{}
	lastMessage     gsmsg.GraphSyncMessage
	lastSender      peer.ID
	connectedPeers  chan peer.ID
}

func (r *receiver) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming gsmsg.GraphSyncMessage) {
	r.lastSender = sender
	r.lastMessage = incoming
	select {
	case <-ctx.Done():
	case r.messageReceived <- struct{}{}:
	}
}

func (r *receiver) ReceiveError(err error) {
}

func (r *receiver) Connected(p peer.ID) {
	r.connectedPeers <- p
}

func (r *receiver) Disconnected(p peer.ID) {
}

func TestMessageSendAndReceive(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	host1, err := mn.GenPeer()
	require.NoError(t, err)
	host2, err := mn.GenPeer()
	require.NoError(t, err)
	err = mn.LinkAll()
	require.NoError(t, err)
	gsnet1 := NewFromLibp2pHost(host1)
	gsnet2 := NewFromLibp2pHost(host2)
	r := &receiver{
		messageReceived: make(chan struct{}),
		connectedPeers:  make(chan peer.ID, 2),
	}
	gsnet1.SetDelegate(r)
	gsnet2.SetDelegate(r)

	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	selector := ssb.Matcher().Node()
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: testutil.RandomBytes(100),
	}
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	status := graphsync.RequestAcknowledged

	sent := gsmsg.New()
	sent.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	sent.AddResponse(gsmsg.NewResponse(id, status, extension))

	err = gsnet1.ConnectTo(ctx, host2.ID())
	require.NoError(t, err, "did not connect peers")

	err = gsnet1.SendMessage(ctx, host2.ID(), sent)
	require.NoError(t, err)

	testutil.AssertDoesReceive(ctx, t, r.messageReceived, "message did not send")

	require.Equal(t, r.lastSender, host1.ID(), "incorrect host sent message")

	received := r.lastMessage

	sentRequests := sent.Requests()
	require.Len(t, sentRequests, 1, "did not add request to sent message")
	sentRequest := sentRequests[0]
	receivedRequests := received.Requests()
	require.Len(t, receivedRequests, 1, "did not add request to received message")
	receivedRequest := receivedRequests[0]
	require.Equal(t, receivedRequest.ID(), sentRequest.ID())
	require.Equal(t, receivedRequest.IsCancel(), sentRequest.IsCancel())
	require.Equal(t, receivedRequest.Priority(), sentRequest.Priority())
	require.Equal(t, receivedRequest.Root().String(), sentRequest.Root().String())
	require.Equal(t, receivedRequest.Selector(), sentRequest.Selector())

	sentResponses := sent.Responses()
	require.Len(t, sentResponses, 1, "did not add response to sent message")
	sentResponse := sentResponses[0]
	receivedResponses := received.Responses()
	require.Len(t, receivedResponses, 1, "did not add response to received message")
	receivedResponse := receivedResponses[0]
	extensionData, found := receivedResponse.Extension(extensionName)
	require.Equal(t, receivedResponse.RequestID(), sentResponse.RequestID())
	require.Equal(t, receivedResponse.Status(), sentResponse.Status())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	for i := 0; i < 2; i++ {
		testutil.AssertDoesReceive(ctx, t, r.connectedPeers, "peers were not notified")
	}

}
