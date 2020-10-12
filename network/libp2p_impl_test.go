package network_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/network"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

// Receiver is an interface for receiving messages from the DataTransferNetwork.
type receiver struct {
	messageReceived    chan struct{}
	lastRequest        datatransfer.Request
	lastRestartRequest datatransfer.Request
	lastResponse       datatransfer.Response
	lastSender         peer.ID
	connectedPeers     chan peer.ID
}

func (r *receiver) ReceiveRequest(
	ctx context.Context,
	sender peer.ID,
	incoming datatransfer.Request) {
	r.lastSender = sender
	r.lastRequest = incoming
	select {
	case <-ctx.Done():
	case r.messageReceived <- struct{}{}:
	}
}

func (r *receiver) ReceiveResponse(
	ctx context.Context,
	sender peer.ID,
	incoming datatransfer.Response) {
	r.lastSender = sender
	r.lastResponse = incoming
	select {
	case <-ctx.Done():
	case r.messageReceived <- struct{}{}:
	}
}

func (r *receiver) ReceiveError(err error) {
}

func (r *receiver) ReceiveRestartExistingChannelRequest(ctx context.Context, sender peer.ID, incoming datatransfer.Request) {
	r.lastSender = sender
	r.lastRestartRequest = incoming
	select {
	case <-ctx.Done():
	case r.messageReceived <- struct{}{}:
	}
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

	dtnet1 := network.NewFromLibp2pHost(host1)
	dtnet2 := network.NewFromLibp2pHost(host2)
	r := &receiver{
		messageReceived: make(chan struct{}),
		connectedPeers:  make(chan peer.ID, 2),
	}
	dtnet1.SetDelegate(r)
	dtnet2.SetDelegate(r)

	err = dtnet1.ConnectTo(ctx, host2.ID())
	require.NoError(t, err)

	t.Run("Send Request", func(t *testing.T) {
		baseCid := testutil.GenerateCids(1)[0]
		selector := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher().Node()
		isPull := false
		id := datatransfer.TransferID(rand.Int31())
		voucher := testutil.NewFakeDTType()
		request, err := message.NewRequest(id, false, isPull, voucher.Type(), voucher, baseCid, selector)
		require.NoError(t, err)
		require.NoError(t, dtnet1.SendMessage(ctx, host2.ID(), request))

		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case <-r.messageReceived:
		}

		sender := r.lastSender
		require.Equal(t, sender, host1.ID())

		receivedRequest := r.lastRequest
		require.NotNil(t, receivedRequest)

		assert.Equal(t, request.TransferID(), receivedRequest.TransferID())
		assert.Equal(t, request.IsCancel(), receivedRequest.IsCancel())
		assert.Equal(t, request.IsPull(), receivedRequest.IsPull())
		assert.Equal(t, request.IsRequest(), receivedRequest.IsRequest())
		assert.True(t, receivedRequest.BaseCid().Equals(request.BaseCid()))
		testutil.AssertEqualFakeDTVoucher(t, request, receivedRequest)
		testutil.AssertEqualSelector(t, request, receivedRequest)
	})

	t.Run("Send Response", func(t *testing.T) {
		accepted := false
		id := datatransfer.TransferID(rand.Int31())
		voucherResult := testutil.NewFakeDTType()
		response, err := message.NewResponse(id, accepted, false, voucherResult.Type(), voucherResult)
		require.NoError(t, err)
		require.NoError(t, dtnet2.SendMessage(ctx, host1.ID(), response))

		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case <-r.messageReceived:
		}

		sender := r.lastSender
		require.NotNil(t, sender)
		assert.Equal(t, sender, host2.ID())

		receivedResponse := r.lastResponse

		assert.Equal(t, response.TransferID(), receivedResponse.TransferID())
		assert.Equal(t, response.Accepted(), receivedResponse.Accepted())
		assert.Equal(t, response.IsRequest(), receivedResponse.IsRequest())
		testutil.AssertEqualFakeDTVoucherResult(t, response, receivedResponse)
	})

	t.Run("Send Restart Request", func(t *testing.T) {
		peers := testutil.GeneratePeers(2)
		id := datatransfer.TransferID(rand.Int31())
		chId := datatransfer.ChannelID{Initiator: peers[0],
			Responder: peers[1], ID: id}

		request := message.RestartExistingChannelRequest(chId)
		require.NoError(t, dtnet1.SendMessage(ctx, host2.ID(), request))

		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case <-r.messageReceived:
		}

		sender := r.lastSender
		require.Equal(t, sender, host1.ID())

		receivedRequest := r.lastRestartRequest
		require.NotNil(t, receivedRequest)
		achid, err := receivedRequest.RestartChannelId()
		require.NoError(t, err)
		require.Equal(t, chId, achid)
	})

}
