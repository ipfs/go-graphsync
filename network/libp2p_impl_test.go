package network_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

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
	mn := mocknet.New()

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

// Wrap a host so that we can mock out errors when calling NewStream
type wrappedHost struct {
	host.Host
	errs chan error
}

func (w wrappedHost) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (libp2pnet.Stream, error) {
	var err error
	select {
	case err = <-w.errs:
	default:
	}
	if err != nil {
		return nil, err
	}

	return w.Host.NewStream(ctx, p, pids...)
}

// TestSendMessageRetry verifies that if the number of retry attempts
// is greater than the number of errors, SendMessage will succeed.
func TestSendMessageRetry(t *testing.T) {
	tcases := []struct {
		attempts   int
		errors     int
		expSuccess bool
	}{{
		attempts:   1,
		errors:     0,
		expSuccess: true,
	}, {
		attempts:   1,
		errors:     1,
		expSuccess: false,
	}, {
		attempts:   2,
		errors:     1,
		expSuccess: true,
	}, {
		attempts:   2,
		errors:     2,
		expSuccess: false,
	}}
	for _, tcase := range tcases {
		name := fmt.Sprintf("%d attempts, %d errors", tcase.attempts, tcase.errors)
		t.Run(name, func(t *testing.T) {
			// create network
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			mn := mocknet.New()

			host1, err := mn.GenPeer()
			require.NoError(t, err)

			// Create a wrapped host that will return tcase.errors errors from
			// NewStream
			mockHost1 := &wrappedHost{
				Host: host1,
				errs: make(chan error, tcase.errors),
			}
			for i := 0; i < tcase.errors; i++ {
				mockHost1.errs <- xerrors.Errorf("network err")
			}
			host1 = mockHost1

			host2, err := mn.GenPeer()
			require.NoError(t, err)
			err = mn.LinkAll()
			require.NoError(t, err)

			retry := network.RetryParameters(
				time.Millisecond,
				time.Millisecond,
				float64(tcase.attempts),
				1)
			dtnet1 := network.NewFromLibp2pHost(host1, retry)
			dtnet2 := network.NewFromLibp2pHost(host2)
			r := &receiver{
				messageReceived: make(chan struct{}),
				connectedPeers:  make(chan peer.ID, 2),
			}
			dtnet1.SetDelegate(r)
			dtnet2.SetDelegate(r)

			err = dtnet1.ConnectTo(ctx, host2.ID())
			require.NoError(t, err)

			baseCid := testutil.GenerateCids(1)[0]
			selector := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher().Node()
			isPull := false
			id := datatransfer.TransferID(rand.Int31())
			voucher := testutil.NewFakeDTType()
			request, err := message.NewRequest(id, false, isPull, voucher.Type(), voucher, baseCid, selector)
			require.NoError(t, err)

			err = dtnet1.SendMessage(ctx, host2.ID(), request)
			if !tcase.expSuccess {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			select {
			case <-ctx.Done():
				t.Fatal("did not receive message sent")
			case <-r.messageReceived:
			}

			sender := r.lastSender
			require.Equal(t, sender, host1.ID())

			receivedRequest := r.lastRequest
			require.NotNil(t, receivedRequest)
		})
	}
}
