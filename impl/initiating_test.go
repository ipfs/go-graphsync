package impl_test

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-storedcounter"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels"
	. "github.com/filecoin-project/go-data-transfer/impl"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

func TestDataTransferInitiating(t *testing.T) {
	// create network
	ctx := context.Background()
	testCases := map[string]struct {
		expectedEvents []datatransfer.EventCode
		verify         func(t *testing.T, h *harness)
	}{
		"OpenPushDataTransfer": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				require.Equal(t, channelID.Initiator, h.peers[0])
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 1)
				messageReceived := h.network.SentMessages[0]
				require.Equal(t, messageReceived.PeerID, h.peers[1])
				received := messageReceived.Message
				require.True(t, received.IsRequest())
				receivedRequest, ok := received.(datatransfer.Request)
				require.True(t, ok)
				require.Equal(t, receivedRequest.TransferID(), channelID.ID)
				require.Equal(t, receivedRequest.BaseCid(), h.baseCid)
				require.False(t, receivedRequest.IsCancel())
				require.False(t, receivedRequest.IsPull())
				receivedSelector, err := receivedRequest.Selector()
				require.NoError(t, err)
				require.Equal(t, receivedSelector, h.stor)
				testutil.AssertFakeDTVoucher(t, receivedRequest, h.voucher)
			},
		},
		"OpenPullDataTransfer": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				require.Equal(t, channelID.Initiator, h.peers[0])
				require.Len(t, h.network.SentMessages, 0)
				require.Len(t, h.transport.OpenedChannels, 1)
				openChannel := h.transport.OpenedChannels[0]
				require.Equal(t, openChannel.ChannelID, channelID)
				require.Equal(t, openChannel.DataSender, h.peers[1])
				require.Equal(t, openChannel.Root, cidlink.Link{Cid: h.baseCid})
				require.Equal(t, openChannel.Selector, h.stor)
				require.True(t, openChannel.Message.IsRequest())
				receivedRequest, ok := openChannel.Message.(datatransfer.Request)
				require.True(t, ok)
				require.Equal(t, receivedRequest.TransferID(), channelID.ID)
				require.Equal(t, receivedRequest.BaseCid(), h.baseCid)
				require.False(t, receivedRequest.IsCancel())
				require.True(t, receivedRequest.IsPull())
				receivedSelector, err := receivedRequest.Selector()
				require.NoError(t, err)
				require.Equal(t, receivedSelector, h.stor)
				testutil.AssertFakeDTVoucher(t, receivedRequest, h.voucher)
			},
		},
		"SendVoucher with no channel open": {
			verify: func(t *testing.T, h *harness) {
				err := h.dt.SendVoucher(h.ctx, datatransfer.ChannelID{Initiator: h.peers[1], Responder: h.peers[0], ID: 999999}, h.voucher)
				require.EqualError(t, err, channels.ErrNotFound.Error())
			},
		},
		"SendVoucher with channel open, push succeeds": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucher},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				voucher := testutil.NewFakeDTType()
				err = h.dt.SendVoucher(ctx, channelID, voucher)
				require.NoError(t, err)
				require.Len(t, h.network.SentMessages, 2)
				received := h.network.SentMessages[1].Message
				require.True(t, received.IsRequest())
				receivedRequest, ok := received.(datatransfer.Request)
				require.True(t, ok)
				require.True(t, receivedRequest.IsVoucher())
				require.False(t, receivedRequest.IsCancel())
				testutil.AssertFakeDTVoucher(t, receivedRequest, voucher)
			},
		},
		"SendVoucher with channel open, pull succeeds": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucher},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				voucher := testutil.NewFakeDTType()
				err = h.dt.SendVoucher(ctx, channelID, voucher)
				require.NoError(t, err)
				require.Len(t, h.transport.OpenedChannels, 1)
				require.Len(t, h.network.SentMessages, 1)
				received := h.network.SentMessages[0].Message
				require.True(t, received.IsRequest())
				receivedRequest, ok := received.(datatransfer.Request)
				require.True(t, ok)
				require.False(t, receivedRequest.IsCancel())
				require.True(t, receivedRequest.IsVoucher())
				testutil.AssertFakeDTVoucher(t, receivedRequest, voucher)
			},
		},
		"reregister voucher type again errors": {
			verify: func(t *testing.T, h *harness) {
				voucher := testutil.NewFakeDTType()
				sv := testutil.NewStubbedValidator()
				err := h.dt.RegisterVoucherType(h.voucher, sv)
				require.NoError(t, err)
				err = h.dt.RegisterVoucherType(voucher, sv)
				require.EqualError(t, err, "error registering voucher type: identifier already registered: FakeDTType")
			},
		},
		"reregister non pointer errors": {
			verify: func(t *testing.T, h *harness) {
				sv := testutil.NewStubbedValidator()
				err := h.dt.RegisterVoucherType(h.voucher, sv)
				require.NoError(t, err)
				err = h.dt.RegisterVoucherType(testutil.FakeDTType{}, sv)
				require.EqualError(t, err, "error registering voucher type: registering entry type FakeDTType: type must be a pointer")
			},
		},
		"success response": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept, datatransfer.ResumeResponder},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				response, err := message.NewResponse(channelID.ID, true, false, datatransfer.EmptyTypeIdentifier, nil)
				require.NoError(t, err)
				err = h.transport.EventHandler.OnResponseReceived(channelID, response)
				require.NoError(t, err)
			},
		},
		"success response, w/ voucher result": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept, datatransfer.ResumeResponder},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				response, err := message.NewResponse(channelID.ID, true, false, h.voucherResult.Type(), h.voucherResult)
				require.NoError(t, err)
				err = h.transport.EventHandler.OnResponseReceived(channelID, response)
				require.NoError(t, err)
			},
		},
		"push request, pause behavior": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept, datatransfer.ResumeResponder, datatransfer.PauseInitiator, datatransfer.ResumeInitiator},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				response, err := message.NewResponse(channelID.ID, true, false, datatransfer.EmptyTypeIdentifier, nil)
				require.NoError(t, err)
				err = h.transport.EventHandler.OnResponseReceived(channelID, response)
				require.NoError(t, err)
				err = h.dt.PauseDataTransferChannel(h.ctx, channelID)
				require.NoError(t, err)
				require.Len(t, h.transport.PausedChannels, 1)
				require.Equal(t, h.transport.PausedChannels[0], channelID)
				require.Len(t, h.network.SentMessages, 2)
				pauseMessage := h.network.SentMessages[1].Message
				require.True(t, pauseMessage.IsUpdate())
				require.True(t, pauseMessage.IsPaused())
				require.True(t, pauseMessage.IsRequest())
				require.False(t, pauseMessage.IsCancel())
				require.Equal(t, pauseMessage.TransferID(), channelID.ID)
				err = h.dt.ResumeDataTransferChannel(h.ctx, channelID)
				require.NoError(t, err)
				require.Len(t, h.transport.ResumedChannels, 1)
				resumedChannel := h.transport.ResumedChannels[0]
				require.Equal(t, resumedChannel.ChannelID, channelID)
				resumeMessage := resumedChannel.Message
				require.True(t, resumeMessage.IsUpdate())
				require.False(t, resumeMessage.IsPaused())
				require.True(t, resumeMessage.IsRequest())
				require.False(t, resumeMessage.IsCancel())
				require.Equal(t, resumeMessage.TransferID(), channelID.ID)
			},
		},
		"close push request": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Cancel, datatransfer.CleanupComplete},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				err = h.dt.CloseDataTransferChannel(h.ctx, channelID)
				require.NoError(t, err)
				require.Len(t, h.transport.ClosedChannels, 1)
				require.Equal(t, h.transport.ClosedChannels[0], channelID)
				require.Len(t, h.network.SentMessages, 2)
				cancelMessage := h.network.SentMessages[1].Message
				require.False(t, cancelMessage.IsUpdate())
				require.False(t, cancelMessage.IsPaused())
				require.True(t, cancelMessage.IsRequest())
				require.True(t, cancelMessage.IsCancel())
				require.Equal(t, cancelMessage.TransferID(), channelID.ID)
			},
		},
		"pull request, pause behavior": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept, datatransfer.ResumeResponder, datatransfer.PauseInitiator, datatransfer.ResumeInitiator},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				response, err := message.NewResponse(channelID.ID, true, false, datatransfer.EmptyTypeIdentifier, nil)
				require.NoError(t, err)
				err = h.transport.EventHandler.OnResponseReceived(channelID, response)
				require.NoError(t, err)
				err = h.dt.PauseDataTransferChannel(h.ctx, channelID)
				require.NoError(t, err)
				require.Len(t, h.transport.PausedChannels, 1)
				require.Equal(t, h.transport.PausedChannels[0], channelID)
				require.Len(t, h.network.SentMessages, 1)
				pauseMessage := h.network.SentMessages[0].Message
				require.True(t, pauseMessage.IsUpdate())
				require.True(t, pauseMessage.IsPaused())
				require.True(t, pauseMessage.IsRequest())
				require.False(t, pauseMessage.IsCancel())
				require.Equal(t, pauseMessage.TransferID(), channelID.ID)
				err = h.dt.ResumeDataTransferChannel(h.ctx, channelID)
				require.NoError(t, err)
				require.Len(t, h.transport.ResumedChannels, 1)
				resumedChannel := h.transport.ResumedChannels[0]
				require.Equal(t, resumedChannel.ChannelID, channelID)
				resumeMessage := resumedChannel.Message
				require.True(t, resumeMessage.IsUpdate())
				require.False(t, resumeMessage.IsPaused())
				require.True(t, resumeMessage.IsRequest())
				require.False(t, resumeMessage.IsCancel())
				require.Equal(t, resumeMessage.TransferID(), channelID.ID)
			},
		},
		"close pull request": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Cancel, datatransfer.CleanupComplete},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				err = h.dt.CloseDataTransferChannel(h.ctx, channelID)
				require.NoError(t, err)
				require.Len(t, h.transport.ClosedChannels, 1)
				require.Equal(t, h.transport.ClosedChannels[0], channelID)
				require.Len(t, h.network.SentMessages, 1)
				cancelMessage := h.network.SentMessages[0].Message
				require.False(t, cancelMessage.IsUpdate())
				require.False(t, cancelMessage.IsPaused())
				require.True(t, cancelMessage.IsRequest())
				require.True(t, cancelMessage.IsCancel())
				require.Equal(t, cancelMessage.TransferID(), channelID.ID)
			},
		},
		"customizing push transfer": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open},
			verify: func(t *testing.T, h *harness) {
				err := h.dt.RegisterTransportConfigurer(h.voucher, func(channelID datatransfer.ChannelID, voucher datatransfer.Voucher, transport datatransfer.Transport) {
					ft, ok := transport.(*testutil.FakeTransport)
					if !ok {
						return
					}
					ft.RecordCustomizedTransfer(channelID, voucher)
				})
				require.NoError(t, err)
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				require.Len(t, h.transport.CustomizedTransfers, 1)
				customizedTransfer := h.transport.CustomizedTransfers[0]
				require.Equal(t, channelID, customizedTransfer.ChannelID)
				require.Equal(t, h.voucher, customizedTransfer.Voucher)
			},
		},
		"customizing pull transfer": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open},
			verify: func(t *testing.T, h *harness) {
				err := h.dt.RegisterTransportConfigurer(h.voucher, func(channelID datatransfer.ChannelID, voucher datatransfer.Voucher, transport datatransfer.Transport) {
					ft, ok := transport.(*testutil.FakeTransport)
					if !ok {
						return
					}
					ft.RecordCustomizedTransfer(channelID, voucher)
				})
				require.NoError(t, err)
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				require.Len(t, h.transport.CustomizedTransfers, 1)
				customizedTransfer := h.transport.CustomizedTransfers[0]
				require.Equal(t, channelID, customizedTransfer.ChannelID)
				require.Equal(t, h.voucher, customizedTransfer.Voucher)
			},
		},
	}
	for testCase, verify := range testCases {
		t.Run(testCase, func(t *testing.T) {
			h := &harness{}
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			h.ctx = ctx
			h.peers = testutil.GeneratePeers(2)
			h.network = testutil.NewFakeNetwork(h.peers[0])
			h.transport = testutil.NewFakeTransport()
			h.ds = dss.MutexWrap(datastore.NewMapDatastore())
			h.storedCounter = storedcounter.New(h.ds, datastore.NewKey("counter"))
			dt, err := NewDataTransfer(h.ds, h.network, h.transport, h.storedCounter)
			require.NoError(t, err)
			err = dt.Start(ctx)
			require.NoError(t, err)
			h.dt = dt
			ev := eventVerifier{
				expectedEvents: verify.expectedEvents,
				events:         make(chan datatransfer.EventCode, len(verify.expectedEvents)),
			}
			ev.setup(t, dt)
			h.stor = testutil.AllSelector()
			h.voucher = testutil.NewFakeDTType()
			h.voucherResult = testutil.NewFakeDTType()
			err = h.dt.RegisterVoucherResultType(h.voucherResult)
			require.NoError(t, err)
			h.baseCid = testutil.GenerateCids(1)[0]
			verify.verify(t, h)
			ev.verify(ctx, t)
		})
	}
}

type harness struct {
	ctx           context.Context
	peers         []peer.ID
	network       *testutil.FakeNetwork
	transport     *testutil.FakeTransport
	ds            datastore.Datastore
	storedCounter *storedcounter.StoredCounter
	dt            datatransfer.Manager
	stor          ipld.Node
	voucher       *testutil.FakeDTType
	voucherResult *testutil.FakeDTType
	baseCid       cid.Cid
}

type eventVerifier struct {
	expectedEvents []datatransfer.EventCode
	events         chan datatransfer.EventCode
}

func (e eventVerifier) setup(t *testing.T, dt datatransfer.Manager) {
	if len(e.expectedEvents) > 0 {
		received := 0
		max := len(e.expectedEvents)
		dt.SubscribeToEvents(func(evt datatransfer.Event, state datatransfer.ChannelState) {
			received++
			if received > max {
				t.Fatalf("received too many events: %s", datatransfer.Events[evt.Code])
			}
			e.events <- evt.Code
		})
	}
}

func (e eventVerifier) verify(ctx context.Context, t *testing.T) {
	if len(e.expectedEvents) > 0 {
		receivedEvents := make([]datatransfer.EventCode, 0, len(e.expectedEvents))
		for i := 0; i < len(e.expectedEvents); i++ {
			select {
			case <-ctx.Done():
				t.Fatal("did not receive expected events")
			case event := <-e.events:
				receivedEvents = append(receivedEvents, event)
			}
		}
		require.Equal(t, e.expectedEvents, receivedEvents)
	}
}
