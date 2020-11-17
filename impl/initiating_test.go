package impl_test

import (
	"context"
	"math/rand"
	"os"
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
		options        []DataTransferOption
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
		"Remove Timed-out request": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Error, datatransfer.CleanupComplete},
			options:        []DataTransferOption{ChannelRemoveTimeout(10 * time.Millisecond)},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NoError(t, h.transport.EventHandler.OnRequestTimedOut(ctx, channelID))
				// need time for the events to take place
				time.Sleep(1 * time.Second)
			},
		},
		"Remove disconnected request": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Disconnected, datatransfer.Error, datatransfer.CleanupComplete},
			options:        []DataTransferOption{ChannelRemoveTimeout(10 * time.Millisecond)},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NoError(t, h.transport.EventHandler.OnRequestDisconnected(ctx, channelID))
				// need time for the events to take place
				time.Sleep(1 * time.Second)
			},
		},
		"Remove disconnected push request": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept, datatransfer.ResumeResponder, datatransfer.Disconnected, datatransfer.Error, datatransfer.CleanupComplete},
			options:        []DataTransferOption{ChannelRemoveTimeout(10 * time.Millisecond)},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				response, err := message.NewResponse(channelID.ID, true, false, datatransfer.EmptyTypeIdentifier, nil)
				require.NoError(t, err)
				err = h.transport.EventHandler.OnResponseReceived(channelID, response)
				require.NoError(t, err)
				require.NoError(t, h.transport.EventHandler.OnRequestDisconnected(ctx, channelID))
				// need time for the events to take place
				time.Sleep(1 * time.Second)
			},
		},
		"Disconnected request resumes": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Disconnected, datatransfer.DataReceived},
			options:        []DataTransferOption{ChannelRemoveTimeout(10 * time.Millisecond)},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NoError(t, h.transport.EventHandler.OnRequestDisconnected(ctx, channelID))
				testCids := testutil.GenerateCids(1)
				require.NoError(t, h.transport.EventHandler.OnDataReceived(channelID, cidlink.Link{Cid: testCids[0]}, uint64(12345)))

				// need time for the events to take place
				time.Sleep(1 * time.Second)
			},
		},
		"Disconnected request resumes, push": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept, datatransfer.ResumeResponder, datatransfer.Disconnected, datatransfer.DataSent},
			options:        []DataTransferOption{ChannelRemoveTimeout(10 * time.Millisecond)},
			verify: func(t *testing.T, h *harness) {
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				response, err := message.NewResponse(channelID.ID, true, false, datatransfer.EmptyTypeIdentifier, nil)
				require.NoError(t, err)
				err = h.transport.EventHandler.OnResponseReceived(channelID, response)
				require.NoError(t, err)
				require.NoError(t, h.transport.EventHandler.OnRequestDisconnected(ctx, channelID))
				testCids := testutil.GenerateCids(1)
				require.NoError(t, h.transport.EventHandler.OnDataSent(channelID, cidlink.Link{Cid: testCids[0]}, uint64(12345)))

				// need time for the events to take place
				time.Sleep(1 * time.Second)
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

		// test for new protocol -> new protocol
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
			dt, err := NewDataTransfer(h.ds, os.TempDir(), h.network, h.transport, h.storedCounter, verify.options...)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt)
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

func TestDataTransferRestartInitiating(t *testing.T) {
	// create network
	ctx := context.Background()
	testCases := map[string]struct {
		expectedEvents []datatransfer.EventCode
		verify         func(t *testing.T, h *harness)
	}{
		"RestartDataTransferChannel: Manager Peer Create Pull Restart works": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.DataReceived, datatransfer.DataReceived},
			verify: func(t *testing.T, h *harness) {
				// open a pull channel
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				require.Len(t, h.transport.OpenedChannels, 1)
				require.Len(t, h.network.SentMessages, 0)

				// some cids should already be received
				testCids := testutil.GenerateCids(2)
				ev, ok := h.dt.(datatransfer.EventsHandler)
				require.True(t, ok)
				require.NoError(t, ev.OnDataReceived(channelID, cidlink.Link{Cid: testCids[0]}, 12345))
				require.NoError(t, ev.OnDataReceived(channelID, cidlink.Link{Cid: testCids[1]}, 12345))

				// restart that pull channel
				err = h.dt.RestartDataTransferChannel(ctx, channelID)
				require.NoError(t, err)
				require.Len(t, h.transport.OpenedChannels, 2)
				require.Len(t, h.network.SentMessages, 0)

				openChannel := h.transport.OpenedChannels[1]
				require.Equal(t, openChannel.ChannelID, channelID)
				require.Equal(t, openChannel.DataSender, h.peers[1])
				require.Equal(t, openChannel.Root, cidlink.Link{Cid: h.baseCid})
				require.Equal(t, openChannel.Selector, h.stor)
				require.True(t, openChannel.Message.IsRequest())
				// received cids should be a part of the channel req
				require.Equal(t, []cid.Cid{testCids[0], testCids[1]}, openChannel.DoNotSendCids)

				receivedRequest, ok := openChannel.Message.(datatransfer.Request)
				require.True(t, ok)
				require.Equal(t, receivedRequest.TransferID(), channelID.ID)
				require.Equal(t, receivedRequest.BaseCid(), h.baseCid)
				require.False(t, receivedRequest.IsCancel())
				require.True(t, receivedRequest.IsPull())
				// assert the second channel open is a restart request
				require.True(t, receivedRequest.IsRestart())

				// voucher should be sent correctly
				receivedSelector, err := receivedRequest.Selector()
				require.NoError(t, err)
				require.Equal(t, receivedSelector, h.stor)
				testutil.AssertFakeDTVoucher(t, receivedRequest, h.voucher)
			},
		},
		"RestartDataTransferChannel: Manager Peer Create Push Restart works": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open},
			verify: func(t *testing.T, h *harness) {
				// open a push channel
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 1)

				// restart that push channel
				err = h.dt.RestartDataTransferChannel(ctx, channelID)
				require.NoError(t, err)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 2)

				// assert restart request is well formed
				messageReceived := h.network.SentMessages[1]
				require.Equal(t, messageReceived.PeerID, h.peers[1])
				received := messageReceived.Message
				require.True(t, received.IsRequest())
				receivedRequest, ok := received.(datatransfer.Request)
				require.True(t, ok)
				require.Equal(t, receivedRequest.TransferID(), channelID.ID)
				require.Equal(t, receivedRequest.BaseCid(), h.baseCid)
				require.False(t, receivedRequest.IsCancel())
				require.False(t, receivedRequest.IsPull())
				require.True(t, receivedRequest.IsRestart())

				// assert voucher is sent correctly
				receivedSelector, err := receivedRequest.Selector()
				require.NoError(t, err)
				require.Equal(t, receivedSelector, h.stor)
				testutil.AssertFakeDTVoucher(t, receivedRequest, h.voucher)
			},
		},
		"RestartDataTransferChannel: Manager Peer Receive Push Restart works ": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept},
			verify: func(t *testing.T, h *harness) {
				ctx := context.Background()
				// receive a push request
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				require.Len(t, h.transport.OpenedChannels, 1)
				require.Len(t, h.network.SentMessages, 0)
				require.Len(t, h.voucherValidator.ValidationsReceived, 1)

				// restart the push request received above and validate it
				chid := datatransfer.ChannelID{Initiator: h.peers[1], Responder: h.peers[0], ID: h.pushRequest.TransferID()}
				require.NoError(t, h.dt.RestartDataTransferChannel(ctx, chid))
				require.Len(t, h.voucherValidator.ValidationsReceived, 2)
				require.Len(t, h.transport.OpenedChannels, 1)
				require.Len(t, h.network.SentMessages, 1)

				// assert validation on restart
				vmsg := h.voucherValidator.ValidationsReceived[1]
				require.Equal(t, h.voucher, vmsg.Voucher)
				require.False(t, vmsg.IsPull)
				require.Equal(t, h.stor, vmsg.Selector)
				require.Equal(t, h.baseCid, vmsg.BaseCid)
				require.Equal(t, h.peers[1], vmsg.Other)

				// assert req was sent correctly
				req := h.network.SentMessages[0]
				require.Equal(t, req.PeerID, h.peers[1])
				received := req.Message
				require.True(t, received.IsRequest())
				receivedRequest, ok := received.(datatransfer.Request)
				require.True(t, ok)
				require.True(t, receivedRequest.IsRestartExistingChannelRequest())
				achId, err := receivedRequest.RestartChannelId()
				require.NoError(t, err)
				require.Equal(t, chid, achId)

				h.voucherValidator.ExpectSuccessPush()
			},
		},
		"RestartDataTransferChannel: Manager Peer Receive Pull Restart works ": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept},
			verify: func(t *testing.T, h *harness) {
				ctx := context.Background()
				// receive a pull request
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pullRequest)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 1)
				require.Len(t, h.voucherValidator.ValidationsReceived, 1)

				// restart the pull request received above
				h.voucherValidator.ExpectSuccessPull()
				chid := datatransfer.ChannelID{Initiator: h.peers[1], Responder: h.peers[0], ID: h.pullRequest.TransferID()}
				require.NoError(t, h.dt.RestartDataTransferChannel(ctx, chid))
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 2)
				require.Len(t, h.voucherValidator.ValidationsReceived, 2)

				// assert validation on restart
				vmsg := h.voucherValidator.ValidationsReceived[1]
				require.Equal(t, h.voucher, vmsg.Voucher)
				require.True(t, vmsg.IsPull)
				require.Equal(t, h.stor, vmsg.Selector)
				require.Equal(t, h.baseCid, vmsg.BaseCid)
				require.Equal(t, h.peers[1], vmsg.Other)

				// assert req was sent correctly
				req := h.network.SentMessages[1]
				require.Equal(t, req.PeerID, h.peers[1])
				received := req.Message
				require.True(t, received.IsRequest())
				receivedRequest, ok := received.(datatransfer.Request)
				require.True(t, ok)
				require.True(t, receivedRequest.IsRestartExistingChannelRequest())
				achId, err := receivedRequest.RestartChannelId()
				require.NoError(t, err)
				require.Equal(t, chid, achId)
			},
		},
		"RestartDataTransferChannel: Manager Peer Receive Pull Restart fails if validation fails ": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept},
			verify: func(t *testing.T, h *harness) {
				ctx := context.Background()
				// receive a pull request
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pullRequest)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 1)
				require.Len(t, h.voucherValidator.ValidationsReceived, 1)

				// restart the pull request received above
				h.voucherValidator.ExpectErrorPull()
				chid := datatransfer.ChannelID{Initiator: h.peers[1], Responder: h.peers[0], ID: h.pullRequest.TransferID()}
				require.EqualError(t, h.dt.RestartDataTransferChannel(ctx, chid), "failed to restart channel, validation error: something went wrong")
			},
		},
		"RestartDataTransferChannel: Manager Peer Receive Push Restart fails if validation fails ": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept},
			verify: func(t *testing.T, h *harness) {
				ctx := context.Background()
				// receive a push request
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				require.Len(t, h.transport.OpenedChannels, 1)
				require.Len(t, h.network.SentMessages, 0)
				require.Len(t, h.voucherValidator.ValidationsReceived, 1)

				// restart the pull request received above
				h.voucherValidator.ExpectErrorPush()
				chid := datatransfer.ChannelID{Initiator: h.peers[1], Responder: h.peers[0], ID: h.pushRequest.TransferID()}
				require.EqualError(t, h.dt.RestartDataTransferChannel(ctx, chid), "failed to restart channel, validation error: something went wrong")
			},
		},
		"Fails if channel does not exist": {
			expectedEvents: nil,
			verify: func(t *testing.T, h *harness) {
				channelId := datatransfer.ChannelID{}
				require.Error(t, h.dt.RestartDataTransferChannel(context.Background(), channelId))
			},
		},
	}

	for testCase, verify := range testCases {
		t.Run(testCase, func(t *testing.T) {
			h := &harness{}
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			// create the harness
			h.ctx = ctx
			h.peers = testutil.GeneratePeers(2)
			h.network = testutil.NewFakeNetwork(h.peers[0])
			h.transport = testutil.NewFakeTransport()
			h.ds = dss.MutexWrap(datastore.NewMapDatastore())
			h.storedCounter = storedcounter.New(h.ds, datastore.NewKey("counter"))
			h.voucherValidator = testutil.NewStubbedValidator()

			// setup data transfer``
			dt, err := NewDataTransfer(h.ds, os.TempDir(), h.network, h.transport, h.storedCounter)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt)
			h.dt = dt

			// setup eventing
			ev := eventVerifier{
				expectedEvents: verify.expectedEvents,
				events:         make(chan datatransfer.EventCode, len(verify.expectedEvents)),
			}
			ev.setup(t, dt)

			// setup voucher processing
			h.stor = testutil.AllSelector()
			h.voucher = testutil.NewFakeDTType()
			require.NoError(t, h.dt.RegisterVoucherType(h.voucher, h.voucherValidator))
			h.voucherResult = testutil.NewFakeDTType()
			err = h.dt.RegisterVoucherResultType(h.voucherResult)
			require.NoError(t, err)
			h.baseCid = testutil.GenerateCids(1)[0]

			h.id = datatransfer.TransferID(rand.Int31())
			h.pushRequest, err = message.NewRequest(h.id, false, false, h.voucher.Type(), h.voucher, h.baseCid, h.stor)
			require.NoError(t, err)
			h.pullRequest, err = message.NewRequest(h.id, false, true, h.voucher.Type(), h.voucher, h.baseCid, h.stor)
			require.NoError(t, err)

			// run tests steps and verify
			verify.verify(t, h)
			ev.verify(ctx, t)
			h.voucherValidator.VerifyExpectations(t)
		})
	}
}

type harness struct {
	ctx              context.Context
	peers            []peer.ID
	network          *testutil.FakeNetwork
	transport        *testutil.FakeTransport
	ds               datastore.Batching
	storedCounter    *storedcounter.StoredCounter
	dt               datatransfer.Manager
	voucherValidator *testutil.StubbedValidator
	stor             ipld.Node
	voucher          *testutil.FakeDTType
	voucherResult    *testutil.FakeDTType
	baseCid          cid.Cid

	id          datatransfer.TransferID
	pushRequest datatransfer.Request
	pullRequest datatransfer.Request
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
