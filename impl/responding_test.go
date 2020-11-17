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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-storedcounter"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	. "github.com/filecoin-project/go-data-transfer/impl"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

func TestDataTransferResponding(t *testing.T) {
	// create network
	ctx := context.Background()
	testCases := map[string]struct {
		expectedEvents       []datatransfer.EventCode
		configureValidator   func(sv *testutil.StubbedValidator)
		configureRevalidator func(sv *testutil.StubbedRevalidator)
		verify               func(t *testing.T, h *receiverHarness)
	}{
		"new push request validates": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				require.Len(t, h.sv.ValidationsReceived, 1)
				validation := h.sv.ValidationsReceived[0]
				assert.False(t, validation.IsPull)
				assert.Equal(t, h.peers[1], validation.Other)
				assert.Equal(t, h.voucher, validation.Voucher)
				assert.Equal(t, h.baseCid, validation.BaseCid)
				assert.Equal(t, h.stor, validation.Selector)

				require.Len(t, h.transport.OpenedChannels, 1)
				openChannel := h.transport.OpenedChannels[0]
				require.Equal(t, openChannel.ChannelID, channelID(h.id, h.peers))
				require.Equal(t, openChannel.DataSender, h.peers[1])
				require.Equal(t, openChannel.Root, cidlink.Link{Cid: h.baseCid})
				require.Equal(t, openChannel.Selector, h.stor)
				require.False(t, openChannel.Message.IsRequest())
				response, ok := openChannel.Message.(datatransfer.Response)
				require.True(t, ok)
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.False(t, response.IsPaused())
				require.True(t, response.IsNew())
				require.True(t, response.IsVoucherResult())
			},
		},
		"new push request errors": {
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectErrorPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				require.Len(t, h.network.SentMessages, 1)
				responseMessage := h.network.SentMessages[0].Message
				require.False(t, responseMessage.IsRequest())
				response, ok := responseMessage.(datatransfer.Response)
				require.True(t, ok)
				require.False(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.False(t, response.IsPaused())
				require.True(t, response.IsNew())
				require.True(t, response.IsVoucherResult())
			},
		},
		"new push request pauses": {
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectPausePush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)

				require.Len(t, h.transport.OpenedChannels, 1)
				openChannel := h.transport.OpenedChannels[0]
				require.Equal(t, openChannel.ChannelID, channelID(h.id, h.peers))
				require.Equal(t, openChannel.DataSender, h.peers[1])
				require.Equal(t, openChannel.Root, cidlink.Link{Cid: h.baseCid})
				require.Equal(t, openChannel.Selector, h.stor)
				require.False(t, openChannel.Message.IsRequest())
				response, ok := openChannel.Message.(datatransfer.Response)
				require.True(t, ok)
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.True(t, response.IsPaused())
				require.True(t, response.IsNew())
				require.True(t, response.IsVoucherResult())
				require.Len(t, h.transport.PausedChannels, 1)
				require.Equal(t, channelID(h.id, h.peers), h.transport.PausedChannels[0])
			},
		},
		"new pull request validates": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPull()
			},
			verify: func(t *testing.T, h *receiverHarness) {
				response, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				require.Len(t, h.sv.ValidationsReceived, 1)
				validation := h.sv.ValidationsReceived[0]
				assert.True(t, validation.IsPull)
				assert.Equal(t, h.peers[1], validation.Other)
				assert.Equal(t, h.voucher, validation.Voucher)
				assert.Equal(t, h.baseCid, validation.BaseCid)
				assert.Equal(t, h.stor, validation.Selector)
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.False(t, response.IsPaused())
				require.True(t, response.IsNew())
				require.True(t, response.IsVoucherResult())
			},
		},
		"new pull request errors": {
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectErrorPull()
			},
			verify: func(t *testing.T, h *receiverHarness) {
				response, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.Error(t, err)
				require.False(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.False(t, response.IsPaused())
				require.True(t, response.IsNew())
				require.True(t, response.IsVoucherResult())
			},
		},
		"new pull request pauses": {
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectPausePull()
			},
			verify: func(t *testing.T, h *receiverHarness) {
				response, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.EqualError(t, err, datatransfer.ErrPause.Error())

				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.True(t, response.IsPaused())
				require.True(t, response.IsNew())
				require.True(t, response.IsVoucherResult())
				require.True(t, response.EmptyVoucherResult())
			},
		},
		"send vouchers from responder fails, push request": {
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				newVoucher := testutil.NewFakeDTType()
				err := h.dt.SendVoucher(h.ctx, channelID(h.id, h.peers), newVoucher)
				require.EqualError(t, err, "cannot send voucher for request we did not initiate")
			},
		},
		"send vouchers from responder fails, pull request": {
			verify: func(t *testing.T, h *receiverHarness) {
				_, _ = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				newVoucher := testutil.NewFakeDTType()
				err := h.dt.SendVoucher(h.ctx, channelID(h.id, h.peers), newVoucher)
				require.EqualError(t, err, "cannot send voucher for request we did not initiate")
			},
		},
		"receive voucher": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept, datatransfer.NewVoucher, datatransfer.ResumeResponder},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.voucherUpdate)
				require.EqualError(t, err, datatransfer.ErrResume.Error())
			},
		},
		"receive pause, unpause": {
			expectedEvents: []datatransfer.EventCode{
				datatransfer.Open,
				datatransfer.NewVoucherResult,
				datatransfer.Accept,
				datatransfer.PauseInitiator,
				datatransfer.ResumeInitiator},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pauseUpdate)
				require.NoError(t, err)
				_, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.resumeUpdate)
				require.NoError(t, err)
			},
		},
		"receive pause, set pause local, receive unpause": {
			expectedEvents: []datatransfer.EventCode{
				datatransfer.Open,
				datatransfer.NewVoucherResult,
				datatransfer.Accept,
				datatransfer.PauseInitiator,
				datatransfer.PauseResponder,
				datatransfer.ResumeInitiator},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pauseUpdate)
				require.NoError(t, err)
				err = h.dt.PauseDataTransferChannel(h.ctx, channelID(h.id, h.peers))
				require.NoError(t, err)
				_, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.resumeUpdate)
				require.EqualError(t, err, datatransfer.ErrPause.Error())
			},
		},
		"receive cancel": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept, datatransfer.Cancel, datatransfer.CleanupComplete},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.cancelUpdate)
				require.NoError(t, err)
				require.Len(t, h.transport.CleanedUpChannels, 1)
				require.Equal(t, channelID(h.id, h.peers), h.transport.CleanedUpChannels[0])
			},
		},
		"validate and revalidate successfully, push": {
			expectedEvents: []datatransfer.EventCode{
				datatransfer.Open,
				datatransfer.NewVoucherResult,
				datatransfer.Accept,
				datatransfer.DataReceived,
				datatransfer.NewVoucherResult,
				datatransfer.PauseResponder,
				datatransfer.NewVoucher,
				datatransfer.NewVoucherResult,
				datatransfer.ResumeResponder,
			},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			configureRevalidator: func(srv *testutil.StubbedRevalidator) {
				srv.ExpectPausePushCheck()
				srv.StubRevalidationResult(testutil.NewFakeDTType())
				srv.ExpectSuccessRevalidation()
				srv.StubCheckResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				err := h.transport.EventHandler.OnDataReceived(
					channelID(h.id, h.peers),
					cidlink.Link{Cid: testutil.GenerateCids(1)[0]},
					12345)
				require.EqualError(t, err, datatransfer.ErrPause.Error())
				require.Len(t, h.network.SentMessages, 1)
				response, ok := h.network.SentMessages[0].Message.(datatransfer.Response)
				require.True(t, ok)
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.True(t, response.IsPaused())
				require.True(t, response.IsVoucherResult())
				require.False(t, response.EmptyVoucherResult())
				response, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.voucherUpdate)
				require.EqualError(t, err, datatransfer.ErrResume.Error())
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.False(t, response.IsPaused())
				require.True(t, response.IsVoucherResult())
				require.False(t, response.EmptyVoucherResult())
			},
		},
		"validate and revalidate with err": {
			expectedEvents: []datatransfer.EventCode{
				datatransfer.Open,
				datatransfer.NewVoucherResult,
				datatransfer.Accept,
				datatransfer.DataReceived,
				datatransfer.NewVoucherResult,
			},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			configureRevalidator: func(srv *testutil.StubbedRevalidator) {
				srv.ExpectErrorPushCheck()
				srv.StubRevalidationResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				err := h.transport.EventHandler.OnDataReceived(
					channelID(h.id, h.peers),
					cidlink.Link{Cid: testutil.GenerateCids(1)[0]},
					12345)
				require.Error(t, err)
				require.Len(t, h.network.SentMessages, 1)
				response, ok := h.network.SentMessages[0].Message.(datatransfer.Response)
				require.True(t, ok)
				require.False(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.False(t, response.IsPaused())
				require.True(t, response.IsVoucherResult())
				require.False(t, response.EmptyVoucherResult())
			},
		},
		"validate and revalidate with err with second voucher": {
			expectedEvents: []datatransfer.EventCode{
				datatransfer.Open,
				datatransfer.NewVoucherResult,
				datatransfer.Accept,
				datatransfer.DataReceived,
				datatransfer.NewVoucherResult,
				datatransfer.PauseResponder,
				datatransfer.NewVoucher,
				datatransfer.NewVoucherResult,
			},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			configureRevalidator: func(srv *testutil.StubbedRevalidator) {
				srv.ExpectPausePushCheck()
				srv.StubRevalidationResult(testutil.NewFakeDTType())
				srv.ExpectErrorRevalidation()
				srv.StubCheckResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				err := h.transport.EventHandler.OnDataReceived(
					channelID(h.id, h.peers),
					cidlink.Link{Cid: testutil.GenerateCids(1)[0]},
					12345)
				require.EqualError(t, err, datatransfer.ErrPause.Error())
				require.Len(t, h.network.SentMessages, 1)
				response, ok := h.network.SentMessages[0].Message.(datatransfer.Response)
				require.True(t, ok)
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.True(t, response.IsPaused())
				require.True(t, response.IsVoucherResult())
				require.False(t, response.EmptyVoucherResult())
				response, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.voucherUpdate)
				require.Error(t, err)
				require.False(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.False(t, response.IsPaused())
				require.True(t, response.IsVoucherResult())
				require.False(t, response.EmptyVoucherResult())
			},
		},
		"validate and revalidate successfully, pull": {
			expectedEvents: []datatransfer.EventCode{
				datatransfer.Open,
				datatransfer.NewVoucherResult,
				datatransfer.Accept,
				datatransfer.DataQueued,
				datatransfer.NewVoucherResult,
				datatransfer.PauseResponder,
				datatransfer.NewVoucher,
				datatransfer.NewVoucherResult,
				datatransfer.ResumeResponder,
			},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPull()
				sv.StubResult(testutil.NewFakeDTType())
			},
			configureRevalidator: func(srv *testutil.StubbedRevalidator) {
				srv.ExpectPausePullCheck()
				srv.StubRevalidationResult(testutil.NewFakeDTType())
				srv.ExpectSuccessRevalidation()
				srv.StubCheckResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				msg, err := h.transport.EventHandler.OnDataQueued(
					channelID(h.id, h.peers),
					cidlink.Link{Cid: testutil.GenerateCids(1)[0]},
					12345)
				require.EqualError(t, err, datatransfer.ErrPause.Error())
				response, ok := msg.(datatransfer.Response)
				require.True(t, ok)
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.True(t, response.IsPaused())
				require.True(t, response.IsVoucherResult())
				require.False(t, response.EmptyVoucherResult())
				response, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.voucherUpdate)
				require.EqualError(t, err, datatransfer.ErrResume.Error())
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.False(t, response.IsPaused())
				require.True(t, response.IsVoucherResult())
				require.False(t, response.EmptyVoucherResult())
			},
		},
		"validated, finalize, and complete successfully": {
			expectedEvents: []datatransfer.EventCode{
				datatransfer.Open,
				datatransfer.NewVoucherResult,
				datatransfer.Accept,
				datatransfer.NewVoucherResult,
				datatransfer.BeginFinalizing,
				datatransfer.NewVoucher,
				datatransfer.ResumeResponder,
				datatransfer.CleanupComplete,
			},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPull()
				sv.StubResult(testutil.NewFakeDTType())
			},
			configureRevalidator: func(srv *testutil.StubbedRevalidator) {
				srv.ExpectPauseComplete()
				srv.StubRevalidationResult(testutil.NewFakeDTType())
				srv.ExpectSuccessRevalidation()
			},
			verify: func(t *testing.T, h *receiverHarness) {
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				err = h.transport.EventHandler.OnChannelCompleted(channelID(h.id, h.peers), true)
				require.NoError(t, err)
				require.Len(t, h.network.SentMessages, 1)
				response, ok := h.network.SentMessages[0].Message.(datatransfer.Response)
				require.True(t, ok)
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.True(t, response.IsPaused())
				require.True(t, response.IsVoucherResult())
				require.False(t, response.EmptyVoucherResult())
				response, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.voucherUpdate)
				require.EqualError(t, err, datatransfer.ErrResume.Error())
				require.Equal(t, response.TransferID(), h.id)
				require.True(t, response.IsVoucherResult())
				require.False(t, response.IsPaused())
			},
		},
		"validated, incomplete response": {
			expectedEvents: []datatransfer.EventCode{
				datatransfer.Open,
				datatransfer.NewVoucherResult,
				datatransfer.Accept,
				datatransfer.Error,
				datatransfer.CleanupComplete,
			},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPull()
				sv.StubResult(testutil.NewFakeDTType())
			},
			configureRevalidator: func(srv *testutil.StubbedRevalidator) {
			},
			verify: func(t *testing.T, h *receiverHarness) {
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				err = h.transport.EventHandler.OnChannelCompleted(channelID(h.id, h.peers), false)
				require.NoError(t, err)
			},
		},
		"new push request, customized transport": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				err := h.dt.RegisterTransportConfigurer(h.voucher, func(channelID datatransfer.ChannelID, voucher datatransfer.Voucher, transport datatransfer.Transport) {
					ft, ok := transport.(*testutil.FakeTransport)
					if !ok {
						return
					}
					ft.RecordCustomizedTransfer(channelID, voucher)
				})
				require.NoError(t, err)
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				require.Len(t, h.transport.CustomizedTransfers, 1)
				customizedTransfer := h.transport.CustomizedTransfers[0]
				require.Equal(t, channelID(h.id, h.peers), customizedTransfer.ChannelID)
				require.Equal(t, h.voucher, customizedTransfer.Voucher)
			},
		},
		"new pull request, customized transport": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPull()
			},
			verify: func(t *testing.T, h *receiverHarness) {
				err := h.dt.RegisterTransportConfigurer(h.voucher, func(channelID datatransfer.ChannelID, voucher datatransfer.Voucher, transport datatransfer.Transport) {
					ft, ok := transport.(*testutil.FakeTransport)
					if !ok {
						return
					}
					ft.RecordCustomizedTransfer(channelID, voucher)
				})
				require.NoError(t, err)
				_, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				require.Len(t, h.transport.CustomizedTransfers, 1)
				customizedTransfer := h.transport.CustomizedTransfers[0]
				require.Equal(t, channelID(h.id, h.peers), customizedTransfer.ChannelID)
				require.Equal(t, h.voucher, customizedTransfer.Voucher)
			},
		},
	}
	for testCase, verify := range testCases {
		t.Run(testCase, func(t *testing.T) {
			h := &receiverHarness{}
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			h.ctx = ctx
			h.peers = testutil.GeneratePeers(2)
			h.network = testutil.NewFakeNetwork(h.peers[0])
			h.transport = testutil.NewFakeTransport()
			h.ds = dss.MutexWrap(datastore.NewMapDatastore())
			h.storedCounter = storedcounter.New(h.ds, datastore.NewKey("counter"))
			dt, err := NewDataTransfer(h.ds, os.TempDir(), h.network, h.transport, h.storedCounter)
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
			h.baseCid = testutil.GenerateCids(1)[0]
			h.id = datatransfer.TransferID(rand.Int31())
			h.pullRequest, err = message.NewRequest(h.id, false, true, h.voucher.Type(), h.voucher, h.baseCid, h.stor)
			require.NoError(t, err)
			h.pushRequest, err = message.NewRequest(h.id, false, false, h.voucher.Type(), h.voucher, h.baseCid, h.stor)
			require.NoError(t, err)
			h.pauseUpdate = message.UpdateRequest(h.id, true)
			require.NoError(t, err)
			h.resumeUpdate = message.UpdateRequest(h.id, false)
			require.NoError(t, err)
			updateVoucher := testutil.NewFakeDTType()
			h.voucherUpdate, err = message.VoucherRequest(h.id, updateVoucher.Type(), updateVoucher)
			h.cancelUpdate = message.CancelRequest(h.id)
			require.NoError(t, err)
			h.sv = testutil.NewStubbedValidator()
			if verify.configureValidator != nil {
				verify.configureValidator(h.sv)
			}
			require.NoError(t, h.dt.RegisterVoucherType(h.voucher, h.sv))
			h.srv = testutil.NewStubbedRevalidator()
			if verify.configureRevalidator != nil {
				verify.configureRevalidator(h.srv)
			}
			err = h.dt.RegisterRevalidator(updateVoucher, h.srv)
			require.NoError(t, err)
			verify.verify(t, h)
			h.sv.VerifyExpectations(t)
			h.srv.VerifyExpectations(t)
			ev.verify(ctx, t)
		})
	}
}

func TestDataTransferRestartResponding(t *testing.T) {
	// create network
	ctx := context.Background()
	testCases := map[string]struct {
		expectedEvents       []datatransfer.EventCode
		configureValidator   func(sv *testutil.StubbedValidator)
		configureRevalidator func(sv *testutil.StubbedRevalidator)
		verify               func(t *testing.T, h *receiverHarness)
	}{
		"receiving a pull restart response": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Restart, datatransfer.ResumeResponder},
			verify: func(t *testing.T, h *receiverHarness) {
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)

				response, err := message.RestartResponse(channelID.ID, true, false, datatransfer.EmptyTypeIdentifier, nil)
				require.NoError(t, err)
				err = h.transport.EventHandler.OnResponseReceived(channelID, response)
				require.NoError(t, err)
			},
		},
		"receiving a push restart request validates and opens a channel for pull": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept,
				datatransfer.DataReceived, datatransfer.DataReceived, datatransfer.NewVoucherResult, datatransfer.Restart},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPush()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				// receive an incoming push
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				require.Len(t, h.sv.ValidationsReceived, 1)
				require.Len(t, h.transport.OpenedChannels, 1)
				require.Len(t, h.network.SentMessages, 0)

				// some cids are received
				chid := datatransfer.ChannelID{Initiator: h.peers[1], Responder: h.peers[0], ID: h.pushRequest.TransferID()}
				testCids := testutil.GenerateCids(2)
				ev, ok := h.dt.(datatransfer.EventsHandler)
				require.True(t, ok)
				require.NoError(t, ev.OnDataReceived(chid, cidlink.Link{Cid: testCids[0]}, 12345))
				require.NoError(t, ev.OnDataReceived(chid, cidlink.Link{Cid: testCids[1]}, 12345))

				// receive restart push request
				req, err := message.NewRequest(h.pushRequest.TransferID(), true, false, h.voucher.Type(), h.voucher,
					h.baseCid, h.stor)
				require.NoError(t, err)
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], req)
				require.Len(t, h.sv.ValidationsReceived, 2)
				require.Len(t, h.transport.OpenedChannels, 2)
				require.Len(t, h.network.SentMessages, 0)

				// validate channel that is opened second time
				openChannel := h.transport.OpenedChannels[1]
				require.Equal(t, openChannel.ChannelID, channelID(h.id, h.peers))
				require.Equal(t, openChannel.DataSender, h.peers[1])
				require.Equal(t, openChannel.Root, cidlink.Link{Cid: h.baseCid})
				require.Equal(t, openChannel.Selector, h.stor)
				// assert do not send cids are sent
				require.Equal(t, []cid.Cid{testCids[0], testCids[1]}, openChannel.DoNotSendCids)
				require.False(t, openChannel.Message.IsRequest())
				response, ok := openChannel.Message.(datatransfer.Response)
				require.True(t, ok)
				require.True(t, response.IsRestart())
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.False(t, response.IsPaused())
				require.False(t, response.IsNew())
				require.True(t, response.IsVoucherResult())

				// validate the voucher that is validated the second time
				vmsg := h.sv.ValidationsReceived[1]
				require.Equal(t, h.voucher, vmsg.Voucher)
				require.False(t, vmsg.IsPull)
				require.Equal(t, h.stor, vmsg.Selector)
				require.Equal(t, h.baseCid, vmsg.BaseCid)
				require.Equal(t, h.peers[1], vmsg.Other)
			},
		},
		"receiving a pull restart request validates and sends a success response": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept,
				datatransfer.NewVoucherResult, datatransfer.Restart},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPull()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				// receive an incoming pull
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				require.Len(t, h.sv.ValidationsReceived, 1)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 0)

				// receive restart pull request
				restartReq, err := message.NewRequest(h.id, true, true, h.voucher.Type(), h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				response, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), restartReq)
				require.NoError(t, err)
				require.Len(t, h.sv.ValidationsReceived, 2)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 0)

				// validate response
				require.True(t, response.IsRestart())
				require.True(t, response.Accepted())
				require.Equal(t, response.TransferID(), h.id)
				require.False(t, response.IsUpdate())
				require.False(t, response.IsCancel())
				require.False(t, response.IsPaused())
				require.False(t, response.IsNew())
				require.True(t, response.IsVoucherResult())

				// validate the voucher that is validated the second time
				vmsg := h.sv.ValidationsReceived[1]
				require.Equal(t, h.voucher, vmsg.Voucher)
				require.True(t, vmsg.IsPull)
				require.Equal(t, h.stor, vmsg.Selector)
				require.Equal(t, h.baseCid, vmsg.BaseCid)
				require.Equal(t, h.peers[1], vmsg.Other)
			},
		},
		"restart request fails if channel does not exist": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPull()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				// receive an incoming pull
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				require.Len(t, h.sv.ValidationsReceived, 1)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 0)

				// receive restart pull request
				restartReq, err := message.NewRequest(h.id, true, true, h.voucher.Type(), h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				p := testutil.GeneratePeers(1)[0]
				chid := datatransfer.ChannelID{ID: h.pullRequest.TransferID(), Initiator: p, Responder: h.peers[0]}
				_, err = h.transport.EventHandler.OnRequestReceived(chid, restartReq)
				require.EqualError(t, err, "No channel for this channel ID")
			},
		},
		"restart request fails if voucher validation fails": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept},
			verify: func(t *testing.T, h *receiverHarness) {
				// receive an incoming pull
				h.sv.ExpectSuccessPull()
				h.sv.StubResult(testutil.NewFakeDTType())
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				require.Len(t, h.sv.ValidationsReceived, 1)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 0)

				// receive restart pull request
				h.sv.ExpectErrorPull()
				restartReq, err := message.NewRequest(h.id, true, true, h.voucher.Type(), h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				_, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), restartReq)
				require.EqualError(t, err, "failed to validate voucher: something went wrong")
			},
		},
		"restart request fails if base cid does not match": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPull()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				// receive an incoming pull
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				require.Len(t, h.sv.ValidationsReceived, 1)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 0)

				// receive restart pull request
				randCid := testutil.GenerateCids(1)[0]
				restartReq, err := message.NewRequest(h.id, true, true, h.voucher.Type(), h.voucher, randCid, h.stor)
				require.NoError(t, err)
				_, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), restartReq)
				require.EqualError(t, err, "base cid does not match")
			},
		},
		"restart request fails if voucher type is not decodable": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPull()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				// receive an incoming pull
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				require.Len(t, h.sv.ValidationsReceived, 1)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 0)

				// receive restart pull request

				restartReq, err := message.NewRequest(h.id, true, true, "rand", h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				_, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), restartReq)
				require.EqualError(t, err, "failed to decode request voucher: unknown voucher type: rand")
			},
		},
		"restart request fails if voucher does not match": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.NewVoucherResult, datatransfer.Accept},
			configureValidator: func(sv *testutil.StubbedValidator) {
				sv.ExpectSuccessPull()
				sv.StubResult(testutil.NewFakeDTType())
			},
			verify: func(t *testing.T, h *receiverHarness) {
				// receive an incoming pull
				_, err := h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), h.pullRequest)
				require.NoError(t, err)
				require.Len(t, h.sv.ValidationsReceived, 1)
				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 0)

				// receive restart pull request
				v := testutil.NewFakeDTType()
				v.Data = "rand"
				restartReq, err := message.NewRequest(h.id, true, true, h.voucher.Type(), v, h.baseCid, h.stor)
				require.NoError(t, err)
				_, err = h.transport.EventHandler.OnRequestReceived(channelID(h.id, h.peers), restartReq)
				require.EqualError(t, err, "channel and request vouchers do not match")
			},
		},
		"ReceiveRestartExistingChannelRequest: Reopen Pull Channel": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.DataReceived, datatransfer.DataReceived},
			configureValidator: func(sv *testutil.StubbedValidator) {
			},
			verify: func(t *testing.T, h *receiverHarness) {
				// create an outgoing pull channel first
				channelID, err := h.dt.OpenPullDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)

				// some cids should already be received
				testCids := testutil.GenerateCids(2)
				ev, ok := h.dt.(datatransfer.EventsHandler)
				require.True(t, ok)
				require.NoError(t, ev.OnDataReceived(channelID, cidlink.Link{Cid: testCids[0]}, 12345))
				require.NoError(t, ev.OnDataReceived(channelID, cidlink.Link{Cid: testCids[1]}, 12345))

				// send a request to restart the same pull channel
				restartReq := message.RestartExistingChannelRequest(channelID)
				h.network.Delegate.ReceiveRestartExistingChannelRequest(ctx, h.peers[1], restartReq)

				require.Len(t, h.transport.OpenedChannels, 2)
				require.Len(t, h.network.SentMessages, 0)

				// assert correct channel was created in response to this
				require.Len(t, h.transport.OpenedChannels, 2)
				openChannel := h.transport.OpenedChannels[1]
				require.Equal(t, openChannel.ChannelID, channelID)
				require.Equal(t, openChannel.DataSender, h.peers[1])
				require.Equal(t, openChannel.Root, cidlink.Link{Cid: h.baseCid})
				require.Equal(t, openChannel.Selector, h.stor)
				require.True(t, openChannel.Message.IsRequest())
				// received cids should be a part of the channel req
				require.Equal(t, []cid.Cid{testCids[0], testCids[1]}, openChannel.DoNotSendCids)

				// assert a restart request is in the channel
				request, ok := openChannel.Message.(datatransfer.Request)
				require.True(t, ok)
				require.True(t, request.IsRestart())
				require.Equal(t, request.TransferID(), channelID.ID)
				require.Equal(t, request.BaseCid(), h.baseCid)
				require.False(t, request.IsCancel())
				require.True(t, request.IsPull())
				require.False(t, request.IsNew())

				// voucher should be sent correctly
				receivedSelector, err := request.Selector()
				require.NoError(t, err)
				require.Equal(t, receivedSelector, h.stor)
				testutil.AssertFakeDTVoucher(t, request, h.voucher)
			},
		},
		"ReceiveRestartExistingChannelRequest: Resend Push Request": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open},
			configureValidator: func(sv *testutil.StubbedValidator) {
			},
			verify: func(t *testing.T, h *receiverHarness) {
				// create an outgoing push request first
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, h.peers[1], h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)

				// send a request to restart the same push request
				restartReq := message.RestartExistingChannelRequest(channelID)
				h.network.Delegate.ReceiveRestartExistingChannelRequest(ctx, h.peers[1], restartReq)

				// assert correct message was sent in response to this
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
				require.False(t, receivedRequest.IsNew())

				// assert voucher is sent correctly
				receivedSelector, err := receivedRequest.Selector()
				require.NoError(t, err)
				require.Equal(t, receivedSelector, h.stor)
				testutil.AssertFakeDTVoucher(t, receivedRequest, h.voucher)
			},
		},
		"ReceiveRestartExistingChannelRequest: errors if peer is not the initiator": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open, datatransfer.Accept},
			configureValidator: func(sv *testutil.StubbedValidator) {
			},
			verify: func(t *testing.T, h *receiverHarness) {
				// create an incoming push first
				h.network.Delegate.ReceiveRequest(h.ctx, h.peers[1], h.pushRequest)
				require.Len(t, h.sv.ValidationsReceived, 1)

				// restart req does not anything as we are not the initiator
				chid := datatransfer.ChannelID{Initiator: h.peers[1], Responder: h.peers[0], ID: h.pushRequest.TransferID()}
				restartReq := message.RestartExistingChannelRequest(chid)
				h.network.Delegate.ReceiveRestartExistingChannelRequest(ctx, h.peers[1], restartReq)

				require.Len(t, h.transport.OpenedChannels, 1)
				require.Len(t, h.network.SentMessages, 0)
			},
		},
		"ReceiveRestartExistingChannelRequest: errors if sending peer is not the counter-party on the channel": {
			expectedEvents: []datatransfer.EventCode{datatransfer.Open},
			configureValidator: func(sv *testutil.StubbedValidator) {
			},
			verify: func(t *testing.T, h *receiverHarness) {
				// create an outgoing push request first
				p := testutil.GeneratePeers(1)[0]
				channelID, err := h.dt.OpenPushDataChannel(h.ctx, p, h.voucher, h.baseCid, h.stor)
				require.NoError(t, err)
				require.NotEmpty(t, channelID)

				// sending peer is not the counter-party on the channel
				restartReq := message.RestartExistingChannelRequest(channelID)
				h.network.Delegate.ReceiveRestartExistingChannelRequest(ctx, h.peers[1], restartReq)

				require.Len(t, h.transport.OpenedChannels, 0)
				require.Len(t, h.network.SentMessages, 1)
			},
		},
	}
	for testCase, verify := range testCases {
		t.Run(testCase, func(t *testing.T) {
			h := &receiverHarness{}
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			h.ctx = ctx
			h.peers = testutil.GeneratePeers(2)
			h.network = testutil.NewFakeNetwork(h.peers[0])
			h.transport = testutil.NewFakeTransport()
			h.ds = dss.MutexWrap(datastore.NewMapDatastore())
			h.storedCounter = storedcounter.New(h.ds, datastore.NewKey("counter"))
			dt, err := NewDataTransfer(h.ds, os.TempDir(), h.network, h.transport, h.storedCounter)
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
			h.baseCid = testutil.GenerateCids(1)[0]
			h.id = datatransfer.TransferID(rand.Int31())
			h.pullRequest, err = message.NewRequest(h.id, false, true, h.voucher.Type(), h.voucher, h.baseCid, h.stor)
			require.NoError(t, err)
			h.pushRequest, err = message.NewRequest(h.id, false, false, h.voucher.Type(), h.voucher, h.baseCid, h.stor)
			require.NoError(t, err)

			h.sv = testutil.NewStubbedValidator()
			if verify.configureValidator != nil {
				verify.configureValidator(h.sv)
			}
			require.NoError(t, h.dt.RegisterVoucherType(h.voucher, h.sv))

			verify.verify(t, h)
			h.sv.VerifyExpectations(t)
			ev.verify(ctx, t)
		})
	}
}

type receiverHarness struct {
	id            datatransfer.TransferID
	pushRequest   datatransfer.Request
	pullRequest   datatransfer.Request
	voucherUpdate datatransfer.Request
	pauseUpdate   datatransfer.Request
	resumeUpdate  datatransfer.Request
	cancelUpdate  datatransfer.Request
	ctx           context.Context
	peers         []peer.ID
	network       *testutil.FakeNetwork
	transport     *testutil.FakeTransport
	sv            *testutil.StubbedValidator
	srv           *testutil.StubbedRevalidator
	ds            datastore.Batching
	storedCounter *storedcounter.StoredCounter
	dt            datatransfer.Manager
	stor          ipld.Node
	voucher       *testutil.FakeDTType
	baseCid       cid.Cid
}

func channelID(id datatransfer.TransferID, peers []peer.ID) datatransfer.ChannelID {
	return datatransfer.ChannelID{ID: id, Initiator: peers[1], Responder: peers[0]}
}
