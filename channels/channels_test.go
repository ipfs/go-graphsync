package channels_test

import (
	"context"
	"errors"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels"
	"github.com/filecoin-project/go-data-transfer/encoding"
	"github.com/filecoin-project/go-data-transfer/testutil"
	"github.com/ipfs/go-datastore"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
)

func TestChannels(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	decoderByType := func(identifier datatransfer.TypeIdentifier) (encoding.Decoder, bool) {
		if identifier == testutil.NewFakeDTType().Type() {
			decoder, err := encoding.NewDecoder(testutil.NewFakeDTType())
			if err != nil {
				return nil, false
			}
			return decoder, true
		}
		return nil, false
	}
	ds := datastore.NewMapDatastore()
	received := make(chan event)
	notifier := func(evt datatransfer.Event, chst datatransfer.ChannelState) {
		received <- event{evt, chst}
	}
	net := testutil.NewFakeNetwork(testutil.GeneratePeers(1)[0])
	channelList, err := channels.New(ds, notifier, decoderByType, decoderByType, net)
	require.NoError(t, err)

	tid1 := datatransfer.TransferID(0)
	tid2 := datatransfer.TransferID(1)
	fv1 := &testutil.FakeDTType{}
	fv2 := &testutil.FakeDTType{}
	cids := testutil.GenerateCids(2)
	selector := builder.NewSelectorSpecBuilder(basicnode.Style.Any).Matcher().Node()
	peers := testutil.GeneratePeers(4)

	t.Run("adding channels", func(t *testing.T) {
		chid, err := channelList.CreateNew(tid1, cids[0], selector, fv1, peers[0], peers[0], peers[1])
		require.NoError(t, err)
		require.Equal(t, peers[0], chid.Initiator)
		require.Equal(t, tid1, chid.ID)

		// cannot add twice for same channel id
		_, err = channelList.CreateNew(tid1, cids[1], selector, fv2, peers[0], peers[1], peers[0])
		require.Error(t, err)
		state := checkEvent(ctx, t, received, datatransfer.Open)
		require.Equal(t, datatransfer.Requested, state.Status())
		// can add for different id
		chid, err = channelList.CreateNew(tid2, cids[1], selector, fv2, peers[3], peers[2], peers[3])
		require.NoError(t, err)
		require.Equal(t, peers[3], chid.Initiator)
		require.Equal(t, tid2, chid.ID)
		state = checkEvent(ctx, t, received, datatransfer.Open)
		require.Equal(t, datatransfer.Requested, state.Status())
	})

	t.Run("in progress channels", func(t *testing.T) {
		inProgress, err := channelList.InProgress(ctx)
		require.NoError(t, err)
		require.Len(t, inProgress, 2)
		require.Contains(t, inProgress, datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.Contains(t, inProgress, datatransfer.ChannelID{Initiator: peers[3], Responder: peers[2], ID: tid2})
	})

	t.Run("get by id", func(t *testing.T) {
		state, err := channelList.GetByID(ctx, datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		require.NotEqual(t, channels.EmptyChannelState, state)
		require.Equal(t, cids[0], state.BaseCID())
		require.Equal(t, selector, state.Selector())
		require.Equal(t, fv1, state.Voucher())
		require.Equal(t, peers[0], state.Sender())
		require.Equal(t, peers[1], state.Recipient())

		// empty if channel does not exist
		state, err = channelList.GetByID(ctx, datatransfer.ChannelID{Initiator: peers[1], Responder: peers[1], ID: tid1})
		require.Equal(t, nil, state)
		require.EqualError(t, err, channels.ErrNotFound.Error())

		// works for other channel as well
		state, err = channelList.GetByID(ctx, datatransfer.ChannelID{Initiator: peers[3], Responder: peers[2], ID: tid2})
		require.NotEqual(t, nil, state)
		require.NoError(t, err)
	})

	t.Run("accept", func(t *testing.T) {
		state, err := channelList.GetByID(ctx, datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		require.Equal(t, state.Status(), datatransfer.Requested)

		err = channelList.Accept(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.Accept)
		require.Equal(t, state.Status(), datatransfer.Ongoing)

		err = channelList.Accept(datatransfer.ChannelID{Initiator: peers[1], Responder: peers[0], ID: tid1})
		require.EqualError(t, err, channels.ErrNotFound.Error())
	})

	t.Run("updating send/receive values", func(t *testing.T) {
		state, err := channelList.GetByID(ctx, datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		require.Equal(t, uint64(0), state.Received())
		require.Equal(t, uint64(0), state.Sent())

		err = channelList.IncrementReceived(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1}, 50)
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.Progress)
		require.Equal(t, uint64(50), state.Received())
		require.Equal(t, uint64(0), state.Sent())

		err = channelList.IncrementSent(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1}, 100)
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.Progress)
		require.Equal(t, uint64(50), state.Received())
		require.Equal(t, uint64(100), state.Sent())

		// errors if channel does not exist
		err = channelList.IncrementReceived(datatransfer.ChannelID{Initiator: peers[1], Responder: peers[0], ID: tid1}, 200)
		require.EqualError(t, err, channels.ErrNotFound.Error())
		err = channelList.IncrementSent(datatransfer.ChannelID{Initiator: peers[1], Responder: peers[0], ID: tid1}, 200)
		require.EqualError(t, err, channels.ErrNotFound.Error())

		err = channelList.IncrementReceived(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1}, 50)
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.Progress)
		require.Equal(t, uint64(100), state.Received())
		require.Equal(t, uint64(100), state.Sent())

		err = channelList.IncrementSent(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1}, 25)
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.Progress)
		require.Equal(t, uint64(100), state.Received())
		require.Equal(t, uint64(125), state.Sent())
	})

	t.Run("pause/resume", func(t *testing.T) {
		state, err := channelList.GetByID(ctx, datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		require.Equal(t, datatransfer.Ongoing, state.Status())

		err = channelList.PauseInitiator(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.PauseInitiator)
		require.Equal(t, datatransfer.InitiatorPaused, state.Status())

		err = channelList.PauseResponder(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.PauseResponder)
		require.Equal(t, datatransfer.BothPaused, state.Status())

		err = channelList.ResumeInitiator(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.ResumeInitiator)
		require.Equal(t, datatransfer.ResponderPaused, state.Status())

		err = channelList.ResumeResponder(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.ResumeResponder)
		require.Equal(t, datatransfer.Ongoing, state.Status())
	})

	t.Run("new vouchers & voucherResults", func(t *testing.T) {
		fv3 := testutil.NewFakeDTType()
		fvr1 := testutil.NewFakeDTType()

		state, err := channelList.GetByID(ctx, datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		require.Equal(t, []datatransfer.Voucher{fv1}, state.Vouchers())
		require.Equal(t, fv1, state.Voucher())
		require.Equal(t, fv1, state.LastVoucher())

		err = channelList.NewVoucher(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1}, fv3)
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.NewVoucher)
		require.Equal(t, []datatransfer.Voucher{fv1, fv3}, state.Vouchers())
		require.Equal(t, fv1, state.Voucher())
		require.Equal(t, fv3, state.LastVoucher())

		state, err = channelList.GetByID(ctx, datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		require.Equal(t, []datatransfer.VoucherResult{}, state.VoucherResults())

		err = channelList.NewVoucherResult(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1}, fvr1)
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.NewVoucherResult)
		require.Equal(t, []datatransfer.VoucherResult{fvr1}, state.VoucherResults())
		require.Equal(t, fvr1, state.LastVoucherResult())
	})

	t.Run("test finality", func(t *testing.T) {
		state, err := channelList.GetByID(ctx, datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		require.Equal(t, datatransfer.Ongoing, state.Status())

		err = channelList.Complete(datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: tid1})
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.Complete)
		require.Equal(t, datatransfer.Completing, state.Status())
		state = checkEvent(ctx, t, received, datatransfer.CleanupComplete)
		require.Equal(t, datatransfer.Completed, state.Status())

		state, err = channelList.GetByID(ctx, datatransfer.ChannelID{Initiator: peers[3], Responder: peers[2], ID: tid2})
		require.NoError(t, err)
		require.Equal(t, datatransfer.Requested, state.Status())

		err = channelList.Error(datatransfer.ChannelID{Initiator: peers[3], Responder: peers[2], ID: tid2}, errors.New("something went wrong"))
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.Error)
		require.Equal(t, datatransfer.Failing, state.Status())
		require.Equal(t, "something went wrong", state.Message())
		state = checkEvent(ctx, t, received, datatransfer.CleanupComplete)
		require.Equal(t, datatransfer.Failed, state.Status())

		chid, err := channelList.CreateNew(tid2, cids[1], selector, fv2, peers[2], peers[1], peers[2])
		require.NoError(t, err)
		require.Equal(t, peers[2], chid.Initiator)
		require.Equal(t, tid2, chid.ID)
		state = checkEvent(ctx, t, received, datatransfer.Open)
		require.Equal(t, datatransfer.Requested, state.Status())

		err = channelList.Cancel(datatransfer.ChannelID{Initiator: peers[2], Responder: peers[1], ID: tid2})
		require.NoError(t, err)
		state = checkEvent(ctx, t, received, datatransfer.Cancel)
		require.Equal(t, datatransfer.Cancelling, state.Status())
		state = checkEvent(ctx, t, received, datatransfer.CleanupComplete)
		require.Equal(t, datatransfer.Cancelled, state.Status())
	})
}

type event struct {
	event datatransfer.Event
	state datatransfer.ChannelState
}

func checkEvent(ctx context.Context, t *testing.T, received chan event, code datatransfer.EventCode) datatransfer.ChannelState {
	var evt event
	select {
	case evt = <-received:
	case <-ctx.Done():
		t.Fatal("did not receive event")
	}
	require.Equal(t, code, evt.event.Code)
	return evt.state
}
