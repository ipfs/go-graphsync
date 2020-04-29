package channels_test

import (
	"testing"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels"
	"github.com/filecoin-project/go-data-transfer/testutil"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
)

type fakeVoucher struct{}

func (fv *fakeVoucher) ToBytes() ([]byte, error) {
	panic("not implemented")
}

func (fv *fakeVoucher) FromBytes(_ []byte) error {
	panic("not implemented")
}

func (fv *fakeVoucher) Type() string {
	panic("not implemented")
}

func TestChannels(t *testing.T) {
	channels := channels.New()

	tid1 := datatransfer.TransferID(0)
	tid2 := datatransfer.TransferID(1)
	fv1 := &fakeVoucher{}
	fv2 := &fakeVoucher{}
	cids := testutil.GenerateCids(2)
	selector := builder.NewSelectorSpecBuilder(basicnode.Style.Any).Matcher().Node()
	peers := testutil.GeneratePeers(4)

	t.Run("adding channels", func(t *testing.T) {
		chid, err := channels.CreateNew(tid1, cids[0], selector, fv1, peers[0], peers[0], peers[1])
		require.NoError(t, err)
		require.Equal(t, peers[0], chid.Initiator)
		require.Equal(t, tid1, chid.ID)

		// cannot add twice for same channel id
		_, err = channels.CreateNew(tid1, cids[1], selector, fv2, peers[0], peers[1], peers[0])
		require.Error(t, err)

		// can add for different id
		chid, err = channels.CreateNew(tid2, cids[1], selector, fv2, peers[3], peers[2], peers[3])
		require.NoError(t, err)
		require.Equal(t, peers[3], chid.Initiator)
		require.Equal(t, tid2, chid.ID)
	})

	t.Run("in progress channels", func(t *testing.T) {
		inProgress := channels.InProgress()
		require.Len(t, inProgress, 2)
		require.Contains(t, inProgress, datatransfer.ChannelID{Initiator: peers[0], ID: tid1})
		require.Contains(t, inProgress, datatransfer.ChannelID{Initiator: peers[3], ID: tid2})
	})

	t.Run("get by id and sender", func(t *testing.T) {
		state := channels.GetByIDAndSender(datatransfer.ChannelID{Initiator: peers[0], ID: tid1}, peers[0])
		require.NotEqual(t, datatransfer.EmptyChannelState, state)
		require.Equal(t, cids[0], state.BaseCID())
		require.Equal(t, selector, state.Selector())
		require.Equal(t, fv1, state.Voucher())
		require.Equal(t, peers[0], state.Sender())
		require.Equal(t, peers[1], state.Recipient())

		// empty if channel does not exist
		state = channels.GetByIDAndSender(datatransfer.ChannelID{Initiator: peers[1], ID: tid1}, peers[0])
		require.Equal(t, datatransfer.EmptyChannelState, state)
		// empty if channel exists but sender does not match
		state = channels.GetByIDAndSender(datatransfer.ChannelID{Initiator: peers[0], ID: tid1}, peers[1])
		require.Equal(t, datatransfer.EmptyChannelState, state)

		// works for other channel as well
		state = channels.GetByIDAndSender(datatransfer.ChannelID{Initiator: peers[3], ID: tid2}, peers[2])
		require.NotEqual(t, datatransfer.EmptyChannelState, state)
	})
}
