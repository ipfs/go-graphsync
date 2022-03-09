package message1_1_test

import (
	"math/rand"
	"testing"

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	message1_1 "github.com/filecoin-project/go-data-transfer/message/message1_1prime"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

func TestRequestMessageForProtocol(t *testing.T) {
	baseCid := testutil.GenerateCids(1)[0]
	selector := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher().Node()
	isPull := true
	id := datatransfer.TransferID(rand.Int31())
	voucher := testutil.NewFakeDTType()

	// for the new protocols
	request, err := message1_1.NewRequest(id, false, isPull, voucher.Type(), voucher, baseCid, selector)
	require.NoError(t, err)

	out12, err := request.MessageForProtocol(datatransfer.ProtocolDataTransfer1_2)
	require.NoError(t, err)
	require.Equal(t, request, out12)

	req, ok := out12.(datatransfer.Request)
	require.True(t, ok)
	require.False(t, req.IsRestart())
	require.False(t, req.IsRestartExistingChannelRequest())
	require.Equal(t, baseCid, req.BaseCid())
	require.True(t, req.IsPull())
	n, err := req.Selector()
	require.NoError(t, err)
	require.Equal(t, selector, n)
	require.Equal(t, voucher.Type(), req.VoucherType())
}
