package message1_1_test

import (
	"math/rand"
	"testing"

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/message/message1_1"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

func TestRequestMessageForProtocol(t *testing.T) {
	baseCid := testutil.GenerateCids(1)[0]
	selector := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher().Node()
	isPull := true
	id := datatransfer.TransferID(rand.Int31())
	voucher := testutil.NewFakeDTType()

	// for the new protocol
	request, err := message1_1.NewRequest(id, false, isPull, voucher.Type(), voucher, baseCid, selector)
	require.NoError(t, err)

	out, err := request.MessageForProtocol(datatransfer.ProtocolDataTransfer1_1)
	require.NoError(t, err)
	require.Equal(t, request, out)

	// for the old protocol
	out, err = request.MessageForProtocol(datatransfer.ProtocolDataTransfer1_0)
	require.NoError(t, err)
	req, ok := out.(datatransfer.Request)
	require.True(t, ok)
	require.False(t, req.IsRestart())
	require.False(t, req.IsRestartExistingChannelRequest())
	require.Equal(t, baseCid, req.BaseCid())
	require.True(t, req.IsPull())
	n, err := req.Selector()
	require.NoError(t, err)
	require.Equal(t, selector, n)
	require.Equal(t, voucher.Type(), req.VoucherType())

	// random protocol
	out, err = request.MessageForProtocol("RAND")
	require.Error(t, err)
	require.Nil(t, out)
}

func TestRequestMessageForProtocolRestartDowngradeFails(t *testing.T) {
	baseCid := testutil.GenerateCids(1)[0]
	selector := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher().Node()
	isPull := true
	id := datatransfer.TransferID(rand.Int31())
	voucher := testutil.NewFakeDTType()

	request, err := message1_1.NewRequest(id, true, isPull, voucher.Type(), voucher, baseCid, selector)
	require.NoError(t, err)

	out, err := request.MessageForProtocol(datatransfer.ProtocolDataTransfer1_0)
	require.Nil(t, out)
	require.EqualError(t, err, "restart not supported on 1.0")

	req2 := message1_1.RestartExistingChannelRequest(datatransfer.ChannelID{})
	out, err = req2.MessageForProtocol(datatransfer.ProtocolDataTransfer1_0)
	require.Nil(t, out)
	require.EqualError(t, err, "restart not supported on 1.0")
}
