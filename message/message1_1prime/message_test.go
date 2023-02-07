package message1_1_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/encoding"
	message1_1 "github.com/filecoin-project/go-data-transfer/message/message1_1prime"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

func TestNewRequest(t *testing.T) {
	baseCid := testutil.GenerateCids(1)[0]
	selector := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher().Node()
	isPull := true
	id := datatransfer.TransferID(rand.Int31())
	voucher := testutil.NewFakeDTType()
	request, err := message1_1.NewRequest(id, false, isPull, voucher.Type(), voucher, baseCid, selector)
	require.NoError(t, err)
	assert.Equal(t, id, request.TransferID())
	assert.False(t, request.IsCancel())
	assert.False(t, request.IsUpdate())
	assert.True(t, request.IsPull())
	assert.True(t, request.IsRequest())
	assert.Equal(t, baseCid.String(), request.BaseCid().String())
	encoding.NewDecoder(request)
	testutil.AssertFakeDTVoucher(t, request, voucher)
	receivedSelector, err := request.Selector()
	require.NoError(t, err)
	require.Equal(t, selector, receivedSelector)
	// Sanity check to make sure we can cast to datatransfer.Message
	msg, ok := request.(datatransfer.Message)
	require.True(t, ok)

	assert.True(t, msg.IsRequest())
	assert.Equal(t, request.TransferID(), msg.TransferID())
	assert.False(t, msg.IsRestart())
	assert.True(t, msg.IsNew())
}

func TestRestartRequest(t *testing.T) {
	baseCid := testutil.GenerateCids(1)[0]
	selector := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher().Node()
	isPull := true
	id := datatransfer.TransferID(rand.Int31())
	voucher := testutil.NewFakeDTType()
	request, err := message1_1.NewRequest(id, true, isPull, voucher.Type(), voucher, baseCid, selector)
	require.NoError(t, err)
	assert.Equal(t, id, request.TransferID())
	assert.False(t, request.IsCancel())
	assert.False(t, request.IsUpdate())
	assert.True(t, request.IsPull())
	assert.True(t, request.IsRequest())
	assert.Equal(t, baseCid.String(), request.BaseCid().String())
	testutil.AssertFakeDTVoucher(t, request, voucher)
	receivedSelector, err := request.Selector()
	require.NoError(t, err)
	require.Equal(t, selector, receivedSelector)
	// Sanity check to make sure we can cast to datatransfer.Message
	msg, ok := request.(datatransfer.Message)
	require.True(t, ok)

	assert.True(t, msg.IsRequest())
	assert.Equal(t, request.TransferID(), msg.TransferID())
	assert.True(t, msg.IsRestart())
	assert.False(t, msg.IsNew())
}

func TestRestartExistingChannelRequest(t *testing.T) {
	t.Run("round-trip", func(t *testing.T) {
		peers := testutil.GeneratePeers(2)
		tid := uint64(1)
		chid := datatransfer.ChannelID{Initiator: peers[0],
			Responder: peers[1], ID: datatransfer.TransferID(tid)}
		req := message1_1.RestartExistingChannelRequest(chid)

		wbuf := new(bytes.Buffer)
		require.NoError(t, req.ToNet(wbuf))

		desMsg, err := message1_1.FromNet(wbuf)
		require.NoError(t, err)
		req, ok := (desMsg).(datatransfer.Request)
		require.True(t, ok)
		require.True(t, req.IsRestartExistingChannelRequest())
		achid, err := req.RestartChannelId()
		require.NoError(t, err)
		require.Equal(t, chid, achid)
	})
	t.Run("cbor-gen compat", func(t *testing.T) {
		msg, _ := hex.DecodeString("a36449735271f56752657175657374aa6442436964f66454797065076450617573f46450617274f46450756c6cf46453746f72f665566f756368f664565479706066586665724944006e526573746172744368616e6e656c83613161320168526573706f6e7365f6")
		desMsg, err := message1_1.FromNet(bytes.NewReader(msg))
		require.NoError(t, err)
		req, ok := (desMsg).(datatransfer.Request)
		require.True(t, ok)
		require.True(t, req.IsRestartExistingChannelRequest())
		achid, err := req.RestartChannelId()
		require.NoError(t, err)
		tid := uint64(1)
		chid := datatransfer.ChannelID{Initiator: peer.ID("1"),
			Responder: peer.ID("2"), ID: datatransfer.TransferID(tid)}
		require.Equal(t, chid, achid)
	})
}

func TestTransferRequest_MarshalCBOR(t *testing.T) {
	// sanity check MarshalCBOR does its thing w/o error
	req, err := NewTestTransferRequest()
	require.NoError(t, err)
	wbuf := new(bytes.Buffer)
	node := bindnode.Wrap(&req, message1_1.Prototype.TransferRequest.Type())
	err = dagcbor.Encode(node.Representation(), wbuf)
	require.NoError(t, err)
	assert.Greater(t, wbuf.Len(), 0)
}
func TestTransferRequest_UnmarshalCBOR(t *testing.T) {
	t.Run("round-trip", func(t *testing.T) {
		req, err := NewTestTransferRequest()
		require.NoError(t, err)
		wbuf := new(bytes.Buffer)
		// use ToNet / FromNet
		require.NoError(t, req.ToNet(wbuf))

		desMsg, err := message1_1.FromNet(wbuf)
		require.NoError(t, err)

		// Verify round-trip
		assert.Equal(t, req.TransferID(), desMsg.TransferID())
		assert.Equal(t, req.IsRequest(), desMsg.IsRequest())

		desReq := desMsg.(datatransfer.Request)
		assert.Equal(t, req.IsPull(), desReq.IsPull())
		assert.Equal(t, req.IsCancel(), desReq.IsCancel())
		assert.Equal(t, req.BaseCid(), desReq.BaseCid())
		testutil.AssertEqualFakeDTVoucher(t, &req, desReq)
		testutil.AssertEqualSelector(t, &req, desReq)
	})
	t.Run("cbor-gen compat", func(t *testing.T) {
		req, err := NewTestTransferRequest()
		require.NoError(t, err)

		msg, _ := hex.DecodeString("a36449735271f56752657175657374aa6442436964d82a58230012204bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a6454797065006450617573f46450617274f46450756c6cf46453746f72a1612ea065566f756368817864f55ff8f12508b63ef2bfeca7557ae90df6311a5ec1631b4a1fa843310bd9c3a710eaace5a1bdd72ad0bfe049771c11e756338bd93865e645f1adec9b9c99ef407fbd4fc6859e7904c5ad7dc9bd10a5cc16973d5b28ec1a6dd43d9f82f9f18c3d03418e3564565479706a46616b65445454797065665866657249441a4d6582216e526573746172744368616e6e656c8360600068526573706f6e7365f6")
		desMsg, err := message1_1.FromNet(bytes.NewReader(msg))
		require.NoError(t, err)

		// Verify round-trip
		assert.Equal(t, datatransfer.TransferID(1298498081), desMsg.TransferID())
		assert.Equal(t, req.IsRequest(), desMsg.IsRequest())

		desReq := desMsg.(datatransfer.Request)
		assert.Equal(t, req.IsPull(), desReq.IsPull())
		assert.Equal(t, req.IsCancel(), desReq.IsCancel())
		c, _ := cid.Parse("QmTTA2daxGqo5denp6SwLzzkLJm3fuisYEi9CoWsuHpzfb")
		assert.Equal(t, c, desReq.BaseCid())
		testutil.AssertEqualFakeDTVoucher(t, &req, desReq)
		testutil.AssertEqualSelector(t, &req, desReq)
	})
}

func TestResponses(t *testing.T) {
	id := datatransfer.TransferID(rand.Int31())
	voucherResult := testutil.NewFakeDTType()
	response, err := message1_1.NewResponse(id, false, true, voucherResult.Type(), voucherResult) // not accepted
	require.NoError(t, err)
	assert.Equal(t, response.TransferID(), id)
	assert.False(t, response.Accepted())
	assert.True(t, response.IsNew())
	assert.False(t, response.IsUpdate())
	assert.True(t, response.IsPaused())
	assert.False(t, response.IsRequest())
	testutil.AssertFakeDTVoucherResult(t, response, voucherResult)
	// Sanity check to make sure we can cast to datatransfer.Message
	msg, ok := response.(datatransfer.Message)
	require.True(t, ok)

	assert.False(t, msg.IsRequest())
	assert.True(t, msg.IsNew())
	assert.False(t, msg.IsUpdate())
	assert.True(t, msg.IsPaused())
	assert.Equal(t, response.TransferID(), msg.TransferID())
}

func TestTransferResponse_MarshalCBOR(t *testing.T) {
	id := datatransfer.TransferID(rand.Int31())
	voucherResult := testutil.NewFakeDTType()
	response, err := message1_1.NewResponse(id, true, false, voucherResult.Type(), voucherResult) // accepted
	require.NoError(t, err)

	// sanity check that we can marshal data
	wbuf := new(bytes.Buffer)
	require.NoError(t, response.ToNet(wbuf))
	assert.Greater(t, wbuf.Len(), 0)
}

func TestTransferResponse_UnmarshalCBOR(t *testing.T) {
	t.Run("round-trip", func(t *testing.T) {
		id := datatransfer.TransferID(rand.Int31())
		voucherResult := testutil.NewFakeDTType()
		response, err := message1_1.NewResponse(id, true, false, voucherResult.Type(), voucherResult) // accepted
		require.NoError(t, err)

		wbuf := new(bytes.Buffer)
		require.NoError(t, response.ToNet(wbuf))

		// verify round trip
		desMsg, err := message1_1.FromNet(wbuf)
		require.NoError(t, err)
		assert.False(t, desMsg.IsRequest())
		assert.True(t, desMsg.IsNew())
		assert.False(t, desMsg.IsUpdate())
		assert.False(t, desMsg.IsPaused())
		assert.Equal(t, id, desMsg.TransferID())

		desResp, ok := desMsg.(datatransfer.Response)
		require.True(t, ok)
		assert.True(t, desResp.Accepted())
		assert.True(t, desResp.IsNew())
		assert.False(t, desResp.IsUpdate())
		assert.False(t, desMsg.IsPaused())
		testutil.AssertFakeDTVoucherResult(t, desResp, voucherResult)
	})
	t.Run("cbor-gen compat", func(t *testing.T) {
		voucherResult := testutil.NewFakeDTType()
		voucherResult.Data = "\xf5_\xf8\xf1%\b\xb6>\xf2\xbf\xec\xa7Uz\xe9\r\xf61\x1a^\xc1c\x1bJ\x1f\xa8C1\v\xd9ç\x10\xea\xac塽\xd7*п\xe0Iw\x1c\x11\xe7V3\x8b\xd98e\xe6E\xf1\xad웜\x99\xef@\u007f\xbdOƅ\x9ey\x04ŭ}ɽ\x10\xa5\xcc\x16\x97=[(\xec\x1am\xd4=\x9f\x82\xf9\xf1\x8c=\x03A\x8e5"

		msg, _ := hex.DecodeString("a36449735271f46752657175657374f668526573706f6e7365a66454797065006441637074f56450617573f4665866657249441a4d6582216456526573817864f55ff8f12508b63ef2bfeca7557ae90df6311a5ec1631b4a1fa843310bd9c3a710eaace5a1bdd72ad0bfe049771c11e756338bd93865e645f1adec9b9c99ef407fbd4fc6859e7904c5ad7dc9bd10a5cc16973d5b28ec1a6dd43d9f82f9f18c3d03418e3564565479706a46616b65445454797065")
		desMsg, err := message1_1.FromNet(bytes.NewReader(msg))
		require.NoError(t, err)
		assert.False(t, desMsg.IsRequest())
		assert.True(t, desMsg.IsNew())
		assert.False(t, desMsg.IsUpdate())
		assert.False(t, desMsg.IsPaused())
		assert.Equal(t, datatransfer.TransferID(1298498081), desMsg.TransferID())

		desResp, ok := desMsg.(datatransfer.Response)
		require.True(t, ok)
		assert.True(t, desResp.Accepted())
		assert.True(t, desResp.IsNew())
		assert.False(t, desResp.IsUpdate())
		assert.False(t, desMsg.IsPaused())
		testutil.AssertFakeDTVoucherResult(t, desResp, voucherResult)
	})
}

func TestRequestCancel(t *testing.T) {
	t.Run("round-trip", func(t *testing.T) {
		id := datatransfer.TransferID(rand.Int31())
		req := message1_1.CancelRequest(id)
		require.Equal(t, req.TransferID(), id)
		require.True(t, req.IsRequest())
		require.True(t, req.IsCancel())
		require.False(t, req.IsUpdate())

		wbuf := new(bytes.Buffer)
		require.NoError(t, req.ToNet(wbuf))

		deserialized, err := message1_1.FromNet(wbuf)
		require.NoError(t, err)

		deserializedRequest, ok := deserialized.(datatransfer.Request)
		require.True(t, ok)
		require.Equal(t, deserializedRequest.TransferID(), req.TransferID())
		require.Equal(t, deserializedRequest.IsCancel(), req.IsCancel())
		require.Equal(t, deserializedRequest.IsRequest(), req.IsRequest())
		require.Equal(t, deserializedRequest.IsUpdate(), req.IsUpdate())
	})
	t.Run("cbor-gen compat", func(t *testing.T) {
		id := datatransfer.TransferID(1298498081)
		req := message1_1.CancelRequest(id)
		require.Equal(t, req.TransferID(), id)
		require.True(t, req.IsRequest())
		require.True(t, req.IsCancel())
		require.False(t, req.IsUpdate())

		msg, _ := hex.DecodeString("a36449735271f56752657175657374aa6442436964f66454797065026450617573f46450617274f46450756c6cf46453746f72f665566f756368f6645654797060665866657249441a4d6582216e526573746172744368616e6e656c8360600068526573706f6e7365f6")
		deserialized, err := message1_1.FromNet(bytes.NewReader(msg))
		require.NoError(t, err)

		deserializedRequest, ok := deserialized.(datatransfer.Request)
		require.True(t, ok)
		require.Equal(t, deserializedRequest.TransferID(), req.TransferID())
		require.Equal(t, deserializedRequest.IsCancel(), req.IsCancel())
		require.Equal(t, deserializedRequest.IsRequest(), req.IsRequest())
		require.Equal(t, deserializedRequest.IsUpdate(), req.IsUpdate())
	})
}

func TestRequestUpdate(t *testing.T) {
	t.Run("round-trip", func(t *testing.T) {
		id := datatransfer.TransferID(rand.Int31())
		req := message1_1.UpdateRequest(id, true)
		require.Equal(t, req.TransferID(), id)
		require.True(t, req.IsRequest())
		require.False(t, req.IsCancel())
		require.True(t, req.IsUpdate())
		require.True(t, req.IsPaused())

		wbuf := new(bytes.Buffer)
		require.NoError(t, req.ToNet(wbuf))

		deserialized, err := message1_1.FromNet(wbuf)
		require.NoError(t, err)

		deserializedRequest, ok := deserialized.(datatransfer.Request)
		require.True(t, ok)
		require.Equal(t, deserializedRequest.TransferID(), req.TransferID())
		require.Equal(t, deserializedRequest.IsCancel(), req.IsCancel())
		require.Equal(t, deserializedRequest.IsRequest(), req.IsRequest())
		require.Equal(t, deserializedRequest.IsUpdate(), req.IsUpdate())
		require.Equal(t, deserializedRequest.IsPaused(), req.IsPaused())
	})
	t.Run("cbor-gen compat", func(t *testing.T) {
		id := datatransfer.TransferID(1298498081)
		req := message1_1.UpdateRequest(id, true)

		msg, _ := hex.DecodeString("a36449735271f56752657175657374aa6442436964f66454797065016450617573f56450617274f46450756c6cf46453746f72f665566f756368f6645654797060665866657249441a4d6582216e526573746172744368616e6e656c8360600068526573706f6e7365f6")
		deserialized, err := message1_1.FromNet(bytes.NewReader(msg))
		require.NoError(t, err)

		deserializedRequest, ok := deserialized.(datatransfer.Request)
		require.True(t, ok)
		require.Equal(t, deserializedRequest.TransferID(), req.TransferID())
		require.Equal(t, deserializedRequest.IsCancel(), req.IsCancel())
		require.Equal(t, deserializedRequest.IsRequest(), req.IsRequest())
		require.Equal(t, deserializedRequest.IsUpdate(), req.IsUpdate())
		require.Equal(t, deserializedRequest.IsPaused(), req.IsPaused())
	})
}

func TestUpdateResponse(t *testing.T) {
	id := datatransfer.TransferID(rand.Int31())
	response := message1_1.UpdateResponse(id, true) // not accepted
	assert.Equal(t, response.TransferID(), id)
	assert.False(t, response.Accepted())
	assert.False(t, response.IsNew())
	assert.True(t, response.IsUpdate())
	assert.True(t, response.IsPaused())
	assert.False(t, response.IsRequest())

	// Sanity check to make sure we can cast to datatransfer.Message
	msg, ok := response.(datatransfer.Message)
	require.True(t, ok)

	assert.False(t, msg.IsRequest())
	assert.False(t, msg.IsNew())
	assert.True(t, msg.IsUpdate())
	assert.True(t, msg.IsPaused())
	assert.Equal(t, response.TransferID(), msg.TransferID())
}

func TestCancelResponse(t *testing.T) {
	id := datatransfer.TransferID(rand.Int31())
	response := message1_1.CancelResponse(id)
	assert.Equal(t, response.TransferID(), id)
	assert.False(t, response.IsNew())
	assert.False(t, response.IsUpdate())
	assert.True(t, response.IsCancel())
	assert.False(t, response.IsRequest())
	// Sanity check to make sure we can cast to datatransfer.Message
	msg, ok := response.(datatransfer.Message)
	require.True(t, ok)

	assert.False(t, msg.IsRequest())
	assert.False(t, msg.IsNew())
	assert.False(t, msg.IsUpdate())
	assert.True(t, msg.IsCancel())
	assert.Equal(t, response.TransferID(), msg.TransferID())
}

func TestCompleteResponse(t *testing.T) {
	id := datatransfer.TransferID(rand.Int31())
	response, err := message1_1.CompleteResponse(id, true, true, datatransfer.EmptyTypeIdentifier, nil)
	require.NoError(t, err)
	assert.Equal(t, response.TransferID(), id)
	assert.False(t, response.IsNew())
	assert.False(t, response.IsUpdate())
	assert.True(t, response.IsPaused())
	assert.True(t, response.IsVoucherResult())
	assert.True(t, response.EmptyVoucherResult())
	assert.True(t, response.IsComplete())
	assert.False(t, response.IsRequest())
	// Sanity check to make sure we can cast to datatransfer.Message
	msg, ok := response.(datatransfer.Message)
	require.True(t, ok)

	assert.False(t, msg.IsRequest())
	assert.False(t, msg.IsNew())
	assert.False(t, msg.IsUpdate())
	assert.Equal(t, response.TransferID(), msg.TransferID())
}
func TestToNetFromNetEquivalency(t *testing.T) {
	t.Run("round-trip", func(t *testing.T) {
		baseCid := testutil.GenerateCids(1)[0]
		selector := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher().Node()
		isPull := false
		id := datatransfer.TransferID(rand.Int31())
		accepted := false
		voucher := testutil.NewFakeDTType()
		voucherResult := testutil.NewFakeDTType()
		request, err := message1_1.NewRequest(id, false, isPull, voucher.Type(), voucher, baseCid, selector)
		require.NoError(t, err)
		buf := new(bytes.Buffer)
		err = request.ToNet(buf)
		require.NoError(t, err)
		require.Greater(t, buf.Len(), 0)
		deserialized, err := message1_1.FromNet(buf)
		require.NoError(t, err)

		deserializedRequest, ok := deserialized.(datatransfer.Request)
		require.True(t, ok)

		require.Equal(t, deserializedRequest.TransferID(), request.TransferID())
		require.Equal(t, deserializedRequest.IsCancel(), request.IsCancel())
		require.Equal(t, deserializedRequest.IsPull(), request.IsPull())
		require.Equal(t, deserializedRequest.IsRequest(), request.IsRequest())
		require.Equal(t, deserializedRequest.BaseCid(), request.BaseCid())
		testutil.AssertEqualFakeDTVoucher(t, request, deserializedRequest)
		testutil.AssertEqualSelector(t, request, deserializedRequest)

		response, err := message1_1.NewResponse(id, accepted, false, voucherResult.Type(), voucherResult)
		require.NoError(t, err)
		err = response.ToNet(buf)
		require.NoError(t, err)
		deserialized, err = message1_1.FromNet(buf)
		require.NoError(t, err)

		deserializedResponse, ok := deserialized.(datatransfer.Response)
		require.True(t, ok)

		require.Equal(t, deserializedResponse.TransferID(), response.TransferID())
		require.Equal(t, deserializedResponse.Accepted(), response.Accepted())
		require.Equal(t, deserializedResponse.IsRequest(), response.IsRequest())
		require.Equal(t, deserializedResponse.IsUpdate(), response.IsUpdate())
		require.Equal(t, deserializedResponse.IsPaused(), response.IsPaused())
		testutil.AssertEqualFakeDTVoucherResult(t, response, deserializedResponse)

		request = message1_1.CancelRequest(id)
		err = request.ToNet(buf)
		require.NoError(t, err)
		deserialized, err = message1_1.FromNet(buf)
		require.NoError(t, err)

		deserializedRequest, ok = deserialized.(datatransfer.Request)
		require.True(t, ok)

		require.Equal(t, deserializedRequest.TransferID(), request.TransferID())
		require.Equal(t, deserializedRequest.IsCancel(), request.IsCancel())
		require.Equal(t, deserializedRequest.IsRequest(), request.IsRequest())
	})
	t.Run("cbor-gen compat", func(t *testing.T) {
		baseCid := testutil.GenerateCids(1)[0]
		selector := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher().Node()
		isPull := false
		id := datatransfer.TransferID(1298498081)
		accepted := false
		voucher := testutil.NewFakeDTType()
		voucherResult := testutil.NewFakeDTType()
		request, err := message1_1.NewRequest(id, false, isPull, voucher.Type(), voucher, baseCid, selector)
		require.NoError(t, err)
		buf := new(bytes.Buffer)
		err = request.ToNet(buf)
		require.NoError(t, err)
		require.Greater(t, buf.Len(), 0)
		msg, _ := hex.DecodeString("a36449735271f56752657175657374aa6442436964d82a58230012204bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a6454797065006450617573f46450617274f46450756c6cf46453746f72a1612ea065566f756368817864f55ff8f12508b63ef2bfeca7557ae90df6311a5ec1631b4a1fa843310bd9c3a710eaace5a1bdd72ad0bfe049771c11e756338bd93865e645f1adec9b9c99ef407fbd4fc6859e7904c5ad7dc9bd10a5cc16973d5b28ec1a6dd43d9f82f9f18c3d03418e3564565479706a46616b65445454797065665866657249441a4d6582216e526573746172744368616e6e656c8360600068526573706f6e7365f6")
		deserialized, err := message1_1.FromNet(bytes.NewReader(msg))
		require.NoError(t, err)

		deserializedRequest, ok := deserialized.(datatransfer.Request)
		require.True(t, ok)

		require.Equal(t, deserializedRequest.TransferID(), request.TransferID())
		require.Equal(t, deserializedRequest.IsCancel(), request.IsCancel())
		require.Equal(t, deserializedRequest.IsPull(), request.IsPull())
		require.Equal(t, deserializedRequest.IsRequest(), request.IsRequest())
		c, _ := cid.Parse("QmTTA2daxGqo5denp6SwLzzkLJm3fuisYEi9CoWsuHpzfb")
		assert.Equal(t, c, deserializedRequest.BaseCid())
		testutil.AssertEqualFakeDTVoucher(t, request, deserializedRequest)
		testutil.AssertEqualSelector(t, request, deserializedRequest)

		response, err := message1_1.NewResponse(id, accepted, false, voucherResult.Type(), voucherResult)
		require.NoError(t, err)
		err = response.ToNet(buf)
		require.NoError(t, err)
		msg, _ = hex.DecodeString("a36449735271f46752657175657374f668526573706f6e7365a66454797065006441637074f46450617573f4665866657249441a4d65822164565265738178644204cb9a1e34c5f08e9b20aa76090e70020bb56c0ca3d3af7296cd1058a5112890fed218488f084d8df9e4835fb54ad045ffd936e3bf7261b0426c51352a097816ed74482bb9084b4a7ed8adc517f3371e0e0434b511625cd1a41792243dccdcfe88094b64565479706a46616b65445454797065")
		deserialized, err = message1_1.FromNet(bytes.NewReader(msg))
		require.NoError(t, err)

		deserializedResponse, ok := deserialized.(datatransfer.Response)
		require.True(t, ok)

		require.Equal(t, deserializedResponse.TransferID(), response.TransferID())
		require.Equal(t, deserializedResponse.Accepted(), response.Accepted())
		require.Equal(t, deserializedResponse.IsRequest(), response.IsRequest())
		require.Equal(t, deserializedResponse.IsUpdate(), response.IsUpdate())
		require.Equal(t, deserializedResponse.IsPaused(), response.IsPaused())
		testutil.AssertEqualFakeDTVoucherResult(t, response, deserializedResponse)

		request = message1_1.CancelRequest(id)
		err = request.ToNet(buf)
		require.NoError(t, err)
		msg, _ = hex.DecodeString("a36449735271f56752657175657374aa6442436964f66454797065026450617573f46450617274f46450756c6cf46453746f72f665566f756368f6645654797060665866657249441a4d6582216e526573746172744368616e6e656c8360600068526573706f6e7365f6")
		deserialized, err = message1_1.FromNet(bytes.NewReader(msg))
		require.NoError(t, err)

		deserializedRequest, ok = deserialized.(datatransfer.Request)
		require.True(t, ok)

		require.Equal(t, deserializedRequest.TransferID(), request.TransferID())
		require.Equal(t, deserializedRequest.IsCancel(), request.IsCancel())
		require.Equal(t, deserializedRequest.IsRequest(), request.IsRequest())
	})
}

func TestFromNetMessageValidation(t *testing.T) {
	// craft request message with nil request struct
	buf := []byte{0x83, 0xf5, 0xf6, 0xf6}
	msg, err := message1_1.FromNet(bytes.NewBuffer(buf))
	assert.Error(t, err)
	assert.Nil(t, msg)

	// craft response message with nil response struct
	buf = []byte{0x83, 0xf4, 0xf6, 0xf6}
	msg, err = message1_1.FromNet(bytes.NewBuffer(buf))
	assert.Error(t, err)
	assert.Nil(t, msg)
}

func NewTestTransferRequest() (message1_1.TransferRequest1_1, error) {
	bcid := testutil.GenerateCids(1)[0]
	selector := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any).Matcher().Node()
	isPull := false
	id := datatransfer.TransferID(rand.Int31())
	voucher := testutil.NewFakeDTType()
	req, err := message1_1.NewRequest(id, false, isPull, voucher.Type(), voucher, bcid, selector)
	if err != nil {
		return message1_1.TransferRequest1_1{}, err
	}
	tr, ok := req.(*message1_1.TransferRequest1_1)
	if !ok {
		return message1_1.TransferRequest1_1{}, fmt.Errorf("expected *message1_1.TransferRequest1_1")
	}
	return *tr, nil
}
