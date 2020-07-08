package testutil

import (
	"testing"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/message"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
)

// NewDTRequest makes a new DT Request message
func NewDTRequest(t *testing.T, transferID datatransfer.TransferID) message.DataTransferRequest {
	voucher := NewFakeDTType()
	baseCid := GenerateCids(1)[0]
	selector := builder.NewSelectorSpecBuilder(basicnode.Style.Any).Matcher().Node()
	r, err := message.NewRequest(transferID, false, voucher.Type(), voucher, baseCid, selector)
	require.NoError(t, err)
	return r
}

// NewDTResponse makes a new DT Request message
func NewDTResponse(t *testing.T, transferID datatransfer.TransferID) message.DataTransferResponse {
	vresult := NewFakeDTType()
	r, err := message.NewResponse(transferID, false, false, vresult.Type(), vresult)
	require.NoError(t, err)
	return r
}
