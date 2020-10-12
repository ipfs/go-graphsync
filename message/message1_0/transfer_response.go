package message1_0

import (
	"io"

	"github.com/libp2p/go-libp2p-core/protocol"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/encoding"
	"github.com/filecoin-project/go-data-transfer/message/types"
)

//go:generate cbor-gen-for transferResponse

// transferResponse is a private struct that satisfies the datatransfer.Response interface
type transferResponse struct {
	Type   uint64
	Acpt   bool
	Paus   bool
	XferID uint64
	VRes   *cbg.Deferred
	VTyp   datatransfer.TypeIdentifier
}

func (trsp *transferResponse) TransferID() datatransfer.TransferID {
	return datatransfer.TransferID(trsp.XferID)
}

func (trsp *transferResponse) IsRestart() bool {
	return false
}

func (trsp *transferResponse) MessageForProtocol(targetProtocol protocol.ID) (datatransfer.Message, error) {
	switch targetProtocol {
	case datatransfer.ProtocolDataTransfer1_0:
		return trsp, nil
	default:
		return nil, xerrors.Errorf("protocol not supported")
	}
}

// IsRequest always returns false in this case because this is a transfer response
func (trsp *transferResponse) IsRequest() bool {
	return false
}

// IsNew returns true if this is the first response sent
func (trsp *transferResponse) IsNew() bool {
	return trsp.Type == uint64(types.NewMessage)
}

// IsUpdate returns true if this response is an update
func (trsp *transferResponse) IsUpdate() bool {
	return trsp.Type == uint64(types.UpdateMessage)
}

// IsPaused returns true if the responder is paused
func (trsp *transferResponse) IsPaused() bool {
	return trsp.Paus
}

// IsCancel returns true if the responder has cancelled this response
func (trsp *transferResponse) IsCancel() bool {
	return trsp.Type == uint64(types.CancelMessage)
}

// IsComplete returns true if the responder has completed this response
func (trsp *transferResponse) IsComplete() bool {
	return trsp.Type == uint64(types.CompleteMessage)
}

func (trsp *transferResponse) IsVoucherResult() bool {
	return trsp.Type == uint64(types.VoucherResultMessage) || trsp.Type == uint64(types.NewMessage) ||
		trsp.Type == uint64(types.CompleteMessage)
}

// 	Accepted returns true if the request is accepted in the response
func (trsp *transferResponse) Accepted() bool {
	return trsp.Acpt
}

func (trsp *transferResponse) VoucherResultType() datatransfer.TypeIdentifier {
	return trsp.VTyp
}

func (trsp *transferResponse) VoucherResult(decoder encoding.Decoder) (encoding.Encodable, error) {
	if trsp.VRes == nil {
		return nil, xerrors.New("No voucher present to read")
	}
	return decoder.DecodeFromCbor(trsp.VRes.Raw)
}

func (trsp *transferResponse) EmptyVoucherResult() bool {
	return trsp.VTyp == datatransfer.EmptyTypeIdentifier
}

// ToNet serializes a transfer response. It's a wrapper for MarshalCBOR to provide
// symmetry with FromNet
func (trsp *transferResponse) ToNet(w io.Writer) error {
	msg := transferMessage{
		IsRq:     false,
		Request:  nil,
		Response: trsp,
	}
	return msg.MarshalCBOR(w)
}
