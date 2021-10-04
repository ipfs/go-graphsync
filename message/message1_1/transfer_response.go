package message1_1

import (
	"io"

	"github.com/libp2p/go-libp2p-core/protocol"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/encoding"
	"github.com/filecoin-project/go-data-transfer/message/message1_0"
	"github.com/filecoin-project/go-data-transfer/message/types"
)

//go:generate cbor-gen-for --map-encoding transferResponse1_1

// transferResponse1_1 is a private struct that satisfies the datatransfer.Response interface
// It is the response message for the Data Transfer 1.1 and 1.2 Protocol.
type transferResponse1_1 struct {
	Type   uint64
	Acpt   bool
	Paus   bool
	XferID uint64
	VRes   *cbg.Deferred
	VTyp   datatransfer.TypeIdentifier
}

func (trsp *transferResponse1_1) TransferID() datatransfer.TransferID {
	return datatransfer.TransferID(trsp.XferID)
}

// IsRequest always returns false in this case because this is a transfer response
func (trsp *transferResponse1_1) IsRequest() bool {
	return false
}

// IsNew returns true if this is the first response sent
func (trsp *transferResponse1_1) IsNew() bool {
	return trsp.Type == uint64(types.NewMessage)
}

// IsUpdate returns true if this response is an update
func (trsp *transferResponse1_1) IsUpdate() bool {
	return trsp.Type == uint64(types.UpdateMessage)
}

// IsPaused returns true if the responder is paused
func (trsp *transferResponse1_1) IsPaused() bool {
	return trsp.Paus
}

// IsCancel returns true if the responder has cancelled this response
func (trsp *transferResponse1_1) IsCancel() bool {
	return trsp.Type == uint64(types.CancelMessage)
}

// IsComplete returns true if the responder has completed this response
func (trsp *transferResponse1_1) IsComplete() bool {
	return trsp.Type == uint64(types.CompleteMessage)
}

func (trsp *transferResponse1_1) IsVoucherResult() bool {
	return trsp.Type == uint64(types.VoucherResultMessage) || trsp.Type == uint64(types.NewMessage) || trsp.Type == uint64(types.CompleteMessage) ||
		trsp.Type == uint64(types.RestartMessage)
}

// 	Accepted returns true if the request is accepted in the response
func (trsp *transferResponse1_1) Accepted() bool {
	return trsp.Acpt
}

func (trsp *transferResponse1_1) VoucherResultType() datatransfer.TypeIdentifier {
	return trsp.VTyp
}

func (trsp *transferResponse1_1) VoucherResult(decoder encoding.Decoder) (encoding.Encodable, error) {
	if trsp.VRes == nil {
		return nil, xerrors.New("No voucher present to read")
	}
	return decoder.DecodeFromCbor(trsp.VRes.Raw)
}

func (trq *transferResponse1_1) IsRestart() bool {
	return trq.Type == uint64(types.RestartMessage)
}

func (trsp *transferResponse1_1) EmptyVoucherResult() bool {
	return trsp.VTyp == datatransfer.EmptyTypeIdentifier
}

func (trsp *transferResponse1_1) MessageForProtocol(targetProtocol protocol.ID) (datatransfer.Message, error) {
	switch targetProtocol {
	case datatransfer.ProtocolDataTransfer1_2, datatransfer.ProtocolDataTransfer1_1:
		return trsp, nil
	case datatransfer.ProtocolDataTransfer1_0:
		// this should never happen but dosen't hurt to have this here for sanity
		if trsp.IsRestart() {
			return nil, xerrors.New("restart not supported for 1.0 protocol")
		}

		lresp := message1_0.NewTransferResponse(
			trsp.Type,
			trsp.Acpt,
			trsp.Paus,
			trsp.XferID,
			trsp.VRes,
			trsp.VTyp,
		)

		return lresp, nil
	default:
		return nil, xerrors.Errorf("protocol %s not supported", targetProtocol)
	}
}

// ToNet serializes a transfer response. It's a wrapper for MarshalCBOR to provide
// symmetry with FromNet
func (trsp *transferResponse1_1) ToNet(w io.Writer) error {
	msg := transferMessage1_1{
		IsRq:     false,
		Request:  nil,
		Response: trsp,
	}
	return msg.MarshalCBOR(w)
}
