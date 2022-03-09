package message1_1

import (
	"io"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/libp2p/go-libp2p-core/protocol"
	xerrors "golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/encoding"
	"github.com/filecoin-project/go-data-transfer/message/types"
)

// TransferResponse1_1 is a private struct that satisfies the datatransfer.Response interface
// It is the response message for the Data Transfer 1.1 and 1.2 Protocol.
type TransferResponse1_1 struct {
	MessageType           uint64
	RequestAccepted       bool
	Paused                bool
	TransferId            uint64
	VoucherResultPtr      *datamodel.Node
	VoucherTypeIdentifier datatransfer.TypeIdentifier
}

func (trsp *TransferResponse1_1) TransferID() datatransfer.TransferID {
	return datatransfer.TransferID(trsp.TransferId)
}

// IsRequest always returns false in this case because this is a transfer response
func (trsp *TransferResponse1_1) IsRequest() bool {
	return false
}

// IsNew returns true if this is the first response sent
func (trsp *TransferResponse1_1) IsNew() bool {
	return trsp.MessageType == uint64(types.NewMessage)
}

// IsUpdate returns true if this response is an update
func (trsp *TransferResponse1_1) IsUpdate() bool {
	return trsp.MessageType == uint64(types.UpdateMessage)
}

// IsPaused returns true if the responder is paused
func (trsp *TransferResponse1_1) IsPaused() bool {
	return trsp.Paused
}

// IsCancel returns true if the responder has cancelled this response
func (trsp *TransferResponse1_1) IsCancel() bool {
	return trsp.MessageType == uint64(types.CancelMessage)
}

// IsComplete returns true if the responder has completed this response
func (trsp *TransferResponse1_1) IsComplete() bool {
	return trsp.MessageType == uint64(types.CompleteMessage)
}

func (trsp *TransferResponse1_1) IsVoucherResult() bool {
	return trsp.MessageType == uint64(types.VoucherResultMessage) || trsp.MessageType == uint64(types.NewMessage) || trsp.MessageType == uint64(types.CompleteMessage) ||
		trsp.MessageType == uint64(types.RestartMessage)
}

// 	Accepted returns true if the request is accepted in the response
func (trsp *TransferResponse1_1) Accepted() bool {
	return trsp.RequestAccepted
}

func (trsp *TransferResponse1_1) VoucherResultType() datatransfer.TypeIdentifier {
	return trsp.VoucherTypeIdentifier
}

func (trsp *TransferResponse1_1) VoucherResult(decoder encoding.Decoder) (encoding.Encodable, error) {
	if trsp.VoucherResultPtr == nil {
		return nil, xerrors.New("No voucher present to read")
	}
	return decoder.DecodeFromNode(*trsp.VoucherResultPtr)
}

func (trq *TransferResponse1_1) IsRestart() bool {
	return trq.MessageType == uint64(types.RestartMessage)
}

func (trsp *TransferResponse1_1) EmptyVoucherResult() bool {
	return trsp.VoucherTypeIdentifier == datatransfer.EmptyTypeIdentifier
}

func (trsp *TransferResponse1_1) MessageForProtocol(targetProtocol protocol.ID) (datatransfer.Message, error) {
	switch targetProtocol {
	case datatransfer.ProtocolDataTransfer1_2:
		return trsp, nil
	default:
		return nil, xerrors.Errorf("protocol %s not supported", targetProtocol)
	}
}

func (trsp *TransferResponse1_1) toIPLD() schema.TypedNode {
	msg := TransferMessage1_1{
		IsRequest: false,
		Request:   nil,
		Response:  trsp,
	}
	return msg.toIPLD()
}

func (trsp *TransferResponse1_1) ToIPLD() (datamodel.Node, error) {
	return trsp.toIPLD().Representation(), nil
}

// ToNet serializes a transfer response.
func (trsp *TransferResponse1_1) ToNet(w io.Writer) error {
	return dagcbor.Encode(trsp.toIPLD().Representation(), w)
}
