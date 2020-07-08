package message

import (
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cborgen "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/encoding"
)

type messageType uint64

const (
	newMessage messageType = iota
	updateMessage
	cancelMessage
	completeMessage
	voucherMessage
	voucherResultMessage
)

// Reference file: https://github.com/ipfs/go-graphsync/blob/master/message/message.go
// though here we have a simpler message type that serializes/deserializes to two
// different types that share an interface, and we serialize to CBOR and not Protobuf.

// DataTransferMessage is a message for the data transfer protocol
// (either request or response) that can serialize to a protobuf
type DataTransferMessage interface {
	IsRequest() bool
	IsNew() bool
	IsUpdate() bool
	IsPaused() bool
	IsCancel() bool
	TransferID() datatransfer.TransferID
	cborgen.CBORMarshaler
	cborgen.CBORUnmarshaler
	ToNet(w io.Writer) error
}

// DataTransferRequest is a response message for the data transfer protocol
type DataTransferRequest interface {
	DataTransferMessage
	IsPull() bool
	IsVoucher() bool
	VoucherType() datatransfer.TypeIdentifier
	Voucher(decoder encoding.Decoder) (encoding.Encodable, error)
	BaseCid() cid.Cid
	Selector() (ipld.Node, error)
}

// DataTransferResponse is a response message for the data transfer protocol
type DataTransferResponse interface {
	DataTransferMessage
	IsVoucherResult() bool
	IsComplete() bool
	Accepted() bool
	VoucherResultType() datatransfer.TypeIdentifier
	VoucherResult(decoder encoding.Decoder) (encoding.Encodable, error)
	EmptyVoucherResult() bool
}

// NewRequest generates a new request for the data transfer protocol
func NewRequest(id datatransfer.TransferID, isPull bool, vtype datatransfer.TypeIdentifier, voucher encoding.Encodable, baseCid cid.Cid, selector ipld.Node) (DataTransferRequest, error) {
	vbytes, err := encoding.Encode(voucher)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	if baseCid == cid.Undef {
		return nil, xerrors.Errorf("base CID must be defined")
	}
	selBytes, err := encoding.Encode(selector)
	if err != nil {
		return nil, xerrors.Errorf("Error encoding selector")
	}
	return &transferRequest{
		Type:   uint64(newMessage),
		Pull:   isPull,
		Vouch:  &cborgen.Deferred{Raw: vbytes},
		Stor:   &cborgen.Deferred{Raw: selBytes},
		BCid:   &baseCid,
		VTyp:   vtype,
		XferID: uint64(id),
	}, nil
}

// CancelRequest request generates a request to cancel an in progress request
func CancelRequest(id datatransfer.TransferID) DataTransferRequest {
	return &transferRequest{
		Type:   uint64(cancelMessage),
		XferID: uint64(id),
	}
}

// UpdateRequest generates a new request update
func UpdateRequest(id datatransfer.TransferID, isPaused bool) DataTransferRequest {
	return &transferRequest{
		Type:   uint64(updateMessage),
		Paus:   isPaused,
		XferID: uint64(id),
	}
}

// VoucherRequest generates a new request for the data transfer protocol
func VoucherRequest(id datatransfer.TransferID, vtype datatransfer.TypeIdentifier, voucher encoding.Encodable) (DataTransferRequest, error) {
	vbytes, err := encoding.Encode(voucher)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	return &transferRequest{
		Type:   uint64(voucherMessage),
		Vouch:  &cborgen.Deferred{Raw: vbytes},
		VTyp:   vtype,
		XferID: uint64(id),
	}, nil
}

// NewResponse builds a new Data Transfer response
func NewResponse(id datatransfer.TransferID, accepted bool, isPaused bool, voucherResultType datatransfer.TypeIdentifier, voucherResult encoding.Encodable) (DataTransferResponse, error) {
	vbytes, err := encoding.Encode(voucherResult)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	return &transferResponse{
		Acpt:   accepted,
		Type:   uint64(newMessage),
		Paus:   isPaused,
		XferID: uint64(id),
		VTyp:   voucherResultType,
		VRes:   &cborgen.Deferred{Raw: vbytes},
	}, nil
}

// VoucherResultResponse builds a new response for a voucher result
func VoucherResultResponse(id datatransfer.TransferID, accepted bool, isPaused bool, voucherResultType datatransfer.TypeIdentifier, voucherResult encoding.Encodable) (DataTransferResponse, error) {
	vbytes, err := encoding.Encode(voucherResult)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	return &transferResponse{
		Acpt:   accepted,
		Type:   uint64(voucherResultMessage),
		Paus:   isPaused,
		XferID: uint64(id),
		VTyp:   voucherResultType,
		VRes:   &cborgen.Deferred{Raw: vbytes},
	}, nil
}

// UpdateResponse returns a new update response
func UpdateResponse(id datatransfer.TransferID, isPaused bool) DataTransferResponse {
	return &transferResponse{
		Type:   uint64(updateMessage),
		Paus:   isPaused,
		XferID: uint64(id),
	}
}

// CancelResponse makes a new cancel response message
func CancelResponse(id datatransfer.TransferID) DataTransferResponse {
	return &transferResponse{
		Type:   uint64(cancelMessage),
		XferID: uint64(id),
	}
}

// CompleteResponse returns a new complete response message
func CompleteResponse(id datatransfer.TransferID, isAccepted bool, isPaused bool, voucherResultType datatransfer.TypeIdentifier, voucherResult encoding.Encodable) (DataTransferResponse, error) {
	vbytes, err := encoding.Encode(voucherResult)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	return &transferResponse{
		Type:   uint64(completeMessage),
		Acpt:   isAccepted,
		Paus:   isPaused,
		VTyp:   voucherResultType,
		VRes:   &cborgen.Deferred{Raw: vbytes},
		XferID: uint64(id),
	}, nil
}

// FromNet can read a network stream to deserialize a GraphSyncMessage
func FromNet(r io.Reader) (DataTransferMessage, error) {
	tresp := transferMessage{}
	err := tresp.UnmarshalCBOR(r)
	if tresp.IsRequest() {
		return tresp.Request, nil
	}
	return tresp.Response, err
}
