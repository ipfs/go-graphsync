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

// NewRequest generates a new request for the data transfer protocol
func NewRequest(id datatransfer.TransferID, isPull bool, vtype datatransfer.TypeIdentifier, voucher encoding.Encodable, baseCid cid.Cid, selector ipld.Node) (datatransfer.Request, error) {
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
func CancelRequest(id datatransfer.TransferID) datatransfer.Request {
	return &transferRequest{
		Type:   uint64(cancelMessage),
		XferID: uint64(id),
	}
}

// UpdateRequest generates a new request update
func UpdateRequest(id datatransfer.TransferID, isPaused bool) datatransfer.Request {
	return &transferRequest{
		Type:   uint64(updateMessage),
		Paus:   isPaused,
		XferID: uint64(id),
	}
}

// VoucherRequest generates a new request for the data transfer protocol
func VoucherRequest(id datatransfer.TransferID, vtype datatransfer.TypeIdentifier, voucher encoding.Encodable) (datatransfer.Request, error) {
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
func NewResponse(id datatransfer.TransferID, accepted bool, isPaused bool, voucherResultType datatransfer.TypeIdentifier, voucherResult encoding.Encodable) (datatransfer.Response, error) {
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
func VoucherResultResponse(id datatransfer.TransferID, accepted bool, isPaused bool, voucherResultType datatransfer.TypeIdentifier, voucherResult encoding.Encodable) (datatransfer.Response, error) {
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
func UpdateResponse(id datatransfer.TransferID, isPaused bool) datatransfer.Response {
	return &transferResponse{
		Type:   uint64(updateMessage),
		Paus:   isPaused,
		XferID: uint64(id),
	}
}

// CancelResponse makes a new cancel response message
func CancelResponse(id datatransfer.TransferID) datatransfer.Response {
	return &transferResponse{
		Type:   uint64(cancelMessage),
		XferID: uint64(id),
	}
}

// CompleteResponse returns a new complete response message
func CompleteResponse(id datatransfer.TransferID, isAccepted bool, isPaused bool, voucherResultType datatransfer.TypeIdentifier, voucherResult encoding.Encodable) (datatransfer.Response, error) {
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
func FromNet(r io.Reader) (datatransfer.Message, error) {
	tresp := transferMessage{}
	err := tresp.UnmarshalCBOR(r)
	if err != nil {
		return nil, err
	}

	if (tresp.IsRequest() && tresp.Request == nil) || (!tresp.IsRequest() && tresp.Response == nil) {
		return nil, xerrors.Errorf("invalid/malformed message")
	}

	if tresp.IsRequest() {
		return tresp.Request, nil
	}
	return tresp.Response, nil
}
