package message

import (
	"bytes"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/encoding"
)

//go:generate cbor-gen-for transferRequest

// transferRequest is a struct that fulfills the datatransfer.Request interface.
// its members are exported to be used by cbor-gen
type transferRequest struct {
	BCid   *cid.Cid
	Type   uint64
	Paus   bool
	Part   bool
	Pull   bool
	Stor   *cbg.Deferred
	Vouch  *cbg.Deferred
	VTyp   datatransfer.TypeIdentifier
	XferID uint64
}

// IsRequest always returns true in this case because this is a transfer request
func (trq *transferRequest) IsRequest() bool {
	return true
}

func (trq *transferRequest) IsNew() bool {
	return trq.Type == uint64(newMessage)
}

func (trq *transferRequest) IsUpdate() bool {
	return trq.Type == uint64(updateMessage)
}

func (trq *transferRequest) IsVoucher() bool {
	return trq.Type == uint64(voucherMessage) || trq.Type == uint64(newMessage)
}

func (trq *transferRequest) IsPaused() bool {
	return trq.Paus
}

func (trq *transferRequest) TransferID() datatransfer.TransferID {
	return datatransfer.TransferID(trq.XferID)
}

// ========= datatransfer.Request interface
// IsPull returns true if this is a data pull request
func (trq *transferRequest) IsPull() bool {
	return trq.Pull
}

// VoucherType returns the Voucher ID
func (trq *transferRequest) VoucherType() datatransfer.TypeIdentifier {
	return trq.VTyp
}

// Voucher returns the Voucher bytes
func (trq *transferRequest) Voucher(decoder encoding.Decoder) (encoding.Encodable, error) {
	if trq.Vouch == nil {
		return nil, xerrors.New("No voucher present to read")
	}
	return decoder.DecodeFromCbor(trq.Vouch.Raw)
}

func (trq *transferRequest) EmptyVoucher() bool {
	return trq.VTyp == datatransfer.EmptyTypeIdentifier
}

// BaseCid returns the Base CID
func (trq *transferRequest) BaseCid() cid.Cid {
	if trq.BCid == nil {
		return cid.Undef
	}
	return *trq.BCid
}

// Selector returns the message Selector bytes
func (trq *transferRequest) Selector() (ipld.Node, error) {
	if trq.Stor == nil {
		return nil, xerrors.New("No selector present to read")
	}
	builder := basicnode.Prototype.Any.NewBuilder()
	reader := bytes.NewReader(trq.Stor.Raw)
	err := dagcbor.Decoder(builder, reader)
	if err != nil {
		return nil, xerrors.Errorf("Error decoding selector: %w", err)
	}
	return builder.Build(), nil
}

// IsCancel returns true if this is a cancel request
func (trq *transferRequest) IsCancel() bool {
	return trq.Type == uint64(cancelMessage)
}

// IsPartial returns true if this is a partial request
func (trq *transferRequest) IsPartial() bool {
	return trq.Part
}

// ToNet serializes a transfer request. It's a wrapper for MarshalCBOR to provide
// symmetry with FromNet
func (trq *transferRequest) ToNet(w io.Writer) error {
	msg := transferMessage{
		IsRq:     true,
		Request:  trq,
		Response: nil,
	}
	return msg.MarshalCBOR(w)
}
