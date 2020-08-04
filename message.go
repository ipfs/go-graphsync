package datatransfer

import (
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cborgen "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-data-transfer/encoding"
)

// Message is a message for the data transfer protocol
// (either request or response) that can serialize to a protobuf
type Message interface {
	IsRequest() bool
	IsNew() bool
	IsUpdate() bool
	IsPaused() bool
	IsCancel() bool
	TransferID() TransferID
	cborgen.CBORMarshaler
	cborgen.CBORUnmarshaler
	ToNet(w io.Writer) error
}

// Request is a response message for the data transfer protocol
type Request interface {
	Message
	IsPull() bool
	IsVoucher() bool
	VoucherType() TypeIdentifier
	Voucher(decoder encoding.Decoder) (encoding.Encodable, error)
	BaseCid() cid.Cid
	Selector() (ipld.Node, error)
}

// Response is a response message for the data transfer protocol
type Response interface {
	Message
	IsVoucherResult() bool
	IsComplete() bool
	Accepted() bool
	VoucherResultType() TypeIdentifier
	VoucherResult(decoder encoding.Decoder) (encoding.Encodable, error)
	EmptyVoucherResult() bool
}
