package channels

import (
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

//go:generate cbor-gen-for internalChannelState encodedVoucher encodedVoucherResult

type encodedVoucher struct {
	// Vouchers identifier for decoding
	Type datatransfer.TypeIdentifier
	// used to verify this channel
	Voucher *cbg.Deferred
}

type encodedVoucherResult struct {
	// Vouchers identifier for decoding
	Type datatransfer.TypeIdentifier
	// used to verify this channel
	VoucherResult *cbg.Deferred
}

type internalChannelState struct {
	// an identifier for this channel shared by request and responder, set by requester through protocol
	TransferID datatransfer.TransferID
	// Initiator is the person who intiated this datatransfer request
	Initiator peer.ID
	// Responder is the person who is responding to this datatransfer request
	Responder peer.ID
	// base CID for the piece being transferred
	BaseCid cid.Cid
	// portion of Piece to return, specified by an IPLD selector
	Selector *cbg.Deferred
	// the party that is sending the data (not who initiated the request)
	Sender peer.ID
	// the party that is receiving the data (not who initiated the request)
	Recipient peer.ID
	// expected amount of data to be transferred
	TotalSize uint64
	// current status of this deal
	Status datatransfer.Status
	// total bytes sent from this node (0 if receiver)
	Sent uint64
	// total bytes received by this node (0 if sender)
	Received uint64
	// more informative status on a channel
	Message        string
	Vouchers       []encodedVoucher
	VoucherResults []encodedVoucherResult
}

func (c internalChannelState) ToChannelState(voucherDecoder DecoderByTypeFunc, voucherResultDecoder DecoderByTypeFunc) datatransfer.ChannelState {
	return channelState{
		isPull:               c.Initiator == c.Recipient,
		transferID:           c.TransferID,
		baseCid:              c.BaseCid,
		selector:             c.Selector,
		sender:               c.Sender,
		recipient:            c.Recipient,
		totalSize:            c.TotalSize,
		status:               c.Status,
		sent:                 c.Sent,
		received:             c.Received,
		message:              c.Message,
		vouchers:             c.Vouchers,
		voucherResults:       c.VoucherResults,
		voucherResultDecoder: voucherResultDecoder,
		voucherDecoder:       voucherDecoder,
	}
}
