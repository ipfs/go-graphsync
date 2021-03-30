package internal

import (
	"fmt"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

//go:generate cbor-gen-for --map-encoding ChannelState EncodedVoucher EncodedVoucherResult

// EncodedVoucher is how the voucher is stored on disk
type EncodedVoucher struct {
	// Vouchers identifier for decoding
	Type datatransfer.TypeIdentifier
	// used to verify this channel
	Voucher *cbg.Deferred
}

// EncodedVoucherResult is how the voucher result is stored on disk
type EncodedVoucherResult struct {
	// Vouchers identifier for decoding
	Type datatransfer.TypeIdentifier
	// used to verify this channel
	VoucherResult *cbg.Deferred
}

// ChannelState is the internal representation on disk for the channel fsm
type ChannelState struct {
	// PeerId of the manager peer
	SelfPeer peer.ID
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
	// total bytes read from this node and queued for sending (0 if receiver)
	Queued uint64
	// total bytes sent from this node (0 if receiver)
	Sent uint64
	// total bytes received by this node (0 if sender)
	Received uint64
	// more informative status on a channel
	Message        string
	Vouchers       []EncodedVoucher
	VoucherResults []EncodedVoucherResult

	// Stages traces the execution fo a data transfer.
	//
	// EXPERIMENTAL; subject to change.
	Stages *datatransfer.ChannelStages
}

// AddLog takes an fmt string with arguments, and adds the formatted string to
// the logs for the current deal stage.
//
// EXPERIMENTAL; subject to change.
func (cs *ChannelState) AddLog(msg string, a ...interface{}) {
	if len(a) > 0 {
		msg = fmt.Sprintf(msg, a...)
	}

	stage := datatransfer.Statuses[cs.Status]

	cs.Stages.AddLog(stage, msg)
}
