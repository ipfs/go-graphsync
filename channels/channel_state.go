package channels

import (
	"bytes"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	peer "github.com/libp2p/go-libp2p-core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels/internal"
)

// channelState is immutable channel data plus mutable state
type channelState struct {
	// peerId of the manager peer
	selfPeer peer.ID
	// an identifier for this channel shared by request and responder, set by requester through protocol
	transferID datatransfer.TransferID
	// base CID for the piece being transferred
	baseCid cid.Cid
	// portion of Piece to return, specified by an IPLD selector
	selector *cbg.Deferred
	// the party that is sending the data (not who initiated the request)
	sender peer.ID
	// the party that is receiving the data (not who initiated the request)
	recipient peer.ID
	// expected amount of data to be transferred
	totalSize uint64
	// current status of this deal
	status datatransfer.Status
	// isPull indicates if this is a push or pull request
	isPull bool
	// total bytes read from this node and queued for sending (0 if receiver)
	queued uint64
	// total bytes sent from this node (0 if receiver)
	sent uint64
	// total bytes received by this node (0 if sender)
	received uint64
	// more informative status on a channel
	message string
	// additional vouchers
	vouchers []internal.EncodedVoucher
	// additional voucherResults
	voucherResults       []internal.EncodedVoucherResult
	voucherResultDecoder DecoderByTypeFunc
	voucherDecoder       DecoderByTypeFunc

	receivedCids []cid.Cid
}

// EmptyChannelState is the zero value for channel state, meaning not present
var EmptyChannelState = channelState{}

// Status is the current status of this channel
func (c channelState) Status() datatransfer.Status { return c.status }

// Received returns the number of bytes received
func (c channelState) Queued() uint64 { return c.queued }

// Sent returns the number of bytes sent
func (c channelState) Sent() uint64 { return c.sent }

// Received returns the number of bytes received
func (c channelState) Received() uint64 { return c.received }

// TransferID returns the transfer id for this channel
func (c channelState) TransferID() datatransfer.TransferID { return c.transferID }

// BaseCID returns the CID that is at the root of this data transfer
func (c channelState) BaseCID() cid.Cid { return c.baseCid }

// Selector returns the IPLD selector for this data transfer (represented as
// an IPLD node)
func (c channelState) Selector() ipld.Node {
	builder := basicnode.Prototype.Any.NewBuilder()
	reader := bytes.NewReader(c.selector.Raw)
	err := dagcbor.Decoder(builder, reader)
	if err != nil {
		log.Error(err)
	}
	return builder.Build()
}

// Voucher returns the voucher for this data transfer
func (c channelState) Voucher() datatransfer.Voucher {
	decoder, _ := c.voucherDecoder(c.vouchers[0].Type)
	encodable, _ := decoder.DecodeFromCbor(c.vouchers[0].Voucher.Raw)
	return encodable.(datatransfer.Voucher)
}

// ReceivedCids returns the cids received so far on this channel
func (c channelState) ReceivedCids() []cid.Cid {
	return c.receivedCids
}

// Sender returns the peer id for the node that is sending data
func (c channelState) Sender() peer.ID { return c.sender }

// Recipient returns the peer id for the node that is receiving data
func (c channelState) Recipient() peer.ID { return c.recipient }

// TotalSize returns the total size for the data being transferred
func (c channelState) TotalSize() uint64 { return c.totalSize }

// IsPull returns whether this is a pull request based on who initiated it
func (c channelState) IsPull() bool {
	return c.isPull
}

func (c channelState) ChannelID() datatransfer.ChannelID {
	if c.isPull {
		return datatransfer.ChannelID{ID: c.transferID, Initiator: c.recipient, Responder: c.sender}
	}
	return datatransfer.ChannelID{ID: c.transferID, Initiator: c.sender, Responder: c.recipient}
}

func (c channelState) Message() string {
	return c.message
}

func (c channelState) Vouchers() []datatransfer.Voucher {
	vouchers := make([]datatransfer.Voucher, 0, len(c.vouchers))
	for _, encoded := range c.vouchers {
		decoder, _ := c.voucherDecoder(encoded.Type)
		encodable, _ := decoder.DecodeFromCbor(encoded.Voucher.Raw)
		vouchers = append(vouchers, encodable.(datatransfer.Voucher))
	}
	return vouchers
}

func (c channelState) LastVoucher() datatransfer.Voucher {
	decoder, _ := c.voucherDecoder(c.vouchers[len(c.vouchers)-1].Type)
	encodable, _ := decoder.DecodeFromCbor(c.vouchers[len(c.vouchers)-1].Voucher.Raw)
	return encodable.(datatransfer.Voucher)
}

func (c channelState) LastVoucherResult() datatransfer.VoucherResult {
	decoder, _ := c.voucherResultDecoder(c.voucherResults[len(c.voucherResults)-1].Type)
	encodable, _ := decoder.DecodeFromCbor(c.voucherResults[len(c.voucherResults)-1].VoucherResult.Raw)
	return encodable.(datatransfer.VoucherResult)
}

func (c channelState) VoucherResults() []datatransfer.VoucherResult {
	voucherResults := make([]datatransfer.VoucherResult, 0, len(c.voucherResults))
	for _, encoded := range c.voucherResults {
		decoder, _ := c.voucherResultDecoder(encoded.Type)
		encodable, _ := decoder.DecodeFromCbor(encoded.VoucherResult.Raw)
		voucherResults = append(voucherResults, encodable.(datatransfer.VoucherResult))
	}
	return voucherResults
}

func (c channelState) SelfPeer() peer.ID {
	return c.selfPeer
}

func (c channelState) OtherPeer() peer.ID {
	if c.sender == c.selfPeer {
		return c.recipient
	}
	return c.sender
}

func fromInternalChannelState(c internal.ChannelState, voucherDecoder DecoderByTypeFunc, voucherResultDecoder DecoderByTypeFunc) datatransfer.ChannelState {
	return channelState{
		selfPeer:             c.SelfPeer,
		isPull:               c.Initiator == c.Recipient,
		transferID:           c.TransferID,
		baseCid:              c.BaseCid,
		selector:             c.Selector,
		sender:               c.Sender,
		recipient:            c.Recipient,
		totalSize:            c.TotalSize,
		status:               c.Status,
		queued:               c.Queued,
		sent:                 c.Sent,
		received:             c.Received,
		message:              c.Message,
		vouchers:             c.Vouchers,
		voucherResults:       c.VoucherResults,
		voucherResultDecoder: voucherResultDecoder,
		voucherDecoder:       voucherDecoder,

		receivedCids: c.ReceivedCids,
	}
}

var _ datatransfer.ChannelState = channelState{}
