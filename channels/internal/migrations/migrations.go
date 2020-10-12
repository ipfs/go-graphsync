package migrations

import (
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/filecoin-project/go-ds-versioning/pkg/versioned"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels/internal"
)

//go:generate cbor-gen-for ChannelState0 EncodedVoucher0 EncodedVoucherResult0

// EncodedVoucher0 is version 0 of EncodedVoucher
type EncodedVoucher0 struct {
	// Vouchers identifier for decoding
	Type datatransfer.TypeIdentifier
	// used to verify this channel
	Voucher *cbg.Deferred
}

// EncodedVoucherResult0 is version 0 of EncodedVoucherResult
type EncodedVoucherResult0 struct {
	// Vouchers identifier for decoding
	Type datatransfer.TypeIdentifier
	// used to verify this channel
	VoucherResult *cbg.Deferred
}

// ChannelState0 is version 0 of ChannelState
type ChannelState0 struct {
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
	Vouchers       []EncodedVoucher0
	VoucherResults []EncodedVoucherResult0
}

// MigrateEncodedVoucher0To1 converts a tuple encoded voucher to a map encoded voucher
func MigrateEncodedVoucher0To1(oldV EncodedVoucher0) internal.EncodedVoucher {
	return internal.EncodedVoucher{
		Type:    oldV.Type,
		Voucher: oldV.Voucher,
	}
}

// MigrateEncodedVoucherResult0To1 converts a tuple encoded voucher to a map encoded voucher
func MigrateEncodedVoucherResult0To1(oldV EncodedVoucherResult0) internal.EncodedVoucherResult {
	return internal.EncodedVoucherResult{
		Type:          oldV.Type,
		VoucherResult: oldV.VoucherResult,
	}
}

// GetMigrateChannelState0To1 returns a conversion function for migrating v0 channel state to v1
func GetMigrateChannelState0To1(selfPeer peer.ID) func(*ChannelState0) (*internal.ChannelState, error) {
	return func(oldCs *ChannelState0) (*internal.ChannelState, error) {
		encodedVouchers := make([]internal.EncodedVoucher, 0, len(oldCs.Vouchers))
		for _, ev0 := range oldCs.Vouchers {
			encodedVouchers = append(encodedVouchers, MigrateEncodedVoucher0To1(ev0))
		}
		encodedVoucherResults := make([]internal.EncodedVoucherResult, 0, len(oldCs.VoucherResults))
		for _, evr0 := range oldCs.VoucherResults {
			encodedVoucherResults = append(encodedVoucherResults, MigrateEncodedVoucherResult0To1(evr0))
		}
		return &internal.ChannelState{
			SelfPeer:       selfPeer,
			TransferID:     oldCs.TransferID,
			Initiator:      oldCs.Initiator,
			Responder:      oldCs.Responder,
			BaseCid:        oldCs.BaseCid,
			Selector:       oldCs.Selector,
			Sender:         oldCs.Sender,
			Recipient:      oldCs.Recipient,
			TotalSize:      oldCs.TotalSize,
			Status:         oldCs.Status,
			Sent:           oldCs.Sent,
			Received:       oldCs.Received,
			Message:        oldCs.Message,
			Vouchers:       encodedVouchers,
			VoucherResults: encodedVoucherResults,
			ReceivedCids:   nil,
		}, nil
	}
}

// GetChannelStateMigrations returns a migration list for the channel states
func GetChannelStateMigrations(selfPeer peer.ID) (versioning.VersionedMigrationList, error) {
	channelStateMigration0To1 := GetMigrateChannelState0To1(selfPeer)
	return versioned.BuilderList{
		versioned.NewVersionedBuilder(channelStateMigration0To1, versioning.VersionKey("1")),
	}.Build()
}
