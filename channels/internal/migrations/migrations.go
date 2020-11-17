package migrations

import (
	peer "github.com/libp2p/go-libp2p-core/peer"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/filecoin-project/go-ds-versioning/pkg/versioned"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels/internal"
	v0 "github.com/filecoin-project/go-data-transfer/channels/internal/migrations/v0"
	v1 "github.com/filecoin-project/go-data-transfer/channels/internal/migrations/v1"
	"github.com/filecoin-project/go-data-transfer/cidlists"
)

// MigrateEncodedVoucher0To1 converts a tuple encoded voucher to a map encoded voucher
func MigrateEncodedVoucher0To1(oldV v0.EncodedVoucher) internal.EncodedVoucher {
	return internal.EncodedVoucher{
		Type:    oldV.Type,
		Voucher: oldV.Voucher,
	}
}

// MigrateEncodedVoucherResult0To1 converts a tuple encoded voucher to a map encoded voucher
func MigrateEncodedVoucherResult0To1(oldV v0.EncodedVoucherResult) internal.EncodedVoucherResult {
	return internal.EncodedVoucherResult{
		Type:          oldV.Type,
		VoucherResult: oldV.VoucherResult,
	}
}

// GetMigrateChannelState0To1 returns a conversion function for migrating v0 channel state to v1
func GetMigrateChannelState0To1(selfPeer peer.ID) func(*v0.ChannelState) (*v1.ChannelState, error) {
	return func(oldCs *v0.ChannelState) (*v1.ChannelState, error) {
		encodedVouchers := make([]internal.EncodedVoucher, 0, len(oldCs.Vouchers))
		for _, ev0 := range oldCs.Vouchers {
			encodedVouchers = append(encodedVouchers, MigrateEncodedVoucher0To1(ev0))
		}
		encodedVoucherResults := make([]internal.EncodedVoucherResult, 0, len(oldCs.VoucherResults))
		for _, evr0 := range oldCs.VoucherResults {
			encodedVoucherResults = append(encodedVoucherResults, MigrateEncodedVoucherResult0To1(evr0))
		}
		return &v1.ChannelState{
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

// GetMigrateChannelState1To2 returns a conversion function for migrating v1 channel state to v2 channel state
func GetMigrateChannelState1To2(cidLists cidlists.CIDLists) func(*v1.ChannelState) (*internal.ChannelState, error) {
	return func(oldCs *v1.ChannelState) (*internal.ChannelState, error) {
		err := cidLists.CreateList(datatransfer.ChannelID{ID: oldCs.TransferID, Initiator: oldCs.Initiator, Responder: oldCs.Responder}, oldCs.ReceivedCids)
		if err != nil {
			return nil, err
		}
		return &internal.ChannelState{
			SelfPeer:       oldCs.SelfPeer,
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
			Vouchers:       oldCs.Vouchers,
			VoucherResults: oldCs.VoucherResults,
		}, nil
	}
}

// GetChannelStateMigrations returns a migration list for the channel states
func GetChannelStateMigrations(selfPeer peer.ID, cidLists cidlists.CIDLists) (versioning.VersionedMigrationList, error) {
	channelStateMigration0To1 := GetMigrateChannelState0To1(selfPeer)
	channelStateMigration1To2 := GetMigrateChannelState1To2(cidLists)
	return versioned.BuilderList{
		versioned.NewVersionedBuilder(channelStateMigration0To1, versioning.VersionKey("1")),
		versioned.NewVersionedBuilder(channelStateMigration1To2, versioning.VersionKey("2")).OldVersion("1"),
	}.Build()
}
