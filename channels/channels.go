package channels

import (
	"context"
	"errors"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	peer "github.com/libp2p/go-libp2p-core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	versionedfsm "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
	"github.com/filecoin-project/go-statemachine/fsm"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels/internal"
	"github.com/filecoin-project/go-data-transfer/channels/internal/migrations"
	"github.com/filecoin-project/go-data-transfer/encoding"
)

type DecoderByTypeFunc func(identifier datatransfer.TypeIdentifier) (encoding.Decoder, bool)

type Notifier func(datatransfer.Event, datatransfer.ChannelState)

// ErrNotFound is returned when a channel cannot be found with a given channel ID
var ErrNotFound = errors.New("No channel for this channel ID")

// ErrWrongType is returned when a caller attempts to change the type of implementation data after setting it
var ErrWrongType = errors.New("Cannot change type of implementation specific data after setting it")

// Channels is a thread safe list of channels
type Channels struct {
	notifier             Notifier
	voucherDecoder       DecoderByTypeFunc
	voucherResultDecoder DecoderByTypeFunc
	stateMachines        fsm.Group
	migrateStateMachines func(context.Context) error
}

// ChannelEnvironment -- just a proxy for DTNetwork for now
type ChannelEnvironment interface {
	Protect(id peer.ID, tag string)
	Unprotect(id peer.ID, tag string) bool
	ID() peer.ID
	CleanupChannel(chid datatransfer.ChannelID)
}

// New returns a new thread safe list of channels
func New(ds datastore.Batching,
	notifier Notifier,
	voucherDecoder DecoderByTypeFunc,
	voucherResultDecoder DecoderByTypeFunc,
	env ChannelEnvironment,
	selfPeer peer.ID) (*Channels, error) {
	c := &Channels{
		notifier:             notifier,
		voucherDecoder:       voucherDecoder,
		voucherResultDecoder: voucherResultDecoder,
	}
	channelMigrations, err := migrations.GetChannelStateMigrations(selfPeer)
	if err != nil {
		return nil, err
	}
	c.stateMachines, c.migrateStateMachines, err = versionedfsm.NewVersionedFSM(ds, fsm.Parameters{
		Environment:     env,
		StateType:       internal.ChannelState{},
		StateKeyField:   "Status",
		Events:          ChannelEvents,
		StateEntryFuncs: ChannelStateEntryFuncs,
		Notifier:        c.dispatch,
		FinalityStates:  ChannelFinalityStates,
	}, channelMigrations, versioning.VersionKey("1"))
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Start migrates the channel data store as needed
func (c *Channels) Start(ctx context.Context) error {
	return c.migrateStateMachines(ctx)
}

func (c *Channels) dispatch(eventName fsm.EventName, channel fsm.StateType) {
	evtCode, ok := eventName.(datatransfer.EventCode)
	if !ok {
		log.Errorf("dropped bad event %v", eventName)
	}
	realChannel, ok := channel.(internal.ChannelState)
	if !ok {
		log.Errorf("not a ClientDeal %v", channel)
	}
	evt := datatransfer.Event{
		Code:      evtCode,
		Message:   realChannel.Message,
		Timestamp: time.Now(),
	}

	c.notifier(evt, fromInternalChannelState(realChannel, c.voucherDecoder, c.voucherResultDecoder))
}

// CreateNew creates a new channel id and channel state and saves to channels.
// returns error if the channel exists already.
func (c *Channels) CreateNew(selfPeer peer.ID, tid datatransfer.TransferID, baseCid cid.Cid, selector ipld.Node, voucher datatransfer.Voucher, initiator, dataSender, dataReceiver peer.ID) (datatransfer.ChannelID, error) {
	var responder peer.ID
	if dataSender == initiator {
		responder = dataReceiver
	} else {
		responder = dataSender
	}
	chid := datatransfer.ChannelID{Initiator: initiator, Responder: responder, ID: tid}
	voucherBytes, err := encoding.Encode(voucher)
	if err != nil {
		return datatransfer.ChannelID{}, err
	}
	selBytes, err := encoding.Encode(selector)
	if err != nil {
		return datatransfer.ChannelID{}, err
	}
	err = c.stateMachines.Begin(chid, &internal.ChannelState{
		SelfPeer:   selfPeer,
		TransferID: tid,
		Initiator:  initiator,
		Responder:  responder,
		BaseCid:    baseCid,
		Selector:   &cbg.Deferred{Raw: selBytes},
		Sender:     dataSender,
		Recipient:  dataReceiver,
		Vouchers: []internal.EncodedVoucher{
			{
				Type: voucher.Type(),
				Voucher: &cbg.Deferred{
					Raw: voucherBytes,
				},
			},
		},
		Status:       datatransfer.Requested,
		ReceivedCids: nil,
	})
	if err != nil {
		return datatransfer.ChannelID{}, err
	}
	return chid, c.stateMachines.Send(chid, datatransfer.Open)
}

// InProgress returns a list of in progress channels
func (c *Channels) InProgress() (map[datatransfer.ChannelID]datatransfer.ChannelState, error) {
	var internalChannels []internal.ChannelState
	err := c.stateMachines.List(&internalChannels)
	if err != nil {
		return nil, err
	}
	channels := make(map[datatransfer.ChannelID]datatransfer.ChannelState, len(internalChannels))
	for _, internalChannel := range internalChannels {
		channels[datatransfer.ChannelID{ID: internalChannel.TransferID, Responder: internalChannel.Responder, Initiator: internalChannel.Initiator}] =
			fromInternalChannelState(internalChannel, c.voucherDecoder, c.voucherResultDecoder)
	}
	return channels, nil
}

// GetByID searches for a channel in the slice of channels with id `chid`.
// Returns datatransfer.EmptyChannelState if there is no channel with that id
func (c *Channels) GetByID(ctx context.Context, chid datatransfer.ChannelID) (datatransfer.ChannelState, error) {
	var internalChannel internal.ChannelState
	err := c.stateMachines.GetSync(ctx, chid, &internalChannel)
	if err != nil {
		return nil, ErrNotFound
	}
	return fromInternalChannelState(internalChannel, c.voucherDecoder, c.voucherResultDecoder), nil
}

// Accept marks a data transfer as accepted
func (c *Channels) Accept(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.Accept)
}

// Restart marks a data transfer as restarted
func (c *Channels) Restart(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.Restart)
}

func (c *Channels) CompleteCleanupOnRestart(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.CompleteCleanupOnRestart)
}

func (c *Channels) DataSent(chid datatransfer.ChannelID, cid cid.Cid, delta uint64) error {
	return c.send(chid, datatransfer.DataSent, delta, cid)
}

func (c *Channels) DataQueued(chid datatransfer.ChannelID, cid cid.Cid, delta uint64) error {
	return c.send(chid, datatransfer.DataQueued, delta, cid)
}

func (c *Channels) DataReceived(chid datatransfer.ChannelID, cid cid.Cid, delta uint64) error {
	return c.send(chid, datatransfer.DataReceived, delta, cid)
}

// PauseInitiator pauses the initator of this channel
func (c *Channels) PauseInitiator(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.PauseInitiator)
}

// PauseResponder pauses the responder of this channel
func (c *Channels) PauseResponder(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.PauseResponder)
}

// ResumeInitiator resumes the initator of this channel
func (c *Channels) ResumeInitiator(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.ResumeInitiator)
}

// ResumeResponder resumes the responder of this channel
func (c *Channels) ResumeResponder(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.ResumeResponder)
}

// NewVoucher records a new voucher for this channel
func (c *Channels) NewVoucher(chid datatransfer.ChannelID, voucher datatransfer.Voucher) error {
	voucherBytes, err := encoding.Encode(voucher)
	if err != nil {
		return err
	}
	return c.send(chid, datatransfer.NewVoucher, voucher.Type(), voucherBytes)
}

// NewVoucherResult records a new voucher result for this channel
func (c *Channels) NewVoucherResult(chid datatransfer.ChannelID, voucherResult datatransfer.VoucherResult) error {
	voucherResultBytes, err := encoding.Encode(voucherResult)
	if err != nil {
		return err
	}
	return c.send(chid, datatransfer.NewVoucherResult, voucherResult.Type(), voucherResultBytes)
}

// Complete indicates responder has completed sending/receiving data
func (c *Channels) Complete(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.Complete)
}

// FinishTransfer an initiator has finished sending/receiving data
func (c *Channels) FinishTransfer(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.FinishTransfer)
}

// ResponderCompletes indicates an initator has finished receiving data from a responder
func (c *Channels) ResponderCompletes(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.ResponderCompletes)
}

// ResponderBeginsFinalization indicates a responder has finished processing but is awaiting confirmation from the initiator
func (c *Channels) ResponderBeginsFinalization(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.ResponderBeginsFinalization)
}

// BeginFinalizing indicates a responder has finished processing but is awaiting confirmation from the initiator
func (c *Channels) BeginFinalizing(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.BeginFinalizing)
}

// Cancel indicates a channel was cancelled prematurely
func (c *Channels) Cancel(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.Cancel)
}

// Error indicates something that went wrong on a channel
func (c *Channels) Error(chid datatransfer.ChannelID, err error) error {
	return c.send(chid, datatransfer.Error, err)
}

func (c *Channels) Disconnected(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.Disconnected)
}

// HasChannel returns true if the given channel id is being tracked
func (c *Channels) HasChannel(chid datatransfer.ChannelID) (bool, error) {
	return c.stateMachines.Has(chid)
}

func (c *Channels) send(chid datatransfer.ChannelID, code datatransfer.EventCode, args ...interface{}) error {
	has, err := c.stateMachines.Has(chid)
	if err != nil {
		return err
	}
	if !has {
		return ErrNotFound
	}
	return c.stateMachines.Send(chid, code, args...)
}
