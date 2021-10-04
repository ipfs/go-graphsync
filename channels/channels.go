package channels

import (
	"context"
	"errors"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipld/go-ipld-prime"
	peer "github.com/libp2p/go-libp2p-core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	versionedfsm "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
	"github.com/filecoin-project/go-statemachine"
	"github.com/filecoin-project/go-statemachine/fsm"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels/internal"
	"github.com/filecoin-project/go-data-transfer/channels/internal/migrations"
	"github.com/filecoin-project/go-data-transfer/cidlists"
	"github.com/filecoin-project/go-data-transfer/cidsets"
	"github.com/filecoin-project/go-data-transfer/encoding"
)

type DecoderByTypeFunc func(identifier datatransfer.TypeIdentifier) (encoding.Decoder, bool)

type ReceivedCidsReader interface {
	ToArray(chid datatransfer.ChannelID) ([]cid.Cid, error)
	Len(chid datatransfer.ChannelID) (int, error)
}

type Notifier func(datatransfer.Event, datatransfer.ChannelState)

// ErrNotFound is returned when a channel cannot be found with a given channel ID
type ErrNotFound struct {
	ChannelID datatransfer.ChannelID
}

func (e *ErrNotFound) Error() string {
	return "No channel for channel ID " + e.ChannelID.String()
}

func NewErrNotFound(chid datatransfer.ChannelID) error {
	return &ErrNotFound{ChannelID: chid}
}

// ErrWrongType is returned when a caller attempts to change the type of implementation data after setting it
var ErrWrongType = errors.New("Cannot change type of implementation specific data after setting it")

// Channels is a thread safe list of channels
type Channels struct {
	notifier             Notifier
	voucherDecoder       DecoderByTypeFunc
	voucherResultDecoder DecoderByTypeFunc
	stateMachines        fsm.Group
	migrateStateMachines func(context.Context) error
	seenCIDs             *cidsets.CIDSetManager
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
	cidLists cidlists.CIDLists,
	notifier Notifier,
	voucherDecoder DecoderByTypeFunc,
	voucherResultDecoder DecoderByTypeFunc,
	env ChannelEnvironment,
	selfPeer peer.ID) (*Channels, error) {

	seenCIDsDS := namespace.Wrap(ds, datastore.NewKey("seencids"))
	c := &Channels{
		seenCIDs:             cidsets.NewCIDSetManager(seenCIDsDS),
		notifier:             notifier,
		voucherDecoder:       voucherDecoder,
		voucherResultDecoder: voucherResultDecoder,
	}
	channelMigrations, err := migrations.GetChannelStateMigrations(selfPeer, cidLists)
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
	}, channelMigrations, versioning.VersionKey("2"))
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
	log.Debugw("process data transfer listeners", "name", datatransfer.Events[evtCode], "transfer ID", realChannel.TransferID)
	c.notifier(evt, c.fromInternalChannelState(realChannel))

	// When the channel has been cleaned up, remove the caches of seen cids
	if evt.Code == datatransfer.CleanupComplete {
		chid := datatransfer.ChannelID{
			Initiator: realChannel.Initiator,
			Responder: realChannel.Responder,
			ID:        realChannel.TransferID,
		}
		err := c.removeSeenCIDCaches(chid)
		if err != nil {
			log.Errorf("failed to clean up channel %s: %s", err)
		}
	}
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
		Stages:     &datatransfer.ChannelStages{},
		Vouchers: []internal.EncodedVoucher{
			{
				Type: voucher.Type(),
				Voucher: &cbg.Deferred{
					Raw: voucherBytes,
				},
			},
		},
		Status: datatransfer.Requested,
	})
	if err != nil {
		log.Errorw("failed to create new tracking channel for data-transfer", "channelID", chid, "err", err)
		return datatransfer.ChannelID{}, err
	}
	log.Debugw("created tracking channel for data-transfer, emitting channel Open event", "channelID", chid)
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
			c.fromInternalChannelState(internalChannel)
	}
	return channels, nil
}

// GetByID searches for a channel in the slice of channels with id `chid`.
// Returns datatransfer.EmptyChannelState if there is no channel with that id
func (c *Channels) GetByID(ctx context.Context, chid datatransfer.ChannelID) (datatransfer.ChannelState, error) {
	var internalChannel internal.ChannelState
	err := c.stateMachines.GetSync(ctx, chid, &internalChannel)
	if err != nil {
		return nil, NewErrNotFound(chid)
	}
	return c.fromInternalChannelState(internalChannel), nil
}

// Accept marks a data transfer as accepted
func (c *Channels) Accept(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.Accept)
}

func (c *Channels) TransferRequestQueued(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.TransferRequestQueued)
}

// Restart marks a data transfer as restarted
func (c *Channels) Restart(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.Restart)
}

func (c *Channels) CompleteCleanupOnRestart(chid datatransfer.ChannelID) error {
	return c.send(chid, datatransfer.CompleteCleanupOnRestart)
}

// Returns true if this is the first time the block has been sent
func (c *Channels) DataSent(chid datatransfer.ChannelID, k cid.Cid, delta uint64) (bool, error) {
	return c.fireProgressEvent(chid, datatransfer.DataSent, datatransfer.DataSentProgress, k, delta)
}

// Returns true if this is the first time the block has been queued
func (c *Channels) DataQueued(chid datatransfer.ChannelID, k cid.Cid, delta uint64) (bool, error) {
	return c.fireProgressEvent(chid, datatransfer.DataQueued, datatransfer.DataQueuedProgress, k, delta)
}

// Returns true if this is the first time the block has been received
func (c *Channels) DataReceived(chid datatransfer.ChannelID, k cid.Cid, delta uint64, index int64) (bool, error) {
	if err := c.checkChannelExists(chid, datatransfer.DataReceived); err != nil {
		return false, err
	}

	// Check if the block has already been seen
	sid := seenCidsSetID(chid, datatransfer.DataReceived)
	seen, err := c.seenCIDs.InsertSetCID(sid, k)
	if err != nil {
		return false, err
	}

	// If the block has not been seen before, fire the progress event
	if !seen {
		if err := c.stateMachines.Send(chid, datatransfer.DataReceivedProgress, delta); err != nil {
			return false, err
		}
	}

	// Fire the regular event
	return !seen, c.stateMachines.Send(chid, datatransfer.DataReceived, index)
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
	err := c.send(chid, datatransfer.Cancel)

	// If there was an error because the state machine already terminated,
	// sending a Cancel event is redundant anyway, so just ignore it
	if errors.Is(err, statemachine.ErrTerminated) {
		return nil
	}

	return err
}

// Error indicates something that went wrong on a channel
func (c *Channels) Error(chid datatransfer.ChannelID, err error) error {
	return c.send(chid, datatransfer.Error, err)
}

// Disconnected indicates that the connection went down and it was not possible
// to restart it
func (c *Channels) Disconnected(chid datatransfer.ChannelID, err error) error {
	return c.send(chid, datatransfer.Disconnected, err)
}

// RequestCancelled indicates that a transport layer request was cancelled by the
// request opener
func (c *Channels) RequestCancelled(chid datatransfer.ChannelID, err error) error {
	return c.send(chid, datatransfer.RequestCancelled, err)
}

// SendDataError indicates that the transport layer had an error trying
// to send data to the remote peer
func (c *Channels) SendDataError(chid datatransfer.ChannelID, err error) error {
	return c.send(chid, datatransfer.SendDataError, err)
}

// ReceiveDataError indicates that the transport layer had an error receiving
// data from the remote peer
func (c *Channels) ReceiveDataError(chid datatransfer.ChannelID, err error) error {
	return c.send(chid, datatransfer.ReceiveDataError, err)
}

// HasChannel returns true if the given channel id is being tracked
func (c *Channels) HasChannel(chid datatransfer.ChannelID) (bool, error) {
	return c.stateMachines.Has(chid)
}

// removeSeenCIDCaches cleans up the caches of "seen" blocks, ie
// blocks that have already been queued / sent / received
func (c *Channels) removeSeenCIDCaches(chid datatransfer.ChannelID) error {
	// Clean up seen block caches
	progressStates := []datatransfer.EventCode{
		datatransfer.DataQueued,
		datatransfer.DataSent,
		datatransfer.DataReceived,
	}
	for _, evt := range progressStates {
		sid := seenCidsSetID(chid, evt)
		err := c.seenCIDs.DeleteSet(sid)
		if err != nil {
			return err
		}
	}
	return nil
}

// fireProgressEvent fires
// - an event for queuing / sending / receiving blocks
// - a corresponding "progress" event if the block has not been seen before
// For example, if a block is being sent for the first time, the method will
// fire both DataSent AND DataSentProgress.
// If a block is resent, the method will fire DataSent but not DataSentProgress.
// Returns true if the block is new (both the event and a progress event were fired).
func (c *Channels) fireProgressEvent(chid datatransfer.ChannelID, evt datatransfer.EventCode, progressEvt datatransfer.EventCode, k cid.Cid, delta uint64) (bool, error) {
	if err := c.checkChannelExists(chid, evt); err != nil {
		return false, err
	}

	// Check if the block has already been seen
	sid := seenCidsSetID(chid, evt)
	seen, err := c.seenCIDs.InsertSetCID(sid, k)
	if err != nil {
		return false, err
	}

	// If the block has not been seen before, fire the progress event
	if !seen {
		if err := c.stateMachines.Send(chid, progressEvt, delta); err != nil {
			return false, err
		}
	}

	// Fire the regular event
	return !seen, c.stateMachines.Send(chid, evt)
}

func (c *Channels) send(chid datatransfer.ChannelID, code datatransfer.EventCode, args ...interface{}) error {
	err := c.checkChannelExists(chid, code)
	if err != nil {
		return err
	}
	log.Debugw("send data transfer event", "name", datatransfer.Events[code], "transfer ID", chid.ID, "args", args)
	return c.stateMachines.Send(chid, code, args...)
}

func (c *Channels) checkChannelExists(chid datatransfer.ChannelID, code datatransfer.EventCode) error {
	has, err := c.stateMachines.Has(chid)
	if err != nil {
		return err
	}
	if !has {
		return xerrors.Errorf("cannot send FSM event %s to data-transfer channel %s: %w",
			datatransfer.Events[code], chid, NewErrNotFound(chid))
	}
	return nil
}

// Get the ID of the CID set for the given channel ID and event code.
// The CID set stores a unique list of queued / sent / received CIDs.
func seenCidsSetID(chid datatransfer.ChannelID, evt datatransfer.EventCode) cidsets.SetID {
	return cidsets.SetID(chid.String() + "/" + datatransfer.Events[evt])
}

// Convert from the internally used channel state format to the externally exposed ChannelState
func (c *Channels) fromInternalChannelState(ch internal.ChannelState) datatransfer.ChannelState {
	rcr := &receivedCidsReader{seenCIDs: c.seenCIDs}
	return fromInternalChannelState(ch, c.voucherDecoder, c.voucherResultDecoder, rcr)
}

// Implements the ReceivedCidsReader interface so that the internal channel
// state has access to the received CIDs.
// The interface is used (instead of passing these values directly)
// so the values can be loaded lazily. Reading all CIDs from the datastore
// is an expensive operation so we want to avoid doing it unless necessary.
// Note that the received CIDs get cleaned up when the channel completes, so
// these methods will return an empty array after that point.
type receivedCidsReader struct {
	seenCIDs *cidsets.CIDSetManager
}

func (r *receivedCidsReader) ToArray(chid datatransfer.ChannelID) ([]cid.Cid, error) {
	sid := seenCidsSetID(chid, datatransfer.DataReceived)
	return r.seenCIDs.SetToArray(sid)
}

func (r *receivedCidsReader) Len(chid datatransfer.ChannelID) (int, error) {
	sid := seenCidsSetID(chid, datatransfer.DataReceived)
	return r.seenCIDs.SetLen(sid)
}

var _ ReceivedCidsReader = (*receivedCidsReader)(nil)
