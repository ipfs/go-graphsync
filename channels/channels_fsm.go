package channels

import (
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-statemachine/fsm"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels/internal"
)

var log = logging.Logger("data-transfer")

// ChannelEvents describe the events taht can
var ChannelEvents = fsm.Events{
	fsm.Event(datatransfer.Open).FromAny().To(datatransfer.Requested),
	fsm.Event(datatransfer.Accept).From(datatransfer.Requested).To(datatransfer.Ongoing),
	fsm.Event(datatransfer.Restart).FromAny().ToNoChange().Action(func(chst *internal.ChannelState) error {
		chst.Message = ""
		return nil
	}),

	fsm.Event(datatransfer.Cancel).FromAny().To(datatransfer.Cancelling),

	fsm.Event(datatransfer.DataReceived).FromMany(
		datatransfer.Requested,
		datatransfer.Ongoing,
		datatransfer.InitiatorPaused,
		datatransfer.ResponderPaused,
		datatransfer.BothPaused,
		datatransfer.ResponderCompleted,
		datatransfer.ResponderFinalizing).ToNoChange().Action(func(chst *internal.ChannelState, delta uint64, c cid.Cid) error {
		chst.Received += delta
		chst.ReceivedCids = append(chst.ReceivedCids, c)
		return nil
	}),

	fsm.Event(datatransfer.DataSent).FromMany(
		datatransfer.Requested,
		datatransfer.Ongoing,
		datatransfer.InitiatorPaused,
		datatransfer.ResponderPaused,
		datatransfer.BothPaused,
		datatransfer.ResponderCompleted,
		datatransfer.ResponderFinalizing).ToNoChange().Action(func(chst *internal.ChannelState, delta uint64, c cid.Cid) error {
		chst.Sent += delta
		return nil
	}),
	fsm.Event(datatransfer.DataQueued).FromMany(
		datatransfer.Requested,
		datatransfer.Ongoing,
		datatransfer.InitiatorPaused,
		datatransfer.ResponderPaused,
		datatransfer.BothPaused,
		datatransfer.ResponderCompleted,
		datatransfer.ResponderFinalizing).ToNoChange().Action(func(chst *internal.ChannelState, delta uint64, c cid.Cid) error {
		chst.Queued += delta
		return nil
	}),
	fsm.Event(datatransfer.Disconnected).FromAny().ToNoChange().Action(func(chst *internal.ChannelState) error {
		chst.Message = datatransfer.ErrDisconnected.Error()
		return nil
	}),

	fsm.Event(datatransfer.Error).FromAny().To(datatransfer.Failing).Action(func(chst *internal.ChannelState, err error) error {
		chst.Message = err.Error()
		return nil
	}),
	fsm.Event(datatransfer.NewVoucher).FromAny().ToNoChange().
		Action(func(chst *internal.ChannelState, vtype datatransfer.TypeIdentifier, voucherBytes []byte) error {
			chst.Vouchers = append(chst.Vouchers, internal.EncodedVoucher{Type: vtype, Voucher: &cbg.Deferred{Raw: voucherBytes}})
			return nil
		}),
	fsm.Event(datatransfer.NewVoucherResult).FromAny().ToNoChange().
		Action(func(chst *internal.ChannelState, vtype datatransfer.TypeIdentifier, voucherResultBytes []byte) error {
			chst.VoucherResults = append(chst.VoucherResults,
				internal.EncodedVoucherResult{Type: vtype, VoucherResult: &cbg.Deferred{Raw: voucherResultBytes}})
			return nil
		}),
	fsm.Event(datatransfer.PauseInitiator).
		FromMany(datatransfer.Requested, datatransfer.Ongoing).To(datatransfer.InitiatorPaused).
		From(datatransfer.ResponderPaused).To(datatransfer.BothPaused).
		FromAny().ToJustRecord(),
	fsm.Event(datatransfer.PauseResponder).
		FromMany(datatransfer.Requested, datatransfer.Ongoing).To(datatransfer.ResponderPaused).
		From(datatransfer.InitiatorPaused).To(datatransfer.BothPaused).
		FromAny().ToJustRecord(),
	fsm.Event(datatransfer.ResumeInitiator).
		From(datatransfer.InitiatorPaused).To(datatransfer.Ongoing).
		From(datatransfer.BothPaused).To(datatransfer.ResponderPaused).
		FromAny().ToJustRecord(),
	fsm.Event(datatransfer.ResumeResponder).
		From(datatransfer.ResponderPaused).To(datatransfer.Ongoing).
		From(datatransfer.BothPaused).To(datatransfer.InitiatorPaused).
		From(datatransfer.Finalizing).To(datatransfer.Completing).
		FromAny().ToJustRecord(),
	fsm.Event(datatransfer.FinishTransfer).
		FromAny().To(datatransfer.TransferFinished).
		FromMany(datatransfer.Failing, datatransfer.Cancelling).ToJustRecord().
		From(datatransfer.ResponderCompleted).To(datatransfer.Completing).
		From(datatransfer.ResponderFinalizing).To(datatransfer.ResponderFinalizingTransferFinished),
	fsm.Event(datatransfer.ResponderBeginsFinalization).
		FromAny().To(datatransfer.ResponderFinalizing).
		FromMany(datatransfer.Failing, datatransfer.Cancelling).ToJustRecord().
		From(datatransfer.TransferFinished).To(datatransfer.ResponderFinalizingTransferFinished),
	fsm.Event(datatransfer.ResponderCompletes).
		FromAny().To(datatransfer.ResponderCompleted).
		FromMany(datatransfer.Failing, datatransfer.Cancelling).ToJustRecord().
		From(datatransfer.ResponderPaused).To(datatransfer.ResponderFinalizing).
		From(datatransfer.TransferFinished).To(datatransfer.Completing).
		From(datatransfer.ResponderFinalizing).To(datatransfer.ResponderCompleted).
		From(datatransfer.ResponderFinalizingTransferFinished).To(datatransfer.Completing),
	fsm.Event(datatransfer.BeginFinalizing).FromAny().To(datatransfer.Finalizing),
	fsm.Event(datatransfer.Complete).FromAny().To(datatransfer.Completing),
	fsm.Event(datatransfer.CleanupComplete).
		From(datatransfer.Cancelling).To(datatransfer.Cancelled).
		From(datatransfer.Failing).To(datatransfer.Failed).
		From(datatransfer.Completing).To(datatransfer.Completed),

	// will kickoff state handlers for channels that were cleaning up
	fsm.Event(datatransfer.CompleteCleanupOnRestart).FromAny().ToNoChange(),
}

// ChannelStateEntryFuncs are handlers called as we enter different states
// (currently unused for this fsm)
var ChannelStateEntryFuncs = fsm.StateEntryFuncs{
	datatransfer.Cancelling: cleanupConnection,
	datatransfer.Failing:    cleanupConnection,
	datatransfer.Completing: cleanupConnection,
}

func cleanupConnection(ctx fsm.Context, env ChannelEnvironment, channel internal.ChannelState) error {
	otherParty := channel.Initiator
	if otherParty == env.ID() {
		otherParty = channel.Responder
	}
	env.CleanupChannel(datatransfer.ChannelID{ID: channel.TransferID, Initiator: channel.Initiator, Responder: channel.Responder})
	env.Unprotect(otherParty, datatransfer.ChannelID{ID: channel.TransferID, Initiator: channel.Initiator, Responder: channel.Responder}.String())
	return ctx.Trigger(datatransfer.CleanupComplete)
}

// CleanupStates are the penultimate states for a channel
var CleanupStates = []fsm.StateKey{
	datatransfer.Cancelling,
	datatransfer.Completing,
	datatransfer.Failing,
}

// ChannelFinalityStates are the final states for a channel
var ChannelFinalityStates = []fsm.StateKey{
	datatransfer.Cancelled,
	datatransfer.Completed,
	datatransfer.Failed,
}

// IsChannelTerminated returns true if the channel is in a finality state
func IsChannelTerminated(st datatransfer.Status) bool {
	for _, s := range ChannelFinalityStates {
		if s == st {
			return true
		}
	}

	return false
}

// IsChannelCleaningUp returns true if channel was being cleaned up and finished
func IsChannelCleaningUp(st datatransfer.Status) bool {
	for _, s := range CleanupStates {
		if s == st {
			return true
		}
	}

	return false
}
