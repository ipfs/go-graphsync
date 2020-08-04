package channels

import (
	logging "github.com/ipfs/go-log"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-statemachine/fsm"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

var log = logging.Logger("data-transfer")

// ChannelEvents describe the events taht can
var ChannelEvents = fsm.Events{
	fsm.Event(datatransfer.Open).FromAny().To(datatransfer.Requested),
	fsm.Event(datatransfer.Accept).From(datatransfer.Requested).To(datatransfer.Ongoing),
	fsm.Event(datatransfer.Cancel).FromAny().To(datatransfer.Cancelling),
	fsm.Event(datatransfer.Progress).FromMany(
		datatransfer.Requested,
		datatransfer.Ongoing,
		datatransfer.InitiatorPaused,
		datatransfer.ResponderPaused,
		datatransfer.BothPaused,
		datatransfer.ResponderCompleted,
		datatransfer.ResponderFinalizing).ToNoChange().Action(func(chst *internalChannelState, deltaSent uint64, deltaReceived uint64) error {
		chst.Received += deltaReceived
		chst.Sent += deltaSent
		return nil
	}),
	fsm.Event(datatransfer.Error).FromAny().To(datatransfer.Failing).Action(func(chst *internalChannelState, err error) error {
		chst.Message = err.Error()
		return nil
	}),
	fsm.Event(datatransfer.NewVoucher).FromAny().ToNoChange().
		Action(func(chst *internalChannelState, vtype datatransfer.TypeIdentifier, voucherBytes []byte) error {
			chst.Vouchers = append(chst.Vouchers, encodedVoucher{Type: vtype, Voucher: &cbg.Deferred{Raw: voucherBytes}})
			return nil
		}),
	fsm.Event(datatransfer.NewVoucherResult).FromAny().ToNoChange().
		Action(func(chst *internalChannelState, vtype datatransfer.TypeIdentifier, voucherResultBytes []byte) error {
			chst.VoucherResults = append(chst.VoucherResults,
				encodedVoucherResult{Type: vtype, VoucherResult: &cbg.Deferred{Raw: voucherResultBytes}})
			return nil
		}),
	fsm.Event(datatransfer.PauseInitiator).
		FromMany(datatransfer.Requested, datatransfer.Ongoing).To(datatransfer.InitiatorPaused).
		From(datatransfer.ResponderPaused).To(datatransfer.BothPaused).
		FromAny().ToNoChange(),
	fsm.Event(datatransfer.PauseResponder).
		FromMany(datatransfer.Requested, datatransfer.Ongoing).To(datatransfer.ResponderPaused).
		From(datatransfer.InitiatorPaused).To(datatransfer.BothPaused).
		FromAny().ToNoChange(),
	fsm.Event(datatransfer.ResumeInitiator).
		From(datatransfer.InitiatorPaused).To(datatransfer.Ongoing).
		From(datatransfer.BothPaused).To(datatransfer.ResponderPaused).
		FromAny().ToNoChange(),
	fsm.Event(datatransfer.ResumeResponder).
		From(datatransfer.ResponderPaused).To(datatransfer.Ongoing).
		From(datatransfer.BothPaused).To(datatransfer.InitiatorPaused).
		From(datatransfer.Finalizing).To(datatransfer.Completing).
		FromAny().ToNoChange(),
	fsm.Event(datatransfer.FinishTransfer).
		FromAny().To(datatransfer.TransferFinished).
		From(datatransfer.ResponderCompleted).To(datatransfer.Completing).
		From(datatransfer.ResponderFinalizing).To(datatransfer.ResponderFinalizingTransferFinished),
	fsm.Event(datatransfer.ResponderBeginsFinalization).
		FromAny().To(datatransfer.ResponderFinalizing).
		From(datatransfer.TransferFinished).To(datatransfer.ResponderFinalizingTransferFinished),
	fsm.Event(datatransfer.ResponderCompletes).
		FromAny().To(datatransfer.ResponderCompleted).
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
}

// ChannelStateEntryFuncs are handlers called as we enter different states
// (currently unused for this fsm)
var ChannelStateEntryFuncs = fsm.StateEntryFuncs{
	datatransfer.Cancelling: cleanupConnection,
	datatransfer.Failing:    cleanupConnection,
	datatransfer.Completing: cleanupConnection,
}

func cleanupConnection(ctx fsm.Context, env ChannelEnvironment, channel internalChannelState) error {
	otherParty := channel.Initiator
	if otherParty == env.ID() {
		otherParty = channel.Responder
	}
	env.CleanupChannel(datatransfer.ChannelID{ID: channel.TransferID, Initiator: channel.Initiator, Responder: channel.Responder})
	env.Unprotect(otherParty, datatransfer.ChannelID{ID: channel.TransferID, Initiator: channel.Initiator, Responder: channel.Responder}.String())
	return ctx.Trigger(datatransfer.CleanupComplete)
}

// ChannelFinalityStates are the final states for a channel
var ChannelFinalityStates = []fsm.StateKey{
	datatransfer.Cancelled,
	datatransfer.Completed,
	datatransfer.Failed,
}
