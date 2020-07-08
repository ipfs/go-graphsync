package impl

import (
	"context"
	"errors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/encoding"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/registry"
	"github.com/filecoin-project/go-data-transfer/transport"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

func (m *manager) OnChannelOpened(chid datatransfer.ChannelID) error {
	has, err := m.channels.HasChannel(chid)
	if err != nil {
		return err
	}
	if !has {
		return datatransfer.ErrChannelNotFound
	}
	return nil
}

func (m *manager) OnDataReceived(chid datatransfer.ChannelID, link ipld.Link, size uint64) error {
	err := m.channels.IncrementReceived(chid, size)
	if err != nil {
		return err
	}
	if chid.Initiator != m.peerID {
		var result datatransfer.VoucherResult
		var err error
		_ = m.revalidators.Each(func(_ datatransfer.TypeIdentifier, _ encoding.Decoder, processor registry.Processor) error {
			revalidator := processor.(datatransfer.Revalidator)
			result, err = revalidator.OnPushDataReceived(chid, size)
			return err
		})
		if err != nil || result != nil {
			msg, err := m.processRevalidationResult(chid, result, err)
			if msg != nil {
				if err := m.dataTransferNetwork.SendMessage(context.TODO(), chid.Initiator, msg); err != nil {
					return err
				}
			}
			return err
		}
	}
	return nil
}

func (m *manager) OnDataSent(chid datatransfer.ChannelID, link ipld.Link, size uint64) (message.DataTransferMessage, error) {
	err := m.channels.IncrementSent(chid, size)
	if err != nil {
		return nil, err
	}
	if chid.Initiator != m.peerID {
		var result datatransfer.VoucherResult
		var err error
		_ = m.revalidators.Each(func(_ datatransfer.TypeIdentifier, _ encoding.Decoder, processor registry.Processor) error {
			revalidator := processor.(datatransfer.Revalidator)
			result, err = revalidator.OnPullDataSent(chid, size)
			return err
		})
		if err != nil || result != nil {
			return m.processRevalidationResult(chid, result, err)
		}
	}
	return nil, nil
}

func (m *manager) OnRequestReceived(chid datatransfer.ChannelID, request message.DataTransferRequest) (message.DataTransferResponse, error) {
	if request.IsNew() {
		return m.receiveNewRequest(chid.Initiator, request)
	}
	if request.IsCancel() {
		m.transport.CleanupChannel(chid)
		return nil, m.channels.Cancel(chid)
	}
	if request.IsVoucher() {
		return m.processUpdateVoucher(chid, request)
	}
	if request.IsPaused() {
		return nil, m.pauseOther(chid)
	}
	err := m.resumeOther(chid)
	if err != nil {
		return nil, err
	}
	chst, err := m.channels.GetByID(context.TODO(), chid)
	if err != nil {
		return nil, err
	}
	if chst.Status() == datatransfer.ResponderPaused ||
		chst.Status() == datatransfer.ResponderFinalizing {
		return nil, transport.ErrPause
	}
	return nil, nil
}

func (m *manager) OnResponseReceived(chid datatransfer.ChannelID, response message.DataTransferResponse) error {
	if response.IsCancel() {
		return m.channels.Cancel(chid)
	}
	if response.IsComplete() && response.Accepted() {
		if !response.IsPaused() {
			return m.channels.ResponderCompletes(chid)
		}
		err := m.channels.ResponderBeginsFinalization(chid)
		if err != nil {
			return nil
		}
	}
	if response.IsVoucherResult() {
		if !response.EmptyVoucherResult() {
			vresult, err := m.decodeVoucherResult(response)
			if err != nil {
				return err
			}
			err = m.channels.NewVoucherResult(chid, vresult)
			if err != nil {
				return err
			}
		}
		if !response.Accepted() {
			return m.channels.Error(chid, errors.New("Response Rejected"))
		}
		if response.IsNew() {
			err := m.channels.Accept(chid)
			if err != nil {
				return err
			}
		}
	}
	if response.IsPaused() {
		return m.pauseOther(chid)
	}
	return m.resumeOther(chid)
}

func (m *manager) OnChannelCompleted(chid datatransfer.ChannelID, success bool) error {
	if success {
		if chid.Initiator != m.peerID {
			msg, err := m.completeMessage(chid)
			if err != nil {
				return nil
			}
			if msg != nil {
				if err := m.dataTransferNetwork.SendMessage(context.TODO(), chid.Initiator, msg); err != nil {
					_ = m.channels.Error(chid, err)
					return err
				}
			}
			if msg.Accepted() {
				if msg.IsPaused() {
					return m.channels.BeginFinalizing(chid)
				}
				return m.channels.Complete(chid)
			}
			return m.channels.Error(chid, err)
		}
		return m.channels.FinishTransfer(chid)
	}
	return m.channels.Error(chid, errors.New("incomplete response"))
}

func (m *manager) receiveNewRequest(
	initiator peer.ID,
	incoming message.DataTransferRequest) (message.DataTransferResponse, error) {
	result, err := m.acceptRequest(initiator, incoming)
	msg, msgErr := m.response(true, err, incoming.TransferID(), result)
	if msgErr != nil {
		return nil, msgErr
	}
	// convert to the transport error for pauses
	if err == datatransfer.ErrPause {
		err = transport.ErrPause
	}
	return msg, err
}

func (m *manager) acceptRequest(
	initiator peer.ID,
	incoming message.DataTransferRequest) (datatransfer.VoucherResult, error) {

	stor, err := incoming.Selector()
	if err != nil {
		return nil, err
	}

	voucher, result, err := m.validateVoucher(initiator, incoming, incoming.IsPull(), incoming.BaseCid(), stor)
	if err != nil && err != datatransfer.ErrPause {
		return result, err
	}
	voucherErr := err

	var dataSender, dataReceiver peer.ID
	if incoming.IsPull() {
		dataSender = m.peerID
		dataReceiver = initiator
	} else {
		dataSender = initiator
		dataReceiver = m.peerID
	}

	chid, err := m.channels.CreateNew(incoming.TransferID(), incoming.BaseCid(), stor, voucher, initiator, dataSender, dataReceiver)
	if err != nil {
		return result, err
	}
	if result != nil {
		err := m.channels.NewVoucherResult(chid, result)
		if err != nil {
			return result, err
		}
	}
	if err := m.channels.Accept(chid); err != nil {
		return result, err
	}
	m.dataTransferNetwork.Protect(initiator, chid.String())
	if voucherErr == datatransfer.ErrPause {
		err := m.channels.PauseResponder(chid)
		if err != nil {
			return result, err
		}
	}
	return result, voucherErr
}

// validateVoucher converts a voucher in an incoming message to its appropriate
// voucher struct, then runs the validator and returns the results.
// returns error if:
//   * reading voucher fails
//   * deserialization of selector fails
//   * validation fails
func (m *manager) validateVoucher(sender peer.ID,
	incoming message.DataTransferRequest,
	isPull bool,
	baseCid cid.Cid,
	stor ipld.Node) (datatransfer.Voucher, datatransfer.VoucherResult, error) {
	vouch, err := m.decodeVoucher(incoming, m.validatedTypes)
	if err != nil {
		return nil, nil, err
	}
	var validatorFunc func(peer.ID, datatransfer.Voucher, cid.Cid, ipld.Node) (datatransfer.VoucherResult, error)
	processor, _ := m.validatedTypes.Processor(vouch.Type())
	validator := processor.(datatransfer.RequestValidator)
	if isPull {
		validatorFunc = validator.ValidatePull
	} else {
		validatorFunc = validator.ValidatePush
	}

	result, err := validatorFunc(sender, vouch, baseCid, stor)
	return vouch, result, err
}

// revalidateVoucher converts a voucher in an incoming message to its appropriate
// voucher struct, then runs the revalidator and returns the results.
// returns error if:
//   * reading voucher fails
//   * deserialization of selector fails
//   * validation fails
func (m *manager) revalidateVoucher(chid datatransfer.ChannelID,
	incoming message.DataTransferRequest) (datatransfer.Voucher, datatransfer.VoucherResult, error) {
	vouch, err := m.decodeVoucher(incoming, m.revalidators)
	if err != nil {
		return nil, nil, err
	}
	processor, _ := m.revalidators.Processor(vouch.Type())
	validator := processor.(datatransfer.Revalidator)

	result, err := validator.Revalidate(chid, vouch)
	return vouch, result, err
}

func (m *manager) processUpdateVoucher(chid datatransfer.ChannelID, request message.DataTransferRequest) (message.DataTransferResponse, error) {
	vouch, result, voucherErr := m.revalidateVoucher(chid, request)
	if vouch != nil {
		err := m.channels.NewVoucher(chid, vouch)
		if err != nil {
			return nil, err
		}
	}
	return m.processRevalidationResult(chid, result, voucherErr)
}

func (m *manager) processRevalidationResult(chid datatransfer.ChannelID, result datatransfer.VoucherResult, resultErr error) (message.DataTransferResponse, error) {
	vresMessage, err := m.response(false, resultErr, chid.ID, result)
	if err != nil {
		return nil, err
	}

	if result != nil {
		err := m.channels.NewVoucherResult(chid, result)
		if err != nil {
			return nil, err
		}
	}
	if resultErr == datatransfer.ErrPause {
		err := m.pause(chid)
		if err != nil {
			return nil, err
		}
		return vresMessage, transport.ErrPause
	}

	if resultErr == nil {
		err = m.resume(chid)
		if err != nil {
			return nil, err
		}
		return vresMessage, transport.ErrResume
	}
	return vresMessage, resultErr
}

func (m *manager) completeMessage(chid datatransfer.ChannelID) (message.DataTransferResponse, error) {
	var result datatransfer.VoucherResult
	var resultErr error
	_ = m.revalidators.Each(func(_ datatransfer.TypeIdentifier, _ encoding.Decoder, processor registry.Processor) error {
		revalidator := processor.(datatransfer.Revalidator)
		result, resultErr = revalidator.OnComplete(chid)
		return resultErr
	})
	vtype := datatransfer.EmptyTypeIdentifier
	if result != nil {
		vtype = result.Type()
		err := m.channels.NewVoucherResult(chid, result)
		if err != nil {
			return nil, err
		}
	}

	return message.CompleteResponse(chid.ID,
		resultErr == nil || resultErr == datatransfer.ErrPause,
		resultErr == datatransfer.ErrPause, vtype, result)
}
