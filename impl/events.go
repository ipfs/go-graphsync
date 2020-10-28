package impl

import (
	"context"
	"errors"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels"
	"github.com/filecoin-project/go-data-transfer/encoding"
	"github.com/filecoin-project/go-data-transfer/registry"
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
	err := m.channels.DataReceived(chid, link.(cidlink.Link).Cid, size)
	if err != nil {
		return err
	}
	if chid.Initiator != m.peerID {
		var result datatransfer.VoucherResult
		var err error
		var handled bool
		_ = m.revalidators.Each(func(_ datatransfer.TypeIdentifier, _ encoding.Decoder, processor registry.Processor) error {
			revalidator := processor.(datatransfer.Revalidator)
			handled, result, err = revalidator.OnPushDataReceived(chid, size)
			if handled {
				return errors.New("stop processing")
			}
			return nil
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

func (m *manager) OnDataQueued(chid datatransfer.ChannelID, link ipld.Link, size uint64) (datatransfer.Message, error) {
	if err := m.channels.DataQueued(chid, link.(cidlink.Link).Cid, size); err != nil {
		return nil, err
	}
	if chid.Initiator != m.peerID {
		var result datatransfer.VoucherResult
		var err error
		var handled bool
		_ = m.revalidators.Each(func(_ datatransfer.TypeIdentifier, _ encoding.Decoder, processor registry.Processor) error {
			revalidator := processor.(datatransfer.Revalidator)
			handled, result, err = revalidator.OnPullDataSent(chid, size)
			if handled {
				return errors.New("stop processing")
			}
			return nil
		})
		if err != nil || result != nil {
			return m.processRevalidationResult(chid, result, err)
		}
	}

	return nil, nil
}

func (m *manager) OnDataSent(chid datatransfer.ChannelID, link ipld.Link, size uint64) error {
	return m.channels.DataSent(chid, link.(cidlink.Link).Cid, size)
}

func (m *manager) OnRequestReceived(chid datatransfer.ChannelID, request datatransfer.Request) (datatransfer.Response, error) {
	if request.IsRestart() {
		return m.receiveRestartRequest(chid, request)
	}

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
		return nil, datatransfer.ErrPause
	}
	return nil, nil
}

func (m *manager) OnResponseReceived(chid datatransfer.ChannelID, response datatransfer.Response) error {
	if response.IsCancel() {
		return m.channels.Cancel(chid)
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
			return m.channels.Error(chid, datatransfer.ErrRejected)
		}
		if response.IsNew() {
			err := m.channels.Accept(chid)
			if err != nil {
				return err
			}
		}

		if response.IsRestart() {
			err := m.channels.Restart(chid)
			if err != nil {
				return err
			}
		}
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
	if response.IsPaused() {
		return m.pauseOther(chid)
	}
	return m.resumeOther(chid)
}

func (m *manager) OnRequestTimedOut(ctx context.Context, chid datatransfer.ChannelID) error {
	log.Warnf("channel %+v has timed out", chid)

	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(m.channelRemoveTimeout):
			channel, err := m.channels.GetByID(ctx, chid)
			if err == nil {
				if !(channels.IsChannelTerminated(channel.Status()) ||
					channels.IsChannelCleaningUp(channel.Status())) {
					if err := m.channels.Error(chid, datatransfer.ErrRemoved); err != nil {
						log.Errorf("failed to cancel timed-out channel: %v", err)
						return
					}
					log.Warnf("channel %+v has ben cancelled because of timeout", chid)
				}
			}
		}
	}()

	return nil
}

func (m *manager) OnRequestDisconnected(ctx context.Context, chid datatransfer.ChannelID) error {
	log.Warnf("channel %+v has stalled or disconnected", chid)

	// mark peer disconnected for informational purposes
	err := m.channels.Disconnected(chid)
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(m.channelRemoveTimeout):
			channel, err := m.channels.GetByID(ctx, chid)
			if err == nil {
				if !(channels.IsChannelTerminated(channel.Status()) ||
					channels.IsChannelCleaningUp(channel.Status())) {
					if err := m.channels.Error(chid, datatransfer.ErrRemoved); err != nil {
						log.Errorf("failed to cancel timed-out channel: %v", err)
						return
					}
					log.Warnf("channel %+v has ben cancelled because of timeout", chid)
				}
			}
		}
	}()

	return nil
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
					log.Warnf("failed to send completion message, err : %v", err)
					return m.OnRequestDisconnected(context.TODO(), chid)
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
	chst, err := m.channels.GetByID(context.TODO(), chid)
	if err != nil {
		return err
	}
	// send an error, but only if we haven't already errored for some reason
	if chst.Status() != datatransfer.Failing && chst.Status() != datatransfer.Failed {
		return m.channels.Error(chid, datatransfer.ErrIncomplete)
	}
	return nil
}

func (m *manager) receiveRestartRequest(chid datatransfer.ChannelID, incoming datatransfer.Request) (datatransfer.Response, error) {
	result, err := m.restartRequest(chid, incoming)
	msg, msgErr := m.response(true, false, err, incoming.TransferID(), result)
	if msgErr != nil {
		return nil, msgErr
	}
	return msg, err
}

func (m *manager) receiveNewRequest(
	initiator peer.ID,
	incoming datatransfer.Request) (datatransfer.Response, error) {
	result, err := m.acceptRequest(initiator, incoming)
	msg, msgErr := m.response(false, true, err, incoming.TransferID(), result)
	if msgErr != nil {
		return nil, msgErr
	}
	return msg, err
}

func (m *manager) restartRequest(chid datatransfer.ChannelID,
	incoming datatransfer.Request) (datatransfer.VoucherResult, error) {

	initiator := chid.Initiator
	if m.peerID == initiator {
		return nil, xerrors.New("initiator cannot be manager peer for a restart request")
	}

	if err := m.validateRestartRequest(context.Background(), initiator, chid, incoming); err != nil {
		return nil, err
	}

	stor, err := incoming.Selector()
	if err != nil {
		return nil, err
	}

	voucher, result, err := m.validateVoucher(initiator, incoming, incoming.IsPull(), incoming.BaseCid(), stor)
	if err != nil && err != datatransfer.ErrPause {
		return result, xerrors.Errorf("failed to validate voucher: %w", err)
	}
	voucherErr := err

	if result != nil {
		err := m.channels.NewVoucherResult(chid, result)
		if err != nil {
			return result, err
		}
	}
	if err := m.channels.Restart(chid); err != nil {
		return result, err
	}
	processor, has := m.transportConfigurers.Processor(voucher.Type())
	if has {
		transportConfigurer := processor.(datatransfer.TransportConfigurer)
		transportConfigurer(chid, voucher, m.transport)
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

func (m *manager) acceptRequest(
	initiator peer.ID,
	incoming datatransfer.Request) (datatransfer.VoucherResult, error) {

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

	chid, err := m.channels.CreateNew(m.peerID, incoming.TransferID(), incoming.BaseCid(), stor, voucher, initiator, dataSender, dataReceiver)
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
	processor, has := m.transportConfigurers.Processor(voucher.Type())
	if has {
		transportConfigurer := processor.(datatransfer.TransportConfigurer)
		transportConfigurer(chid, voucher, m.transport)
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
	incoming datatransfer.Request,
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
	incoming datatransfer.Request) (datatransfer.Voucher, datatransfer.VoucherResult, error) {
	vouch, err := m.decodeVoucher(incoming, m.revalidators)
	if err != nil {
		return nil, nil, err
	}
	processor, _ := m.revalidators.Processor(vouch.Type())
	validator := processor.(datatransfer.Revalidator)

	result, err := validator.Revalidate(chid, vouch)
	return vouch, result, err
}

func (m *manager) processUpdateVoucher(chid datatransfer.ChannelID, request datatransfer.Request) (datatransfer.Response, error) {
	vouch, result, voucherErr := m.revalidateVoucher(chid, request)
	if vouch != nil {
		err := m.channels.NewVoucher(chid, vouch)
		if err != nil {
			return nil, err
		}
	}
	return m.processRevalidationResult(chid, result, voucherErr)
}

func (m *manager) revalidationResponse(chid datatransfer.ChannelID, result datatransfer.VoucherResult, resultErr error) (datatransfer.Response, error) {
	chst, err := m.channels.GetByID(context.TODO(), chid)
	if err != nil {
		return nil, err
	}
	if chst.Status() == datatransfer.Finalizing {
		return m.completeResponse(resultErr, chid.ID, result)
	}
	return m.response(false, false, resultErr, chid.ID, result)
}

func (m *manager) processRevalidationResult(chid datatransfer.ChannelID, result datatransfer.VoucherResult, resultErr error) (datatransfer.Response, error) {
	vresMessage, err := m.revalidationResponse(chid, result, resultErr)

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
		return vresMessage, datatransfer.ErrPause
	}

	if resultErr == nil {
		err = m.resume(chid)
		if err != nil {
			return nil, err
		}
		return vresMessage, datatransfer.ErrResume
	}
	return vresMessage, resultErr
}

func (m *manager) completeMessage(chid datatransfer.ChannelID) (datatransfer.Response, error) {
	var result datatransfer.VoucherResult
	var resultErr error
	var handled bool
	_ = m.revalidators.Each(func(_ datatransfer.TypeIdentifier, _ encoding.Decoder, processor registry.Processor) error {
		revalidator := processor.(datatransfer.Revalidator)
		handled, result, resultErr = revalidator.OnComplete(chid)
		if handled {
			return errors.New("stop processing")
		}
		return nil
	})
	if result != nil {
		err := m.channels.NewVoucherResult(chid, result)
		if err != nil {
			return nil, err
		}
	}

	return m.completeResponse(resultErr, chid.ID, result)
}
