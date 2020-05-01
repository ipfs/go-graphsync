package graphsyncimpl

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	xerrors "golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/message"
)

type graphsyncReceiver struct {
	impl *graphsyncImpl
}

// ReceiveRequest takes an incoming data transfer request, validates the voucher and
// processes the message.
func (receiver *graphsyncReceiver) ReceiveRequest(
	ctx context.Context,
	initiator peer.ID,
	incoming message.DataTransferRequest) {

	voucher, err := receiver.validateVoucher(initiator, incoming)
	if err != nil {
		receiver.impl.sendResponse(ctx, false, initiator, incoming.TransferID())
		return
	}
	stor, _ := incoming.Selector()
	root := cidlink.Link{Cid: incoming.BaseCid()}

	var dataSender, dataReceiver peer.ID
	if incoming.IsPull() {
		dataSender = receiver.impl.peerID
		dataReceiver = initiator
	} else {
		dataSender = initiator
		dataReceiver = receiver.impl.peerID
		receiver.impl.sendGsRequest(ctx, initiator, incoming.TransferID(), incoming.IsPull(), dataSender, root, stor)
	}

	_, err = receiver.impl.channels.CreateNew(incoming.TransferID(), incoming.BaseCid(), stor, voucher, initiator, dataSender, dataReceiver)
	if err != nil {
		log.Error(err)
		receiver.impl.sendResponse(ctx, false, initiator, incoming.TransferID())
		return
	}
	receiver.impl.sendResponse(ctx, true, initiator, incoming.TransferID())
}

// validateVoucher converts a voucher in an incoming message to its appropriate
// voucher struct, then runs the validator and returns the results.
// returns error if:
//   * reading voucher fails
//   * deserialization of selector fails
//   * validation fails
func (receiver *graphsyncReceiver) validateVoucher(sender peer.ID, incoming message.DataTransferRequest) (datatransfer.Voucher, error) {

	vtypStr := datatransfer.TypeIdentifier(incoming.VoucherType())
	decoder, has := receiver.impl.validatedTypes.Decoder(vtypStr)
	if !has {
		return nil, xerrors.Errorf("unknown voucher type: %s", vtypStr)
	}
	encodable, err := incoming.Voucher(decoder)
	if err != nil {
		return nil, err
	}
	vouch := encodable.(datatransfer.Registerable)

	var validatorFunc func(peer.ID, datatransfer.Voucher, cid.Cid, ipld.Node) error
	processor, _ := receiver.impl.validatedTypes.Processor(vtypStr)
	validator := processor.(datatransfer.RequestValidator)
	if incoming.IsPull() {
		validatorFunc = validator.ValidatePull
	} else {
		validatorFunc = validator.ValidatePush
	}

	stor, err := incoming.Selector()
	if err != nil {
		return vouch, err
	}

	if err = validatorFunc(sender, vouch, incoming.BaseCid(), stor); err != nil {
		return nil, err
	}

	return vouch, nil
}

// ReceiveResponse handles responses to our  Push or Pull data transfer request.
// It schedules a graphsync transfer only if our Pull Request is accepted.
func (receiver *graphsyncReceiver) ReceiveResponse(
	ctx context.Context,
	sender peer.ID,
	incoming message.DataTransferResponse) {
	evt := datatransfer.Event{
		Code:      datatransfer.Error,
		Message:   "",
		Timestamp: time.Now(),
	}
	chst := datatransfer.EmptyChannelState
	if incoming.Accepted() {
		// if we are handling a response to a pull request then they are sending data and the
		// initiator is us. construct a channel id for a pull request that we initiated and see
		// if there is one in our saved channel list. otherwise we should not respond.
		chid := datatransfer.ChannelID{Initiator: receiver.impl.peerID, ID: incoming.TransferID()}

		// if we are handling a response to a pull request then they are sending data and the
		// initiator is us
		if chst = receiver.impl.channels.GetByIDAndSender(chid, sender); chst != datatransfer.EmptyChannelState {
			baseCid := chst.BaseCID()
			root := cidlink.Link{Cid: baseCid}
			receiver.impl.sendGsRequest(ctx, receiver.impl.peerID, incoming.TransferID(), true, sender, root, chst.Selector())
			evt.Code = datatransfer.Progress
		}
	}
	err := receiver.impl.pubSub.Publish(internalEvent{evt, chst})
	if err != nil {
		log.Warnf("err publishing DT event: %s", err.Error())
	}
}

func (receiver *graphsyncReceiver) ReceiveError(error) {}
