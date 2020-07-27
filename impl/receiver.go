package impl

import (
	"context"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

type receiver struct {
	manager *manager
}

// ReceiveRequest takes an incoming data transfer request, validates the voucher and
// processes the message.
func (r *receiver) ReceiveRequest(
	ctx context.Context,
	initiator peer.ID,
	incoming datatransfer.Request) {
	err := r.receiveRequest(ctx, initiator, incoming)
	if err != nil {
		log.Warn(err)
	}
}

func (r *receiver) receiveRequest(ctx context.Context, initiator peer.ID, incoming datatransfer.Request) error {
	chid := datatransfer.ChannelID{Initiator: initiator, Responder: r.manager.peerID, ID: incoming.TransferID()}
	response, receiveErr := r.manager.OnRequestReceived(chid, incoming)

	if receiveErr == datatransfer.ErrResume {
		chst, err := r.manager.channels.GetByID(ctx, chid)
		if err != nil {
			return err
		}
		if resumeTransportStatesResponder.Contains(chst.Status()) {
			return r.manager.transport.(datatransfer.PauseableTransport).ResumeChannel(ctx, response, chid)
		}
		receiveErr = nil
	}

	if response != nil {
		if response.IsNew() && response.Accepted() && !incoming.IsPull() {
			stor, _ := incoming.Selector()
			if err := r.manager.transport.OpenChannel(ctx, initiator, chid, cidlink.Link{Cid: incoming.BaseCid()}, stor, response); err != nil {
				return err
			}
		} else {
			if err := r.manager.dataTransferNetwork.SendMessage(ctx, initiator, response); err != nil {
				return err
			}
		}
	}

	if receiveErr == datatransfer.ErrPause {
		return r.manager.transport.(datatransfer.PauseableTransport).PauseChannel(ctx, chid)
	}

	if receiveErr != nil {
		_ = r.manager.transport.CloseChannel(ctx, chid)
		return receiveErr
	}

	return nil
}

// ReceiveResponse handles responses to our  Push or Pull data transfer request.
// It schedules a transfer only if our Pull Request is accepted.
func (r *receiver) ReceiveResponse(
	ctx context.Context,
	sender peer.ID,
	incoming datatransfer.Response) {
	err := r.receiveResponse(ctx, sender, incoming)
	if err != nil {
		log.Error(err)
	}
}
func (r *receiver) receiveResponse(
	ctx context.Context,
	sender peer.ID,
	incoming datatransfer.Response) error {
	chid := datatransfer.ChannelID{Initiator: r.manager.peerID, Responder: sender, ID: incoming.TransferID()}
	err := r.manager.OnResponseReceived(chid, incoming)
	if err == datatransfer.ErrPause {
		return r.manager.transport.(datatransfer.PauseableTransport).PauseChannel(ctx, chid)
	}
	if err != nil {
		_ = r.manager.transport.CloseChannel(ctx, chid)
		return err
	}
	return nil
}

func (r *receiver) ReceiveError(err error) {
	log.Errorf("received error message on data transfer: %s", err.Error())
}
