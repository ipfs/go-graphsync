package impl

import (
	"context"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/registry"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"
)

type statusList []datatransfer.Status

func (sl statusList) Contains(s datatransfer.Status) bool {
	for _, ts := range sl {
		if ts == s {
			return true
		}
	}
	return false
}

var resumeTransportStatesResponder = statusList{
	datatransfer.Requested,
	datatransfer.Ongoing,
	datatransfer.InitiatorPaused,
}

// newRequest encapsulates message creation
func (m *manager) newRequest(ctx context.Context, selector ipld.Node, isPull bool, voucher datatransfer.Voucher, baseCid cid.Cid, to peer.ID) (message.DataTransferRequest, error) {
	next, err := m.storedCounter.Next()
	if err != nil {
		return nil, err
	}
	tid := datatransfer.TransferID(next)
	return message.NewRequest(tid, isPull, voucher.Type(), voucher, baseCid, selector)
}

func (m *manager) response(isNew bool, err error, tid datatransfer.TransferID, voucherResult datatransfer.VoucherResult) (message.DataTransferResponse, error) {
	isAccepted := err == nil || err == datatransfer.ErrPause
	isPaused := err == datatransfer.ErrPause
	resultType := datatransfer.EmptyTypeIdentifier
	if voucherResult != nil {
		resultType = voucherResult.Type()
	}
	if isNew {
		return message.NewResponse(tid, isAccepted, isPaused, resultType, voucherResult)
	}
	return message.VoucherResultResponse(tid, isAccepted, isPaused, resultType, voucherResult)
}

func (m *manager) resume(chid datatransfer.ChannelID) error {
	if chid.Initiator == m.peerID {
		return m.channels.ResumeInitiator(chid)
	}
	return m.channels.ResumeResponder(chid)
}

func (m *manager) pause(chid datatransfer.ChannelID) error {
	if chid.Initiator == m.peerID {
		return m.channels.PauseInitiator(chid)
	}
	return m.channels.PauseResponder(chid)
}

func (m *manager) resumeOther(chid datatransfer.ChannelID) error {
	if chid.Responder == m.peerID {
		return m.channels.ResumeInitiator(chid)
	}
	return m.channels.ResumeResponder(chid)
}

func (m *manager) pauseOther(chid datatransfer.ChannelID) error {
	if chid.Responder == m.peerID {
		return m.channels.PauseInitiator(chid)
	}
	return m.channels.PauseResponder(chid)
}

func (m *manager) resumeMessage(chid datatransfer.ChannelID) message.DataTransferMessage {
	if chid.Initiator == m.peerID {
		return message.UpdateRequest(chid.ID, false)
	}
	return message.UpdateResponse(chid.ID, false)
}

func (m *manager) pauseMessage(chid datatransfer.ChannelID) message.DataTransferMessage {
	if chid.Initiator == m.peerID {
		return message.UpdateRequest(chid.ID, true)
	}
	return message.UpdateResponse(chid.ID, true)
}

func (m *manager) cancelMessage(chid datatransfer.ChannelID) message.DataTransferMessage {
	if chid.Initiator == m.peerID {
		return message.CancelRequest(chid.ID)
	}
	return message.CancelResponse(chid.ID)
}

func (m *manager) decodeVoucherResult(response message.DataTransferResponse) (datatransfer.VoucherResult, error) {
	vtypStr := datatransfer.TypeIdentifier(response.VoucherResultType())
	decoder, has := m.resultTypes.Decoder(vtypStr)
	if !has {
		return nil, xerrors.Errorf("unknown voucher result type: %s", vtypStr)
	}
	encodable, err := response.VoucherResult(decoder)
	if err != nil {
		return nil, err
	}
	return encodable.(datatransfer.Registerable), nil
}

func (m *manager) decodeVoucher(request message.DataTransferRequest, registry *registry.Registry) (datatransfer.Voucher, error) {
	vtypStr := datatransfer.TypeIdentifier(request.VoucherType())
	decoder, has := registry.Decoder(vtypStr)
	if !has {
		return nil, xerrors.Errorf("unknown voucher type: %s", vtypStr)
	}
	encodable, err := request.Voucher(decoder)
	if err != nil {
		return nil, err
	}
	return encodable.(datatransfer.Registerable), nil
}
