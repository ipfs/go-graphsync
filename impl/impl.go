package impl

import (
	"context"
	"errors"
	"fmt"

	"github.com/hannahhoward/go-pubsub"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-storedcounter"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels"
	"github.com/filecoin-project/go-data-transfer/encoding"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/network"
	"github.com/filecoin-project/go-data-transfer/registry"
)

var log = logging.Logger("dt-impl")

type manager struct {
	dataTransferNetwork  network.DataTransferNetwork
	validatedTypes       *registry.Registry
	resultTypes          *registry.Registry
	revalidators         *registry.Registry
	transportConfigurers *registry.Registry
	pubSub               *pubsub.PubSub
	channels             *channels.Channels
	peerID               peer.ID
	transport            datatransfer.Transport
	storedCounter        *storedcounter.StoredCounter
}

type internalEvent struct {
	evt   datatransfer.Event
	state datatransfer.ChannelState
}

func dispatcher(evt pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	ie, ok := evt.(internalEvent)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb, ok := subscriberFn.(datatransfer.Subscriber)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb(ie.evt, ie.state)
	return nil
}

// NewDataTransfer initializes a new instance of a data transfer manager
func NewDataTransfer(ds datastore.Datastore, dataTransferNetwork network.DataTransferNetwork, transport datatransfer.Transport, storedCounter *storedcounter.StoredCounter) (datatransfer.Manager, error) {
	m := &manager{
		dataTransferNetwork:  dataTransferNetwork,
		validatedTypes:       registry.NewRegistry(),
		resultTypes:          registry.NewRegistry(),
		revalidators:         registry.NewRegistry(),
		transportConfigurers: registry.NewRegistry(),
		pubSub:               pubsub.New(dispatcher),
		peerID:               dataTransferNetwork.ID(),
		transport:            transport,
		storedCounter:        storedCounter,
	}
	channels, err := channels.New(ds, m.notifier, m.voucherDecoder, m.resultTypes.Decoder, &channelEnvironment{m})
	if err != nil {
		return nil, err
	}
	m.channels = channels
	return m, nil
}

func (m *manager) voucherDecoder(voucherType datatransfer.TypeIdentifier) (encoding.Decoder, bool) {
	decoder, has := m.validatedTypes.Decoder(voucherType)
	if !has {
		return m.revalidators.Decoder(voucherType)
	}
	return decoder, true
}

func (m *manager) notifier(evt datatransfer.Event, chst datatransfer.ChannelState) {
	err := m.pubSub.Publish(internalEvent{evt, chst})
	if err != nil {
		log.Warnf("err publishing DT event: %s", err.Error())
	}
}

// Start initializes data transfer processing
func (m *manager) Start(ctx context.Context) error {
	dtReceiver := &receiver{m}
	m.dataTransferNetwork.SetDelegate(dtReceiver)
	return m.transport.SetEventHandler(m)
}

// Stop terminates all data transfers and ends processing
func (m *manager) Stop() error {
	return nil
}

// RegisterVoucherType registers a validator for the given voucher type
// returns error if:
// * voucher type does not implement voucher
// * there is a voucher type registered with an identical identifier
// * voucherType's Kind is not reflect.Ptr
func (m *manager) RegisterVoucherType(voucherType datatransfer.Voucher, validator datatransfer.RequestValidator) error {
	err := m.validatedTypes.Register(voucherType, validator)
	if err != nil {
		return xerrors.Errorf("error registering voucher type: %w", err)
	}
	return nil
}

// OpenPushDataChannel opens a data transfer that will send data to the recipient peer and
// transfer parts of the piece that match the selector
func (m *manager) OpenPushDataChannel(ctx context.Context, requestTo peer.ID, voucher datatransfer.Voucher, baseCid cid.Cid, selector ipld.Node) (datatransfer.ChannelID, error) {
	req, err := m.newRequest(ctx, selector, false, voucher, baseCid, requestTo)
	if err != nil {
		return datatransfer.ChannelID{}, err
	}

	chid, err := m.channels.CreateNew(req.TransferID(), baseCid, selector, voucher,
		m.peerID, m.peerID, requestTo) // initiator = us, sender = us, receiver = them
	if err != nil {
		return chid, err
	}
	processor, has := m.transportConfigurers.Processor(voucher.Type())
	if has {
		transportConfigurer := processor.(datatransfer.TransportConfigurer)
		transportConfigurer(chid, voucher, m.transport)
	}
	m.dataTransferNetwork.Protect(requestTo, chid.String())
	if err := m.dataTransferNetwork.SendMessage(ctx, requestTo, req); err != nil {
		err = fmt.Errorf("Unable to send request: %w", err)
		_ = m.channels.Error(chid, err)
		return chid, err
	}
	return chid, nil
}

// OpenPullDataChannel opens a data transfer that will request data from the sending peer and
// transfer parts of the piece that match the selector
func (m *manager) OpenPullDataChannel(ctx context.Context, requestTo peer.ID, voucher datatransfer.Voucher, baseCid cid.Cid, selector ipld.Node) (datatransfer.ChannelID, error) {
	req, err := m.newRequest(ctx, selector, true, voucher, baseCid, requestTo)
	if err != nil {
		return datatransfer.ChannelID{}, err
	}
	// initiator = us, sender = them, receiver = us
	chid, err := m.channels.CreateNew(req.TransferID(), baseCid, selector, voucher,
		m.peerID, requestTo, m.peerID)
	if err != nil {
		return chid, err
	}
	processor, has := m.transportConfigurers.Processor(voucher.Type())
	if has {
		transportConfigurer := processor.(datatransfer.TransportConfigurer)
		transportConfigurer(chid, voucher, m.transport)
	}
	m.dataTransferNetwork.Protect(requestTo, chid.String())
	if err := m.transport.OpenChannel(ctx, requestTo, chid, cidlink.Link{Cid: baseCid}, selector, req); err != nil {
		err = fmt.Errorf("Unable to send request: %w", err)
		_ = m.channels.Error(chid, err)
		return chid, err
	}
	return chid, nil
}

// SendVoucher sends an intermediate voucher as needed when the receiver sends a request for revalidation
func (m *manager) SendVoucher(ctx context.Context, channelID datatransfer.ChannelID, voucher datatransfer.Voucher) error {
	chst, err := m.channels.GetByID(ctx, channelID)
	if err != nil {
		return err
	}
	if channelID.Initiator != m.peerID {
		return errors.New("cannot send voucher for request we did not initiate")
	}
	updateRequest, err := message.VoucherRequest(channelID.ID, voucher.Type(), voucher)
	if err != nil {
		return err
	}
	if err := m.dataTransferNetwork.SendMessage(ctx, chst.OtherParty(m.peerID), updateRequest); err != nil {
		err = fmt.Errorf("Unable to send request: %w", err)
		_ = m.channels.Error(channelID, err)
		return err
	}
	return m.channels.NewVoucher(channelID, voucher)
}

// close an open channel (effectively a cancel)
func (m *manager) CloseDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	chst, err := m.channels.GetByID(ctx, chid)
	if err != nil {
		return err
	}
	err = m.transport.CloseChannel(ctx, chid)
	if err != nil {
		return err
	}

	if err := m.dataTransferNetwork.SendMessage(ctx, chst.OtherParty(m.peerID), m.cancelMessage(chid)); err != nil {
		err = fmt.Errorf("Unable to send cancel message: %w", err)
		_ = m.channels.Error(chid, err)
		return err
	}

	return m.channels.Cancel(chid)
}

// pause a running data transfer channel
func (m *manager) PauseDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error {

	pausable, ok := m.transport.(datatransfer.PauseableTransport)
	if !ok {
		return errors.New("unsupported")
	}

	err := pausable.PauseChannel(ctx, chid)
	if err != nil {
		log.Warnf("Error attempting to pause at transport level: %s", err.Error())
	}

	if err := m.dataTransferNetwork.SendMessage(ctx, chid.OtherParty(m.peerID), m.pauseMessage(chid)); err != nil {
		err = fmt.Errorf("Unable to send pause message: %w", err)
		_ = m.channels.Error(chid, err)
		return err
	}

	return m.pause(chid)
}

// resume a running data transfer channel
func (m *manager) ResumeDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	pausable, ok := m.transport.(datatransfer.PauseableTransport)
	if !ok {
		return errors.New("unsupported")
	}

	err := pausable.ResumeChannel(ctx, m.resumeMessage(chid), chid)
	if err != nil {
		log.Warnf("Error attempting to pause at transport level: %s", err.Error())
	}

	return m.resume(chid)
}

// get status of a transfer
func (m *manager) TransferChannelStatus(ctx context.Context, chid datatransfer.ChannelID) datatransfer.Status {
	chst, err := m.channels.GetByID(ctx, chid)
	if err != nil {
		return datatransfer.ChannelNotFoundError
	}
	return chst.Status()
}

// get notified when certain types of events happen
func (m *manager) SubscribeToEvents(subscriber datatransfer.Subscriber) datatransfer.Unsubscribe {
	return datatransfer.Unsubscribe(m.pubSub.Subscribe(subscriber))
}

// get all in progress transfers
func (m *manager) InProgressChannels(ctx context.Context) (map[datatransfer.ChannelID]datatransfer.ChannelState, error) {
	return m.channels.InProgress(ctx)
}

// RegisterRevalidator registers a revalidator for the given voucher type
// Note: this is the voucher type used to revalidate. It can share a name
// with the initial validator type and CAN be the same type, or a different type.
// The revalidator can simply be the sampe as the original request validator,
// or a different validator that satisfies the revalidator interface.
func (m *manager) RegisterRevalidator(voucherType datatransfer.Voucher, revalidator datatransfer.Revalidator) error {
	err := m.revalidators.Register(voucherType, revalidator)
	if err != nil {
		return xerrors.Errorf("error registering revalidator type: %w", err)
	}
	return nil
}

// RegisterVoucherResultType allows deserialization of a voucher result,
// so that a listener can read the metadata
func (m *manager) RegisterVoucherResultType(resultType datatransfer.VoucherResult) error {
	err := m.resultTypes.Register(resultType, nil)
	if err != nil {
		return xerrors.Errorf("error registering voucher type: %w", err)
	}
	return nil
}

// RegisterTransportConfigurer registers the given transport configurer to be run on requests with the given voucher
// type
func (m *manager) RegisterTransportConfigurer(voucherType datatransfer.Voucher, configurer datatransfer.TransportConfigurer) error {
	err := m.transportConfigurers.Register(voucherType, configurer)
	if err != nil {
		return xerrors.Errorf("error registering transport configurer: %w", err)
	}
	return nil
}
