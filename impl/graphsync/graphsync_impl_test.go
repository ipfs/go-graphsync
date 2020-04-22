package graphsyncimpl_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/encoding/dagcbor"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	. "github.com/filecoin-project/go-data-transfer/impl/graphsync"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/network"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

type receivedMessage struct {
	message message.DataTransferMessage
	sender  peer.ID
}

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type receiver struct {
	messageReceived chan receivedMessage
}

func (r *receiver) ReceiveRequest(
	ctx context.Context,
	sender peer.ID,
	incoming message.DataTransferRequest) {

	select {
	case <-ctx.Done():
	case r.messageReceived <- receivedMessage{incoming, sender}:
	}
}

func (r *receiver) ReceiveResponse(
	ctx context.Context,
	sender peer.ID,
	incoming message.DataTransferResponse) {

	select {
	case <-ctx.Done():
	case r.messageReceived <- receivedMessage{incoming, sender}:
	}
}

func (r *receiver) ReceiveError(err error) {
}

type fakeDTType struct {
	data string
}

func (ft *fakeDTType) ToBytes() ([]byte, error) {
	return []byte(ft.data), nil
}

func (ft *fakeDTType) FromBytes(data []byte) error {
	ft.data = string(data)
	return nil
}

func (ft *fakeDTType) Type() string {
	return "FakeDTType"
}

func TestDataTransferOneWay(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1
	host2 := gsData.Host2
	// setup receiving peer to just record message coming in
	dtnet2 := network.NewFromLibp2pHost(host2)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	dtnet2.SetDelegate(r)

	gs := gsData.SetupGraphsyncHost1()
	dt := NewGraphSyncDataTransfer(host1, gs, gsData.StoredCounter1)

	t.Run("OpenPushDataTransfer", func(t *testing.T) {
		ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())

		// this is the selector for "get the whole DAG"
		// TODO: support storage deals with custom payload selectors
		stor := ssb.ExploreRecursive(selector.RecursionLimitNone(),
			ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

		voucher := fakeDTType{"applesauce"}
		baseCid := testutil.GenerateCids(1)[0]
		channelID, err := dt.OpenPushDataChannel(ctx, host2.ID(), &voucher, baseCid, stor)
		require.NoError(t, err)
		require.NotNil(t, channelID)
		require.Equal(t, channelID.Initiator, host1.ID())
		require.NoError(t, err)

		var messageReceived receivedMessage
		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case messageReceived = <-r.messageReceived:
		}

		sender := messageReceived.sender
		require.Equal(t, sender, host1.ID())

		received := messageReceived.message
		require.True(t, received.IsRequest())
		receivedRequest, ok := received.(message.DataTransferRequest)
		require.True(t, ok)

		require.Equal(t, receivedRequest.TransferID(), channelID.ID)
		require.Equal(t, receivedRequest.BaseCid(), baseCid)
		require.False(t, receivedRequest.IsCancel())
		require.False(t, receivedRequest.IsPull())
		reader := bytes.NewReader(receivedRequest.Selector())
		receivedSelector, err := dagcbor.Decoder(ipldfree.NodeBuilder(), reader)
		require.NoError(t, err)
		require.Equal(t, receivedSelector, stor)
		receivedVoucher := new(fakeDTType)
		err = receivedVoucher.FromBytes(receivedRequest.Voucher())
		require.NoError(t, err)
		require.Equal(t, *receivedVoucher, voucher)
		require.Equal(t, receivedRequest.VoucherType(), voucher.Type())
	})

	t.Run("OpenPullDataTransfer", func(t *testing.T) {
		ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())

		stor := ssb.ExploreRecursive(selector.RecursionLimitNone(),
			ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

		voucher := fakeDTType{"applesauce"}
		baseCid := testutil.GenerateCids(1)[0]
		channelID, err := dt.OpenPullDataChannel(ctx, host2.ID(), &voucher, baseCid, stor)
		require.NoError(t, err)
		require.NotNil(t, channelID)
		require.Equal(t, channelID.Initiator, host1.ID())
		require.NoError(t, err)

		var messageReceived receivedMessage
		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case messageReceived = <-r.messageReceived:
		}

		sender := messageReceived.sender
		require.Equal(t, sender, host1.ID())

		received := messageReceived.message
		require.True(t, received.IsRequest())
		receivedRequest, ok := received.(message.DataTransferRequest)
		require.True(t, ok)

		require.Equal(t, receivedRequest.TransferID(), channelID.ID)
		require.Equal(t, receivedRequest.BaseCid(), baseCid)
		require.False(t, receivedRequest.IsCancel())
		require.True(t, receivedRequest.IsPull())
		reader := bytes.NewReader(receivedRequest.Selector())
		receivedSelector, err := dagcbor.Decoder(ipldfree.NodeBuilder(), reader)
		require.NoError(t, err)
		require.Equal(t, receivedSelector, stor)
		receivedVoucher := new(fakeDTType)
		err = receivedVoucher.FromBytes(receivedRequest.Voucher())
		require.NoError(t, err)
		require.Equal(t, *receivedVoucher, voucher)
		require.Equal(t, receivedRequest.VoucherType(), voucher.Type())
	})
}

type receivedValidation struct {
	isPull   bool
	other    peer.ID
	voucher  datatransfer.Voucher
	baseCid  cid.Cid
	selector ipld.Node
}

type fakeValidator struct {
	ctx                 context.Context
	validationsReceived chan receivedValidation
}

func (fv *fakeValidator) ValidatePush(
	sender peer.ID,
	voucher datatransfer.Voucher,
	baseCid cid.Cid,
	selector ipld.Node) error {

	select {
	case <-fv.ctx.Done():
	case fv.validationsReceived <- receivedValidation{false, sender, voucher, baseCid, selector}:
	}
	return nil
}

func (fv *fakeValidator) ValidatePull(
	receiver peer.ID,
	voucher datatransfer.Voucher,
	baseCid cid.Cid,
	selector ipld.Node) error {

	select {
	case <-fv.ctx.Done():
	case fv.validationsReceived <- receivedValidation{true, receiver, voucher, baseCid, selector}:
	}
	return nil
}

func TestDataTransferValidation(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1
	host2 := gsData.Host2
	dtnet1 := network.NewFromLibp2pHost(host1)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	dtnet1.SetDelegate(r)

	gs2 := testutil.NewFakeGraphSync()

	fv := &fakeValidator{ctx, make(chan receivedValidation)}

	id := datatransfer.TransferID(rand.Int31())
	var buffer bytes.Buffer
	require.NoError(t, dagcbor.Encoder(gsData.AllSelector, &buffer))

	t.Run("ValidatePush", func(t *testing.T) {
		dt2 := NewGraphSyncDataTransfer(host2, gs2, gsData.StoredCounter2)
		err := dt2.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), fv)
		require.NoError(t, err)
		// create push request
		voucher, baseCid, request := createDTRequest(t, false, id, buffer.Bytes())

		require.NoError(t, dtnet1.SendMessage(ctx, host2.ID(), request))

		var validation receivedValidation
		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case validation = <-fv.validationsReceived:
			assert.False(t, validation.isPull)
		}

		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case _ = <-r.messageReceived:
		}

		assert.False(t, validation.isPull)
		assert.Equal(t, host1.ID(), validation.other)
		assert.Equal(t, &voucher, validation.voucher)
		assert.Equal(t, baseCid, validation.baseCid)
		assert.Equal(t, gsData.AllSelector, validation.selector)
	})

	t.Run("ValidatePull", func(t *testing.T) {
		// create pull request
		voucher, baseCid, request := createDTRequest(t, true, id, buffer.Bytes())
		require.NoError(t, dtnet1.SendMessage(ctx, host2.ID(), request))

		var validation receivedValidation
		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case validation = <-fv.validationsReceived:
		}
		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case _ = <-r.messageReceived:
		}

		assert.True(t, validation.isPull)
		assert.Equal(t, validation.other, host1.ID())
		assert.Equal(t, &voucher, validation.voucher)
		assert.Equal(t, baseCid, validation.baseCid)
		assert.Equal(t, gsData.AllSelector, validation.selector)
	})
}

func createDTRequest(t *testing.T, isPull bool, id datatransfer.TransferID, selectorBytes []byte) (fakeDTType, cid.Cid, message.DataTransferRequest) {
	voucher := fakeDTType{"applesauce"}
	baseCid := testutil.GenerateCids(1)[0]
	voucherBytes, err := voucher.ToBytes()
	require.NoError(t, err)
	request := message.NewRequest(id, isPull, voucher.Type(), voucherBytes, baseCid, selectorBytes)
	return voucher, baseCid, request
}

type stubbedValidator struct {
	didPush    bool
	didPull    bool
	expectPush bool
	expectPull bool
	pushError  error
	pullError  error
}

func newSV() *stubbedValidator {
	return &stubbedValidator{false, false, false, false, nil, nil}
}

func (sv *stubbedValidator) ValidatePush(
	sender peer.ID,
	voucher datatransfer.Voucher,
	baseCid cid.Cid,
	selector ipld.Node) error {
	sv.didPush = true
	return sv.pushError
}

func (sv *stubbedValidator) ValidatePull(
	receiver peer.ID,
	voucher datatransfer.Voucher,
	baseCid cid.Cid,
	selector ipld.Node) error {
	sv.didPull = true
	return sv.pullError
}

func (sv *stubbedValidator) stubErrorPush() {
	sv.pushError = errors.New("something went wrong")
}

func (sv *stubbedValidator) stubSuccessPush() {
	sv.pullError = nil
}

func (sv *stubbedValidator) expectSuccessPush() {
	sv.expectPush = true
	sv.stubSuccessPush()
}

func (sv *stubbedValidator) expectErrorPush() {
	sv.expectPush = true
	sv.stubErrorPush()
}

func (sv *stubbedValidator) stubErrorPull() {
	sv.pullError = errors.New("something went wrong")
}

func (sv *stubbedValidator) stubSuccessPull() {
	sv.pullError = nil
}

func (sv *stubbedValidator) expectSuccessPull() {
	sv.expectPull = true
	sv.stubSuccessPull()
}

func (sv *stubbedValidator) expectErrorPull() {
	sv.expectPull = true
	sv.stubErrorPull()
}

func (sv *stubbedValidator) verifyExpectations(t *testing.T) {
	if sv.expectPush {
		require.True(t, sv.didPush)
	}
	if sv.expectPull {
		require.True(t, sv.didPull)
	}
}

func TestGraphsyncImpl_RegisterVoucherType(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1

	gs1 := testutil.NewFakeGraphSync()
	dt := NewGraphSyncDataTransfer(host1, gs1, gsData.StoredCounter1)
	fv := &fakeValidator{ctx, make(chan receivedValidation)}

	// a voucher type can be registered
	assert.NoError(t, dt.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), fv))

	// it cannot be re-registered
	assert.EqualError(t, dt.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), fv), "voucher type already registered: *graphsyncimpl_test.fakeDTType")

	// it must be registered as a pointer
	assert.EqualError(t, dt.RegisterVoucherType(reflect.TypeOf(fakeDTType{}), fv),
		"voucherType must be a reflect.Ptr Kind")
}

func TestDataTransferSubscribing(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1
	host2 := gsData.Host2

	gs1 := testutil.NewFakeGraphSync()
	gs2 := testutil.NewFakeGraphSync()
	sv := newSV()
	sv.stubErrorPull()
	sv.stubErrorPush()
	dt2 := NewGraphSyncDataTransfer(host2, gs2, gsData.StoredCounter2)
	require.NoError(t, dt2.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), sv))
	voucher := fakeDTType{"applesauce"}
	baseCid := testutil.GenerateCids(1)[0]

	dt1 := NewGraphSyncDataTransfer(host1, gs1, gsData.StoredCounter1)

	subscribe1Calls := make(chan struct{}, 1)
	subscribe1 := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			subscribe1Calls <- struct{}{}
		}
	}
	subscribe2Calls := make(chan struct{}, 1)
	subscribe2 := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			subscribe2Calls <- struct{}{}
		}
	}
	unsub1 := dt1.SubscribeToEvents(subscribe1)
	unsub2 := dt1.SubscribeToEvents(subscribe2)
	_, err := dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, baseCid, gsData.AllSelector)
	require.NoError(t, err)
	select {
	case <-ctx.Done():
		t.Fatal("subscribed events not received")
	case <-subscribe1Calls:
	}
	select {
	case <-ctx.Done():
		t.Fatal("subscribed events not received")
	case <-subscribe2Calls:
	}
	unsub1()
	unsub2()

	subscribe3Calls := make(chan struct{}, 1)
	subscribe3 := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			subscribe3Calls <- struct{}{}
		}
	}
	subscribe4Calls := make(chan struct{}, 1)
	subscribe4 := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			subscribe4Calls <- struct{}{}
		}
	}
	unsub3 := dt1.SubscribeToEvents(subscribe3)
	unsub4 := dt1.SubscribeToEvents(subscribe4)
	_, err = dt1.OpenPullDataChannel(ctx, host2.ID(), &voucher, baseCid, gsData.AllSelector)
	require.NoError(t, err)
	select {
	case <-ctx.Done():
		t.Fatal("subscribed events not received")
	case <-subscribe1Calls:
		t.Fatal("received channel that should have been unsubscribed")
	case <-subscribe2Calls:
		t.Fatal("received channel that should have been unsubscribed")
	case <-subscribe3Calls:
	}
	select {
	case <-ctx.Done():
		t.Fatal("subscribed events not received")
	case <-subscribe1Calls:
		t.Fatal("received channel that should have been unsubscribed")
	case <-subscribe2Calls:
		t.Fatal("received channel that should have been unsubscribed")
	case <-subscribe4Calls:
	}
	unsub3()
	unsub4()
}

func TestDataTransferInitiatingPushGraphsyncRequests(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1
	host2 := gsData.Host2

	gs2 := testutil.NewFakeGraphSync()

	// setup receiving peer to just record message coming in
	dtnet1 := network.NewFromLibp2pHost(host1)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	dtnet1.SetDelegate(r)

	id := datatransfer.TransferID(rand.Int31())
	var buffer bytes.Buffer

	err := dagcbor.Encoder(gsData.AllSelector, &buffer)
	require.NoError(t, err)

	_, baseCid, request := createDTRequest(t, false, id, buffer.Bytes())

	t.Run("with successful validation", func(t *testing.T) {
		sv := newSV()
		sv.expectSuccessPush()

		dt2 := NewGraphSyncDataTransfer(host2, gs2, gsData.StoredCounter2)
		require.NoError(t, dt2.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), sv))

		require.NoError(t, dtnet1.SendMessage(ctx, host2.ID(), request))
		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case <-r.messageReceived:
		}
		sv.verifyExpectations(t)

		requestReceived := gs2.AssertRequestReceived(ctx, t)

		sv.verifyExpectations(t)

		receiver := requestReceived.P
		require.Equal(t, receiver, host1.ID())

		cl, ok := requestReceived.Root.(cidlink.Link)
		require.True(t, ok)
		require.Equal(t, baseCid, cl.Cid)

		require.Equal(t, gsData.AllSelector, requestReceived.Selector)

	})

	t.Run("with error validation", func(t *testing.T) {
		sv := newSV()
		sv.expectErrorPush()

		dt2 := NewGraphSyncDataTransfer(host2, gs2, gsData.StoredCounter2)
		require.NoError(t, dt2.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), sv))

		require.NoError(t, dtnet1.SendMessage(ctx, host2.ID(), request))
		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case <-r.messageReceived:
		}
		sv.verifyExpectations(t)

		// no graphsync request should be scheduled
		gs2.AssertNoRequestReceived(t)

	})

}

func TestDataTransferInitiatingPullGraphsyncRequests(t *testing.T) {
	ctx := context.Background()
	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1 // initiates the pull request
	host2 := gsData.Host2 // sends the data

	voucher := fakeDTType{"applesauce"}
	baseCid := testutil.GenerateCids(1)[0]

	t.Run("with successful validation", func(t *testing.T) {
		gs1Init := testutil.NewFakeGraphSync()
		gs2Sender := testutil.NewFakeGraphSync()

		sv := newSV()
		sv.expectSuccessPull()

		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		dtInit := NewGraphSyncDataTransfer(host1, gs1Init, gsData.StoredCounter1)
		dtSender := NewGraphSyncDataTransfer(host2, gs2Sender, gsData.StoredCounter2)
		err := dtSender.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), sv)
		require.NoError(t, err)

		_, err = dtInit.OpenPullDataChannel(ctx, host2.ID(), &voucher, baseCid, gsData.AllSelector)
		require.NoError(t, err)

		requestReceived := gs1Init.AssertRequestReceived(ctx, t)
		sv.verifyExpectations(t)

		receiver := requestReceived.P
		require.Equal(t, receiver, host2.ID())

		cl, ok := requestReceived.Root.(cidlink.Link)
		require.True(t, ok)
		require.Equal(t, baseCid.String(), cl.Cid.String())

		require.Equal(t, gsData.AllSelector, requestReceived.Selector)
	})

	t.Run("with error validation", func(t *testing.T) {
		gs1 := testutil.NewFakeGraphSync()
		gs2 := testutil.NewFakeGraphSync()

		dt1 := NewGraphSyncDataTransfer(host1, gs1, gsData.StoredCounter1)
		sv := newSV()
		sv.expectErrorPull()

		dt2 := NewGraphSyncDataTransfer(host2, gs2, gsData.StoredCounter2)
		err := dt2.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), sv)
		require.NoError(t, err)

		subscribeCalls := make(chan struct{}, 1)
		subscribe := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
			if event.Code == datatransfer.Error {
				subscribeCalls <- struct{}{}
			}
		}
		unsub := dt1.SubscribeToEvents(subscribe)
		_, err = dt1.OpenPullDataChannel(ctx, host2.ID(), &voucher, baseCid, gsData.AllSelector)
		require.NoError(t, err)

		select {
		case <-ctx.Done():
			t.Fatal("subscribed events not received")
		case <-subscribeCalls:
		}

		sv.verifyExpectations(t)

		// no graphsync request should be scheduled
		gs1.AssertNoRequestReceived(t)
		unsub()
	})

	t.Run("does not schedule graphsync request if is push request", func(t *testing.T) {
		gs1 := testutil.NewFakeGraphSync()
		gs2 := testutil.NewFakeGraphSync()

		sv := newSV()
		sv.expectSuccessPush()

		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		dt1 := NewGraphSyncDataTransfer(host1, gs1, gsData.StoredCounter1)
		dt2 := NewGraphSyncDataTransfer(host2, gs2, gsData.StoredCounter2)
		err := dt2.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), sv)
		require.NoError(t, err)

		subscribeCalls := make(chan struct{}, 1)
		subscribe := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
			if event.Code == datatransfer.Error {
				subscribeCalls <- struct{}{}
			}
		}
		unsub := dt1.SubscribeToEvents(subscribe)
		_, err = dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, baseCid, gsData.AllSelector)
		require.NoError(t, err)

		select {
		case <-ctx.Done():
			t.Fatal("subscribed events not received")
		case <-subscribeCalls:
		}
		sv.verifyExpectations(t)

		// no graphsync request should be scheduled
		gs1.AssertNoRequestReceived(t)
		unsub()
	})
}

type receivedGraphSyncMessage struct {
	message gsmsg.GraphSyncMessage
	p       peer.ID
}

type fakeGraphSyncReceiver struct {
	receivedMessages chan receivedGraphSyncMessage
}

func (fgsr *fakeGraphSyncReceiver) ReceiveMessage(ctx context.Context, sender peer.ID, incoming gsmsg.GraphSyncMessage) {
	select {
	case <-ctx.Done():
	case fgsr.receivedMessages <- receivedGraphSyncMessage{incoming, sender}:
	}
}

func (fgsr *fakeGraphSyncReceiver) ReceiveError(_ error) {
}
func (fgsr *fakeGraphSyncReceiver) Connected(p peer.ID) {
}
func (fgsr *fakeGraphSyncReceiver) Disconnected(p peer.ID) {
}

func (fgsr *fakeGraphSyncReceiver) consumeResponses(ctx context.Context, t *testing.T) graphsync.ResponseStatusCode {
	var gsMessageReceived receivedGraphSyncMessage
	for {
		select {
		case <-ctx.Done():
			t.Fail()
		case gsMessageReceived = <-fgsr.receivedMessages:
			responses := gsMessageReceived.message.Responses()
			if (len(responses) > 0) && gsmsg.IsTerminalResponseCode(responses[0].Status()) {
				return responses[0].Status()
			}
		}
	}
}

func TestRespondingToPushGraphsyncRequests(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1 // initiator and data sender
	host2 := gsData.Host2 // data recipient, makes graphsync request for data
	voucher := fakeDTType{"applesauce"}
	link := gsData.LoadUnixFSFile(t, false)

	// setup receiving peer to just record message coming in
	dtnet2 := network.NewFromLibp2pHost(host2)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	dtnet2.SetDelegate(r)

	gsr := &fakeGraphSyncReceiver{
		receivedMessages: make(chan receivedGraphSyncMessage),
	}
	gsData.GsNet2.SetDelegate(gsr)

	gs1 := gsData.SetupGraphsyncHost1()
	dt1 := NewGraphSyncDataTransfer(host1, gs1, gsData.StoredCounter1)

	t.Run("when request is initiated", func(t *testing.T) {
		_, err := dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, link.(cidlink.Link).Cid, gsData.AllSelector)
		require.NoError(t, err)

		var messageReceived receivedMessage
		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case messageReceived = <-r.messageReceived:
		}
		requestReceived := messageReceived.message.(message.DataTransferRequest)

		var buf bytes.Buffer
		extStruct := &ExtensionDataTransferData{TransferID: uint64(requestReceived.TransferID()), Initiator: host1.ID()}
		err = extStruct.MarshalCBOR(&buf)
		require.NoError(t, err)
		extData := buf.Bytes()

		request := gsmsg.NewRequest(graphsync.RequestID(rand.Int31()), link.(cidlink.Link).Cid, gsData.AllSelector, graphsync.Priority(rand.Int31()), graphsync.ExtensionData{
			Name: ExtensionDataTransfer,
			Data: extData,
		})
		gsmessage := gsmsg.New()
		gsmessage.AddRequest(request)
		require.NoError(t, gsData.GsNet2.SendMessage(ctx, host1.ID(), gsmessage))

		status := gsr.consumeResponses(ctx, t)
		require.False(t, gsmsg.IsTerminalFailureCode(status))
	})

	t.Run("when no request is initiated", func(t *testing.T) {
		var buf bytes.Buffer
		extStruct := &ExtensionDataTransferData{TransferID: rand.Uint64(), Initiator: host1.ID()}
		err := extStruct.MarshalCBOR(&buf)
		require.NoError(t, err)
		extData := buf.Bytes()

		request := gsmsg.NewRequest(graphsync.RequestID(rand.Int31()), link.(cidlink.Link).Cid, gsData.AllSelector, graphsync.Priority(rand.Int31()), graphsync.ExtensionData{
			Name: ExtensionDataTransfer,
			Data: extData,
		})
		gsmessage := gsmsg.New()
		gsmessage.AddRequest(request)
		require.NoError(t, gsData.GsNet2.SendMessage(ctx, host1.ID(), gsmessage))

		status := gsr.consumeResponses(ctx, t)
		require.True(t, gsmsg.IsTerminalFailureCode(status))
	})
}

func TestResponseHookWhenExtensionNotFound(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1 // initiator and data sender
	host2 := gsData.Host2 // data recipient, makes graphsync request for data
	voucher := fakeDTType{"applesauce"}
	link := gsData.LoadUnixFSFile(t, false)

	// setup receiving peer to just record message coming in
	dtnet2 := network.NewFromLibp2pHost(host2)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	dtnet2.SetDelegate(r)

	gsr := &fakeGraphSyncReceiver{
		receivedMessages: make(chan receivedGraphSyncMessage),
	}
	gsData.GsNet2.SetDelegate(gsr)

	gs1 := gsData.SetupGraphsyncHost1()
	dt1 := NewGraphSyncDataTransfer(host1, gs1, gsData.StoredCounter1)

	t.Run("when it's not our extension, does not error and does not validate", func(t *testing.T) {
		//register a hook that validates the request so we don't fail in gs because the request
		//never gets processed
		validateHook := func(p peer.ID, req graphsync.RequestData, ha graphsync.IncomingRequestHookActions) {
			ha.ValidateRequest()
		}
		gs1.RegisterIncomingRequestHook(validateHook)

		_, err := dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, link.(cidlink.Link).Cid, gsData.AllSelector)
		require.NoError(t, err)

		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case <-r.messageReceived:
		}

		request := gsmsg.NewRequest(graphsync.RequestID(rand.Int31()), link.(cidlink.Link).Cid, gsData.AllSelector, graphsync.Priority(rand.Int31()))
		gsmessage := gsmsg.New()
		gsmessage.AddRequest(request)
		require.NoError(t, gsData.GsNet2.SendMessage(ctx, host1.ID(), gsmessage))

		status := gsr.consumeResponses(ctx, t)
		assert.False(t, gsmsg.IsTerminalFailureCode(status))
	})
}

func TestRespondingToPullGraphsyncRequests(t *testing.T) {
	//create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1 // initiator, and recipient, makes graphync request
	host2 := gsData.Host2 // data sender

	// setup receiving peer to just record message coming in
	dtnet1 := network.NewFromLibp2pHost(host1)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	dtnet1.SetDelegate(r)

	gsr := &fakeGraphSyncReceiver{
		receivedMessages: make(chan receivedGraphSyncMessage),
	}
	gsData.GsNet1.SetDelegate(gsr)

	gs2 := gsData.SetupGraphsyncHost2()

	link := gsData.LoadUnixFSFile(t, true)

	id := datatransfer.TransferID(rand.Int31())
	var buf bytes.Buffer
	err := dagcbor.Encoder(gsData.AllSelector, &buf)
	require.NoError(t, err)
	selectorBytes := buf.Bytes()

	t.Run("When a pull request is initiated and validated", func(t *testing.T) {
		sv := newSV()
		sv.expectSuccessPull()

		dt1 := NewGraphSyncDataTransfer(host2, gs2, gsData.StoredCounter2)
		require.NoError(t, dt1.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), sv))

		_, _, request := createDTRequest(t, true, id, selectorBytes)
		require.NoError(t, dtnet1.SendMessage(ctx, host2.ID(), request))
		var messageReceived receivedMessage
		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case messageReceived = <-r.messageReceived:
		}
		sv.verifyExpectations(t)
		receivedResponse, ok := messageReceived.message.(message.DataTransferResponse)
		require.True(t, ok)
		require.True(t, receivedResponse.Accepted())
		extStruct := &ExtensionDataTransferData{
			TransferID: uint64(receivedResponse.TransferID()),
			Initiator:  host1.ID(),
			IsPull:     true,
		}

		var buf2 = bytes.Buffer{}
		err = extStruct.MarshalCBOR(&buf2)
		require.NoError(t, err)
		extData := buf2.Bytes()

		gsRequest := gsmsg.NewRequest(graphsync.RequestID(rand.Int31()), link.(cidlink.Link).Cid, gsData.AllSelector, graphsync.Priority(rand.Int31()), graphsync.ExtensionData{
			Name: ExtensionDataTransfer,
			Data: extData,
		})

		// initiator requests data over graphsync network
		gsmessage := gsmsg.New()
		gsmessage.AddRequest(gsRequest)
		require.NoError(t, gsData.GsNet1.SendMessage(ctx, host2.ID(), gsmessage))
		status := gsr.consumeResponses(ctx, t)
		require.False(t, gsmsg.IsTerminalFailureCode(status))
	})

	t.Run("When request is not initiated, graphsync response is error", func(t *testing.T) {
		_ = NewGraphSyncDataTransfer(host2, gs2, gsData.StoredCounter2)
		extStruct := &ExtensionDataTransferData{TransferID: rand.Uint64(), Initiator: host1.ID()}

		var buf2 bytes.Buffer
		err = extStruct.MarshalCBOR(&buf2)
		require.NoError(t, err)
		extData := buf2.Bytes()
		request := gsmsg.NewRequest(graphsync.RequestID(rand.Int31()), link.(cidlink.Link).Cid, gsData.AllSelector, graphsync.Priority(rand.Int31()), graphsync.ExtensionData{
			Name: ExtensionDataTransfer,
			Data: extData,
		})
		gsmessage := gsmsg.New()
		gsmessage.AddRequest(request)

		// non-initiator requests data over graphsync network, but should not get it
		// because there was no previous request
		require.NoError(t, gsData.GsNet1.SendMessage(ctx, host2.ID(), gsmessage))
		status := gsr.consumeResponses(ctx, t)
		require.True(t, gsmsg.IsTerminalFailureCode(status))
	})
}

func TestDataTransferPushRoundTrip(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1 // initiator, data sender
	host2 := gsData.Host2 // data recipient

	root := gsData.LoadUnixFSFile(t, false)
	rootCid := root.(cidlink.Link).Cid
	gs1 := gsData.SetupGraphsyncHost1()
	gs2 := gsData.SetupGraphsyncHost2()

	dt1 := NewGraphSyncDataTransfer(host1, gs1, gsData.StoredCounter1)
	dt2 := NewGraphSyncDataTransfer(host2, gs2, gsData.StoredCounter2)

	finished := make(chan struct{}, 2)
	var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Complete {
			finished <- struct{}{}
		}
	}
	dt1.SubscribeToEvents(subscriber)
	dt2.SubscribeToEvents(subscriber)
	voucher := fakeDTType{"applesauce"}
	sv := newSV()
	sv.expectSuccessPull()
	require.NoError(t, dt2.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), sv))

	chid, err := dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, rootCid, gsData.AllSelector)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("Did not complete succcessful data transfer")
		case <-finished:
		}
	}
	gsData.VerifyFileTransferred(t, root, true)
	assert.Equal(t, chid.Initiator, host1.ID())
}

func TestDataTransferPullRoundTrip(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	gsData := testutil.NewGraphsyncTestingData(ctx, t)
	host1 := gsData.Host1
	host2 := gsData.Host2

	root := gsData.LoadUnixFSFile(t, false)
	rootCid := root.(cidlink.Link).Cid
	gs1 := gsData.SetupGraphsyncHost1()
	gs2 := gsData.SetupGraphsyncHost2()

	dt1 := NewGraphSyncDataTransfer(host1, gs1, gsData.StoredCounter1)
	dt2 := NewGraphSyncDataTransfer(host2, gs2, gsData.StoredCounter2)

	finished := make(chan struct{}, 2)
	var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Complete {
			finished <- struct{}{}
		}
	}
	dt1.SubscribeToEvents(subscriber)
	dt2.SubscribeToEvents(subscriber)
	voucher := fakeDTType{"applesauce"}
	sv := newSV()
	sv.expectSuccessPull()
	require.NoError(t, dt1.RegisterVoucherType(reflect.TypeOf(&fakeDTType{}), sv))

	_, err := dt2.OpenPullDataChannel(ctx, host1.ID(), &voucher, rootCid, gsData.AllSelector)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("Did not complete succcessful data transfer")
		case <-finished:
		}
	}
	gsData.VerifyFileTransferred(t, root, true)
}
