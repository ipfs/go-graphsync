package graphsync

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"

	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

type receivedMessage struct {
	message gsmsg.GraphSyncMessage
	sender  peer.ID
}

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type receiver struct {
	messageReceived chan receivedMessage
}

func (r *receiver) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming gsmsg.GraphSyncMessage) {

	select {
	case <-ctx.Done():
	case r.messageReceived <- receivedMessage{incoming, sender}:
	}
}

func (r *receiver) ReceiveError(err error) {
}

func TestMakeRequestToNetwork(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	// setup network
	host1, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	host2, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	err = mn.LinkAll()
	if err != nil {
		t.Fatal("error linking hosts")
	}

	gsnet1 := gsnet.NewFromLibp2pHost(host1)

	// setup receiving peer to just record message coming in
	gsnet2 := gsnet.NewFromLibp2pHost(host2)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	gsnet2.SetDelegate(r)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testbridge.NewMockStore(blockStore)
	bridge := testbridge.NewMockIPLDBridge()
	graphSync := New(ctx, gsnet1, bridge, loader, storer)

	cids := testutil.GenerateCids(5)
	spec := testbridge.NewMockSelectorSpec(cids)
	requestCtx, requestCancel := context.WithCancel(ctx)
	defer requestCancel()
	graphSync.Request(requestCtx, host2.ID(), spec)

	var message receivedMessage
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case message = <-r.messageReceived:
	}

	sender := message.sender
	if sender != host1.ID() {
		t.Fatal("received message from wrong node")
	}

	received := message.message
	receivedRequests := received.Requests()
	if len(receivedRequests) != 1 {
		t.Fatal("Did not add request to received message")
	}
	receivedRequest := receivedRequests[0]
	receivedSpec, err := bridge.DecodeNode(receivedRequest.Selector())
	if err != nil {
		t.Fatal("unable to decode transmitted selector")
	}
	if !reflect.DeepEqual(spec, receivedSpec) {
		t.Fatal("did not transmit selector spec correctly")
	}
}

func TestSendResponseToIncomingRequest(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	// setup network
	host1, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	host2, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	err = mn.LinkAll()
	if err != nil {
		t.Fatal("error linking hosts")
	}

	gsnet1 := gsnet.NewFromLibp2pHost(host1)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	gsnet1.SetDelegate(r)

	// setup receiving peer to just record message coming in
	gsnet2 := gsnet.NewFromLibp2pHost(host2)

	blks := testutil.GenerateBlocksOfSize(5, 100)

	blockStore := make(map[ipld.Link][]byte)
	for _, block := range blks {
		blockStore[cidlink.Link{Cid: block.Cid()}] = block.RawData()
	}
	loader, storer := testbridge.NewMockStore(blockStore)
	bridge := testbridge.NewMockIPLDBridge()

	// initialize graphsync on second node to response to requests
	New(ctx, gsnet2, bridge, loader, storer)

	cids := make([]cid.Cid, 0, 7)
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	// append block that should be deduped
	cids = append(cids, blks[0].Cid())

	unknownCid := testutil.GenerateCids(1)[0]
	cids = append(cids, unknownCid)

	spec := testbridge.NewMockSelectorSpec(cids)
	selectorData, err := bridge.EncodeNode(spec)
	if err != nil {
		t.Fatal("could not encode selector spec")
	}
	requestID := gsmsg.GraphSyncRequestID(rand.Int31())

	message := gsmsg.New()
	message.AddRequest(gsmsg.NewRequest(requestID, selectorData, gsmsg.GraphSyncPriority(math.MaxInt32)))
	// send request across network
	gsnet1.SendMessage(ctx, host2.ID(), message)
	// read the values sent back to requestor
	var received gsmsg.GraphSyncMessage
	var receivedBlocks []blocks.Block
readAllMessages:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("did not receive complete response")
		case message := <-r.messageReceived:
			sender := message.sender
			if sender != host2.ID() {
				t.Fatal("received message from wrong node")
			}

			received = message.message
			receivedBlocks = append(receivedBlocks, received.Blocks()...)
			receivedResponses := received.Responses()
			if len(receivedResponses) != 1 {
				t.Fatal("Did not receive response")
			}
			if receivedResponses[0].RequestID() != requestID {
				t.Fatal("Sent response for incorrect request id")
			}
			if receivedResponses[0].Status() != gsmsg.PartialResponse {
				break readAllMessages
			}
		}
	}

	if len(receivedBlocks) != len(blks) {
		t.Fatal("Send incorrect number of blocks or there were duplicate blocks")
	}

	// there should have been a missing CID
	if received.Responses()[0].Status() != gsmsg.RequestCompletedPartial {
		t.Fatal("transmitted full response when only partial was transmitted")
	}
}

func TestGraphsyncRoundTrip(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	// setup network
	host1, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	host2, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	err = mn.LinkAll()
	if err != nil {
		t.Fatal("error linking hosts")
	}

	gsnet1 := gsnet.NewFromLibp2pHost(host1)

	blockStore1 := make(map[ipld.Link][]byte)
	loader1, storer1 := testbridge.NewMockStore(blockStore1)
	bridge1 := testbridge.NewMockIPLDBridge()

	// initialize graphsync on second node to response to requests
	requestor := New(ctx, gsnet1, bridge1, loader1, storer1)

	// setup receiving peer to just record message coming in
	gsnet2 := gsnet.NewFromLibp2pHost(host2)

	blks := testutil.GenerateBlocksOfSize(5, 100)

	blockStore2 := make(map[ipld.Link][]byte)
	for _, block := range blks {
		blockStore2[cidlink.Link{Cid: block.Cid()}] = block.RawData()
	}
	loader2, storer2 := testbridge.NewMockStore(blockStore2)
	bridge2 := testbridge.NewMockIPLDBridge()

	// initialize graphsync on second node to response to requests
	New(ctx, gsnet2, bridge2, loader2, storer2)

	cids := make([]cid.Cid, 0, 7)
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	// append block that should be deduped
	cids = append(cids, blks[0].Cid())

	unknownCid := testutil.GenerateCids(1)[0]
	cids = append(cids, unknownCid)

	spec := testbridge.NewMockSelectorSpec(cids)

	progressChan, errChan := requestor.Request(ctx, host2.ID(), spec)

	responses := testutil.CollectResponses(ctx, t, progressChan)
	errs := testutil.CollectErrors(ctx, t, errChan)

	expectedErr := fmt.Sprintf("Remote Peer Is Missing Block: %s", unknownCid.String())
	if len(errs) != 1 || errs[0].Error() != expectedErr {
		t.Fatal("did not transmit error for missing CID")
	}

	if len(responses) != 6 {
		t.Fatal("did not traverse all nodes")
	}
	for i, response := range responses {
		k := response.LastBlock.Link.(cidlink.Link).Cid
		var expectedCid cid.Cid
		if i == 5 {
			expectedCid = blks[0].Cid()
		} else {
			expectedCid = blks[i].Cid()
		}
		if k != expectedCid {
			t.Fatal("did not send the correct cids in order")
		}
	}

	// verify data was stored in blockstore
	if len(blockStore1) != 5 {
		t.Fatal("did not store all blocks")
	}
	for link, data := range blockStore1 {
		block, err := blocks.NewBlockWithCid(data, link.(cidlink.Link).Cid)
		if err != nil || !testutil.ContainsBlock(blks, block) {
			t.Fatal("Stored wrong block")
		}
	}
}
