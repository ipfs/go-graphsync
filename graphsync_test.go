package graphsync

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type receiver struct {
	messageReceived chan struct{}
	lastMessage     gsmsg.GraphSyncMessage
	lastSender      peer.ID
}

func (r *receiver) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming gsmsg.GraphSyncMessage) {
	r.lastSender = sender
	r.lastMessage = incoming
	select {
	case <-ctx.Done():
	case r.messageReceived <- struct{}{}:
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
		messageReceived: make(chan struct{}),
	}
	gsnet2.SetDelegate(r)

	loader := func(ipldLink ipld.Link, lnkCtx ipldbridge.LinkContext) (io.Reader, error) {
		return nil, fmt.Errorf("unable to load block")
	}
	bridge := testbridge.NewMockIPLDBridge()
	graphSync := New(ctx, gsnet1, bridge, loader)

	cids := testutil.GenerateCids(5)
	spec := testbridge.NewMockSelectorSpec(cids)
	requestCtx, requestCancel := context.WithCancel(ctx)
	defer requestCancel()
	graphSync.Request(requestCtx, host2.ID(), spec)

	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case <-r.messageReceived:
	}

	sender := r.lastSender
	if sender != host1.ID() {
		t.Fatal("received message from wrong node")
	}

	received := r.lastMessage
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
