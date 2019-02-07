package network

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"time"

	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testselector"
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

func TestMessageSendAndReceive(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

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
	_, err = mn.ConnectPeers(host1.ID(), host2.ID())
	if err != nil {
		t.Fatal("error linking peers")
	}
	gsnet1 := NewFromLibp2pHost(host1,
		testselector.MockDecodeSelectorFunc,
		testselector.MockDecodeSelectionResponseFunc)
	gsnet2 := NewFromLibp2pHost(host2,
		testselector.MockDecodeSelectorFunc,
		testselector.MockDecodeSelectionResponseFunc)
	r := &receiver{
		messageReceived: make(chan struct{}),
	}
	gsnet1.SetDelegate(r)
	gsnet2.SetDelegate(r)

	selector := testselector.GenerateSelector()
	root := testselector.GenerateRootCid()
	selectionResponse := testselector.GenerateSelectionResponse()
	id := gsmsg.GraphSyncRequestID(rand.Int31())
	priority := gsmsg.GraphSyncPriority(rand.Int31())
	status := gsmsg.RequestAcknowledged

	sent := gsmsg.New()
	sent.AddRequest(id, selector, root, priority)
	sent.AddResponse(id, status, selectionResponse)

	gsnet1.SendMessage(ctx, host2.ID(), sent)

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

	sentRequests := sent.Requests()
	if len(sentRequests) != 1 {
		t.Fatal("Did not add request to sent message")
	}
	sentRequest := sentRequests[0]
	receivedRequests := received.Requests()
	if len(receivedRequests) != 1 {
		t.Fatal("Did not add request to received message")
	}
	receivedRequest := receivedRequests[0]
	if receivedRequest.ID() != sentRequest.ID() ||
		receivedRequest.IsCancel() != sentRequest.IsCancel() ||
		receivedRequest.Priority() != sentRequest.Priority() ||
		!reflect.DeepEqual(receivedRequest.Root(), sentRequest.Root()) ||
		!reflect.DeepEqual(receivedRequest.Selector(), sentRequest.Selector()) {
		t.Fatal("Sent message requests did not match received message requests")
	}

	sentResponses := sent.Responses()
	if len(sentResponses) != 1 {
		t.Fatal("Did not add response to sent message")
	}
	sentResponse := sentResponses[0]
	receivedResponses := received.Responses()
	if len(receivedResponses) != 1 {
		t.Fatal("Did not add response to received message")
	}
	receivedResponse := receivedResponses[0]
	if receivedResponse.RequestID() != sentResponse.RequestID() ||
		receivedResponse.Status() != sentResponse.Status() ||
		!reflect.DeepEqual(receivedResponse.Response(), sentResponse.Response()) {
		t.Fatal("Sent message responses did not match received message responses")
	}
}
