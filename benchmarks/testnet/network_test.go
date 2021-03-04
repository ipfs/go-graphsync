package testnet_test

import (
	"context"
	"sync"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/libp2p/go-libp2p-core/peer"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/benchmarks/testnet"
	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
)

func TestSendMessageAsyncButWaitForResponse(t *testing.T) {
	net := testnet.VirtualNetwork(delay.Fixed(0))
	responderPeer := tnet.RandIdentityOrFatal(t)
	waiter := net.Adapter(tnet.RandIdentityOrFatal(t))
	responder := net.Adapter(responderPeer)

	var wg sync.WaitGroup

	wg.Add(1)

	expectedStr := "received async"

	responder.SetDelegate(lambda(func(
		ctx context.Context,
		fromWaiter peer.ID,
		msgFromWaiter gsmsg.GraphSyncMessage) {

		builder := gsmsg.NewBuilder(gsmsg.Topic(0))
		builder.AddBlock(blocks.NewBlock([]byte(expectedStr)))
		msgToWaiter, err := builder.Build()
		require.NoError(t, err)
		err = waiter.SendMessage(ctx, fromWaiter, msgToWaiter)
		if err != nil {
			t.Error(err)
		}
	}))

	waiter.SetDelegate(lambda(func(
		ctx context.Context,
		fromResponder peer.ID,
		msgFromResponder gsmsg.GraphSyncMessage) {

		// TODO assert that this came from the correct peer and that the message contents are as expected
		ok := false
		for _, b := range msgFromResponder.Blocks() {
			if string(b.RawData()) == expectedStr {
				wg.Done()
				ok = true
			}
		}

		if !ok {
			t.Fatal("Message not received from the responder")
		}
	}))

	builder := gsmsg.NewBuilder(gsmsg.Topic(0))
	builder.AddBlock(blocks.NewBlock([]byte("data")))
	messageSentAsync, err := builder.Build()
	require.NoError(t, err)
	errSending := waiter.SendMessage(
		context.Background(), responderPeer.ID(), messageSentAsync)
	if errSending != nil {
		t.Fatal(errSending)
	}

	wg.Wait() // until waiter delegate function is executed
}

type receiverFunc func(ctx context.Context, p peer.ID,
	incoming gsmsg.GraphSyncMessage)

// lambda returns a Receiver instance given a receiver function
func lambda(f receiverFunc) gsnet.Receiver {
	return &lambdaImpl{
		f: f,
	}
}

type lambdaImpl struct {
	f func(ctx context.Context, p peer.ID, incoming gsmsg.GraphSyncMessage)
}

func (lam *lambdaImpl) ReceiveMessage(ctx context.Context,
	p peer.ID, incoming gsmsg.GraphSyncMessage) {
	lam.f(ctx, p, incoming)
}

func (lam *lambdaImpl) ReceiveError(_ peer.ID, _ error) {
	// TODO log error
}

func (lam *lambdaImpl) Connected(p peer.ID) {
	// TODO
}
func (lam *lambdaImpl) Disconnected(peer.ID) {
	// TODO
}
