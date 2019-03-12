package messagequeue

import (
	"context"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync/testutil"

	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"

	peer "github.com/libp2p/go-libp2p-peer"
)

type fakeMessageNetwork struct {
	connectError       error
	messageSenderError error
	messageSender      gsnet.MessageSender
	wait               *sync.WaitGroup
}

func (fmn *fakeMessageNetwork) ConnectTo(context.Context, peer.ID) error {
	return fmn.connectError
}

func (fmn *fakeMessageNetwork) NewMessageSender(context.Context, peer.ID) (gsnet.MessageSender, error) {
	fmn.wait.Done()
	if fmn.messageSenderError == nil {
		return fmn.messageSender, nil
	}
	return nil, fmn.messageSenderError
}

type fakeMessageSender struct {
	sendError    error
	fullClosed   chan<- struct{}
	reset        chan<- struct{}
	messagesSent chan<- gsmsg.GraphSyncMessage
}

func (fms *fakeMessageSender) SendMsg(ctx context.Context, msg gsmsg.GraphSyncMessage) error {
	fms.messagesSent <- msg
	return fms.sendError
}
func (fms *fakeMessageSender) Close() error { fms.fullClosed <- struct{}{}; return nil }
func (fms *fakeMessageSender) Reset() error { fms.reset <- struct{}{}; return nil }

func TestStartupAndShutdown(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}

	messageQueue := New(ctx, peer, messageNetwork)
	messageQueue.Startup()
	id := gsmsg.GraphSyncRequestID(rand.Int31())
	priority := gsmsg.GraphSyncPriority(rand.Int31())
	selector := testutil.RandomBytes(100)

	waitGroup.Add(1)
	messageQueue.AddRequest(id, selector, priority)

	select {
	case <-ctx.Done():
		t.Fatal("message was not sent")
	case <-messagesSent:
	}

	messageQueue.Shutdown()

	select {
	case <-resetChan:
		t.Fatal("message sender should have been closed but was reset")
	case <-fullClosedChan:
	case <-ctx.Done():
		t.Fatal("message sender should have been closed but wasn't")
	}
}

func TestDedupingMessages(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}

	messageQueue := New(ctx, peer, messageNetwork)
	messageQueue.Startup()
	waitGroup.Add(1)
	id := gsmsg.GraphSyncRequestID(rand.Int31())
	priority := gsmsg.GraphSyncPriority(rand.Int31())
	selector := testutil.RandomBytes(100)

	messageQueue.AddRequest(id, selector, priority)
	// wait for send attempt
	waitGroup.Wait()
	id2 := gsmsg.GraphSyncRequestID(rand.Int31())
	priority2 := gsmsg.GraphSyncPriority(rand.Int31())
	selector2 := testutil.RandomBytes(100)
	id3 := gsmsg.GraphSyncRequestID(rand.Int31())
	priority3 := gsmsg.GraphSyncPriority(rand.Int31())
	selector3 := testutil.RandomBytes(100)
	messageQueue.AddRequest(id2, selector2, priority2)
	messageQueue.AddRequest(id3, selector3, priority3)

	select {
	case <-ctx.Done():
		t.Fatal("no messages were sent")
	case message := <-messagesSent:
		requests := message.Requests()
		if len(requests) != 1 {
			t.Fatal("Incorrect number of requests in first message")
		}
		request := requests[0]
		if request.ID() != id ||
			request.IsCancel() != false ||
			request.Priority() != priority ||
			!reflect.DeepEqual(request.Selector(), selector) {
			t.Fatal("Did not properly add request to message")
		}
	}
	select {
	case <-ctx.Done():
		t.Fatal("no messages were sent")
	case message := <-messagesSent:
		requests := message.Requests()
		if len(requests) != 2 {
			t.Fatal("Incorrect number of requests in second message")
		}
		for _, request := range requests {
			if request.ID() == id2 {
				if request.IsCancel() != false ||
					request.Priority() != priority2 ||
					!reflect.DeepEqual(request.Selector(), selector2) {
					t.Fatal("Did not properly add request to message")
				}
			} else if request.ID() == id3 {
				if request.IsCancel() != false ||
					request.Priority() != priority3 ||
					!reflect.DeepEqual(request.Selector(), selector3) {
					t.Fatal("Did not properly add request to message")
				}
			} else {
				t.Fatal("incorrect request added to message")
			}
		}
	}
}
