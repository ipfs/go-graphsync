package messagequeue

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	allocator2 "github.com/ipfs/go-graphsync/allocator"
	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/testutil"
)

const sendMessageTimeout = 10 * time.Minute
const messageSendRetries = 10

type fakeMessageNetwork struct {
	connectError       error
	messageSenderError error
	messageSender      gsnet.MessageSender
	wait               *sync.WaitGroup
}

func (fmn *fakeMessageNetwork) ConnectTo(context.Context, peer.ID) error {
	return fmn.connectError
}

func (fmn *fakeMessageNetwork) NewMessageSender(context.Context, peer.ID, gsnet.MessageSenderOpts) (gsnet.MessageSender, error) {
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
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, peer, messageNetwork, allocator, messageSendRetries, sendMessageTimeout)
	messageQueue.Startup()
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	root := testutil.GenerateCids(1)[0]

	waitGroup.Add(1)
	messageQueue.AllocateAndBuildMessage(0, func(b *gsmsg.Builder) {
		b.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	}, []notifications.Notifee{})

	testutil.AssertDoesReceive(ctx, t, messagesSent, "message was not sent")

	messageQueue.Shutdown()

	testutil.AssertDoesReceiveFirst(t, fullClosedChan, "message sender should be closed", resetChan, ctx.Done())
}

func TestShutdownDuringMessageSend(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{
		fmt.Errorf("Something went wrong"),
		fullClosedChan,
		resetChan,
		messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, peer, messageNetwork, allocator, messageSendRetries, sendMessageTimeout)
	messageQueue.Startup()
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	root := testutil.GenerateCids(1)[0]

	// setup a message and advance as far as beginning to send it
	waitGroup.Add(1)
	messageQueue.AllocateAndBuildMessage(0, func(b *gsmsg.Builder) {
		b.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	}, []notifications.Notifee{})
	waitGroup.Wait()

	// now shut down
	messageQueue.Shutdown()

	// let the message send attempt complete and fail (as it would if
	// the connection were closed)
	testutil.AssertDoesReceive(ctx, t, messagesSent, "message send not attempted")

	// verify the connection is reset after a failed send attempt
	testutil.AssertDoesReceiveFirst(t, resetChan, "message sender was not reset", fullClosedChan, ctx.Done())

	// now verify after it's reset, no further retries, connection
	// resets, or attempts to close the connection, cause the queue
	// should realize it's shut down and stop processing
	// FIXME: this relies on time passing -- 100 ms to be exact
	// and we should instead mock out time as a dependency
	waitGroup.Add(1)
	testutil.AssertDoesReceiveFirst(t, ctx.Done(), "further message operations should not occur", messagesSent, resetChan, fullClosedChan)
}

func TestProcessingNotification(t *testing.T) {
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
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, peer, messageNetwork, allocator, messageSendRetries, sendMessageTimeout)
	messageQueue.Startup()
	waitGroup.Add(1)
	blks := testutil.GenerateBlocksOfSize(3, 128)

	responseID := graphsync.RequestID(rand.Int31())
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: testutil.RandomBytes(100),
	}
	status := graphsync.RequestCompletedFull
	expectedTopic := "testTopic"
	notifee, verifier := testutil.NewTestNotifee(expectedTopic, 5)
	messageQueue.AllocateAndBuildMessage(0, func(b *gsmsg.Builder) {
		b.AddResponseCode(responseID, status)
		b.AddExtensionData(responseID, extension)
	}, []notifications.Notifee{notifee})

	// wait for send attempt
	waitGroup.Wait()

	var message gsmsg.GraphSyncMessage
	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")
	receivedBlocks := message.Blocks()
	for _, block := range receivedBlocks {
		testutil.AssertContainsBlock(t, blks, block)
	}
	firstResponse := message.Responses()[0]
	extensionData, found := firstResponse.Extension(extensionName)
	require.Equal(t, responseID, firstResponse.RequestID())
	require.Equal(t, status, firstResponse.Status())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	verifier.ExpectEvents(ctx, t, []notifications.Event{
		Event{Name: Queued},
		Event{Name: Sent},
	})
	verifier.ExpectClose(ctx, t)
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
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, peer, messageNetwork, allocator, messageSendRetries, sendMessageTimeout)
	messageQueue.Startup()
	waitGroup.Add(1)
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	root := testutil.GenerateCids(1)[0]

	messageQueue.AllocateAndBuildMessage(0, func(b *gsmsg.Builder) {
		b.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	}, []notifications.Notifee{})
	// wait for send attempt
	waitGroup.Wait()
	id2 := graphsync.RequestID(rand.Int31())
	priority2 := graphsync.Priority(rand.Int31())
	selector2 := ssb.ExploreAll(ssb.Matcher()).Node()
	root2 := testutil.GenerateCids(1)[0]
	id3 := graphsync.RequestID(rand.Int31())
	priority3 := graphsync.Priority(rand.Int31())
	selector3 := ssb.ExploreIndex(0, ssb.Matcher()).Node()
	root3 := testutil.GenerateCids(1)[0]

	messageQueue.AllocateAndBuildMessage(0, func(b *gsmsg.Builder) {
		b.AddRequest(gsmsg.NewRequest(id2, root2, selector2, priority2))
		b.AddRequest(gsmsg.NewRequest(id3, root3, selector3, priority3))
	}, []notifications.Notifee{})

	var message gsmsg.GraphSyncMessage
	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")

	requests := message.Requests()
	require.Len(t, requests, 1, "number of requests in first message was not 1")
	request := requests[0]
	require.Equal(t, id, request.ID())
	require.False(t, request.IsCancel())
	require.Equal(t, priority, request.Priority())
	require.Equal(t, selector, request.Selector())

	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not senf")

	requests = message.Requests()
	require.Len(t, requests, 2, "number of requests in second message was not 2")
	for _, request := range requests {
		if request.ID() == id2 {
			require.False(t, request.IsCancel())
			require.Equal(t, priority2, request.Priority())
			require.Equal(t, selector2, request.Selector())
		} else if request.ID() == id3 {
			require.False(t, request.IsCancel())
			require.Equal(t, priority3, request.Priority())
			require.Equal(t, selector3, request.Selector())
		} else {
			t.Fatal("incorrect request added to message")
		}
	}
}

func TestSendsVeryLargeBlocksResponses(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, peer, messageNetwork, allocator, messageSendRetries, sendMessageTimeout)
	messageQueue.Startup()
	waitGroup.Add(1)

	// generate large blocks before proceeding
	blks := testutil.GenerateBlocksOfSize(5, 1000000)
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[0].RawData())), func(b *gsmsg.Builder) {
		b.AddBlock(blks[0])
	}, []notifications.Notifee{})
	waitGroup.Wait()
	var message gsmsg.GraphSyncMessage
	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")

	msgBlks := message.Blocks()
	require.Len(t, msgBlks, 1, "number of blks in first message was not 1")
	require.True(t, blks[0].Cid().Equals(msgBlks[0].Cid()))

	// Send 3 very large blocks
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[1].RawData())), func(b *gsmsg.Builder) {
		b.AddBlock(blks[1])
	}, []notifications.Notifee{})
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[2].RawData())), func(b *gsmsg.Builder) {
		b.AddBlock(blks[2])
	}, []notifications.Notifee{})
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[3].RawData())), func(b *gsmsg.Builder) {
		b.AddBlock(blks[3])
	}, []notifications.Notifee{})

	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")
	msgBlks = message.Blocks()
	require.Len(t, msgBlks, 1, "number of blks in first message was not 1")
	require.True(t, blks[1].Cid().Equals(msgBlks[0].Cid()))

	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")
	msgBlks = message.Blocks()
	require.Len(t, msgBlks, 1, "number of blks in first message was not 1")
	require.True(t, blks[2].Cid().Equals(msgBlks[0].Cid()))

	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")
	msgBlks = message.Blocks()
	require.Len(t, msgBlks, 1, "number of blks in first message was not 1")
	require.True(t, blks[3].Cid().Equals(msgBlks[0].Cid()))
}

func TestSendsResponsesMemoryPressure(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	p := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}

	// use allocator with very small limit
	allocator := allocator2.NewAllocator(1000, 1000)

	messageQueue := New(ctx, p, messageNetwork, allocator, messageSendRetries, sendMessageTimeout)
	messageQueue.Startup()
	waitGroup.Add(1)

	// start sending block that exceeds memory limit
	blks := testutil.GenerateBlocksOfSize(2, 999)
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[0].RawData())), func(b *gsmsg.Builder) {
		b.AddBlock(blks[0])
	}, []notifications.Notifee{})

	finishes := make(chan string, 2)
	go func() {
		// attempt to send second block. Should block until memory is released
		messageQueue.AllocateAndBuildMessage(uint64(len(blks[1].RawData())), func(b *gsmsg.Builder) {
			b.AddBlock(blks[1])
		}, []notifications.Notifee{})
		finishes <- "sent message"
	}()

	// assert transaction does not complete within 200ms because it is waiting on memory
	ctx2, cancel2 := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel2()
	select {
	case <-finishes:
		t.Fatal("transaction failed to wait on memory")
	case <-ctx2.Done():
	}

	// Allow first message to complete sending
	<-messagesSent

	// assert message is now queued within 200ms
	ctx2, cancel2 = context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel2()
	select {
	case <-finishes:
		cancel2()
	case <-ctx2.Done():
		t.Fatal("timeout waiting for transaction to complete")
	}
}
