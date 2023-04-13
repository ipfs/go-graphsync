package messagequeue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	allocator2 "github.com/ipfs/go-graphsync/allocator"
	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestStartupAndShutdown(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	targetPeer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, targetPeer, messageNetwork, allocator, messageSendRetries, sendMessageTimeout, func(peer.ID) {})
	messageQueue.Startup()
	id := graphsync.NewRequestID()
	priority := graphsync.Priority(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	root := testutil.GenerateCids(1)[0]

	waitGroup.Add(1)
	messageQueue.AllocateAndBuildMessage(0, func(b *Builder) {
		b.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	})

	testutil.AssertDoesReceive(ctx, t, messagesSent, "message was not sent")

	messageQueue.Shutdown()

	testutil.AssertDoesReceiveFirst(t, fullClosedChan, "message sender should be closed", resetChan, ctx.Done())
}

func TestShutdownDuringMessageSend(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	targetPeer := testutil.GeneratePeers(1)[0]
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

	messageQueue := New(ctx, targetPeer, messageNetwork, allocator, messageSendRetries, sendMessageTimeout, func(peer.ID) {})
	messageQueue.Startup()
	id := graphsync.NewRequestID()
	priority := graphsync.Priority(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	root := testutil.GenerateCids(1)[0]

	// setup a message and advance as far as beginning to send it
	waitGroup.Add(1)
	messageQueue.AllocateAndBuildMessage(0, func(b *Builder) {
		b.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	})
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

	targetPeer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, targetPeer, messageNetwork, allocator, messageSendRetries, sendMessageTimeout, func(peer.ID) {})
	messageQueue.Startup()
	waitGroup.Add(1)
	blks := testutil.GenerateBlocksOfSize(3, 128)

	responseID := graphsync.NewRequestID()
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: basicnode.NewBytes(testutil.RandomBytes(100)),
	}
	status := graphsync.RequestCompletedFull
	blkData := testutil.NewFakeBlockData()
	subscriber := testutil.NewTestSubscriber(5)
	messageQueue.AllocateAndBuildMessage(0, func(b *Builder) {
		b.AddResponseCode(responseID, status)
		b.AddExtensionData(responseID, extension)
		b.AddBlockData(responseID, blkData)
		b.SetSubscriber(responseID, subscriber)
	})

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

	expectedMetadata := Metadata{
		ResponseCodes: map[graphsync.RequestID]graphsync.ResponseStatusCode{
			responseID: status,
		},
		BlockData: map[graphsync.RequestID][]graphsync.BlockData{
			responseID: {blkData},
		},
	}
	subscriber.ExpectEventsAllTopics(ctx, t, []notifications.Event{
		Event{
			Name:     Queued,
			Metadata: expectedMetadata,
		},
		Event{
			Name:     Sent,
			Metadata: expectedMetadata,
		},
	})
	subscriber.ExpectNCloses(ctx, t, 1)
}

func TestDedupingMessages(t *testing.T) {
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	targetPeer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, targetPeer, messageNetwork, allocator, messageSendRetries, sendMessageTimeout, func(peer.ID) {})
	messageQueue.Startup()
	waitGroup.Add(1)
	id := graphsync.NewRequestID()
	priority := graphsync.Priority(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	root := testutil.GenerateCids(1)[0]

	messageQueue.AllocateAndBuildMessage(0, func(b *Builder) {
		b.AddRequest(gsmsg.NewRequest(id, root, selector, priority))
	})
	// wait for send attempt
	waitGroup.Wait()
	id2 := graphsync.NewRequestID()
	priority2 := graphsync.Priority(rand.Int31())
	selector2 := ssb.ExploreAll(ssb.Matcher()).Node()
	root2 := testutil.GenerateCids(1)[0]
	id3 := graphsync.NewRequestID()
	priority3 := graphsync.Priority(rand.Int31())
	selector3 := ssb.ExploreIndex(0, ssb.Matcher()).Node()
	root3 := testutil.GenerateCids(1)[0]

	messageQueue.AllocateAndBuildMessage(0, func(b *Builder) {
		b.AddRequest(gsmsg.NewRequest(id2, root2, selector2, priority2))
		b.AddRequest(gsmsg.NewRequest(id3, root3, selector3, priority3))
	})

	var message gsmsg.GraphSyncMessage
	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")

	requests := message.Requests()
	require.Len(t, requests, 1, "number of requests in first message was not 1")
	request := requests[0]
	require.Equal(t, id, request.ID())
	require.Equal(t, request.Type(), graphsync.RequestTypeNew)
	require.Equal(t, priority, request.Priority())
	require.Equal(t, selector, request.Selector())

	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not senf")

	requests = message.Requests()
	require.Len(t, requests, 2, "number of requests in second message was not 2")
	for _, request := range requests {
		if request.ID() == id2 {
			require.Equal(t, request.Type(), graphsync.RequestTypeNew)
			require.Equal(t, priority2, request.Priority())
			require.Equal(t, selector2, request.Selector())
		} else if request.ID() == id3 {
			require.Equal(t, request.Type(), graphsync.RequestTypeNew)
			require.Equal(t, priority3, request.Priority())
			require.Equal(t, selector3, request.Selector())
		} else {
			t.Fatal("incorrect request added to message")
		}
	}

	tracing := collectTracing(t)
	require.ElementsMatch(t, []string{
		"message(0)->sendMessage(0)",
		"message(1)->sendMessage(0)",
	}, tracing.TracesToStrings())
}

func TestSendsVeryLargeBlocksResponses(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	targetPeer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	messageQueue := New(ctx, targetPeer, messageNetwork, allocator, messageSendRetries, sendMessageTimeout, func(peer.ID) {})
	messageQueue.Startup()
	waitGroup.Add(1)

	// generate large blocks before proceeding
	blks := testutil.GenerateBlocksOfSize(5, 1000000)
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[0].RawData())), func(b *Builder) {
		b.AddBlock(blks[0])
	})
	waitGroup.Wait()
	var message gsmsg.GraphSyncMessage
	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")

	msgBlks := message.Blocks()
	require.Len(t, msgBlks, 1, "number of blks in first message was not 1")
	require.True(t, blks[0].Cid().Equals(msgBlks[0].Cid()))

	// Send 3 very large blocks
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[1].RawData())), func(b *Builder) {
		b.AddBlock(blks[1])
	})
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[2].RawData())), func(b *Builder) {
		b.AddBlock(blks[2])
	})
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[3].RawData())), func(b *Builder) {
		b.AddBlock(blks[3])
	})

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

	messageQueue := New(ctx, p, messageNetwork, allocator, messageSendRetries, sendMessageTimeout, func(peer.ID) {})
	messageQueue.Startup()
	waitGroup.Add(1)

	// start sending block that exceeds memory limit
	blks := testutil.GenerateBlocksOfSize(2, 999)
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[0].RawData())), func(b *Builder) {
		b.AddBlock(blks[0])
	})

	finishes := make(chan string, 2)
	go func() {
		// attempt to send second block. Should block until memory is released
		messageQueue.AllocateAndBuildMessage(uint64(len(blks[1].RawData())), func(b *Builder) {
			b.AddBlock(blks[1])
		})
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

func TestNetworkErrorClearResponses(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	targetPeer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan gsmsg.GraphSyncMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	requestID1 := graphsync.NewRequestID()
	requestID2 := graphsync.NewRequestID()
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	allocator := allocator2.NewAllocator(1<<30, 1<<30)

	// we use only a retry count of 1 to avoid multiple send attempts for each message
	messageQueue := New(ctx, targetPeer, messageNetwork, allocator, 1, sendMessageTimeout, func(peer.ID) {})
	messageQueue.Startup()
	waitGroup.Add(1)

	// generate large blocks before proceeding
	blks := testutil.GenerateBlocksOfSize(5, 1000000)
	subscriber := testutil.NewTestSubscriber(5)

	messageQueue.AllocateAndBuildMessage(uint64(len(blks[0].RawData())), func(b *Builder) {
		b.AddBlock(blks[0])
		b.AddLink(requestID1, cidlink.Link{Cid: blks[0].Cid()}, graphsync.LinkActionPresent)
		b.SetSubscriber(requestID1, subscriber)
	})
	waitGroup.Wait()
	var message gsmsg.GraphSyncMessage
	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")

	msgBlks := message.Blocks()
	require.Len(t, msgBlks, 1, "number of blks in first message was not 1")
	require.True(t, blks[0].Cid().Equals(msgBlks[0].Cid()))

	expectedMetadata := Metadata{
		ResponseCodes: map[graphsync.RequestID]graphsync.ResponseStatusCode{
			requestID1: graphsync.PartialResponse,
		},
		BlockData: map[graphsync.RequestID][]graphsync.BlockData{},
	}
	subscriber.ExpectEventsAllTopics(ctx, t, []notifications.Event{
		Event{Name: Queued, Metadata: expectedMetadata},
		Event{Name: Sent, Metadata: expectedMetadata},
	})
	subscriber.ExpectNCloses(ctx, t, 1)
	fc1 := &fakeCloser{fms: messageSender}
	fc2 := &fakeCloser{fms: messageSender}
	// Send 3 very large blocks
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[1].RawData())), func(b *Builder) {
		b.AddBlock(blks[1])
		b.SetResponseStream(requestID1, fc1)
		b.AddLink(requestID1, cidlink.Link{Cid: blks[1].Cid()}, graphsync.LinkActionPresent)
	})
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[2].RawData())), func(b *Builder) {
		b.AddBlock(blks[2])
		b.SetResponseStream(requestID1, fc1)
		b.AddLink(requestID1, cidlink.Link{Cid: blks[2].Cid()}, graphsync.LinkActionPresent)
	})
	messageQueue.AllocateAndBuildMessage(uint64(len(blks[3].RawData())), func(b *Builder) {
		b.SetResponseStream(requestID2, fc2)
		b.AddLink(requestID2, cidlink.Link{Cid: blks[3].Cid()}, graphsync.LinkActionPresent)
		b.AddBlock(blks[3])
	})

	messageSender.sendError = errors.New("something went wrong")

	// add one since the stream will get reopened
	waitGroup.Add(1)

	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")
	msgBlks = message.Blocks()
	require.Len(t, msgBlks, 1, "number of blks in first message was not 1")
	require.True(t, blks[1].Cid().Equals(msgBlks[0].Cid()))

	// should skip block2 as it's linked to errored request
	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")
	msgBlks = message.Blocks()
	require.Len(t, msgBlks, 1, "number of blks in first message was not 1")
	require.True(t, blks[3].Cid().Equals(msgBlks[0].Cid()))

	require.True(t, fc1.closed)
	require.False(t, fc2.closed)
}

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

type fakeCloser struct {
	fms    *fakeMessageSender
	closed bool
}

func (fc *fakeCloser) Close() error {
	fc.closed = true
	// clear error so the next send goes through
	fc.fms.sendError = nil
	return nil
}
