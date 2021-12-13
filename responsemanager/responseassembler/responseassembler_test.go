package responseassembler

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestResponseAssemblerSendsResponses(t *testing.T) {
	ctx := context.Background()
	ctx, collectTracing := testutil.SetupTracing(ctx)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.NewRequestID()
	requestID2 := graphsync.NewRequestID()
	requestID3 := graphsync.NewRequestID()

	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)

	var bd1, bd2 graphsync.BlockData

	sub1 := testutil.NewTestSubscriber(10)
	stream1 := responseAssembler.NewStream(ctx, p, requestID1, sub1)
	sub2 := testutil.NewTestSubscriber(10)
	stream2 := responseAssembler.NewStream(ctx, p, requestID2, sub2)
	sub3 := testutil.NewTestSubscriber(10)
	stream3 := responseAssembler.NewStream(ctx, p, requestID3, sub3)

	// send block 0 for request 1
	require.NoError(t, stream1.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	}))
	assertSentOnWire(t, bd1, blks[0])
	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})
	fph.AssertSubscriber(requestID1, sub1)
	fph.AssertResponseStream(requestID1, stream1)
	// send block 0 for request 2 (duplicate block should not be sent)
	require.NoError(t, stream2.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	}))
	assertSentNotOnWire(t, bd1, blks[0])
	fph.AssertResponses(expectedResponses{requestID2: graphsync.PartialResponse})
	fph.AssertSubscriber(requestID2, sub2)
	fph.AssertResponseStream(requestID2, stream2)

	// send more to request 1 and finish request
	require.NoError(t, stream1.Transaction(func(b ResponseBuilder) error {
		// send block 1
		bd1 = b.SendResponse(links[1], blks[1].RawData())
		// block 2 is not found. Assert not sent
		bd2 = b.SendResponse(links[2], nil)
		b.FinishRequest()
		return nil
	}))
	assertSentOnWire(t, bd1, blks[1])
	assertNotSent(t, bd2, blks[2])
	fph.AssertBlocks(blks[1])
	fph.AssertResponses(expectedResponses{
		requestID1: graphsync.RequestCompletedPartial,
	})
	fph.AssertSubscriber(requestID1, sub1)
	fph.AssertResponseStream(requestID1, stream1)

	// send more to request 2
	require.NoError(t, stream2.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[3], blks[3].RawData())
		b.FinishRequest()
		return nil
	}))
	fph.AssertBlocks(blks[3])
	fph.AssertResponses(expectedResponses{
		requestID2: graphsync.RequestCompletedFull,
	})
	fph.AssertSubscriber(requestID2, sub2)
	fph.AssertResponseStream(requestID2, stream2)

	// send to request 3
	require.NoError(t, stream3.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[4], blks[4].RawData())
		return nil
	}))
	fph.AssertBlocks(blks[4])
	fph.AssertResponses(expectedResponses{
		requestID3: graphsync.PartialResponse,
	})
	fph.AssertSubscriber(requestID3, sub3)
	fph.AssertResponseStream(requestID3, stream3)

	// send 2 more to request 3
	require.NoError(t, stream3.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		bd1 = b.SendResponse(links[4], blks[4].RawData())
		return nil
	}))

	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID3: graphsync.PartialResponse})
	fph.AssertSubscriber(requestID3, sub3)
	fph.AssertResponseStream(requestID3, stream3)

	tracing := collectTracing(t)
	require.ElementsMatch(t, []string{
		"transaction(0)->execute(0)->buildMessage(0)",
		"transaction(1)->execute(0)->buildMessage(0)",
		"transaction(2)->execute(0)->buildMessage(0)",
		"transaction(3)->execute(0)->buildMessage(0)",
		"transaction(4)->execute(0)->buildMessage(0)",
		"transaction(5)->execute(0)->buildMessage(0)",
	}, tracing.TracesToStrings())
}

func TestResponseAssemblerCloseStream(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.NewRequestID()
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)

	sub1 := testutil.NewTestSubscriber(10)
	stream1 := responseAssembler.NewStream(ctx, p, requestID1, sub1)
	require.NoError(t, stream1.Transaction(func(b ResponseBuilder) error {
		b.SendResponse(links[0], blks[0].RawData())
		return nil
	}))
	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})
	fph.AssertSubscriber(requestID1, sub1)
	fph.AssertResponseStream(requestID1, stream1)

	// close the response stream
	fph.CloseResponseStream(requestID1)
	fph.Clear()

	require.NoError(t, stream1.Transaction(func(b ResponseBuilder) error {
		b.SendResponse(links[1], blks[1].RawData())
		return nil
	}))
	fph.RefuteBlocks()
	fph.RefuteResponses()
}
func TestResponseAssemblerSendsExtensionData(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.NewRequestID()
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)

	sub1 := testutil.NewTestSubscriber(10)
	stream1 := responseAssembler.NewStream(ctx, p, requestID1, sub1)
	require.NoError(t, stream1.Transaction(func(b ResponseBuilder) error {
		b.SendResponse(links[0], blks[0].RawData())
		return nil
	}))

	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	extensionData1 := testutil.RandomBytes(100)
	extensionName1 := graphsync.ExtensionName("AppleSauce/McGee")
	extension1 := graphsync.ExtensionData{
		Name: extensionName1,
		Data: extensionData1,
	}
	extensionData2 := testutil.RandomBytes(100)
	extensionName2 := graphsync.ExtensionName("HappyLand/Happenstance")
	extension2 := graphsync.ExtensionData{
		Name: extensionName2,
		Data: extensionData2,
	}
	require.NoError(t, stream1.Transaction(func(b ResponseBuilder) error {
		b.SendResponse(links[1], blks[1].RawData())
		b.SendExtensionData(extension1)
		b.SendExtensionData(extension2)
		return nil
	}))

	// let peer reponse manager know last message was sent so message sending can continue
	fph.AssertExtensions([][]graphsync.ExtensionData{{extension1, extension2}})
}

func TestResponseAssemblerSendsResponsesInTransaction(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.NewRequestID()
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)
	sub1 := testutil.NewTestSubscriber(10)
	stream1 := responseAssembler.NewStream(ctx, p, requestID1, sub1)
	var bd1, bd2, bd3 graphsync.BlockData
	err := stream1.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		assertSentOnWire(t, bd1, blks[0])

		fph.RefuteHasMessage()
		fph.RefuteBlocks()
		fph.RefuteResponses()

		bd2 = b.SendResponse(links[1], blks[1].RawData())
		assertSentOnWire(t, bd2, blks[1])
		bd3 = b.SendResponse(links[2], nil)
		assertNotSent(t, bd3, blks[2])
		b.FinishRequest()

		fph.RefuteHasMessage()
		return nil
	})
	require.NoError(t, err)
	fph.AssertBlockData(requestID1, bd1)
	fph.AssertBlockData(requestID1, bd2)
	fph.AssertBlockData(requestID1, bd3)
}

func TestResponseAssemblerIgnoreBlocks(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.NewRequestID()
	requestID2 := graphsync.NewRequestID()
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)
	sub1 := testutil.NewTestSubscriber(10)
	stream1 := responseAssembler.NewStream(ctx, p, requestID1, sub1)
	sub2 := testutil.NewTestSubscriber(10)
	stream2 := responseAssembler.NewStream(ctx, p, requestID2, sub2)

	stream1.IgnoreBlocks(links)

	var bd1, bd2, bd3 graphsync.BlockData

	err := stream1.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)

	assertSentNotOnWire(t, bd1, blks[0])
	fph.RefuteBlocks()
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})
	fph.AssertBlockData(requestID1, bd1)
	err = stream2.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)
	fph.AssertResponses(expectedResponses{
		requestID2: graphsync.PartialResponse,
	})
	fph.AssertBlockData(requestID2, bd1)

	err = stream1.Transaction(func(b ResponseBuilder) error {
		bd2 = b.SendResponse(links[1], blks[1].RawData())
		bd3 = b.SendResponse(links[2], blks[2].RawData())
		b.FinishRequest()
		return nil
	})
	require.NoError(t, err)

	assertSentNotOnWire(t, bd1, blks[0])
	assertSentNotOnWire(t, bd2, blks[1])
	assertSentNotOnWire(t, bd3, blks[2])

	fph.RefuteBlocks()
	fph.AssertResponses(expectedResponses{
		requestID1: graphsync.RequestCompletedFull,
	})
	fph.AssertBlockData(requestID1, bd2)
	fph.AssertBlockData(requestID1, bd3)

	var bd4 graphsync.BlockData
	err = stream2.Transaction(func(b ResponseBuilder) error {
		bd4 = b.SendResponse(links[3], blks[3].RawData())
		b.FinishRequest()
		return nil
	})
	require.NoError(t, err)

	fph.AssertBlocks(blks[3])
	fph.AssertResponses(expectedResponses{requestID2: graphsync.RequestCompletedFull})
	fph.AssertBlockData(requestID2, bd4)
}

func TestResponseAssemblerSkipFirstBlocks(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.NewRequestID()
	requestID2 := graphsync.NewRequestID()
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)

	sub1 := testutil.NewTestSubscriber(10)
	stream1 := responseAssembler.NewStream(ctx, p, requestID1, sub1)
	sub2 := testutil.NewTestSubscriber(10)
	stream2 := responseAssembler.NewStream(ctx, p, requestID2, sub2)

	stream1.SkipFirstBlocks(3)

	var bd1, bd2, bd3, bd4, bd5 graphsync.BlockData

	err := stream1.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)

	assertSentNotOnWire(t, bd1, blks[0])
	fph.RefuteBlocks()
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})
	fph.AssertBlockData(requestID1, bd1)

	err = stream2.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)
	fph.AssertResponses(expectedResponses{
		requestID2: graphsync.PartialResponse,
	})
	fph.AssertBlockData(requestID2, bd1)

	err = stream1.Transaction(func(b ResponseBuilder) error {
		bd2 = b.SendResponse(links[1], blks[1].RawData())
		bd3 = b.SendResponse(links[2], blks[2].RawData())
		return nil
	})
	require.NoError(t, err)

	assertSentNotOnWire(t, bd1, blks[0])
	assertSentNotOnWire(t, bd2, blks[1])
	assertSentNotOnWire(t, bd3, blks[2])

	fph.RefuteBlocks()
	fph.AssertResponses(expectedResponses{
		requestID1: graphsync.PartialResponse,
	})
	fph.AssertBlockData(requestID1, bd2)
	fph.AssertBlockData(requestID1, bd3)

	err = stream1.Transaction(func(b ResponseBuilder) error {
		bd4 = b.SendResponse(links[3], blks[3].RawData())
		bd5 = b.SendResponse(links[4], blks[4].RawData())
		b.FinishRequest()
		return nil
	})
	require.NoError(t, err)

	assertSentOnWire(t, bd4, blks[3])
	assertSentOnWire(t, bd5, blks[4])

	fph.AssertBlocks(blks[3], blks[4])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.RequestCompletedFull})
	fph.AssertBlockData(requestID1, bd4)
	fph.AssertBlockData(requestID1, bd5)

	err = stream2.Transaction(func(b ResponseBuilder) error {
		b.SendResponse(links[3], blks[3].RawData())
		b.FinishRequest()
		return nil
	})
	require.NoError(t, err)

	fph.AssertBlocks(blks[3])
	fph.AssertResponses(expectedResponses{requestID2: graphsync.RequestCompletedFull})

}

func TestResponseAssemblerDupKeys(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.NewRequestID()
	requestID2 := graphsync.NewRequestID()
	requestID3 := graphsync.NewRequestID()
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)
	sub1 := testutil.NewTestSubscriber(10)
	stream1 := responseAssembler.NewStream(ctx, p, requestID1, sub1)
	sub2 := testutil.NewTestSubscriber(10)
	stream2 := responseAssembler.NewStream(ctx, p, requestID2, sub2)
	sub3 := testutil.NewTestSubscriber(10)
	stream3 := responseAssembler.NewStream(ctx, p, requestID3, sub3)

	stream1.DedupKey("applesauce")
	stream3.DedupKey("applesauce")

	var bd1, bd2 graphsync.BlockData

	err := stream1.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)
	assertSentOnWire(t, bd1, blks[0])

	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})
	fph.AssertBlockData(requestID1, bd1)

	err = stream2.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)
	assertSentOnWire(t, bd1, blks[0])
	fph.AssertBlockData(requestID2, bd1)

	err = stream1.Transaction(func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[1], blks[1].RawData())
		bd2 = b.SendResponse(links[2], nil)
		return nil
	})
	require.NoError(t, err)
	assertSentOnWire(t, bd1, blks[1])
	assertNotSent(t, bd2, blks[2])

	fph.AssertBlocks(blks[1])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})
	fph.AssertBlockData(requestID1, bd1)
	fph.AssertBlockData(requestID1, bd2)

	err = stream2.Transaction(func(b ResponseBuilder) error {
		b.SendResponse(links[3], blks[3].RawData())
		b.FinishRequest()
		return nil
	})
	require.NoError(t, err)
	fph.AssertBlocks(blks[3])
	fph.AssertResponses(expectedResponses{requestID2: graphsync.RequestCompletedFull})

	err = stream3.Transaction(func(b ResponseBuilder) error {
		b.SendResponse(links[4], blks[4].RawData())
		return nil
	})
	require.NoError(t, err)
	fph.AssertBlocks(blks[4])
	fph.AssertResponses(expectedResponses{requestID3: graphsync.PartialResponse})

	err = stream3.Transaction(func(b ResponseBuilder) error {
		b.SendResponse(links[0], blks[0].RawData())
		b.SendResponse(links[4], blks[4].RawData())
		return nil
	})
	require.NoError(t, err)

	fph.RefuteBlocks()
	fph.AssertResponses(expectedResponses{requestID3: graphsync.PartialResponse})
}

func findResponseForRequestID(responses []gsmsg.GraphSyncResponse, requestID graphsync.RequestID) (gsmsg.GraphSyncResponse, error) {
	for _, response := range responses {
		if response.RequestID() == requestID {
			return response, nil
		}
	}
	return gsmsg.GraphSyncResponse{}, fmt.Errorf("Response Not Found")
}

func assertSentNotOnWire(t *testing.T, bd graphsync.BlockData, blk blocks.Block) {
	t.Helper()
	require.Equal(t, cidlink.Link{Cid: blk.Cid()}, bd.Link())
	require.Equal(t, uint64(len(blk.RawData())), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
}

func assertSentOnWire(t *testing.T, bd graphsync.BlockData, blk blocks.Block) {
	t.Helper()
	require.Equal(t, cidlink.Link{Cid: blk.Cid()}, bd.Link())
	require.Equal(t, uint64(len(blk.RawData())), bd.BlockSize())
	require.Equal(t, uint64(len(blk.RawData())), bd.BlockSizeOnWire())
}

func assertNotSent(t *testing.T, bd graphsync.BlockData, blk blocks.Block) {
	t.Helper()
	require.Equal(t, cidlink.Link{Cid: blk.Cid()}, bd.Link())
	require.Equal(t, uint64(0), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
}

type fakePeerHandler struct {
	ctx                 context.Context
	t                   *testing.T
	lastResponseStreams map[graphsync.RequestID]io.Closer
	lastBlocks          []blocks.Block
	lastResponses       []gsmsg.GraphSyncResponse
	lastSubscribers     map[graphsync.RequestID]notifications.Subscriber
	lastBlockData       map[graphsync.RequestID][]graphsync.BlockData
	sent                chan struct{}
}

func newFakePeerHandler(ctx context.Context, t *testing.T) *fakePeerHandler {
	t.Helper()
	return &fakePeerHandler{
		lastResponseStreams: map[graphsync.RequestID]io.Closer{},
		lastSubscribers:     map[graphsync.RequestID]notifications.Subscriber{},
		lastBlockData:       map[graphsync.RequestID][]graphsync.BlockData{},
		ctx:                 ctx,
		t:                   t,
	}
}

func (fph *fakePeerHandler) RefuteHasMessage() {
	timer := time.NewTimer(100 * time.Millisecond)
	testutil.AssertDoesReceiveFirst(fph.t, timer.C, "should not send a message", fph.sent)
}

func (fph *fakePeerHandler) AssertBlocks(blks ...blocks.Block) {
	require.Len(fph.t, fph.lastBlocks, len(blks))
	for _, blk := range blks {
		testutil.AssertContainsBlock(fph.t, fph.lastBlocks, blk)
	}
}

func (fph *fakePeerHandler) RefuteBlocks() {
	require.Empty(fph.t, fph.lastBlocks)
}

func (fph *fakePeerHandler) AssertResponseStream(requestID graphsync.RequestID, expected ResponseStream) {
	actual, ok := fph.lastResponseStreams[requestID]
	require.True(fph.t, ok)
	require.Equal(fph.t, expected, actual)
}

func (fph *fakePeerHandler) CloseResponseStream(requestID graphsync.RequestID) {
	actual, ok := fph.lastResponseStreams[requestID]
	require.True(fph.t, ok)
	actual.Close()
}

type expectedResponses map[graphsync.RequestID]graphsync.ResponseStatusCode

func (fph *fakePeerHandler) AssertResponses(responses expectedResponses) {
	require.Len(fph.t, fph.lastResponses, len(responses))
	for requestID, status := range responses {
		response, err := findResponseForRequestID(fph.lastResponses, requestID)
		require.NoError(fph.t, err)
		require.Equal(fph.t, status, response.Status())
	}
}

func (fph *fakePeerHandler) AssertExtensions(extensionSets [][]graphsync.ExtensionData) {
	require.Len(fph.t, fph.lastResponses, len(extensionSets))
	for i, extensions := range extensionSets {
		response := fph.lastResponses[i]
		for _, extension := range extensions {
			returnedData, found := response.Extension(extension.Name)
			require.True(fph.t, found)
			require.Equal(fph.t, extension.Data, returnedData)
		}
	}
}

func (fph *fakePeerHandler) AssertSubscriber(requestID graphsync.RequestID, expected notifications.Subscriber) {
	actual, ok := fph.lastSubscribers[requestID]
	require.True(fph.t, ok)
	require.Equal(fph.t, expected, actual)
}

func (fph *fakePeerHandler) AssertBlockData(requestID graphsync.RequestID, expected graphsync.BlockData) {
	actual, ok := fph.lastBlockData[requestID]
	require.True(fph.t, ok)
	require.Contains(fph.t, actual, expected)
}

func (fph *fakePeerHandler) RefuteResponses() {
	require.Empty(fph.t, fph.lastResponses)
}

func (fph *fakePeerHandler) AllocateAndBuildMessage(p peer.ID, blkSize uint64, buildMessageFn func(*messagequeue.Builder)) {
	builder := messagequeue.NewBuilder(context.TODO(), messagequeue.Topic(0))
	buildMessageFn(builder)

	msg, err := builder.Build()
	require.NoError(fph.t, err)

	fph.sendResponse(p, msg.Responses(), msg.Blocks(), builder.ResponseStreams(), builder.Subscribers(), builder.BlockData())
}

func (fph *fakePeerHandler) sendResponse(p peer.ID,
	responses []gsmsg.GraphSyncResponse,
	blks []blocks.Block,
	responseStreams map[graphsync.RequestID]io.Closer,
	subscribers map[graphsync.RequestID]notifications.Subscriber,
	blockData map[graphsync.RequestID][]graphsync.BlockData) {
	fph.lastResponses = responses
	fph.lastBlocks = blks
	fph.lastResponseStreams = responseStreams
	fph.lastSubscribers = subscribers
	fph.lastBlockData = blockData
}

func (fph *fakePeerHandler) Clear() {
	fph.lastResponses = nil
	fph.lastSubscribers = map[graphsync.RequestID]notifications.Subscriber{}
	fph.lastBlockData = map[graphsync.RequestID][]graphsync.BlockData{}
	fph.lastResponseStreams = map[graphsync.RequestID]io.Closer{}
	fph.lastBlocks = nil
}
