package responseassembler

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestResponseAssemblerSendsResponses(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.RequestID(rand.Int31())
	requestID2 := graphsync.RequestID(rand.Int31())
	requestID3 := graphsync.RequestID(rand.Int31())
	sendResponseNotifee1, _ := testutil.NewTestNotifee(requestID1, 10)
	sendResponseNotifee2, _ := testutil.NewTestNotifee(requestID2, 10)
	sendResponseNotifee3, _ := testutil.NewTestNotifee(requestID3, 10)

	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)

	var bd1, bd2 graphsync.BlockData

	// send block 0 for request 1
	require.NoError(t, responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
		b.AddNotifee(sendResponseNotifee1)
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	}))
	assertSentOnWire(t, bd1, blks[0])
	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})
	fph.AssertNotifees(sendResponseNotifee1)

	// send block 0 for request 2 (duplicate block should not be sent)
	require.NoError(t, responseAssembler.Transaction(p, requestID2, func(b ResponseBuilder) error {
		b.AddNotifee(sendResponseNotifee2)
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	}))
	assertSentNotOnWire(t, bd1, blks[0])
	fph.AssertResponses(expectedResponses{requestID2: graphsync.PartialResponse})
	fph.AssertNotifees(sendResponseNotifee2)

	// send more to request 1 and finish request
	require.NoError(t, responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
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

	// send more to request 2
	require.NoError(t, responseAssembler.Transaction(p, requestID2, func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[3], blks[3].RawData())
		b.FinishRequest()
		return nil
	}))
	fph.AssertBlocks(blks[3])
	fph.AssertResponses(expectedResponses{
		requestID2: graphsync.RequestCompletedFull,
	})

	// send to request 3
	require.NoError(t, responseAssembler.Transaction(p, requestID3, func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[4], blks[4].RawData())
		return nil
	}))
	fph.AssertBlocks(blks[4])
	fph.AssertResponses(expectedResponses{
		requestID3: graphsync.PartialResponse,
	})

	// send 2 more to request 3
	require.NoError(t, responseAssembler.Transaction(p, requestID3, func(b ResponseBuilder) error {
		b.AddNotifee(sendResponseNotifee3)
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		bd1 = b.SendResponse(links[4], blks[4].RawData())
		return nil
	}))

	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID3: graphsync.PartialResponse})
}

func TestResponseAssemblerSendsExtensionData(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.RequestID(rand.Int31())
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)

	require.NoError(t, responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
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
	require.NoError(t, responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
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
	requestID1 := graphsync.RequestID(rand.Int31())
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)
	notifee, _ := testutil.NewTestNotifee("transaction", 10)
	err := responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
		bd := b.SendResponse(links[0], blks[0].RawData())
		assertSentOnWire(t, bd, blks[0])

		fph.RefuteHasMessage()
		fph.RefuteBlocks()
		fph.RefuteResponses()

		bd = b.SendResponse(links[1], blks[1].RawData())
		assertSentOnWire(t, bd, blks[1])
		bd = b.SendResponse(links[2], nil)
		assertNotSent(t, bd, blks[2])
		b.FinishRequest()

		b.AddNotifee(notifee)
		fph.RefuteHasMessage()
		return nil
	})
	require.NoError(t, err)

	fph.AssertNotifees(notifee)
}

func TestResponseAssemblerIgnoreBlocks(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.RequestID(rand.Int31())
	requestID2 := graphsync.RequestID(rand.Int31())
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)

	responseAssembler.IgnoreBlocks(p, requestID1, links)

	var bd1, bd2, bd3 graphsync.BlockData
	err := responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)

	assertSentNotOnWire(t, bd1, blks[0])
	fph.RefuteBlocks()
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	err = responseAssembler.Transaction(p, requestID2, func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)
	fph.AssertResponses(expectedResponses{
		requestID2: graphsync.PartialResponse,
	})

	err = responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
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

	err = responseAssembler.Transaction(p, requestID2, func(b ResponseBuilder) error {
		b.SendResponse(links[3], blks[3].RawData())
		b.FinishRequest()
		return nil
	})
	require.NoError(t, err)

	fph.AssertBlocks(blks[3])
	fph.AssertResponses(expectedResponses{requestID2: graphsync.RequestCompletedFull})

}

func TestResponseAssemblerSkipFirstBlocks(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.RequestID(rand.Int31())
	requestID2 := graphsync.RequestID(rand.Int31())
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)

	responseAssembler.SkipFirstBlocks(p, requestID1, 3)

	var bd1, bd2, bd3, bd4, bd5 graphsync.BlockData
	err := responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)

	assertSentNotOnWire(t, bd1, blks[0])
	fph.RefuteBlocks()
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	err = responseAssembler.Transaction(p, requestID2, func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)
	fph.AssertResponses(expectedResponses{
		requestID2: graphsync.PartialResponse,
	})

	err = responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
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
	err = responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
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

	err = responseAssembler.Transaction(p, requestID2, func(b ResponseBuilder) error {
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
	requestID1 := graphsync.RequestID(rand.Int31())
	requestID2 := graphsync.RequestID(rand.Int31())
	requestID3 := graphsync.RequestID(rand.Int31())
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	responseAssembler := New(ctx, fph)

	responseAssembler.DedupKey(p, requestID1, "applesauce")
	responseAssembler.DedupKey(p, requestID3, "applesauce")

	var bd1, bd2 graphsync.BlockData
	err := responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)
	assertSentOnWire(t, bd1, blks[0])

	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	err = responseAssembler.Transaction(p, requestID2, func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[0], blks[0].RawData())
		return nil
	})
	require.NoError(t, err)
	assertSentOnWire(t, bd1, blks[0])

	err = responseAssembler.Transaction(p, requestID1, func(b ResponseBuilder) error {
		bd1 = b.SendResponse(links[1], blks[1].RawData())
		bd2 = b.SendResponse(links[2], nil)
		return nil
	})
	require.NoError(t, err)
	assertSentOnWire(t, bd1, blks[1])
	assertNotSent(t, bd2, blks[2])

	fph.AssertBlocks(blks[1])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	err = responseAssembler.Transaction(p, requestID2, func(b ResponseBuilder) error {
		b.SendResponse(links[3], blks[3].RawData())
		b.FinishRequest()
		return nil
	})
	require.NoError(t, err)
	fph.AssertBlocks(blks[3])
	fph.AssertResponses(expectedResponses{requestID2: graphsync.RequestCompletedFull})

	err = responseAssembler.Transaction(p, requestID3, func(b ResponseBuilder) error {
		b.SendResponse(links[4], blks[4].RawData())
		return nil
	})
	require.NoError(t, err)
	fph.AssertBlocks(blks[4])
	fph.AssertResponses(expectedResponses{requestID3: graphsync.PartialResponse})

	err = responseAssembler.Transaction(p, requestID3, func(b ResponseBuilder) error {
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
	ctx           context.Context
	t             *testing.T
	lastBlocks    []blocks.Block
	lastResponses []gsmsg.GraphSyncResponse
	lastNotifiees []notifications.Notifee
	sent          chan struct{}
}

func newFakePeerHandler(ctx context.Context, t *testing.T) *fakePeerHandler {
	t.Helper()
	return &fakePeerHandler{
		ctx: ctx,
		t:   t,
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

func (fph *fakePeerHandler) AssertNotifees(notifees ...notifications.Notifee) {
	require.Len(fph.t, fph.lastNotifiees, len(notifees))
	for i, notifee := range notifees {
		require.Equal(fph.t, notifee, fph.lastNotifiees[i])
	}
}

func (fph *fakePeerHandler) RefuteResponses() {
	require.Empty(fph.t, fph.lastResponses)
}

func (fph *fakePeerHandler) AllocateAndBuildMessage(p peer.ID, blkSize uint64, buildMessageFn func(*gsmsg.Builder), notifees []notifications.Notifee) {
	builder := gsmsg.NewBuilder(gsmsg.Topic(0))
	buildMessageFn(builder)

	msg, err := builder.Build()
	require.NoError(fph.t, err)

	fph.sendResponse(p, msg.Responses(), msg.Blocks(), notifees...)
}

func (fph *fakePeerHandler) sendResponse(p peer.ID, responses []gsmsg.GraphSyncResponse, blks []blocks.Block, notifees ...notifications.Notifee) {
	fph.lastResponses = responses
	fph.lastBlocks = blks
	fph.lastNotifiees = notifees
}
