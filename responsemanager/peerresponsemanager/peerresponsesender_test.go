package peerresponsemanager

import (
	"context"
	"errors"
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
	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestPeerResponseSenderSendsResponses(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.RequestID(rand.Int31())
	requestID2 := graphsync.RequestID(rand.Int31())
	requestID3 := graphsync.RequestID(rand.Int31())
	sendResponseNotifee1, sendResponseVerifier1 := testutil.NewTestNotifee(requestID1, 10)
	sendResponseNotifee2, sendResponseVerifier2 := testutil.NewTestNotifee(requestID2, 10)
	sendResponseNotifee3, sendResponseVerifier3 := testutil.NewTestNotifee(requestID3, 10)
	finishNotifee1, finishVerifier1 := testutil.NewTestNotifee(requestID1, 10)
	finishNotifee2, finishVerifier2 := testutil.NewTestNotifee(requestID2, 10)

	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	bd := peerResponseSender.SendResponse(requestID1, links[0], blks[0].RawData(), sendResponseNotifee1)
	assertSentOnWire(t, bd, blks[0])
	fph.AssertHasMessage("did not send first message")

	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	bd = peerResponseSender.SendResponse(requestID2, links[0], blks[0].RawData(), sendResponseNotifee2)
	assertSentNotOnWire(t, bd, blks[0])
	bd = peerResponseSender.SendResponse(requestID1, links[1], blks[1].RawData(), sendResponseNotifee1)
	assertSentOnWire(t, bd, blks[1])
	bd = peerResponseSender.SendResponse(requestID1, links[2], nil, sendResponseNotifee1)
	assertNotSent(t, bd, blks[2])
	peerResponseSender.FinishRequest(requestID1, finishNotifee1)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send second message")
	fph.AssertBlocks(blks[1])
	fph.AssertResponses(expectedResponses{
		requestID1: graphsync.RequestCompletedPartial,
		requestID2: graphsync.PartialResponse,
	})

	peerResponseSender.SendResponse(requestID2, links[3], blks[3].RawData(), sendResponseNotifee2)
	peerResponseSender.SendResponse(requestID3, links[4], blks[4].RawData(), sendResponseNotifee3)
	peerResponseSender.FinishRequest(requestID2, finishNotifee2)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send third message")
	fph.AssertBlocks(blks[3], blks[4])
	fph.AssertResponses(expectedResponses{
		requestID2: graphsync.RequestCompletedFull,
		requestID3: graphsync.PartialResponse,
	})

	peerResponseSender.SendResponse(requestID3, links[0], blks[0].RawData(), sendResponseNotifee3)
	peerResponseSender.SendResponse(requestID3, links[4], blks[4].RawData(), sendResponseNotifee3)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send fourth message")
	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID3: graphsync.PartialResponse})

	fph.notifyError()

	sendResponseVerifier1.ExpectEvents(ctx, t, []notifications.Event{Event{Name: Sent}, Event{Name: Sent}})
	sendResponseVerifier1.ExpectClose(ctx, t)
	sendResponseVerifier2.ExpectEvents(ctx, t, []notifications.Event{Event{Name: Sent}, Event{Name: Sent}})
	sendResponseVerifier2.ExpectClose(ctx, t)
	sendResponseVerifier3.ExpectEvents(ctx, t, []notifications.Event{
		Event{Name: Sent},
		Event{Name: Error, Err: fmt.Errorf("error sending message: %w", errors.New("something went wrong"))},
	})
	sendResponseVerifier3.ExpectClose(ctx, t)

	finishVerifier1.ExpectEvents(ctx, t, []notifications.Event{Event{Name: Sent}})
	finishVerifier1.ExpectClose(ctx, t)
	finishVerifier2.ExpectEvents(ctx, t, []notifications.Event{Event{Name: Sent}})
	finishVerifier2.ExpectClose(ctx, t)
}

func TestPeerResponseSenderSendsVeryLargeBlocksResponses(t *testing.T) {

	p := testutil.GeneratePeers(1)[0]
	requestID1 := graphsync.RequestID(rand.Int31())
	// generate large blocks before proceeding
	blks := testutil.GenerateBlocksOfSize(5, 1000000)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	links := make([]ipld.Link, 0, len(blks))
	for _, block := range blks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	fph := newFakePeerHandler(ctx, t)
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	peerResponseSender.SendResponse(requestID1, links[0], blks[0].RawData())

	fph.AssertHasMessage("did not send first message")
	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	// Send 3 very large blocks
	peerResponseSender.SendResponse(requestID1, links[1], blks[1].RawData())
	peerResponseSender.SendResponse(requestID1, links[2], blks[2].RawData())
	peerResponseSender.SendResponse(requestID1, links[3], blks[3].RawData())

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send second message")
	fph.AssertBlocks(blks[1])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	// Send one more block while waiting
	peerResponseSender.SendResponse(requestID1, links[4], blks[4].RawData())
	peerResponseSender.FinishRequest(requestID1)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send third message")
	fph.AssertBlocks(blks[2])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send fourth message")
	fph.AssertBlocks(blks[3])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send fifth message")
	fph.AssertBlocks(blks[4])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.RequestCompletedFull})

}

func TestPeerResponseSenderSendsExtensionData(t *testing.T) {
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
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	peerResponseSender.SendResponse(requestID1, links[0], blks[0].RawData())

	fph.AssertHasMessage("did not send first message")
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
	peerResponseSender.SendResponse(requestID1, links[1], blks[1].RawData())
	peerResponseSender.SendExtensionData(requestID1, extension1)
	peerResponseSender.SendExtensionData(requestID1, extension2)
	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()
	fph.AssertHasMessage("did not send second message")
	fph.AssertExtensions([][]graphsync.ExtensionData{{extension1, extension2}})

}

func TestPeerResponseSenderSendsResponsesInTransaction(t *testing.T) {
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
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()
	notifee, notifeeVerifier := testutil.NewTestNotifee("transaction", 10)
	err := peerResponseSender.Transaction(requestID1, func(peerResponseSender PeerResponseTransactionSender) error {
		bd := peerResponseSender.SendResponse(links[0], blks[0].RawData())
		assertSentOnWire(t, bd, blks[0])

		fph.RefuteHasMessage()
		fph.RefuteBlocks()
		fph.RefuteResponses()

		bd = peerResponseSender.SendResponse(links[1], blks[1].RawData())
		assertSentOnWire(t, bd, blks[1])
		bd = peerResponseSender.SendResponse(links[2], nil)
		assertNotSent(t, bd, blks[2])
		peerResponseSender.FinishRequest()

		peerResponseSender.AddNotifee(notifee)
		fph.RefuteHasMessage()
		return nil
	})
	require.NoError(t, err)
	fph.AssertHasMessage("should sent first message")

	fph.notifySuccess()
	notifeeVerifier.ExpectEvents(ctx, t, []notifications.Event{Event{Name: Sent}})
	notifeeVerifier.ExpectClose(ctx, t)
}

func TestPeerResponseSenderIgnoreBlocks(t *testing.T) {
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
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	peerResponseSender.IgnoreBlocks(requestID1, links)

	bd := peerResponseSender.SendResponse(requestID1, links[0], blks[0].RawData())
	assertSentNotOnWire(t, bd, blks[0])

	fph.AssertHasMessage("did not send first message")
	fph.RefuteBlocks()
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	bd = peerResponseSender.SendResponse(requestID2, links[0], blks[0].RawData())
	assertSentNotOnWire(t, bd, blks[0])
	bd = peerResponseSender.SendResponse(requestID1, links[1], blks[1].RawData())
	assertSentNotOnWire(t, bd, blks[1])
	bd = peerResponseSender.SendResponse(requestID1, links[2], blks[2].RawData())
	assertSentNotOnWire(t, bd, blks[2])
	peerResponseSender.FinishRequest(requestID1)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send second message")
	fph.RefuteBlocks()
	fph.AssertResponses(expectedResponses{
		requestID1: graphsync.RequestCompletedFull,
		requestID2: graphsync.PartialResponse,
	})

	peerResponseSender.SendResponse(requestID2, links[3], blks[3].RawData())
	peerResponseSender.FinishRequest(requestID2)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send third message")
	fph.AssertBlocks(blks[3])
	fph.AssertResponses(expectedResponses{requestID2: graphsync.RequestCompletedFull})

}

func TestPeerResponseSenderDupKeys(t *testing.T) {
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
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	peerResponseSender.DedupKey(requestID1, "applesauce")
	peerResponseSender.DedupKey(requestID3, "applesauce")

	bd := peerResponseSender.SendResponse(requestID1, links[0], blks[0].RawData())
	assertSentOnWire(t, bd, blks[0])

	fph.AssertHasMessage("did not send first message")
	fph.AssertBlocks(blks[0])
	fph.AssertResponses(expectedResponses{requestID1: graphsync.PartialResponse})

	bd = peerResponseSender.SendResponse(requestID2, links[0], blks[0].RawData())
	assertSentOnWire(t, bd, blks[0])
	bd = peerResponseSender.SendResponse(requestID1, links[1], blks[1].RawData())
	assertSentOnWire(t, bd, blks[1])
	bd = peerResponseSender.SendResponse(requestID1, links[2], nil)
	assertNotSent(t, bd, blks[2])

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send second message")
	fph.AssertBlocks(blks[0], blks[1])
	fph.AssertResponses(expectedResponses{
		requestID1: graphsync.PartialResponse,
		requestID2: graphsync.PartialResponse,
	})

	peerResponseSender.SendResponse(requestID2, links[3], blks[3].RawData())
	peerResponseSender.SendResponse(requestID3, links[4], blks[4].RawData())
	peerResponseSender.FinishRequest(requestID2)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send third message")
	fph.AssertBlocks(blks[3], blks[4])
	fph.AssertResponses(expectedResponses{
		requestID2: graphsync.RequestCompletedFull,
		requestID3: graphsync.PartialResponse,
	})

	peerResponseSender.SendResponse(requestID3, links[0], blks[0].RawData())
	peerResponseSender.SendResponse(requestID3, links[4], blks[4].RawData())

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifySuccess()

	fph.AssertHasMessage("did not send fourth message")
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
	require.Equal(t, cidlink.Link{Cid: blk.Cid()}, bd.Link())
	require.Equal(t, uint64(len(blk.RawData())), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
}

func assertSentOnWire(t *testing.T, bd graphsync.BlockData, blk blocks.Block) {
	require.Equal(t, cidlink.Link{Cid: blk.Cid()}, bd.Link())
	require.Equal(t, uint64(len(blk.RawData())), bd.BlockSize())
	require.Equal(t, uint64(len(blk.RawData())), bd.BlockSizeOnWire())
}

func assertNotSent(t *testing.T, bd graphsync.BlockData, blk blocks.Block) {
	require.Equal(t, cidlink.Link{Cid: blk.Cid()}, bd.Link())
	require.Equal(t, uint64(0), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
}

type fakePeerHandler struct {
	ctx              context.Context
	t                *testing.T
	lastBlocks       []blocks.Block
	lastResponses    []gsmsg.GraphSyncResponse
	sent             chan struct{}
	notifeePublisher *testutil.MockPublisher
}

func newFakePeerHandler(ctx context.Context, t *testing.T) *fakePeerHandler {
	return &fakePeerHandler{
		ctx:              ctx,
		t:                t,
		sent:             make(chan struct{}, 1),
		notifeePublisher: testutil.NewMockPublisher(),
	}
}

func (fph *fakePeerHandler) AssertHasMessage(expectationCheck string) {
	testutil.AssertDoesReceive(fph.ctx, fph.t, fph.sent, expectationCheck)
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

func (fph *fakePeerHandler) RefuteResponses() {
	require.Empty(fph.t, fph.lastResponses)
}

func (fph *fakePeerHandler) SendResponse(p peer.ID, responses []gsmsg.GraphSyncResponse, blks []blocks.Block, notifees ...notifications.Notifee) {
	fph.lastResponses = responses
	fph.lastBlocks = blks
	fph.notifeePublisher.AddNotifees(notifees)
	fph.sent <- struct{}{}
}

func (fph *fakePeerHandler) notifySuccess() {
	fph.notifeePublisher.PublishEvents([]notifications.Event{messagequeue.Event{Name: messagequeue.Queued}, messagequeue.Event{Name: messagequeue.Sent}})
}

func (fph *fakePeerHandler) notifyError() {
	fph.notifeePublisher.PublishEvents([]notifications.Event{messagequeue.Event{Name: messagequeue.Queued}, messagequeue.Event{Name: messagequeue.Error, Err: errors.New("something went wrong")}})
}
