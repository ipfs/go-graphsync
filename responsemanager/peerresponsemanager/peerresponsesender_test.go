package peerresponsemanager

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
	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/testutil"
)

type fakePeerHandler struct {
	lastBlocks    []blocks.Block
	lastResponses []gsmsg.GraphSyncResponse
	sent          chan struct{}
	notifees      []notifications.Notifee
}

func (fph *fakePeerHandler) SendResponse(p peer.ID, responses []gsmsg.GraphSyncResponse, blks []blocks.Block, notifees ...notifications.Notifee) {
	fph.lastResponses = responses
	fph.lastBlocks = blks
	fph.sent <- struct{}{}
	fph.notifees = append(fph.notifees, notifees...)
}

func (fph *fakePeerHandler) notifyQueue(topic notifications.Topic) {
	var newNotifees []notifications.Notifee
	for _, notifee := range fph.notifees {
		if notifee.Topic == topic {
			notifee.Subscriber.OnNext(topic, messagequeue.Event{Name: messagequeue.Queued})
		} else {
			newNotifees = append(newNotifees, notifee)
		}
	}
	fph.notifees = newNotifees
}

func TestPeerResponseSenderSendsResponses(t *testing.T) {
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
	sent := make(chan struct{}, 1)
	fph := &fakePeerHandler{
		sent: sent,
	}
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	bd := peerResponseSender.SendResponse(requestID1, links[0], blks[0].RawData())
	require.Equal(t, links[0], bd.Link())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSize())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSizeOnWire())
	testutil.AssertDoesReceive(ctx, t, sent, "did not send first message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[0].Cid(), fph.lastBlocks[0].Cid(), "did not send correct blocks for first message")

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID1, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())

	require.Len(t, fph.notifees, 1)
	topic := fph.notifees[0].Topic

	bd = peerResponseSender.SendResponse(requestID2, links[0], blks[0].RawData())
	require.Equal(t, links[0], bd.Link())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
	bd = peerResponseSender.SendResponse(requestID1, links[1], blks[1].RawData())
	require.Equal(t, links[1], bd.Link())
	require.Equal(t, uint64(len(blks[1].RawData())), bd.BlockSize())
	require.Equal(t, uint64(len(blks[1].RawData())), bd.BlockSizeOnWire())
	bd = peerResponseSender.SendResponse(requestID1, links[2], nil)
	require.Equal(t, links[2], bd.Link())
	require.Equal(t, uint64(0), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
	peerResponseSender.FinishRequest(requestID1)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send second message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[1].Cid(), fph.lastBlocks[0].Cid(), "did not dedup blocks correctly on second message")

	require.Len(t, fph.lastResponses, 2, "did not send correct number of responses")
	response1, err := findResponseForRequestID(fph.lastResponses, requestID1)
	require.NoError(t, err)
	require.Equal(t, graphsync.RequestCompletedPartial, response1.Status(), "did not send correct response code in second message")
	response2, err := findResponseForRequestID(fph.lastResponses, requestID2)
	require.NoError(t, err)
	require.Equal(t, graphsync.PartialResponse, response2.Status(), "did not send corrent response code in second message")

	require.Len(t, fph.notifees, 1)
	topic = fph.notifees[0].Topic
	peerResponseSender.SendResponse(requestID2, links[3], blks[3].RawData())
	peerResponseSender.SendResponse(requestID3, links[4], blks[4].RawData())
	peerResponseSender.FinishRequest(requestID2)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send third message")

	require.Equal(t, 2, len(fph.lastBlocks))
	testutil.AssertContainsBlock(t, fph.lastBlocks, blks[3])
	testutil.AssertContainsBlock(t, fph.lastBlocks, blks[4])

	require.Len(t, fph.lastResponses, 2, "did not send correct number of responses")
	response2, err = findResponseForRequestID(fph.lastResponses, requestID2)
	require.NoError(t, err)
	require.Equal(t, graphsync.RequestCompletedFull, response2.Status(), "did not send correct response code in third message")
	response3, err := findResponseForRequestID(fph.lastResponses, requestID3)
	require.NoError(t, err)
	require.Equal(t, graphsync.PartialResponse, response3.Status(), "did not send correct response code in third message")
	require.Len(t, fph.notifees, 1)
	topic = fph.notifees[0].Topic

	peerResponseSender.SendResponse(requestID3, links[0], blks[0].RawData())
	peerResponseSender.SendResponse(requestID3, links[4], blks[4].RawData())

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send fourth message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[0].Cid(), fph.lastBlocks[0].Cid(), "Should resend block cause there were no in progress requests")

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID3, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())
	require.Len(t, fph.notifees, 1)
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
	sent := make(chan struct{}, 1)
	fph := &fakePeerHandler{
		sent: sent,
	}
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	peerResponseSender.SendResponse(requestID1, links[0], blks[0].RawData())

	testutil.AssertDoesReceive(ctx, t, sent, "did not send first message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[0].Cid(), fph.lastBlocks[0].Cid(), "did not send correct blocks for first message")

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID1, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())
	require.Len(t, fph.notifees, 1)
	topic := fph.notifees[0].Topic

	// Send 3 very large blocks
	peerResponseSender.SendResponse(requestID1, links[1], blks[1].RawData())
	peerResponseSender.SendResponse(requestID1, links[2], blks[2].RawData())
	peerResponseSender.SendResponse(requestID1, links[3], blks[3].RawData())

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send second message ")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[1].Cid(), fph.lastBlocks[0].Cid(), "Should break up message")

	require.Len(t, fph.lastResponses, 1, "Should break up message")
	require.Len(t, fph.notifees, 1)
	topic = fph.notifees[0].Topic

	// Send one more block while waiting
	peerResponseSender.SendResponse(requestID1, links[4], blks[4].RawData())
	peerResponseSender.FinishRequest(requestID1)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send third message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[2].Cid(), fph.lastBlocks[0].Cid(), "should break up message")

	require.Len(t, fph.lastResponses, 1, "should break up message")
	require.Len(t, fph.notifees, 1)
	topic = fph.notifees[0].Topic

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send fourth message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[3].Cid(), fph.lastBlocks[0].Cid(), "should break up message")

	require.Len(t, fph.lastResponses, 1, "should break up message")
	require.Len(t, fph.notifees, 1)
	topic = fph.notifees[0].Topic

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send fifth message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[4].Cid(), fph.lastBlocks[0].Cid(), "should break up message")

	require.Len(t, fph.lastResponses, 1, "should break up message")
	require.Len(t, fph.notifees, 1)

	response, err := findResponseForRequestID(fph.lastResponses, requestID1)
	require.NoError(t, err)
	require.Equal(t, graphsync.RequestCompletedFull, response.Status(), "did not send corrent response code in fifth message")

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
	sent := make(chan struct{}, 1)
	fph := &fakePeerHandler{
		sent: sent,
	}
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	peerResponseSender.SendResponse(requestID1, links[0], blks[0].RawData())

	testutil.AssertDoesReceive(ctx, t, sent, "did not send first message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[0].Cid(), fph.lastBlocks[0].Cid(), "did not send correct blocks for first message")

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID1, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())
	require.Len(t, fph.notifees, 1)
	topic := fph.notifees[0].Topic

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
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send second message")

	require.Len(t, fph.lastResponses, 1, "did not send correct number of responses for second message")

	lastResponse := fph.lastResponses[0]
	returnedData1, found := lastResponse.Extension(extensionName1)
	require.True(t, found)
	require.Equal(t, extensionData1, returnedData1, "did not encode first extension")

	returnedData2, found := lastResponse.Extension(extensionName2)
	require.True(t, found)
	require.Equal(t, extensionData2, returnedData2, "did not encode first extension")

	require.Len(t, fph.notifees, 1)

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
	sent := make(chan struct{}, 1)
	fph := &fakePeerHandler{
		sent: sent,
	}
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	err := peerResponseSender.Transaction(requestID1, func(peerResponseSender PeerResponseTransactionSender) error {
		bd := peerResponseSender.SendResponse(links[0], blks[0].RawData())
		require.Equal(t, links[0], bd.Link())
		require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSize())
		require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSizeOnWire())

		timer := time.NewTimer(100 * time.Millisecond)
		testutil.AssertDoesReceiveFirst(t, timer.C, "should not send a message", sent)
		require.Len(t, fph.lastBlocks, 0)
		require.Len(t, fph.lastResponses, 0)

		bd = peerResponseSender.SendResponse(links[1], blks[1].RawData())
		require.Equal(t, links[1], bd.Link())
		require.Equal(t, uint64(len(blks[1].RawData())), bd.BlockSize())
		require.Equal(t, uint64(len(blks[1].RawData())), bd.BlockSizeOnWire())
		bd = peerResponseSender.SendResponse(links[2], nil)
		require.Equal(t, links[2], bd.Link())
		require.Equal(t, uint64(0), bd.BlockSize())
		require.Equal(t, uint64(0), bd.BlockSizeOnWire())
		peerResponseSender.FinishRequest()

		timer.Reset(100 * time.Millisecond)
		testutil.AssertDoesReceiveFirst(t, timer.C, "should not send a message", sent)
		return nil
	})
	require.NoError(t, err)
	testutil.AssertDoesReceive(ctx, t, sent, "should sent first message")
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
	sent := make(chan struct{}, 1)
	fph := &fakePeerHandler{
		sent: sent,
	}
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	peerResponseSender.IgnoreBlocks(requestID1, links)

	bd := peerResponseSender.SendResponse(requestID1, links[0], blks[0].RawData())
	require.Equal(t, links[0], bd.Link())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
	testutil.AssertDoesReceive(ctx, t, sent, "did not send first message")

	require.Len(t, fph.lastBlocks, 0)

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID1, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())
	require.Len(t, fph.notifees, 1)
	topic := fph.notifees[0].Topic

	bd = peerResponseSender.SendResponse(requestID2, links[0], blks[0].RawData())
	require.Equal(t, links[0], bd.Link())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
	bd = peerResponseSender.SendResponse(requestID1, links[1], blks[1].RawData())
	require.Equal(t, links[1], bd.Link())
	require.Equal(t, uint64(len(blks[1].RawData())), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
	bd = peerResponseSender.SendResponse(requestID1, links[2], blks[2].RawData())
	require.Equal(t, links[2], bd.Link())
	require.Equal(t, uint64(len(blks[2].RawData())), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
	peerResponseSender.FinishRequest(requestID1)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send second message")

	require.Len(t, fph.lastBlocks, 0)

	require.Len(t, fph.lastResponses, 2, "did not send correct number of responses")
	response1, err := findResponseForRequestID(fph.lastResponses, requestID1)
	require.NoError(t, err)
	require.Equal(t, graphsync.RequestCompletedFull, response1.Status(), "did not send correct response code in second message")
	response2, err := findResponseForRequestID(fph.lastResponses, requestID2)
	require.NoError(t, err)
	require.Equal(t, graphsync.PartialResponse, response2.Status(), "did not send corrent response code in second message")
	require.Len(t, fph.notifees, 1)
	topic = fph.notifees[0].Topic
	peerResponseSender.SendResponse(requestID2, links[3], blks[3].RawData())
	peerResponseSender.FinishRequest(requestID2)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send third message")

	require.Equal(t, 1, len(fph.lastBlocks))
	testutil.AssertContainsBlock(t, fph.lastBlocks, blks[3])

	require.Len(t, fph.lastResponses, 1, "did not send correct number of responses")
	response2, err = findResponseForRequestID(fph.lastResponses, requestID2)
	require.NoError(t, err)
	require.Equal(t, graphsync.RequestCompletedFull, response2.Status(), "did not send correct response code in third message")

	require.Len(t, fph.notifees, 1)
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
	sent := make(chan struct{}, 1)
	fph := &fakePeerHandler{
		sent: sent,
	}
	peerResponseSender := NewResponseSender(ctx, p, fph)
	peerResponseSender.Startup()

	peerResponseSender.DedupKey(requestID1, "applesauce")
	peerResponseSender.DedupKey(requestID3, "applesauce")

	bd := peerResponseSender.SendResponse(requestID1, links[0], blks[0].RawData())
	require.Equal(t, links[0], bd.Link())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSize())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSizeOnWire())
	testutil.AssertDoesReceive(ctx, t, sent, "did not send first message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[0].Cid(), fph.lastBlocks[0].Cid(), "did not send correct blocks for first message")

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID1, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())

	require.Len(t, fph.notifees, 1)
	topic := fph.notifees[0].Topic

	bd = peerResponseSender.SendResponse(requestID2, links[0], blks[0].RawData())
	require.Equal(t, links[0], bd.Link())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSize())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSizeOnWire())
	bd = peerResponseSender.SendResponse(requestID1, links[1], blks[1].RawData())
	require.Equal(t, links[1], bd.Link())
	require.Equal(t, uint64(len(blks[1].RawData())), bd.BlockSize())
	require.Equal(t, uint64(len(blks[1].RawData())), bd.BlockSizeOnWire())
	bd = peerResponseSender.SendResponse(requestID1, links[2], nil)
	require.Equal(t, links[2], bd.Link())
	require.Equal(t, uint64(0), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send second message")

	require.Len(t, fph.lastBlocks, 2)
	require.Equal(t, blks[0].Cid(), fph.lastBlocks[0].Cid(), "did not dedup blocks correctly on second message")
	require.Equal(t, blks[1].Cid(), fph.lastBlocks[1].Cid(), "did not dedup blocks correctly on second message")

	require.Len(t, fph.lastResponses, 2, "did not send correct number of responses")
	response1, err := findResponseForRequestID(fph.lastResponses, requestID1)
	require.NoError(t, err)
	require.Equal(t, graphsync.PartialResponse, response1.Status(), "did not send correct response code in second message")
	response2, err := findResponseForRequestID(fph.lastResponses, requestID2)
	require.NoError(t, err)
	require.Equal(t, graphsync.PartialResponse, response2.Status(), "did not send corrent response code in second message")
	require.Len(t, fph.notifees, 1)
	topic = fph.notifees[0].Topic

	peerResponseSender.SendResponse(requestID2, links[3], blks[3].RawData())
	peerResponseSender.SendResponse(requestID3, links[4], blks[4].RawData())
	peerResponseSender.FinishRequest(requestID2)

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send third message")

	require.Equal(t, 2, len(fph.lastBlocks))
	testutil.AssertContainsBlock(t, fph.lastBlocks, blks[3])
	testutil.AssertContainsBlock(t, fph.lastBlocks, blks[4])

	require.Len(t, fph.lastResponses, 2, "did not send correct number of responses")
	response2, err = findResponseForRequestID(fph.lastResponses, requestID2)
	require.NoError(t, err)
	require.Equal(t, graphsync.RequestCompletedFull, response2.Status(), "did not send correct response code in third message")
	response3, err := findResponseForRequestID(fph.lastResponses, requestID3)
	require.NoError(t, err)
	require.Equal(t, graphsync.PartialResponse, response3.Status(), "did not send correct response code in third message")
	require.Len(t, fph.notifees, 1)
	topic = fph.notifees[0].Topic

	peerResponseSender.SendResponse(requestID3, links[0], blks[0].RawData())
	peerResponseSender.SendResponse(requestID3, links[4], blks[4].RawData())

	// let peer reponse manager know last message was sent so message sending can continue
	fph.notifyQueue(topic)

	testutil.AssertDoesReceive(ctx, t, sent, "did not send fourth message")

	require.Len(t, fph.lastBlocks, 0)

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID3, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())
	require.Len(t, fph.notifees, 1)

}

func findResponseForRequestID(responses []gsmsg.GraphSyncResponse, requestID graphsync.RequestID) (gsmsg.GraphSyncResponse, error) {
	for _, response := range responses {
		if response.RequestID() == requestID {
			return response, nil
		}
	}
	return gsmsg.GraphSyncResponse{}, fmt.Errorf("Response Not Found")
}
