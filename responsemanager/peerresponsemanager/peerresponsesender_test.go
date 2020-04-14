package peerresponsemanager

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync"
	"github.com/stretchr/testify/require"

	blocks "github.com/ipfs/go-block-format"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type fakePeerHandler struct {
	lastBlocks    []blocks.Block
	lastResponses []gsmsg.GraphSyncResponse
	sent          chan struct{}
	done          chan struct{}
}

func (fph *fakePeerHandler) SendResponse(p peer.ID, responses []gsmsg.GraphSyncResponse, blks []blocks.Block) <-chan struct{} {
	fph.lastResponses = responses
	fph.lastBlocks = blks
	fph.sent <- struct{}{}
	return fph.done
}

func TestPeerResponseManagerSendsResponses(t *testing.T) {
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
	done := make(chan struct{}, 1)
	sent := make(chan struct{}, 1)
	fph := &fakePeerHandler{
		done: done,
		sent: sent,
	}
	peerResponseManager := NewResponseSender(ctx, p, fph)
	peerResponseManager.Startup()

	bd := peerResponseManager.SendResponse(requestID1, links[0], blks[0].RawData())
	require.Equal(t, links[0], bd.Link())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSize())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSizeOnWire())
	testutil.AssertDoesReceive(ctx, t, sent, "did not send first message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[0].Cid(), fph.lastBlocks[0].Cid(), "did not send correct blocks for first message")

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID1, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())

	bd = peerResponseManager.SendResponse(requestID2, links[0], blks[0].RawData())
	require.Equal(t, links[0], bd.Link())
	require.Equal(t, uint64(len(blks[0].RawData())), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
	bd = peerResponseManager.SendResponse(requestID1, links[1], blks[1].RawData())
	require.Equal(t, links[1], bd.Link())
	require.Equal(t, uint64(len(blks[1].RawData())), bd.BlockSize())
	require.Equal(t, uint64(len(blks[1].RawData())), bd.BlockSizeOnWire())
	bd = peerResponseManager.SendResponse(requestID1, links[2], nil)
	require.Equal(t, links[2], bd.Link())
	require.Equal(t, uint64(0), bd.BlockSize())
	require.Equal(t, uint64(0), bd.BlockSizeOnWire())
	peerResponseManager.FinishRequest(requestID1)

	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

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

	peerResponseManager.SendResponse(requestID2, links[3], blks[3].RawData())
	peerResponseManager.SendResponse(requestID3, links[4], blks[4].RawData())
	peerResponseManager.FinishRequest(requestID2)

	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

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

	peerResponseManager.SendResponse(requestID3, links[0], blks[0].RawData())
	peerResponseManager.SendResponse(requestID3, links[4], blks[4].RawData())

	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

	testutil.AssertDoesReceive(ctx, t, sent, "did not send fourth message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[0].Cid(), fph.lastBlocks[0].Cid(), "Should resend block cause there were no in progress requests")

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID3, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())
}

func TestPeerResponseManagerSendsVeryLargeBlocksResponses(t *testing.T) {

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
	done := make(chan struct{}, 1)
	sent := make(chan struct{}, 1)
	fph := &fakePeerHandler{
		done: done,
		sent: sent,
	}
	peerResponseManager := NewResponseSender(ctx, p, fph)
	peerResponseManager.Startup()

	peerResponseManager.SendResponse(requestID1, links[0], blks[0].RawData())

	testutil.AssertDoesReceive(ctx, t, sent, "did not send first message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[0].Cid(), fph.lastBlocks[0].Cid(), "did not send correct blocks for first message")

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID1, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())

	// Send 3 very large blocks
	peerResponseManager.SendResponse(requestID1, links[1], blks[1].RawData())
	peerResponseManager.SendResponse(requestID1, links[2], blks[2].RawData())
	peerResponseManager.SendResponse(requestID1, links[3], blks[3].RawData())

	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

	testutil.AssertDoesReceive(ctx, t, sent, "did not send second message ")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[1].Cid(), fph.lastBlocks[0].Cid(), "Should break up message")

	require.Len(t, fph.lastResponses, 1, "Should break up message")

	// Send one more block while waiting
	peerResponseManager.SendResponse(requestID1, links[4], blks[4].RawData())
	peerResponseManager.FinishRequest(requestID1)

	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

	testutil.AssertDoesReceive(ctx, t, sent, "did not send third message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[2].Cid(), fph.lastBlocks[0].Cid(), "should break up message")

	require.Len(t, fph.lastResponses, 1, "should break up message")

	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

	testutil.AssertDoesReceive(ctx, t, sent, "did not send fourth message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[3].Cid(), fph.lastBlocks[0].Cid(), "should break up message")

	require.Len(t, fph.lastResponses, 1, "should break up message")

	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

	testutil.AssertDoesReceive(ctx, t, sent, "did not send fifth message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[4].Cid(), fph.lastBlocks[0].Cid(), "should break up message")

	require.Len(t, fph.lastResponses, 1, "should break up message")

	response, err := findResponseForRequestID(fph.lastResponses, requestID1)
	require.NoError(t, err)
	require.Equal(t, graphsync.RequestCompletedFull, response.Status(), "did not send corrent response code in fifth message")

}

func TestPeerResponseManagerSendsExtensionData(t *testing.T) {
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
	done := make(chan struct{}, 1)
	sent := make(chan struct{}, 1)
	fph := &fakePeerHandler{
		done: done,
		sent: sent,
	}
	peerResponseManager := NewResponseSender(ctx, p, fph)
	peerResponseManager.Startup()

	peerResponseManager.SendResponse(requestID1, links[0], blks[0].RawData())

	testutil.AssertDoesReceive(ctx, t, sent, "did not send first message")

	require.Len(t, fph.lastBlocks, 1)
	require.Equal(t, blks[0].Cid(), fph.lastBlocks[0].Cid(), "did not send correct blocks for first message")

	require.Len(t, fph.lastResponses, 1)
	require.Equal(t, requestID1, fph.lastResponses[0].RequestID())
	require.Equal(t, graphsync.PartialResponse, fph.lastResponses[0].Status())

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
	peerResponseManager.SendResponse(requestID1, links[1], blks[1].RawData())
	peerResponseManager.SendExtensionData(requestID1, extension1)
	peerResponseManager.SendExtensionData(requestID1, extension2)
	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

	testutil.AssertDoesReceive(ctx, t, sent, "did not send second message")

	require.Len(t, fph.lastResponses, 1, "did not send correct number of responses for second message")

	lastResponse := fph.lastResponses[0]
	returnedData1, found := lastResponse.Extension(extensionName1)
	require.True(t, found)
	require.Equal(t, extensionData1, returnedData1, "did not encode first extension")

	returnedData2, found := lastResponse.Extension(extensionName2)
	require.True(t, found)
	require.Equal(t, extensionData2, returnedData2, "did not encode first extension")
}

func findResponseForRequestID(responses []gsmsg.GraphSyncResponse, requestID graphsync.RequestID) (gsmsg.GraphSyncResponse, error) {
	for _, response := range responses {
		if response.RequestID() == requestID {
			return response, nil
		}
	}
	return gsmsg.GraphSyncResponse{}, fmt.Errorf("Response Not Found")
}
