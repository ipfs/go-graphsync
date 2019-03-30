package peerresponsemanager

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-graphsync/testbridge"

	"github.com/ipfs/go-block-format"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking/cid"
	peer "github.com/libp2p/go-libp2p-peer"
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
	ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	requestID1 := gsmsg.GraphSyncRequestID(rand.Int31())
	requestID2 := gsmsg.GraphSyncRequestID(rand.Int31())
	requestID3 := gsmsg.GraphSyncRequestID(rand.Int31())
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
	ipldBridge := testbridge.NewMockIPLDBridge()
	peerResponseManager := New(ctx, p, fph, ipldBridge)
	peerResponseManager.Startup()

	peerResponseManager.SendResponse(requestID1, links[0], blks[0].RawData())

	select {
	case <-ctx.Done():
		t.Fatal("Did not send first message")
	case <-sent:
	}

	if len(fph.lastBlocks) != 1 || fph.lastBlocks[0].Cid() != blks[0].Cid() {
		t.Fatal("Did not send correct blocks for first message")
	}

	if len(fph.lastResponses) != 1 || fph.lastResponses[0].RequestID() != requestID1 ||
		fph.lastResponses[0].Status() != gsmsg.PartialResponse {
		t.Fatal("Did not send correct responses for first message")
	}

	peerResponseManager.SendResponse(requestID2, links[0], blks[0].RawData())
	peerResponseManager.SendResponse(requestID1, links[1], blks[1].RawData())
	peerResponseManager.SendResponse(requestID1, links[2], nil)
	peerResponseManager.FinishRequest(requestID1)

	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

	select {
	case <-ctx.Done():
		t.Fatal("Should have sent second message but didn't")
	case <-sent:
	}

	if len(fph.lastBlocks) != 1 || fph.lastBlocks[0].Cid() != blks[1].Cid() {
		t.Fatal("Did not dedup blocks correctly on second message")
	}

	if len(fph.lastResponses) != 2 {
		t.Fatal("Did not send correct number of responses")
	}
	response1, err := findResponseForRequestID(fph.lastResponses, requestID1)
	if err != nil {
		t.Fatal("Did not send correct response for second message")
	}
	if response1.Status() != gsmsg.RequestCompletedPartial {
		t.Fatal("Did not send proper response code in second message")
	}
	response2, err := findResponseForRequestID(fph.lastResponses, requestID2)
	if err != nil {
		t.Fatal("Did not send correct response for second message")
	}
	if response2.Status() != gsmsg.PartialResponse {
		t.Fatal("Did not send proper response code in second message")
	}

	peerResponseManager.SendResponse(requestID2, links[3], blks[3].RawData())
	peerResponseManager.SendResponse(requestID3, links[4], blks[4].RawData())
	peerResponseManager.FinishRequest(requestID2)

	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

	select {
	case <-ctx.Done():
		t.Fatal("Should have sent third message but didn't")
	case <-sent:
	}

	if len(fph.lastBlocks) != 2 ||
		!testutil.ContainsBlock(fph.lastBlocks, blks[3]) ||
		!testutil.ContainsBlock(fph.lastBlocks, blks[4]) {
		t.Fatal("Did not send correct blocks for third message")
	}

	if len(fph.lastResponses) != 2 {
		t.Fatal("Did not send correct number of responses")
	}
	response2, err = findResponseForRequestID(fph.lastResponses, requestID2)
	if err != nil {
		t.Fatal("Did not send correct response for third message")
	}
	if response2.Status() != gsmsg.RequestCompletedFull {
		t.Fatal("Did not send proper response code in third message")
	}
	response3, err := findResponseForRequestID(fph.lastResponses, requestID3)
	if err != nil {
		t.Fatal("Did not send correct response for third message")
	}
	if response3.Status() != gsmsg.PartialResponse {
		t.Fatal("Did not send proper response code in third message")
	}

	peerResponseManager.SendResponse(requestID3, links[0], blks[0].RawData())
	peerResponseManager.SendResponse(requestID3, links[4], blks[4].RawData())

	// let peer reponse manager know last message was sent so message sending can continue
	done <- struct{}{}

	select {
	case <-ctx.Done():
		t.Fatal("Should have sent third message but didn't")
	case <-sent:
	}

	if len(fph.lastBlocks) != 1 || fph.lastBlocks[0].Cid() != blks[0].Cid() {
		t.Fatal("Should have resent block cause there were no in progress requests but did not")
	}

	if len(fph.lastResponses) != 1 || fph.lastResponses[0].RequestID() != requestID3 ||
		fph.lastResponses[0].Status() != gsmsg.PartialResponse {
		t.Fatal("Did not send correct responses for fourth message")
	}
}

func findResponseForRequestID(responses []gsmsg.GraphSyncResponse, requestID gsmsg.GraphSyncRequestID) (gsmsg.GraphSyncResponse, error) {
	for _, response := range responses {
		if response.RequestID() == requestID {
			return response, nil
		}
	}
	return gsmsg.GraphSyncResponse{}, fmt.Errorf("Response Not Found")
}
