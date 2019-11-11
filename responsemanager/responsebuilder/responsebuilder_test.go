package responsebuilder

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func TestMessageBuilding(t *testing.T) {
	ipldBridge := testbridge.NewMockIPLDBridge()
	rb := New()
	blocks := testutil.GenerateBlocksOfSize(3, 100)
	links := make([]ipld.Link, 0, len(blocks))
	for _, block := range blocks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	requestID1 := gsmsg.GraphSyncRequestID(rand.Int31())
	requestID2 := gsmsg.GraphSyncRequestID(rand.Int31())
	requestID3 := gsmsg.GraphSyncRequestID(rand.Int31())
	requestID4 := gsmsg.GraphSyncRequestID(rand.Int31())

	rb.AddLink(requestID1, links[0], true)
	rb.AddLink(requestID1, links[1], false)
	rb.AddLink(requestID1, links[2], true)

	rb.AddCompletedRequest(requestID1, gsmsg.RequestCompletedPartial)

	rb.AddLink(requestID2, links[1], true)
	rb.AddLink(requestID2, links[2], true)
	rb.AddLink(requestID2, links[1], true)

	rb.AddCompletedRequest(requestID2, gsmsg.RequestCompletedFull)

	rb.AddLink(requestID3, links[0], true)
	rb.AddLink(requestID3, links[1], true)

	rb.AddCompletedRequest(requestID4, gsmsg.RequestCompletedFull)

	for _, block := range blocks {
		rb.AddBlock(block)
	}

	if rb.BlockSize() != 300 {
		t.Fatal("did not calculate block size correctly")
	}
	responses, sentBlocks, err := rb.Build(ipldBridge)

	if err != nil {
		t.Fatal("Error building responses")
	}

	if len(responses) != 4 {
		t.Fatal("Assembled wrong number of responses")
	}

	response1, err := findResponseForRequestID(responses, requestID1)
	if err != nil || response1.Status() != gsmsg.RequestCompletedPartial {
		t.Fatal("did not generate completed partial response")
	}

	response1MetadataRaw, found := response1.Extension(gsmsg.ExtensionMetadata)
	if !found {
		t.Fatal("Metadata not included in response")
	}
	response1Metadata, err := metadata.DecodeMetadata(response1MetadataRaw, ipldBridge)
	if err != nil || !reflect.DeepEqual(response1Metadata, metadata.Metadata{
		metadata.Item{Link: links[0], BlockPresent: true},
		metadata.Item{Link: links[1], BlockPresent: false},
		metadata.Item{Link: links[2], BlockPresent: true},
	}) {
		t.Fatal("Metadata did not match expected")
	}

	response2, err := findResponseForRequestID(responses, requestID2)
	if err != nil || response2.Status() != gsmsg.RequestCompletedFull {
		t.Fatal("did not generate completed partial response")
	}
	response2MetadataRaw, found := response2.Extension(gsmsg.ExtensionMetadata)
	if !found {
		t.Fatal("Metadata not included in response")
	}
	response2Metadata, err := metadata.DecodeMetadata(response2MetadataRaw, ipldBridge)
	if err != nil || !reflect.DeepEqual(response2Metadata, metadata.Metadata{
		metadata.Item{Link: links[1], BlockPresent: true},
		metadata.Item{Link: links[2], BlockPresent: true},
		metadata.Item{Link: links[1], BlockPresent: true},
	}) {
		t.Fatal("Metadata did not match expected")
	}

	response3, err := findResponseForRequestID(responses, requestID3)
	if err != nil || response3.Status() != gsmsg.PartialResponse {
		t.Fatal("did not generate completed partial response")
	}
	response3MetadataRaw, found := response3.Extension(gsmsg.ExtensionMetadata)
	if !found {
		t.Fatal("Metadata not included in response")
	}
	response3Metadata, err := metadata.DecodeMetadata(response3MetadataRaw, ipldBridge)
	if err != nil || !reflect.DeepEqual(response3Metadata, metadata.Metadata{
		metadata.Item{Link: links[0], BlockPresent: true},
		metadata.Item{Link: links[1], BlockPresent: true},
	}) {
		t.Fatal("Metadata did not match expected")
	}

	response4, err := findResponseForRequestID(responses, requestID4)
	if err != nil || response4.Status() != gsmsg.RequestCompletedFull {
		t.Fatal("did not generate completed partial response")
	}

	if len(sentBlocks) != len(blocks) {
		t.Fatal("Did not send all blocks")
	}

	for _, block := range sentBlocks {
		if !testutil.ContainsBlock(blocks, block) {
			t.Fatal("Sent incorrect block")
		}
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
