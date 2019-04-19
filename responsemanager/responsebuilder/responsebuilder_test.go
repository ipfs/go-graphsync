package responsebuilder

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ipld/go-ipld-prime/fluent"

	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking/cid"
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

	rb.AddCompletedRequest(requestID2, gsmsg.RequestCompletedFull)

	rb.AddLink(requestID3, links[0], true)
	rb.AddLink(requestID3, links[1], true)

	rb.AddCompletedRequest(requestID4, gsmsg.RequestCompletedFull)

	for _, block := range blocks {
		rb.AddBlock(block)
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

	response1Metadata, err := ipldBridge.DecodeNode(response1.Extra())
	if err != nil {
		t.Fatal("unable to read metadata from response")
	}
	analyzeMetadata(t, response1Metadata, map[ipld.Link]bool{
		links[0]: true,
		links[1]: false,
		links[2]: true,
	})
	response2, err := findResponseForRequestID(responses, requestID2)
	if err != nil || response2.Status() != gsmsg.RequestCompletedFull {
		t.Fatal("did not generate completed partial response")
	}
	response2Metadata, err := ipldBridge.DecodeNode(response2.Extra())
	if err != nil {
		t.Fatal("unable to read metadata from response")
	}
	analyzeMetadata(t, response2Metadata, map[ipld.Link]bool{
		links[1]: true,
		links[2]: true,
	})

	response3, err := findResponseForRequestID(responses, requestID3)
	if err != nil || response3.Status() != gsmsg.PartialResponse {
		t.Fatal("did not generate completed partial response")
	}
	response3Metadata, err := ipldBridge.DecodeNode(response3.Extra())
	if err != nil {
		t.Fatal("unable to read metadata from response")
	}
	analyzeMetadata(t, response3Metadata, map[ipld.Link]bool{
		links[0]: true,
		links[1]: true,
	})

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

func analyzeMetadata(t *testing.T, metadata ipld.Node, expectedMetadata map[ipld.Link]bool) {
	if metadata.Length() != len(expectedMetadata) {
		t.Fatal("Wrong amount of metadata on first response")
	}
	err := fluent.Recover(func() {
		safeMetadata := fluent.WrapNode(metadata)
		for i := 0; i < len(expectedMetadata); i++ {
			metadatum := safeMetadata.TraverseIndex(i)
			link := metadatum.TraverseField("link").AsLink()
			blockPresent := metadatum.TraverseField("blockPresent").AsBool()
			expectedBlockPresent, ok := expectedMetadata[link]
			if !ok || expectedBlockPresent != blockPresent {
				t.Fatal("Metadata did not match expected")
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}
