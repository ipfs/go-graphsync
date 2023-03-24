package message

import (
	"io"
	"testing"

	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/stretchr/testify/require"

	graphsync "github.com/filecoin-project/boost-graphsync"
	"github.com/filecoin-project/boost-graphsync/testutil"
)

func TestMessageBuilding(t *testing.T) {
	blocks := testutil.GenerateBlocksOfSize(3, 100)
	links := make([]ipld.Link, 0, len(blocks))
	for _, block := range blocks {
		links = append(links, cidlink.Link{Cid: block.Cid()})
	}
	bb := basicnode.Prototype.Bytes.NewBuilder()
	bb.AssignBytes(testutil.RandomBytes(100))
	extensionName1 := graphsync.ExtensionName("AppleSauce/McGee")
	extension1 := graphsync.ExtensionData{
		Name: extensionName1,
		Data: bb.Build(),
	}
	bb = basicnode.Prototype.Bytes.NewBuilder()
	bb.AssignBytes(testutil.RandomBytes(100))
	extensionName2 := graphsync.ExtensionName("HappyLand/Happenstance")
	extension2 := graphsync.ExtensionData{
		Name: extensionName2,
		Data: bb.Build(),
	}
	requestID1 := graphsync.NewRequestID()
	requestID2 := graphsync.NewRequestID()
	requestID3 := graphsync.NewRequestID()
	requestID4 := graphsync.NewRequestID()
	closer := io.NopCloser(nil)
	testCases := map[string]struct {
		build           func(*Builder)
		expectedSize    uint64
		expectedStreams map[graphsync.RequestID]io.Closer
		checkMsg        func(t *testing.T, message GraphSyncMessage)
	}{
		"normal building": {
			build: func(rb *Builder) {

				rb.AddLink(requestID1, links[0], graphsync.LinkActionPresent)
				rb.AddLink(requestID1, links[1], graphsync.LinkActionMissing)
				rb.AddLink(requestID1, links[2], graphsync.LinkActionPresent)

				rb.AddResponseCode(requestID1, graphsync.RequestCompletedPartial)

				rb.AddLink(requestID2, links[1], graphsync.LinkActionPresent)
				rb.AddLink(requestID2, links[2], graphsync.LinkActionPresent)
				rb.AddLink(requestID2, links[1], graphsync.LinkActionPresent)

				rb.AddResponseCode(requestID2, graphsync.RequestCompletedFull)

				rb.AddLink(requestID3, links[0], graphsync.LinkActionPresent)
				rb.AddLink(requestID3, links[1], graphsync.LinkActionDuplicateNotSent)

				rb.AddResponseCode(requestID4, graphsync.RequestCompletedFull)
				rb.AddExtensionData(requestID1, extension1)
				rb.AddExtensionData(requestID3, extension2)
				for _, block := range blocks {
					rb.AddBlock(block)
				}
			},
			expectedSize: 300,
			expectedStreams: map[graphsync.RequestID]io.Closer{
				requestID1: closer,
				requestID2: closer,
			},
			checkMsg: func(t *testing.T, message GraphSyncMessage) {

				responses := message.Responses()
				sentBlocks := message.Blocks()
				require.Len(t, responses, 4, "did not assemble correct number of responses")

				response1 := findResponseForRequestID(t, responses, requestID1)
				require.Equal(t, graphsync.RequestCompletedPartial, response1.Status(), "did not generate completed partial response")
				assertMetadata(t, response1, []GraphSyncLinkMetadatum{
					{Link: links[0].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
					{Link: links[1].(cidlink.Link).Cid, Action: graphsync.LinkActionMissing},
					{Link: links[2].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
				})
				assertExtension(t, response1, extension1)

				response2 := findResponseForRequestID(t, responses, requestID2)
				require.Equal(t, graphsync.RequestCompletedFull, response2.Status(), "did not generate completed full response")
				assertMetadata(t, response2, []GraphSyncLinkMetadatum{
					{Link: links[1].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
					{Link: links[2].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
					{Link: links[1].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
				})

				response3 := findResponseForRequestID(t, responses, requestID3)
				require.Equal(t, graphsync.PartialResponse, response3.Status(), "did not generate partial response")
				assertMetadata(t, response3, []GraphSyncLinkMetadatum{
					{Link: links[0].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
					{Link: links[1].(cidlink.Link).Cid, Action: graphsync.LinkActionDuplicateNotSent},
				})
				assertExtension(t, response3, extension2)

				response4 := findResponseForRequestID(t, responses, requestID4)
				require.Equal(t, graphsync.RequestCompletedFull, response4.Status(), "did not generate completed full response")

				require.Equal(t, len(blocks), len(sentBlocks), "did not send all blocks")

				for _, block := range sentBlocks {
					testutil.AssertContainsBlock(t, blocks, block)
				}
			},
		},
		"message with only extensions": {
			build: func(rb *Builder) {
				rb.AddExtensionData(requestID1, extension1)
				rb.AddExtensionData(requestID2, extension2)
			},
			expectedSize: 0,
			checkMsg: func(t *testing.T, message GraphSyncMessage) {
				responses := message.Responses()

				response1 := findResponseForRequestID(t, responses, requestID1)
				require.Equal(t, graphsync.PartialResponse, response1.Status(), "did not generate partial response")
				assertMetadata(t, response1, nil)
				assertExtension(t, response1, extension1)

				response2 := findResponseForRequestID(t, responses, requestID2)
				require.Equal(t, graphsync.PartialResponse, response2.Status(), "did not generate partial response")
				assertMetadata(t, response2, nil)
				assertExtension(t, response2, extension2)
			},
		},
		"scrub response": {
			build: func(rb *Builder) {

				rb.AddLink(requestID1, links[0], graphsync.LinkActionPresent)
				rb.AddLink(requestID1, links[1], graphsync.LinkActionMissing)
				rb.AddLink(requestID1, links[2], graphsync.LinkActionPresent)

				rb.AddResponseCode(requestID1, graphsync.RequestCompletedPartial)

				rb.AddLink(requestID2, links[1], graphsync.LinkActionPresent)
				rb.AddLink(requestID2, links[2], graphsync.LinkActionPresent)
				rb.AddLink(requestID2, links[1], graphsync.LinkActionPresent)

				rb.AddResponseCode(requestID2, graphsync.RequestCompletedFull)

				rb.AddLink(requestID3, links[1], graphsync.LinkActionPresent)

				rb.AddResponseCode(requestID4, graphsync.RequestCompletedFull)
				rb.AddExtensionData(requestID1, extension1)
				rb.AddExtensionData(requestID3, extension2)
				for _, block := range blocks {
					rb.AddBlock(block)
				}
				rb.ScrubResponses([]graphsync.RequestID{requestID1})
			},
			expectedSize: 200,
			checkMsg: func(t *testing.T, message GraphSyncMessage) {

				responses := message.Responses()
				sentBlocks := message.Blocks()
				require.Len(t, responses, 3, "did not assemble correct number of responses")

				response2 := findResponseForRequestID(t, responses, requestID2)
				require.Equal(t, graphsync.RequestCompletedFull, response2.Status(), "did not generate completed full response")
				assertMetadata(t, response2, []GraphSyncLinkMetadatum{
					{Link: links[1].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
					{Link: links[2].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
					{Link: links[1].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
				})

				response3 := findResponseForRequestID(t, responses, requestID3)
				require.Equal(t, graphsync.PartialResponse, response3.Status(), "did not generate partial response")
				assertMetadata(t, response3, []GraphSyncLinkMetadatum{
					{Link: links[1].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
				})
				assertExtension(t, response3, extension2)

				response4 := findResponseForRequestID(t, responses, requestID4)
				require.Equal(t, graphsync.RequestCompletedFull, response4.Status(), "did not generate completed full response")

				require.Equal(t, len(blocks)-1, len(sentBlocks), "did not send all blocks")

				testutil.AssertContainsBlock(t, sentBlocks, blocks[1])
				testutil.AssertContainsBlock(t, sentBlocks, blocks[2])
				testutil.RefuteContainsBlock(t, sentBlocks, blocks[0])

			},
		},
		"scrub multiple responses": {
			build: func(rb *Builder) {

				rb.AddLink(requestID1, links[0], graphsync.LinkActionPresent)
				rb.AddLink(requestID1, links[1], graphsync.LinkActionMissing)
				rb.AddLink(requestID1, links[2], graphsync.LinkActionPresent)

				rb.AddResponseCode(requestID1, graphsync.RequestCompletedPartial)

				rb.AddLink(requestID2, links[1], graphsync.LinkActionPresent)
				rb.AddLink(requestID2, links[2], graphsync.LinkActionPresent)
				rb.AddLink(requestID2, links[1], graphsync.LinkActionPresent)

				rb.AddResponseCode(requestID2, graphsync.RequestCompletedFull)

				rb.AddLink(requestID3, links[1], graphsync.LinkActionPresent)

				rb.AddResponseCode(requestID4, graphsync.RequestCompletedFull)
				rb.AddExtensionData(requestID1, extension1)
				rb.AddExtensionData(requestID3, extension2)
				for _, block := range blocks {
					rb.AddBlock(block)
				}
				rb.ScrubResponses([]graphsync.RequestID{requestID1, requestID2, requestID4})
			},
			expectedSize: 100,
			checkMsg: func(t *testing.T, message GraphSyncMessage) {

				responses := message.Responses()
				sentBlocks := message.Blocks()
				require.Len(t, responses, 1, "did not assemble correct number of responses")

				response3 := findResponseForRequestID(t, responses, requestID3)
				require.Equal(t, graphsync.PartialResponse, response3.Status(), "did not generate partial response")
				assertMetadata(t, response3, []GraphSyncLinkMetadatum{
					{Link: links[1].(cidlink.Link).Cid, Action: graphsync.LinkActionPresent},
				})
				assertExtension(t, response3, extension2)

				testutil.AssertContainsBlock(t, sentBlocks, blocks[1])
				testutil.RefuteContainsBlock(t, sentBlocks, blocks[2])
				testutil.RefuteContainsBlock(t, sentBlocks, blocks[0])
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			b := NewBuilder()
			data.build(b)
			require.Equal(t, data.expectedSize, b.BlockSize(), "did not calculate block size correctly")
			message, err := b.Build()
			require.NoError(t, err, "build message errored")
			data.checkMsg(t, message)
		})
	}
}

func findResponseForRequestID(t *testing.T, responses []GraphSyncResponse, requestID graphsync.RequestID) GraphSyncResponse {
	for _, response := range responses {
		if response.RequestID() == requestID {
			return response
		}
	}
	require.FailNow(t, "Could not find request")
	return GraphSyncResponse{}
}

func assertExtension(t *testing.T, response GraphSyncResponse, extension graphsync.ExtensionData) {
	returnedExtensionData, found := response.Extension(extension.Name)
	require.True(t, found)
	require.Equal(t, extension.Data, returnedExtensionData, "did not encode extension")
}

func assertMetadata(t *testing.T, response GraphSyncResponse, expectedMetadata []GraphSyncLinkMetadatum) {
	require.Equal(t, expectedMetadata, response.metadata, "incorrect metadata included in response")
}
