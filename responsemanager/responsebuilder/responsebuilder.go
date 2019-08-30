package responsebuilder

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipld/go-ipld-prime"
)

// ResponseBuilder captures componenst of a response message across multiple
// requests for a given peer and then generates the corresponding
// GraphSync message components once responses are ready to send.
type ResponseBuilder struct {
	outgoingBlocks     []blocks.Block
	blkSize            int
	completedResponses map[gsmsg.GraphSyncRequestID]gsmsg.GraphSyncResponseStatusCode
	outgoingResponses  map[gsmsg.GraphSyncRequestID]metadata.Metadata
}

// New generates a new ResponseBuilder.
func New() *ResponseBuilder {
	return &ResponseBuilder{
		completedResponses: make(map[gsmsg.GraphSyncRequestID]gsmsg.GraphSyncResponseStatusCode),
		outgoingResponses:  make(map[gsmsg.GraphSyncRequestID]metadata.Metadata),
	}
}

// AddBlock adds the given block to the response.
func (rb *ResponseBuilder) AddBlock(block blocks.Block) {
	rb.blkSize += len(block.RawData())
	rb.outgoingBlocks = append(rb.outgoingBlocks, block)
}

// BlockSize returns the total size of all blocks in this response
func (rb *ResponseBuilder) BlockSize() int {
	return rb.blkSize
}

// AddLink adds the given link and whether its block is present
// to the response for the given request ID.
func (rb *ResponseBuilder) AddLink(requestID gsmsg.GraphSyncRequestID, link ipld.Link, blockPresent bool) {
	rb.outgoingResponses[requestID] = append(rb.outgoingResponses[requestID], metadata.Item{Link: link, BlockPresent: blockPresent})
}

// AddCompletedRequest marks the given request as completed in the response,
// as well as whether the graphsync request responded with complete or partial
// data.
func (rb *ResponseBuilder) AddCompletedRequest(requestID gsmsg.GraphSyncRequestID, status gsmsg.GraphSyncResponseStatusCode) {
	rb.completedResponses[requestID] = status
	// make sure this completion goes out in next response even if no links are sent
	_, ok := rb.outgoingResponses[requestID]
	if !ok {
		rb.outgoingResponses[requestID] = nil
	}
}

// Empty returns true if there is no content to send
func (rb *ResponseBuilder) Empty() bool {
	return len(rb.outgoingBlocks) == 0 && len(rb.outgoingResponses) == 0
}

// Build assembles and encodes response data from the added requests, links, and blocks.
func (rb *ResponseBuilder) Build(ipldBridge ipldbridge.IPLDBridge) ([]gsmsg.GraphSyncResponse, []blocks.Block, error) {
	responses := make([]gsmsg.GraphSyncResponse, 0, len(rb.outgoingResponses))
	for requestID, linkMap := range rb.outgoingResponses {
		extra, err := metadata.EncodeMetadata(linkMap, ipldBridge)
		if err != nil {
			return nil, nil, err
		}
		status, isComplete := rb.completedResponses[requestID]
		responses = append(responses, gsmsg.NewResponse(requestID, responseCode(status, isComplete), extra))
	}
	return responses, rb.outgoingBlocks, nil
}

func responseCode(status gsmsg.GraphSyncResponseStatusCode, isComplete bool) gsmsg.GraphSyncResponseStatusCode {
	if !isComplete {
		return gsmsg.PartialResponse
	}
	return status
}
