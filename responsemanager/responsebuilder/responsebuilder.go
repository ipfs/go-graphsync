package responsebuilder

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-graphsync"
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
	completedResponses map[graphsync.RequestID]graphsync.ResponseStatusCode
	outgoingResponses  map[graphsync.RequestID]metadata.Metadata
	extensions         map[graphsync.RequestID][]graphsync.ExtensionData
}

// New generates a new ResponseBuilder.
func New() *ResponseBuilder {
	return &ResponseBuilder{
		completedResponses: make(map[graphsync.RequestID]graphsync.ResponseStatusCode),
		outgoingResponses:  make(map[graphsync.RequestID]metadata.Metadata),
		extensions:         make(map[graphsync.RequestID][]graphsync.ExtensionData),
	}
}

// AddBlock adds the given block to the response.
func (rb *ResponseBuilder) AddBlock(block blocks.Block) {
	rb.blkSize += len(block.RawData())
	rb.outgoingBlocks = append(rb.outgoingBlocks, block)
}

// AddExtensionData adds the given extension data to to the response
func (rb *ResponseBuilder) AddExtensionData(requestID graphsync.RequestID, extension graphsync.ExtensionData) {
	rb.extensions[requestID] = append(rb.extensions[requestID], extension)
}

// BlockSize returns the total size of all blocks in this response
func (rb *ResponseBuilder) BlockSize() int {
	return rb.blkSize
}

// AddLink adds the given link and whether its block is present
// to the response for the given request ID.
func (rb *ResponseBuilder) AddLink(requestID graphsync.RequestID, link ipld.Link, blockPresent bool) {
	rb.outgoingResponses[requestID] = append(rb.outgoingResponses[requestID], metadata.Item{Link: link, BlockPresent: blockPresent})
}

// AddCompletedRequest marks the given request as completed in the response,
// as well as whether the graphsync request responded with complete or partial
// data.
func (rb *ResponseBuilder) AddCompletedRequest(requestID graphsync.RequestID, status graphsync.ResponseStatusCode) {
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
		mdRaw, err := metadata.EncodeMetadata(linkMap, ipldBridge)
		if err != nil {
			return nil, nil, err
		}
		rb.extensions[requestID] = append(rb.extensions[requestID], graphsync.ExtensionData{
			Name: graphsync.ExtensionMetadata,
			Data: mdRaw,
		})
		status, isComplete := rb.completedResponses[requestID]
		responses = append(responses, gsmsg.NewResponse(requestID, responseCode(status, isComplete), rb.extensions[requestID]...))
	}
	return responses, rb.outgoingBlocks, nil
}

func responseCode(status graphsync.ResponseStatusCode, isComplete bool) graphsync.ResponseStatusCode {
	if !isComplete {
		return graphsync.PartialResponse
	}
	return status
}
