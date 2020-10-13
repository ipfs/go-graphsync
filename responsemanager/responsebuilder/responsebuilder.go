package responsebuilder

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/metadata"
)

// ResponseBuilder captures componenst of a response message across multiple
// requests for a given peer and then generates the corresponding
// GraphSync message components once responses are ready to send.
type ResponseBuilder struct {
	topic              Index
	outgoingBlocks     []blocks.Block
	blkSize            uint64
	completedResponses map[graphsync.RequestID]graphsync.ResponseStatusCode
	outgoingResponses  map[graphsync.RequestID]metadata.Metadata
	extensions         map[graphsync.RequestID][]graphsync.ExtensionData
}

// Index is an identifier for notifications about this response builder
type Index uint64

// New generates a new ResponseBuilder.
func New(topic Index) *ResponseBuilder {
	return &ResponseBuilder{
		topic:              topic,
		completedResponses: make(map[graphsync.RequestID]graphsync.ResponseStatusCode),
		outgoingResponses:  make(map[graphsync.RequestID]metadata.Metadata),
		extensions:         make(map[graphsync.RequestID][]graphsync.ExtensionData),
	}
}

// AddBlock adds the given block to the response.
func (rb *ResponseBuilder) AddBlock(block blocks.Block) {
	rb.blkSize += uint64(len(block.RawData()))
	rb.outgoingBlocks = append(rb.outgoingBlocks, block)
}

// AddExtensionData adds the given extension data to to the response
func (rb *ResponseBuilder) AddExtensionData(requestID graphsync.RequestID, extension graphsync.ExtensionData) {
	rb.extensions[requestID] = append(rb.extensions[requestID], extension)
}

// BlockSize returns the total size of all blocks in this response
func (rb *ResponseBuilder) BlockSize() uint64 {
	return rb.blkSize
}

// AddLink adds the given link and whether its block is present
// to the response for the given request ID.
func (rb *ResponseBuilder) AddLink(requestID graphsync.RequestID, link ipld.Link, blockPresent bool) {
	rb.outgoingResponses[requestID] = append(rb.outgoingResponses[requestID], metadata.Item{Link: link.(cidlink.Link).Cid, BlockPresent: blockPresent})
}

// AddResponseCode marks the given request as completed in the response,
// as well as whether the graphsync request responded with complete or partial
// data.
func (rb *ResponseBuilder) AddResponseCode(requestID graphsync.RequestID, status graphsync.ResponseStatusCode) {
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
func (rb *ResponseBuilder) Build() ([]gsmsg.GraphSyncResponse, []blocks.Block, error) {
	responses := make([]gsmsg.GraphSyncResponse, 0, len(rb.outgoingResponses))
	for requestID, linkMap := range rb.outgoingResponses {
		mdRaw, err := metadata.EncodeMetadata(linkMap)
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

// Index returns the identifier for notifications sent about this response builder
func (rb *ResponseBuilder) Index() Index {
	return rb.topic
}

func responseCode(status graphsync.ResponseStatusCode, isComplete bool) graphsync.ResponseStatusCode {
	if !isComplete {
		return graphsync.PartialResponse
	}
	return status
}
