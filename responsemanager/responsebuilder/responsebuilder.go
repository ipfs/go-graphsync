package responsebuilder

import (
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipld/go-ipld-prime"
)

// ResponseBuilder captures componenst of a response message across multiple
// requests for a given peer and then generates the corresponding
// GraphSync message components once responses are ready to send.
type ResponseBuilder struct {
	outgoingBlocks     []blocks.Block
	completedResponses map[gsmsg.GraphSyncRequestID]bool
	outgoingResponses  map[gsmsg.GraphSyncRequestID]map[ipld.Link]bool
}

// New generates a new ResponseBuilder.
func New() *ResponseBuilder {
	return &ResponseBuilder{
		completedResponses: make(map[gsmsg.GraphSyncRequestID]bool),
		outgoingResponses:  make(map[gsmsg.GraphSyncRequestID]map[ipld.Link]bool),
	}
}

// AddBlock adds the given block to the response.
func (rb *ResponseBuilder) AddBlock(block blocks.Block) {
	rb.outgoingBlocks = append(rb.outgoingBlocks, block)
}

// AddLink adds the given link and whether its block is present
// to the response for the given request ID.
func (rb *ResponseBuilder) AddLink(requestID gsmsg.GraphSyncRequestID, link ipld.Link, blockPresent bool) {
	linksForRequest, ok := rb.outgoingResponses[requestID]
	if !ok {
		linksForRequest = make(map[ipld.Link]bool)
		rb.outgoingResponses[requestID] = linksForRequest
	}
	linksForRequest[link] = blockPresent
}

// AddCompletedRequest marks the given request as completed in the response,
// as well as whether the graphsync request responded with complete or partial
// data.
func (rb *ResponseBuilder) AddCompletedRequest(requestID gsmsg.GraphSyncRequestID, isComplete bool) {
	rb.completedResponses[requestID] = isComplete
	// make sure this completion goes out in next response even if no links are sent
	_, ok := rb.outgoingResponses[requestID]
	if !ok {
		rb.outgoingResponses[requestID] = make(map[ipld.Link]bool)
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
		extra, err := makeEncodedData(linkMap, ipldBridge)
		if err != nil {
			return nil, nil, err
		}
		isFull, isComplete := rb.completedResponses[requestID]
		responses = append(responses, gsmsg.NewResponse(requestID, responseCode(isFull, isComplete), extra))
	}
	return responses, rb.outgoingBlocks, nil
}

func makeEncodedData(entries map[ipld.Link]bool, ipldBridge ipldbridge.IPLDBridge) ([]byte, error) {
	node, err := ipldBridge.BuildNode(func(nb ipldbridge.NodeBuilder) ipld.Node {
		return nb.CreateList(func(lb ipldbridge.ListBuilder, nb ipldbridge.NodeBuilder) {
			for link, blockPresent := range entries {
				lb.Append(
					nb.CreateMap(func(mb ipldbridge.MapBuilder, knb ipldbridge.NodeBuilder, vnb ipldbridge.NodeBuilder) {
						mb.Insert(knb.CreateString("link"), vnb.CreateLink(link))
						mb.Insert(knb.CreateString("blockPresent"), vnb.CreateBool(blockPresent))
					}),
				)
			}
		})
	})
	if err != nil {
		return nil, err
	}
	return ipldBridge.EncodeNode(node)
}

func responseCode(isFull bool, isComplete bool) gsmsg.GraphSyncResponseStatusCode {
	if !isComplete {
		return gsmsg.PartialResponse
	}
	if isFull {
		return gsmsg.RequestCompletedFull
	}
	return gsmsg.RequestCompletedPartial
}
