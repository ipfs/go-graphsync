package requestmanager

import (
	"context"

	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/metadata"
	ipld "github.com/ipld/go-ipld-prime"
)

func visitToChannel(ctx context.Context, inProgressChan chan ResponseProgress) ipldbridge.AdvVisitFn {
	return func(tp ipldbridge.TraversalProgress, node ipld.Node, tr ipldbridge.TraversalReason) error {
		select {
		case <-ctx.Done():
		case inProgressChan <- ResponseProgress{
			Node:      node,
			Path:      tp.Path,
			LastBlock: tp.LastBlock,
		}:
		}
		return nil
	}
}

func metadataForResponses(responses []gsmsg.GraphSyncResponse, ipldBridge ipldbridge.IPLDBridge) map[gsmsg.GraphSyncRequestID]metadata.Metadata {
	responseMetadata := make(map[gsmsg.GraphSyncRequestID]metadata.Metadata, len(responses))
	for _, response := range responses {
		md, err := metadata.DecodeMetadata(response.Extra(), ipldBridge)
		if err != nil {
			log.Warningf("Unable to decode metadata in response for request id: %d", response.RequestID())
			continue
		}
		responseMetadata[response.RequestID()] = md
	}
	return responseMetadata
}
