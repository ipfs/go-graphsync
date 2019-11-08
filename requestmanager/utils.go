package requestmanager

import (
	"context"

	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/metadata"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	ipld "github.com/ipld/go-ipld-prime"
)

func visitToChannel(ctx context.Context, inProgressChan chan types.ResponseProgress) ipldbridge.AdvVisitFn {
	return func(tp ipldbridge.TraversalProgress, node ipld.Node, tr ipldbridge.TraversalReason) error {
		select {
		case <-ctx.Done():
		case inProgressChan <- types.ResponseProgress{
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
		mdRaw, err := response.Extension(gsmsg.ExtensionMetadata)
		if err != nil {
			log.Warningf("Unable to decode metadata in response for request id: %d", response.RequestID())
			continue
		}
		md, err := metadata.DecodeMetadata(mdRaw, ipldBridge)
		if err != nil {
			log.Warningf("Unable to decode metadata in response for request id: %d", response.RequestID())
			continue
		}
		responseMetadata[response.RequestID()] = md
	}
	return responseMetadata
}
