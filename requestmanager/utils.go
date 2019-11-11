package requestmanager

import (
	"context"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/metadata"
	ipld "github.com/ipld/go-ipld-prime"
)

func visitToChannel(ctx context.Context, inProgressChan chan graphsync.ResponseProgress) ipldbridge.AdvVisitFn {
	return func(tp ipldbridge.TraversalProgress, node ipld.Node, tr ipldbridge.TraversalReason) error {
		select {
		case <-ctx.Done():
		case inProgressChan <- graphsync.ResponseProgress{
			Node:      node,
			Path:      tp.Path,
			LastBlock: tp.LastBlock,
		}:
		}
		return nil
	}
}

func metadataForResponses(responses []gsmsg.GraphSyncResponse, ipldBridge ipldbridge.IPLDBridge) map[graphsync.RequestID]metadata.Metadata {
	responseMetadata := make(map[graphsync.RequestID]metadata.Metadata, len(responses))
	for _, response := range responses {
		mdRaw, found := response.Extension(graphsync.ExtensionMetadata)
		if !found {
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
