package v2

import (
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/message/ipldbind"
	"github.com/ipfs/go-test/random"
)

func TestIPLDRoundTrip(t *testing.T) {
	id1 := graphsync.NewRequestID()
	id2 := graphsync.NewRequestID()
	root1, _ := cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	root2, _ := cid.Decode("bafyreibdoxfay27gf4ye3t5a7aa5h4z2azw7hhhz36qrbf5qleldj76qfy")
	extension1Data := basicnode.NewString("yee haw")
	extension1Name := graphsync.ExtensionName("AppleSauce/McGee")
	extension1 := graphsync.ExtensionData{
		Name: extension1Name,
		Data: extension1Data,
	}
	extension2Data := basicnode.NewBytes(random.Bytes(100))
	extension2Name := graphsync.ExtensionName("Hippity+Hoppity")
	extension2 := graphsync.ExtensionData{
		Name: extension2Name,
		Data: extension2Data,
	}

	requests := map[graphsync.RequestID]message.GraphSyncRequest{
		id1: message.NewRequest(id1, root1, selectorparse.CommonSelector_MatchAllRecursively, graphsync.Priority(101), extension1),
		id2: message.NewRequest(id2, root2, selectorparse.CommonSelector_ExploreAllRecursively, graphsync.Priority(202)),
	}

	metadata := []message.GraphSyncLinkMetadatum{{Link: root2, Action: graphsync.LinkActionMissing}, {Link: root1, Action: graphsync.LinkActionDuplicateNotSent}}

	metadata2 := []message.GraphSyncLinkMetadatum{{Link: root2, Action: graphsync.LinkActionDuplicateDAGSkipped}, {Link: root1, Action: graphsync.LinkActionPresent}}

	responses := map[graphsync.RequestID]message.GraphSyncResponse{
		id1: message.NewResponse(id1, graphsync.RequestFailedContentNotFound, metadata),
		id2: message.NewResponse(id2, graphsync.PartialResponse, metadata2, extension2),
	}

	blks := random.BlocksOfSize(2, 100)

	blocks := map[cid.Cid]blocks.Block{
		blks[0].Cid(): blks[0],
		blks[1].Cid(): blks[1],
	}

	// message format
	gsm := message.NewMessage(requests, responses, blocks)
	// bindnode internal format
	igsm, err := NewMessageHandler().toIPLD(gsm)
	require.NoError(t, err)

	// ipld TypedNode format
	byts, err := ipldbind.BindnodeRegistry.TypeToBytes(igsm, dagcbor.Encode)
	require.NoError(t, err)

	// back to bindnode internal format
	rtigsm, err := ipldbind.BindnodeRegistry.TypeFromBytes(byts, (*ipldbind.GraphSyncMessageRoot)(nil), dagcbor.Decode)
	require.NoError(t, err)

	// back to message format
	rtgsm, err := NewMessageHandler().fromIPLD(rtigsm.(*ipldbind.GraphSyncMessageRoot))
	require.NoError(t, err)

	rtreq := rtgsm.Requests()
	require.Len(t, rtreq, 2)
	rtreqmap := map[graphsync.RequestID]message.GraphSyncRequest{
		rtreq[0].ID(): rtreq[0],
		rtreq[1].ID(): rtreq[1],
	}
	require.NotNil(t, rtreqmap[id1])
	require.NotNil(t, rtreqmap[id2])
	require.Equal(t, rtreqmap[id1].ID(), id1)
	require.Equal(t, rtreqmap[id2].ID(), id2)
	require.Equal(t, rtreqmap[id1].Root(), root1)
	require.Equal(t, rtreqmap[id2].Root(), root2)
	require.True(t, datamodel.DeepEqual(rtreqmap[id1].Selector(), selectorparse.CommonSelector_MatchAllRecursively))
	require.True(t, datamodel.DeepEqual(rtreqmap[id2].Selector(), selectorparse.CommonSelector_ExploreAllRecursively))
	require.Equal(t, rtreqmap[id1].Priority(), graphsync.Priority(101))
	require.Equal(t, rtreqmap[id2].Priority(), graphsync.Priority(202))
	require.Len(t, rtreqmap[id1].ExtensionNames(), 1)
	require.Empty(t, rtreqmap[id2].ExtensionNames())
	rtext1, exists := rtreqmap[id1].Extension(extension1Name)
	require.True(t, exists)
	require.True(t, datamodel.DeepEqual(rtext1, extension1Data))

	rtres := rtgsm.Responses()
	require.Len(t, rtres, 2)
	require.Len(t, rtreq, 2)
	rtresmap := map[graphsync.RequestID]message.GraphSyncResponse{
		rtres[0].RequestID(): rtres[0],
		rtres[1].RequestID(): rtres[1],
	}
	require.NotNil(t, rtresmap[id1])
	require.NotNil(t, rtresmap[id2])
	require.Equal(t, rtresmap[id1].Status(), graphsync.RequestFailedContentNotFound)
	require.Equal(t, rtresmap[id2].Status(), graphsync.PartialResponse)
	gslm1, ok := rtresmap[id1].Metadata().(message.GraphSyncLinkMetadata)
	require.True(t, ok)
	require.Len(t, gslm1.RawMetadata(), 2)
	require.Equal(t, gslm1.RawMetadata(), metadata)
	gslm2, ok := rtresmap[id2].Metadata().(message.GraphSyncLinkMetadata)
	require.True(t, ok)
	require.Len(t, gslm2.RawMetadata(), 2)
	require.Equal(t, gslm2.RawMetadata(), metadata2)
	require.Empty(t, rtresmap[id1].ExtensionNames())
	require.Len(t, rtresmap[id2].ExtensionNames(), 1)
	rtext2, exists := rtresmap[id2].Extension(extension2Name)
	require.True(t, exists)
	require.True(t, datamodel.DeepEqual(rtext2, extension2Data))

	rtblks := rtgsm.Blocks()
	require.Len(t, rtblks, 2)
}
