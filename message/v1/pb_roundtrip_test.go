package v1

import (
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/message"
	pb "github.com/ipfs/go-graphsync/message/pb"
	"github.com/ipfs/go-graphsync/testutil"
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
	extension2Data := basicnode.NewBytes(testutil.RandomBytes(100))
	extension2Name := graphsync.ExtensionName("Hippity+Hoppity")
	extension2 := graphsync.ExtensionData{
		Name: extension2Name,
		Data: extension2Data,
	}

	requests := map[graphsync.RequestID]message.GraphSyncRequest{
		id1: message.NewRequest(id1, root1, selectorparse.CommonSelector_MatchAllRecursively, graphsync.Priority(101), extension1),
		id2: message.NewRequest(id2, root2, selectorparse.CommonSelector_ExploreAllRecursively, graphsync.Priority(202)),
	}

	metadata := message.GraphSyncLinkMetadatum{Link: root2, Action: graphsync.LinkActionMissing}

	responses := map[graphsync.RequestID]message.GraphSyncResponse{
		id1: message.NewResponse(id1, graphsync.RequestFailedContentNotFound, []message.GraphSyncLinkMetadatum{metadata}),
		id2: message.NewResponse(id2, graphsync.PartialResponse, nil, extension2),
	}

	blks := testutil.GenerateBlocksOfSize(2, 100)

	blocks := map[cid.Cid]blocks.Block{
		blks[0].Cid(): blks[0],
		blks[1].Cid(): blks[1],
	}

	mh := NewMessageHandler()
	p := peer.ID("blip")

	// message format
	gsm := message.NewMessage(requests, responses, blocks)
	// proto internal format
	pgsm, err := mh.ToProto(p, gsm)
	require.NoError(t, err)

	// protobuf binary format
	buf, err := proto.Marshal(pgsm)
	require.NoError(t, err)

	// back to proto internal format
	var rtpgsm pb.Message
	err = proto.Unmarshal(buf, &rtpgsm)
	require.NoError(t, err)

	// back to bindnode internal format
	rtgsm, err := mh.fromProto(p, &rtpgsm)
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
	require.Len(t, gslm1.RawMetadata(), 1)
	require.Equal(t, gslm1.RawMetadata()[0], metadata)
	gslm2, ok := rtresmap[id2].Metadata().(message.GraphSyncLinkMetadata)
	require.True(t, ok)
	require.Empty(t, gslm2.RawMetadata())
	require.Empty(t, rtresmap[id1].ExtensionNames())
	require.Len(t, rtresmap[id2].ExtensionNames(), 1)
	rtext2, exists := rtresmap[id2].Extension(extension2Name)
	require.True(t, exists)
	require.True(t, datamodel.DeepEqual(rtext2, extension2Data))

	rtblks := rtgsm.Blocks()
	require.Len(t, rtblks, 2)
}
