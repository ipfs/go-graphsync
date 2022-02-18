package metadata

import (
	"math/rand"
	"testing"

	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/testutil"
)

func TestDecodeEncodeMetadata(t *testing.T) {
	cids := testutil.GenerateCids(10)
	initialMetadata := make(Metadata, 0, 10)
	nd := fluent.MustBuildList(basicnode.Prototype.List, 10, func(fla fluent.ListAssembler) {
		for _, k := range cids {
			blockPresent := rand.Int31()%2 == 0
			initialMetadata = append(initialMetadata, Item{k, blockPresent})
			fla.AssembleValue().CreateMap(2, func(fma fluent.MapAssembler) {
				fma.AssembleEntry("link").AssignLink(cidlink.Link{Cid: k})
				fma.AssembleEntry("blockPresent").AssignBool(blockPresent)
			})
		}
	})

	// verify metadata matches
	encoded := EncodeMetadata(initialMetadata)

	decodedMetadata, err := DecodeMetadata(encoded)
	require.NoError(t, err, "decode errored")
	require.Equal(t, initialMetadata, decodedMetadata, "metadata changed during encoding and decoding")

	// verify metadata is equivalent of IPLD node encoding
	decodedMetadataFromNode, err := DecodeMetadata(nd)
	require.NoError(t, err)
	require.Equal(t, decodedMetadata, decodedMetadataFromNode, "metadata not equal to IPLD encoding")
}
