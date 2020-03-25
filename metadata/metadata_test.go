package metadata

import (
	"math/rand"
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/testutil"
)

func TestDecodeEncodeMetadata(t *testing.T) {
	cids := testutil.GenerateCids(10)
	initialMetadata := make(Metadata, 0, 10)
	for _, k := range cids {
		link := cidlink.Link{Cid: k}
		blockPresent := rand.Int31()%2 == 0
		initialMetadata = append(initialMetadata, Item{link, blockPresent})
	}
	encoded, err := EncodeMetadata(initialMetadata)
	require.NoError(t, err, "Error encoding")
	decodedMetadata, err := DecodeMetadata(encoded)
	require.NoError(t, err, "Error decoding")
	require.Equal(t, initialMetadata, decodedMetadata, "Metadata changed during encoding and decoding")
}
