package cidset

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/boost-graphsync/testutil"
)

func TestDecodeEncodeCidSet(t *testing.T) {
	cids := testutil.GenerateCids(10)
	set := cid.NewSet()
	for _, c := range cids {
		set.Add(c)
	}
	encoded := EncodeCidSet(set)
	decodedCidSet, err := DecodeCidSet(encoded)
	require.NoError(t, err, "decode errored")
	require.Equal(t, decodedCidSet.Len(), set.Len())
	err = decodedCidSet.ForEach(func(c cid.Cid) error {
		require.True(t, set.Has(c))
		return nil
	})
	require.NoError(t, err)
}
