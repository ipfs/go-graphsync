package cidlists_test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/cidlists"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

func TestCIDLists(t *testing.T) {

	baseDir, err := ioutil.TempDir("", "cidlisttest")
	require.NoError(t, err)

	chid1 := datatransfer.ChannelID{ID: datatransfer.TransferID(rand.Uint64()), Initiator: testutil.GeneratePeers(1)[0], Responder: testutil.GeneratePeers(1)[0]}
	chid2 := datatransfer.ChannelID{ID: datatransfer.TransferID(rand.Uint64()), Initiator: testutil.GeneratePeers(1)[0], Responder: testutil.GeneratePeers(1)[0]}
	initialCids1 := testutil.GenerateCids(100)

	cidLists, err := cidlists.NewCIDLists(baseDir)
	require.NoError(t, err)

	t.Run("creating lists", func(t *testing.T) {
		require.NoError(t, cidLists.CreateList(chid1, initialCids1))

		filename := fmt.Sprintf("%d-%s-%s", chid1.ID, chid1.Initiator, chid1.Responder)
		f, err := os.Open(filepath.Join(baseDir, filename))
		require.NoError(t, err)
		f.Close()

		require.NoError(t, cidLists.CreateList(chid2, nil))

		filename = fmt.Sprintf("%d-%s-%s", chid2.ID, chid2.Initiator, chid2.Responder)
		f, err = os.Open(filepath.Join(baseDir, filename))
		require.NoError(t, err)
		f.Close()
	})

	t.Run("reading lists", func(t *testing.T) {
		savedCids1, err := cidLists.ReadList(chid1)
		require.NoError(t, err)
		require.Equal(t, initialCids1, savedCids1)

		savedCids2, err := cidLists.ReadList(chid2)
		require.NoError(t, err)
		require.Nil(t, savedCids2)
	})

	t.Run("appending lists", func(t *testing.T) {
		newCid1 := testutil.GenerateCids(1)[0]
		require.NoError(t, cidLists.AppendList(chid1, newCid1))
		savedCids1, err := cidLists.ReadList(chid1)
		require.NoError(t, err)
		require.Equal(t, append(initialCids1, newCid1), savedCids1)

		newCid2 := testutil.GenerateCids(1)[0]
		require.NoError(t, cidLists.AppendList(chid2, newCid2))
		savedCids2, err := cidLists.ReadList(chid2)
		require.NoError(t, err)
		require.Equal(t, []cid.Cid{newCid2}, savedCids2)
	})

	t.Run("deleting lists", func(t *testing.T) {
		require.NoError(t, cidLists.DeleteList(chid1))

		filename := fmt.Sprintf("%d-%s-%s", chid1.ID, chid1.Initiator, chid1.Responder)
		_, err := os.Open(filepath.Join(baseDir, filename))
		require.Error(t, err)

		require.NoError(t, cidLists.DeleteList(chid2))

		filename = fmt.Sprintf("%d-%s-%s", chid2.ID, chid2.Initiator, chid2.Responder)
		_, err = os.Open(filepath.Join(baseDir, filename))
		require.Error(t, err)
	})
}
