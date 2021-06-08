package cidsets

import (
	"testing"

	ds "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-data-transfer/testutil"
)

func TestCIDSetManager(t *testing.T) {
	cid1 := testutil.GenerateCids(1)[0]

	dstore := ds_sync.MutexWrap(ds.NewMapDatastore())
	mgr := NewCIDSetManager(dstore)
	setID1 := SetID("set1")
	setID2 := SetID("set2")

	// set1: +cid1
	exists, err := mgr.InsertSetCID(setID1, cid1)
	require.NoError(t, err)
	require.False(t, exists)

	// set1: +cid1 (again)
	exists, err = mgr.InsertSetCID(setID1, cid1)
	require.NoError(t, err)
	require.True(t, exists)

	// set2: +cid1
	exists, err = mgr.InsertSetCID(setID2, cid1)
	require.NoError(t, err)
	require.False(t, exists)

	// set2: +cid2 (again)
	exists, err = mgr.InsertSetCID(setID2, cid1)
	require.NoError(t, err)
	require.True(t, exists)

	// delete set1
	err = mgr.DeleteSet(setID1)
	require.NoError(t, err)

	// set1: +cid1
	exists, err = mgr.InsertSetCID(setID1, cid1)
	require.NoError(t, err)
	require.False(t, exists)

	// set1: +cid1 (again)
	exists, err = mgr.InsertSetCID(setID2, cid1)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestCIDSetToArray(t *testing.T) {
	cids := testutil.GenerateCids(2)
	cid1 := cids[0]
	cid2 := cids[1]

	dstore := ds_sync.MutexWrap(ds.NewMapDatastore())
	mgr := NewCIDSetManager(dstore)
	setID1 := SetID("set1")

	// Expect no items in set
	len, err := mgr.SetLen(setID1)
	require.NoError(t, err)
	require.Equal(t, 0, len)

	arr, err := mgr.SetToArray(setID1)
	require.NoError(t, err)
	require.Len(t, arr, 0)

	// set1: +cid1
	exists, err := mgr.InsertSetCID(setID1, cid1)
	require.NoError(t, err)
	require.False(t, exists)

	// Expect 1 cid in set
	len, err = mgr.SetLen(setID1)
	require.NoError(t, err)
	require.Equal(t, 1, len)

	arr, err = mgr.SetToArray(setID1)
	require.NoError(t, err)
	require.Len(t, arr, 1)
	require.Equal(t, arr[0], cid1)

	// set1: +cid1 (again)
	exists, err = mgr.InsertSetCID(setID1, cid1)
	require.NoError(t, err)
	require.True(t, exists)

	// Expect 1 cid in set
	len, err = mgr.SetLen(setID1)
	require.NoError(t, err)
	require.Equal(t, 1, len)

	arr, err = mgr.SetToArray(setID1)
	require.NoError(t, err)
	require.Len(t, arr, 1)
	require.Equal(t, arr[0], cid1)

	// set1: +cid2
	exists, err = mgr.InsertSetCID(setID1, cid2)
	require.NoError(t, err)
	require.False(t, exists)

	// Expect 2 cids in set
	len, err = mgr.SetLen(setID1)
	require.NoError(t, err)
	require.Equal(t, 2, len)

	arr, err = mgr.SetToArray(setID1)
	require.NoError(t, err)
	require.Len(t, arr, 2)
	require.Contains(t, arr, cid1)
	require.Contains(t, arr, cid2)

	// Delete set1
	err = mgr.DeleteSet(setID1)
	require.NoError(t, err)

	// Expect no items in set
	len, err = mgr.SetLen(setID1)
	require.NoError(t, err)
	require.Equal(t, 0, len)

	arr, err = mgr.SetToArray(setID1)
	require.NoError(t, err)
	require.Len(t, arr, 0)
}

// Add items to set then get the length (to make sure that internal caching
// is working correctly)
func TestCIDSetLenAfterInsert(t *testing.T) {
	cids := testutil.GenerateCids(2)
	cid1 := cids[0]
	cid2 := cids[1]

	dstore := ds_sync.MutexWrap(ds.NewMapDatastore())
	mgr := NewCIDSetManager(dstore)
	setID1 := SetID("set1")

	// set1: +cid1
	exists, err := mgr.InsertSetCID(setID1, cid1)
	require.NoError(t, err)
	require.False(t, exists)

	// set1: +cid2
	exists, err = mgr.InsertSetCID(setID1, cid2)
	require.NoError(t, err)
	require.False(t, exists)

	// Expect 2 cids in set
	len, err := mgr.SetLen(setID1)
	require.NoError(t, err)
	require.Equal(t, 2, len)
}

func TestCIDSetRestart(t *testing.T) {
	cids := testutil.GenerateCids(3)
	cid1 := cids[0]
	cid2 := cids[1]
	cid3 := cids[2]

	dstore := ds_sync.MutexWrap(ds.NewMapDatastore())
	mgr := NewCIDSetManager(dstore)
	setID1 := SetID("set1")

	// set1: +cid1
	exists, err := mgr.InsertSetCID(setID1, cid1)
	require.NoError(t, err)
	require.False(t, exists)

	// set1: +cid2
	exists, err = mgr.InsertSetCID(setID1, cid2)
	require.NoError(t, err)
	require.False(t, exists)

	// Expect 2 cids in set
	arr, err := mgr.SetToArray(setID1)
	require.NoError(t, err)
	require.Len(t, arr, 2)
	require.Contains(t, arr, cid1)
	require.Contains(t, arr, cid2)

	// Simulate a restart by creating a new CIDSetManager from the same
	// datastore
	mgr = NewCIDSetManager(dstore)

	// Expect 2 cids in set
	arr, err = mgr.SetToArray(setID1)
	require.NoError(t, err)
	require.Len(t, arr, 2)
	require.Contains(t, arr, cid1)
	require.Contains(t, arr, cid2)

	// set1: +cid3
	exists, err = mgr.InsertSetCID(setID1, cid3)
	require.NoError(t, err)
	require.False(t, exists)

	// Expect 3 cids in set
	arr, err = mgr.SetToArray(setID1)
	require.NoError(t, err)
	require.Len(t, arr, 3)
	require.Contains(t, arr, cid1)
	require.Contains(t, arr, cid2)
	require.Contains(t, arr, cid3)
}
