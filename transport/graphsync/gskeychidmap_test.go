package graphsync

import (
	"testing"

	"github.com/ipfs/go-graphsync"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

func TestRequestIDToChannelIDMap(t *testing.T) {
	m := newRequestIDToChannelIDMap()
	requestID1 := graphsync.NewRequestID()
	requestID2 := graphsync.NewRequestID()
	chid := datatransfer.ChannelID{
		Initiator: "i",
		Responder: "r",
		ID:        1,
	}

	_, ok := m.load(requestID1)
	require.False(t, ok)

	_, any := m.any(requestID1)
	require.False(t, any)

	m.set(requestID1, false, chid)
	ret, ok := m.load(requestID1)
	require.True(t, ok)
	require.Equal(t, chid, ret)

	ret, any = m.any(requestID1)
	require.True(t, any)
	require.Equal(t, chid, ret)

	_, any = m.any(requestID2)
	require.False(t, any)

	m.set(requestID2, true, chid)
	chid, any = m.any(requestID2)
	require.True(t, any)
	require.Equal(t, chid, ret)

	var ks []graphsync.RequestID
	var chids []datatransfer.ChannelID
	m.forEach(func(k graphsync.RequestID, isSending bool, chid datatransfer.ChannelID) {
		ks = append(ks, k)
		chids = append(chids, chid)
	})
	require.Len(t, ks, 2)
	require.Contains(t, ks, requestID1)
	require.Contains(t, ks, requestID2)
	require.Len(t, chids, 2)
	require.Contains(t, chids, chid)

	m.deleteRefs(chid)
	_, ok = m.load(requestID1)
	require.False(t, ok)
	_, ok = m.load(requestID2)
	require.False(t, ok)
}
