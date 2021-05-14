package graphsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

func TestGSKeyToChannelIDMap(t *testing.T) {
	m := newGSKeyToChannelIDMap()
	gsk1 := graphsyncKey{
		requestID: 1,
		p:         "p",
	}
	gsk2 := graphsyncKey{
		requestID: 2,
		p:         "p",
	}
	chid := datatransfer.ChannelID{
		Initiator: "i",
		Responder: "r",
		ID:        1,
	}

	_, ok := m.load(gsk1)
	require.False(t, ok)

	_, any := m.any(gsk1)
	require.False(t, any)

	m.set(gsk1, chid)
	ret, ok := m.load(gsk1)
	require.True(t, ok)
	require.Equal(t, chid, ret)

	ret, any = m.any(gsk1)
	require.True(t, any)
	require.Equal(t, chid, ret)

	_, any = m.any(gsk2)
	require.False(t, any)

	m.set(gsk2, chid)
	chid, any = m.any(gsk2)
	require.True(t, any)
	require.Equal(t, chid, ret)

	var ks []graphsyncKey
	var chids []datatransfer.ChannelID
	m.forEach(func(k graphsyncKey, chid datatransfer.ChannelID) {
		ks = append(ks, k)
		chids = append(chids, chid)
	})
	require.Len(t, ks, 2)
	require.Contains(t, ks, gsk1)
	require.Contains(t, ks, gsk2)
	require.Len(t, chids, 2)
	require.Contains(t, chids, chid)

	m.deleteRefs(chid)
	_, ok = m.load(gsk1)
	require.False(t, ok)
	_, ok = m.load(gsk2)
	require.False(t, ok)
}
