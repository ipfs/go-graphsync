package testutil

import (
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"
)

// TestConnManager implements network.ConnManager and allows you to assert
// behavior
type TestConnManager struct {
	protectedConnsLk sync.RWMutex
	protectedConns   map[peer.ID][]string
}

// NewTestConnManager returns a new TestConnManager
func NewTestConnManager() *TestConnManager {
	return &TestConnManager{protectedConns: make(map[peer.ID][]string)}
}

// Protect simulates protecting a connection (just records occurence)
func (tcm *TestConnManager) Protect(p peer.ID, tag string) {
	tcm.protectedConnsLk.Lock()
	defer tcm.protectedConnsLk.Unlock()
	for _, tagCmp := range tcm.protectedConns[p] {
		if tag == tagCmp {
			return
		}
	}
	tcm.protectedConns[p] = append(tcm.protectedConns[p], tag)
}

// Unprotect simulates unprotecting a connection (just records occurence)
func (tcm *TestConnManager) Unprotect(p peer.ID, tag string) bool {
	tcm.protectedConnsLk.Lock()
	defer tcm.protectedConnsLk.Unlock()
	for i, tagCmp := range tcm.protectedConns[p] {
		if tag == tagCmp {
			tcm.protectedConns[p] = append(tcm.protectedConns[p][:i], tcm.protectedConns[p][i+1:]...)
			break
		}
	}
	return len(tcm.protectedConns[p]) > 0
}

// AssertProtected asserts that the connection is protected by at least one tag
func (tcm *TestConnManager) AssertProtected(t testing.TB, p peer.ID) {
	t.Helper()
	tcm.protectedConnsLk.RLock()
	defer tcm.protectedConnsLk.RUnlock()
	require.True(t, len(tcm.protectedConns[p]) > 0)
}

// RefuteProtected refutes that a connection has been protect
func (tcm *TestConnManager) RefuteProtected(t testing.TB, p peer.ID) {
	t.Helper()
	tcm.protectedConnsLk.RLock()
	defer tcm.protectedConnsLk.RUnlock()
	require.False(t, len(tcm.protectedConns[p]) > 0)
}

// AssertProtectedWithTags verifies the connection is protected with the given
// tags at least
func (tcm *TestConnManager) AssertProtectedWithTags(t testing.TB, p peer.ID, tags ...string) {
	t.Helper()
	tcm.protectedConnsLk.RLock()
	defer tcm.protectedConnsLk.RUnlock()
	for _, tag := range tags {
		require.Contains(t, tcm.protectedConns[p], tag)
	}
}

// RefuteProtectedWithTags verifies the connection is not protected with any of the given
// tags
func (tcm *TestConnManager) RefuteProtectedWithTags(t testing.TB, p peer.ID, tags ...string) {
	t.Helper()
	tcm.protectedConnsLk.RLock()
	defer tcm.protectedConnsLk.RUnlock()
	for _, tag := range tags {
		require.NotContains(t, tcm.protectedConns[p], tag)
	}
}
