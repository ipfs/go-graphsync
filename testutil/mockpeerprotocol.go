package testutil

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-data-transfer/transport/graphsync"
)

type MockPeerProtocol struct {
	lk sync.Mutex
	pp map[peer.ID]protocol.ID
}

var _ graphsync.PeerProtocol = (*MockPeerProtocol)(nil)

func NewMockPeerProtocol() *MockPeerProtocol {
	return &MockPeerProtocol{
		pp: make(map[peer.ID]protocol.ID),
	}
}

func (m *MockPeerProtocol) Protocol(ctx context.Context, id peer.ID) (protocol.ID, error) {
	m.lk.Lock()
	defer m.lk.Unlock()

	proto, ok := m.pp[id]
	if !ok {
		return "", xerrors.Errorf("no protocol set for peer %s", id)
	}
	return proto, nil
}

func (m *MockPeerProtocol) SetProtocol(id peer.ID, proto protocol.ID) {
	m.lk.Lock()
	defer m.lk.Unlock()

	m.pp[id] = proto
}
