package impl

import (
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/libp2p/go-libp2p-core/peer"
)

type channelEnvironment struct {
	m *manager
}

func (ce *channelEnvironment) Protect(id peer.ID, tag string) {
	ce.m.dataTransferNetwork.Protect(id, tag)
}

func (ce *channelEnvironment) Unprotect(id peer.ID, tag string) bool {
	return ce.m.dataTransferNetwork.Unprotect(id, tag)
}

func (ce *channelEnvironment) ID() peer.ID {
	return ce.m.dataTransferNetwork.ID()
}

func (ce *channelEnvironment) CleanupChannel(chid datatransfer.ChannelID) {
	ce.m.transport.CleanupChannel(chid)
}
