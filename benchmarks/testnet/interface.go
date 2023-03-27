package testnet

import (
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"

	gsnet "github.com/filecoin-project/boost-graphsync/network"
)

// Network is an interface for generating graphsync network interfaces
// based on a test network.
type Network interface {
	Adapter(tnet.Identity) gsnet.GraphSyncNetwork
	HasPeer(peer.ID) bool
}
