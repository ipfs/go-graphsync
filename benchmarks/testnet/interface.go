package testnet

import (
	gsnet "github.com/ipfs/go-graphsync/network"

	"github.com/libp2p/go-libp2p-core/peer"
	tnet "github.com/libp2p/go-libp2p-testing/net"
)

// Network is an interface for generating graphsync network interfaces
// based on a test network.
type Network interface {
	Adapter(tnet.Identity) gsnet.GraphSyncNetwork
	HasPeer(peer.ID) bool
}

