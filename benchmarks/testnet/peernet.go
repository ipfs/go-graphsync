package testnet

import (
	"context"

	"github.com/libp2p/go-libp2p-core/peer"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	mockpeernet "github.com/libp2p/go-libp2p/p2p/net/mock"

	gsnet "github.com/ipfs/go-graphsync/network"
)

type peernet struct {
	mockpeernet.Mocknet
}

// StreamNet is a testnet that uses libp2p's MockNet
func StreamNet(ctx context.Context, net mockpeernet.Mocknet) Network {
	return &peernet{net}
}

func (pn *peernet) Adapter(p tnet.Identity) gsnet.GraphSyncNetwork {
	client, err := pn.Mocknet.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		panic(err.Error())
	}
	err = pn.Mocknet.LinkAll()
	if err != nil {
		panic(err.Error())
	}
	return gsnet.NewFromLibp2pHost(client)
}

func (pn *peernet) HasPeer(p peer.ID) bool {
	for _, member := range pn.Mocknet.Peers() {
		if p == member {
			return true
		}
	}
	return false
}

var _ Network = (*peernet)(nil)
