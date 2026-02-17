package testnet

import (
	"context"
	"slices"

	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"
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
	return slices.Contains(pn.Mocknet.Peers(), p)
}

var _ Network = (*peernet)(nil)
