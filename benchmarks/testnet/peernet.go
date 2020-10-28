package testnet

import (
	"context"

	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/libp2p/go-libp2p-core/peer"
	mockpeernet "github.com/libp2p/go-libp2p/p2p/net/mock"

	dtnet "github.com/filecoin-project/go-data-transfer/network"
)

type peernet struct {
	mockpeernet.Mocknet
}

// StreamNet is a testnet that uses libp2p's MockNet
func StreamNet(ctx context.Context, net mockpeernet.Mocknet) Network {
	return &peernet{net}
}

func (pn *peernet) Adapter() (peer.ID, gsnet.GraphSyncNetwork, dtnet.DataTransferNetwork) {
	client, err := pn.Mocknet.GenPeer()
	if err != nil {
		panic(err.Error())
	}
	pn.Mocknet.LinkAll()
	return client.ID(), gsnet.NewFromLibp2pHost(client), dtnet.NewFromLibp2pHost(client)
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
