package peerresponsemanager

import (
	"context"

	"github.com/ipfs/go-graphsync/peermanager"
	peer "github.com/libp2p/go-libp2p-peer"
)

// PeerSenderFactory provides a function that will create a PeerResponseSender.
type PeerSenderFactory func(ctx context.Context, p peer.ID) PeerResponseSender

// PeerReponseManager manages message queues for peers
type PeerReponseManager struct {
	*peermanager.PeerManager
}

// New generates a new peer manager for sending responses
func New(ctx context.Context, createPeerSender PeerSenderFactory) *PeerReponseManager {
	return &PeerReponseManager{
		PeerManager: peermanager.New(ctx, func(ctx context.Context, p peer.ID) peermanager.PeerProcess {
			return createPeerSender(ctx, p)
		}),
	}
}

// SenderForPeer returns a response sender to use with the given peer
func (prm *PeerReponseManager) SenderForPeer(p peer.ID) PeerResponseSender {
	return prm.GetProcess(p).(PeerResponseSender)
}
