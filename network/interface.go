package network

import (
	"github.com/libp2p/go-libp2p/core/protocol"

	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-protocolnetwork/pkg/network"
)

var (
	// ProtocolGraphsync is the protocol identifier for graphsync messages
	ProtocolGraphsync_2_0_0 protocol.ID = "/ipfs/graphsync/2.0.0"
)

// GraphSyncNetwork provides network connectivity for GraphSync.
type GraphSyncNetwork network.ProtocolNetwork[gsmsg.GraphSyncMessage]

type MessageSender = network.MessageSender[gsmsg.GraphSyncMessage]
type MessageSenderOpts = network.MessageSenderOpts

type Receiver = network.Receiver[gsmsg.GraphSyncMessage]
