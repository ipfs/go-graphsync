package network

import (
	"fmt"
	"io"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-protocolnetwork/pkg/network"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"

	gsmsg "github.com/ipfs/go-graphsync/message"
	gsmsgv2 "github.com/ipfs/go-graphsync/message/v2"
)

var log = logging.Logger("graphsync_network")

// Option is an option for configuring the libp2p storage market network
type Option func() network.NetOpt

// GraphsyncProtocols OVERWRITES the default libp2p protocols we use for
// graphsync with the specified protocols
func GraphsyncProtocols(protocols []protocol.ID) Option {
	return func() network.NetOpt {
		return network.SupportedProtocols(supportedProtocols(protocols))
	}
}

// NewFromLibp2pHost returns a GraphSyncNetwork supported by underlying Libp2p host.
func NewFromLibp2pHost(host host.Host, options ...Option) GraphSyncNetwork {

	netOpts := []network.NetOpt{network.SupportedProtocols([]protocol.ID{ProtocolGraphsync_2_0_0})}
	for _, option := range options {
		netOpts = append(netOpts, option())
	}

	messageHandlerSelector := NewMessageHandlerSelector()

	return network.NewFromLibp2pHost[gsmsg.GraphSyncMessage]("graphsync", host, messageHandlerSelector, netOpts...)
}

func NewMessageHandlerSelector() *MessageHandlerSelector {
	return &MessageHandlerSelector{
		v2MessageHandler: gsmsgv2.NewMessageHandler(),
	}
}

// a message.MessageHandler that simply returns an error for any of the calls, allows
// us to simplify erroring on bad protocol within the messageHandlerSelector#Select()
// call so we only have one place to be strict about allowed versions
type messageHandlerErrorer struct {
	err error
}

func (mhe messageHandlerErrorer) FromNet(peer.ID, io.Reader) (gsmsg.GraphSyncMessage, error) {
	return gsmsg.GraphSyncMessage{}, mhe.err
}
func (mhe messageHandlerErrorer) FromMsgReader(peer.ID, msgio.Reader) (gsmsg.GraphSyncMessage, error) {
	return gsmsg.GraphSyncMessage{}, mhe.err
}
func (mhe messageHandlerErrorer) ToNet(peer.ID, gsmsg.GraphSyncMessage, io.Writer) error {
	return mhe.err
}

type MessageHandlerSelector struct {
	v2MessageHandler gsmsg.MessageHandler
}

func (smh MessageHandlerSelector) Select(protocol protocol.ID) network.MessageHandler[gsmsg.GraphSyncMessage] {
	switch protocol {
	case ProtocolGraphsync_2_0_0:
		return smh.v2MessageHandler
	default:
		return messageHandlerErrorer{fmt.Errorf("unrecognized protocol version: %s", protocol)}
	}
}

func supportedProtocols(protocols []protocol.ID) []protocol.ID {
	protocols = make([]protocol.ID, 0)
	for _, proto := range protocols {
		switch proto {
		case ProtocolGraphsync_2_0_0:
			protocols = append([]protocol.ID{}, proto)
		}
	}
	return protocols
}
