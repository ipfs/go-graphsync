package network

import (
	"context"
	"fmt"
	"io"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"

	gsmsg "github.com/ipfs/go-graphsync/message"
)

var log = logging.Logger("graphsync_network")

var sendMessageTimeout = time.Minute * 10

// Option is an option for configuring the libp2p storage market network
type Option func(*libp2pGraphSyncNetwork)

// DataTransferProtocols OVERWRITES the default libp2p protocols we use for
// graphsync with the specified protocols
func GraphsyncProtocols(protocols []protocol.ID) Option {
	return func(gsnet *libp2pGraphSyncNetwork) {
		gsnet.setProtocols(protocols)
	}
}

// NewFromLibp2pHost returns a GraphSyncNetwork supported by underlying Libp2p host.
func NewFromLibp2pHost(host host.Host, options ...Option) GraphSyncNetwork {
	graphSyncNetwork := libp2pGraphSyncNetwork{
		host:           host,
		messageHandler: gsmsg.NewMessageHandler(),
		protocols:      []protocol.ID{ProtocolGraphsync_1_1_0, ProtocolGraphsync_2_0_0, ProtocolGraphsync_1_0_0},
	}

	for _, option := range options {
		option(&graphSyncNetwork)
	}

	return &graphSyncNetwork
}

// libp2pGraphSyncNetwork transforms the libp2p host interface, which sends and receives
// NetMessage objects, into the graphsync network interface.
type libp2pGraphSyncNetwork struct {
	host host.Host
	// inbound messages from the network are forwarded to the receiver
	receiver       Receiver
	messageHandler *gsmsg.MessageHandler
	protocols      []protocol.ID
}

type streamMessageSender struct {
	s              network.Stream
	opts           MessageSenderOpts
	messageHandler *gsmsg.MessageHandler
}

func (s *streamMessageSender) Close() error {
	return s.s.Close()
}

func (s *streamMessageSender) Reset() error {
	return s.s.Reset()
}

func (s *streamMessageSender) SendMsg(ctx context.Context, msg gsmsg.GraphSyncMessage) error {
	return msgToStream(ctx, s.s, s.messageHandler, msg, s.opts.SendTimeout)
}

func msgToStream(ctx context.Context, s network.Stream, mh *gsmsg.MessageHandler, msg gsmsg.GraphSyncMessage, timeout time.Duration) error {
	log.Debugf("Outgoing message with %d requests, %d responses, and %d blocks",
		len(msg.Requests()), len(msg.Responses()), len(msg.Blocks()))

	deadline := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}
	if err := s.SetWriteDeadline(deadline); err != nil {
		log.Warnf("error setting deadline: %s", err)
	}

	switch s.Protocol() {
	case ProtocolGraphsync_1_0_0:
		if err := mh.ToNetV1(s.Conn().RemotePeer(), msg, s); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	case ProtocolGraphsync_1_1_0:
		if err := mh.ToNetV11(msg, s); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	case ProtocolGraphsync_2_0_0:
		if err := mh.ToNet(msg, s); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	default:
		return fmt.Errorf("unrecognized protocol on remote: %s", s.Protocol())
	}

	if err := s.SetWriteDeadline(time.Time{}); err != nil {
		log.Warnf("error resetting deadline: %s", err)
	}
	return nil
}

func (gsnet *libp2pGraphSyncNetwork) NewMessageSender(ctx context.Context, p peer.ID, opts MessageSenderOpts) (MessageSender, error) {
	s, err := gsnet.newStreamToPeer(ctx, p)
	if err != nil {
		return nil, err
	}

	return &streamMessageSender{
		s:              s,
		opts:           setDefaults(opts),
		messageHandler: gsnet.messageHandler,
	}, nil
}

func (gsnet *libp2pGraphSyncNetwork) newStreamToPeer(ctx context.Context, p peer.ID) (network.Stream, error) {
	return gsnet.host.NewStream(ctx, p, gsnet.protocols...)
}

func (gsnet *libp2pGraphSyncNetwork) SendMessage(
	ctx context.Context,
	p peer.ID,
	outgoing gsmsg.GraphSyncMessage) error {

	s, err := gsnet.newStreamToPeer(ctx, p)
	if err != nil {
		return err
	}

	if err = msgToStream(ctx, s, gsnet.messageHandler, outgoing, sendMessageTimeout); err != nil {
		_ = s.Reset()
		return err
	}

	return s.Close()
}

func (gsnet *libp2pGraphSyncNetwork) SetDelegate(r Receiver) {
	gsnet.receiver = r
	for _, p := range gsnet.protocols {
		gsnet.host.SetStreamHandler(p, gsnet.handleNewStream)
	}
	gsnet.host.Network().Notify((*libp2pGraphSyncNotifee)(gsnet))
}

func (gsnet *libp2pGraphSyncNetwork) ConnectTo(ctx context.Context, p peer.ID) error {
	return gsnet.host.Connect(ctx, peer.AddrInfo{ID: p})
}

// handleNewStream receives a new stream from the network.
func (gsnet *libp2pGraphSyncNetwork) handleNewStream(s network.Stream) {
	defer s.Close()

	if gsnet.receiver == nil {
		_ = s.Reset()
		return
	}

	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
	for {
		var received gsmsg.GraphSyncMessage
		var err error
		switch s.Protocol() {
		case ProtocolGraphsync_1_0_0:
			received, err = gsnet.messageHandler.FromMsgReaderV1(s.Conn().RemotePeer(), reader)
		case ProtocolGraphsync_1_1_0:
			received, err = gsnet.messageHandler.FromMsgReaderV11(reader)
		case ProtocolGraphsync_2_0_0:
			received, err = gsnet.messageHandler.FromMsgReader(reader)
		default:
			err = fmt.Errorf("unexpected protocol version %s", s.Protocol())
		}
		p := s.Conn().RemotePeer()

		if err != nil {
			if err != io.EOF {
				_ = s.Reset()
				go gsnet.receiver.ReceiveError(p, err)
				log.Debugf("graphsync net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		ctx := context.Background()
		log.Debugf("graphsync net handleNewStream from %s", s.Conn().RemotePeer())

		gsnet.receiver.ReceiveMessage(ctx, p, received)
	}
}

func (gsnet *libp2pGraphSyncNetwork) ConnectionManager() ConnManager {
	return gsnet.host.ConnManager()
}

func (gsnet *libp2pGraphSyncNetwork) setProtocols(protocols []protocol.ID) {
	gsnet.protocols = make([]protocol.ID, 0)
	for _, proto := range protocols {
		switch proto {
		case ProtocolGraphsync_1_0_0, ProtocolGraphsync_1_1_0, ProtocolGraphsync_2_0_0:
			gsnet.protocols = append([]protocol.ID{}, proto)
		}
	}
}

type libp2pGraphSyncNotifee libp2pGraphSyncNetwork

func (nn *libp2pGraphSyncNotifee) libp2pGraphSyncNetwork() *libp2pGraphSyncNetwork {
	return (*libp2pGraphSyncNetwork)(nn)
}

func (nn *libp2pGraphSyncNotifee) Connected(n network.Network, v network.Conn) {
	nn.libp2pGraphSyncNetwork().receiver.Connected(v.RemotePeer())
}

func (nn *libp2pGraphSyncNotifee) Disconnected(n network.Network, v network.Conn) {
	nn.libp2pGraphSyncNetwork().receiver.Disconnected(v.RemotePeer())
}

func (nn *libp2pGraphSyncNotifee) OpenedStream(n network.Network, v network.Stream) {}
func (nn *libp2pGraphSyncNotifee) ClosedStream(n network.Network, v network.Stream) {}
func (nn *libp2pGraphSyncNotifee) Listen(n network.Network, a ma.Multiaddr)         {}
func (nn *libp2pGraphSyncNotifee) ListenClose(n network.Network, a ma.Multiaddr)    {}

func setDefaults(opts MessageSenderOpts) MessageSenderOpts {
	copy := opts
	if opts.SendTimeout == 0 {
		copy.SendTimeout = sendMessageTimeout
	}
	return copy
}
