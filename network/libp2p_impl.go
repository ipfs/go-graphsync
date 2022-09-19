package network

import (
	"context"
	"fmt"
	"io"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"

	gsmsg "github.com/ipfs/go-graphsync/message"
	gsmsgv2 "github.com/ipfs/go-graphsync/message/v2"
	"github.com/ipfs/go-graphsync/panics"
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

// PanicCallback allows calling code to receive information about panics that
// Graphsync recovers from. Graphsync recovers panics that occur during
// message handling in order to keep the over all system running, although
// they are still treated as standard errors in normal execution flow.
func PanicCallback(callbackFn panics.CallBackFn) Option {
	return func(gsnet *libp2pGraphSyncNetwork) {
		gsnet.panicCallback = callbackFn
	}
}

// NewFromLibp2pHost returns a GraphSyncNetwork supported by underlying Libp2p host.
func NewFromLibp2pHost(host host.Host, options ...Option) GraphSyncNetwork {
	graphSyncNetwork := libp2pGraphSyncNetwork{
		host:      host,
		protocols: []protocol.ID{ProtocolGraphsync_2_0_0},
	}

	for _, option := range options {
		option(&graphSyncNetwork)
	}

	graphSyncNetwork.panicHandler = panics.MakeHandler(graphSyncNetwork.panicCallback)

	graphSyncNetwork.messageHandlerSelector = &messageHandlerSelector{
		v2MessageHandler: gsmsgv2.NewMessageHandler(),
		panicHandler:     graphSyncNetwork.panicHandler,
	}

	return &graphSyncNetwork
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

type messageHandlerSelector struct {
	v2MessageHandler gsmsg.MessageHandler

	panicHandler panics.PanicHandler
}

func (smh messageHandlerSelector) Select(protocol protocol.ID) gsmsg.MessageHandler {
	switch protocol {
	case ProtocolGraphsync_2_0_0:
		return smh.v2MessageHandler
	default:
		return messageHandlerErrorer{fmt.Errorf("unrecognized protocol version: %s", protocol)}
	}
}

// libp2pGraphSyncNetwork transforms the libp2p host interface, which sends and receives
// NetMessage objects, into the graphsync network interface.
type libp2pGraphSyncNetwork struct {
	host host.Host
	// inbound messages from the network are forwarded to the receiver
	receiver               Receiver
	protocols              []protocol.ID
	messageHandlerSelector *messageHandlerSelector
	panicCallback          panics.CallBackFn
	panicHandler           panics.PanicHandler
}

type streamMessageSender struct {
	s                      network.Stream
	opts                   MessageSenderOpts
	messageHandlerSelector *messageHandlerSelector
}

func (s *streamMessageSender) Close() error {
	return s.s.Close()
}

func (s *streamMessageSender) Reset() error {
	return s.s.Reset()
}

func (s *streamMessageSender) SendMsg(ctx context.Context, msg gsmsg.GraphSyncMessage) error {
	return msgToStream(ctx, s.s, s.messageHandlerSelector, msg, s.opts.SendTimeout)
}

func msgToStream(ctx context.Context, s network.Stream, mh *messageHandlerSelector, msg gsmsg.GraphSyncMessage, timeout time.Duration) (err error) {
	defer func() {
		if rerr := mh.panicHandler(recover()); rerr != nil {
			log.Warnf("recovered panic handling message: %s", err)
			err = rerr
		}
	}()

	log.Debugf("Outgoing message with %d requests, %d responses, and %d blocks",
		len(msg.Requests()), len(msg.Responses()), len(msg.Blocks()))

	deadline := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}
	if err := s.SetWriteDeadline(deadline); err != nil {
		log.Warnf("error setting deadline: %s", err)
	}

	if err := mh.Select(s.Protocol()).ToNet(s.Conn().RemotePeer(), msg, s); err != nil {
		log.Debugf("error: %s", err)
		return err
	}

	if err := s.SetWriteDeadline(time.Time{}); err != nil {
		log.Warnf("error resetting deadline: %s", err)
	}

	return err
}

func (gsnet *libp2pGraphSyncNetwork) NewMessageSender(ctx context.Context, p peer.ID, opts MessageSenderOpts) (MessageSender, error) {
	s, err := gsnet.newStreamToPeer(ctx, p)
	if err != nil {
		return nil, err
	}

	return &streamMessageSender{
		s:                      s,
		opts:                   setDefaults(opts),
		messageHandlerSelector: gsnet.messageHandlerSelector,
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

	if err = msgToStream(ctx, s, gsnet.messageHandlerSelector, outgoing, sendMessageTimeout); err != nil {
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
	var p peer.ID

	defer s.Close()
	defer func() {
		if rerr := gsnet.panicHandler(recover()); rerr != nil {
			log.Debugf("graphsync net handleNewStream recovered error from %s error: %s", s.Conn().RemotePeer(), rerr)
			_ = s.Reset()
			go gsnet.receiver.ReceiveError(p, rerr)
		}
	}()

	if gsnet.receiver == nil {
		_ = s.Reset()
		return
	}

	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
	for {
		p = s.Conn().RemotePeer()
		received, err := gsnet.messageHandlerSelector.Select(s.Protocol()).FromMsgReader(s.Conn().RemotePeer(), reader)

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
		case ProtocolGraphsync_2_0_0:
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
