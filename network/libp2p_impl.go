package network

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"

	gsmsg "github.com/ipfs/go-graphsync/message"

	ggio "github.com/gogo/protobuf/io"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

var log = logging.Logger("graphsync_network")

var sendMessageTimeout = time.Minute * 10

// NewFromLibp2pHost returns a GraphSyncNetwork supported by underlying Libp2p host.
func NewFromLibp2pHost(host host.Host,
	decodeSelectorFunc gsmsg.DecodeSelectorFunc,
	decodeSelectionResponseFunc gsmsg.DecodeSelectionResponseFunc) GraphSyncNetwork {
	graphSyncNetwork := libp2pGraphSyncNetwork{
		host:                        host,
		decodeSelectorFunc:          decodeSelectorFunc,
		decodeSelectionResponseFunc: decodeSelectionResponseFunc,
	}
	host.SetStreamHandler(ProtocolGraphsync, graphSyncNetwork.handleNewStream)

	return &graphSyncNetwork
}

// libp2pGraphSyncNetwork transforms the libp2p host interface, which sends and receives
// NetMessage objects, into the graphsync network interface.
type libp2pGraphSyncNetwork struct {
	host                        host.Host
	decodeSelectionResponseFunc gsmsg.DecodeSelectionResponseFunc
	decodeSelectorFunc          gsmsg.DecodeSelectorFunc
	// inbound messages from the network are forwarded to the receiver
	receiver Receiver
}

type streamMessageSender struct {
	s inet.Stream
}

func (s *streamMessageSender) Close() error {
	return inet.FullClose(s.s)
}

func (s *streamMessageSender) Reset() error {
	return s.s.Reset()
}

func (s *streamMessageSender) SendMsg(ctx context.Context, msg gsmsg.GraphSyncMessage) error {
	return msgToStream(ctx, s.s, msg)
}

func msgToStream(ctx context.Context, s inet.Stream, msg gsmsg.GraphSyncMessage) error {
	deadline := time.Now().Add(sendMessageTimeout)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}
	if err := s.SetWriteDeadline(deadline); err != nil {
		log.Warningf("error setting deadline: %s", err)
	}

	w := bufio.NewWriter(s)

	switch s.Protocol() {
	case ProtocolGraphsync:
		if err := msg.ToNet(w); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	default:
		return fmt.Errorf("unrecognized protocol on remote: %s", s.Protocol())
	}

	if err := w.Flush(); err != nil {
		log.Debugf("error: %s", err)
		return err
	}

	if err := s.SetWriteDeadline(time.Time{}); err != nil {
		log.Warningf("error resetting deadline: %s", err)
	}
	return nil
}

func (gsnet *libp2pGraphSyncNetwork) NewMessageSender(ctx context.Context, p peer.ID) (MessageSender, error) {
	s, err := gsnet.newStreamToPeer(ctx, p)
	if err != nil {
		return nil, err
	}

	return &streamMessageSender{s: s}, nil
}

func (gsnet *libp2pGraphSyncNetwork) newStreamToPeer(ctx context.Context, p peer.ID) (inet.Stream, error) {
	return gsnet.host.NewStream(ctx, p, ProtocolGraphsync)
}

func (gsnet *libp2pGraphSyncNetwork) SendMessage(
	ctx context.Context,
	p peer.ID,
	outgoing gsmsg.GraphSyncMessage) error {

	s, err := gsnet.newStreamToPeer(ctx, p)
	if err != nil {
		return err
	}

	if err = msgToStream(ctx, s, outgoing); err != nil {
		s.Reset()
		return err
	}

	// TODO(https://github.com/libp2p/go-libp2p-net/issues/28): Avoid this goroutine.
	go inet.AwaitEOF(s)
	return s.Close()

}

func (gsnet *libp2pGraphSyncNetwork) SetDelegate(r Receiver) {
	gsnet.receiver = r
}

func (gsnet *libp2pGraphSyncNetwork) ConnectTo(ctx context.Context, p peer.ID) error {
	return gsnet.host.Connect(ctx, pstore.PeerInfo{ID: p})
}

// handleNewStream receives a new stream from the network.
func (gsnet *libp2pGraphSyncNetwork) handleNewStream(s inet.Stream) {
	defer s.Close()

	if gsnet.receiver == nil {
		s.Reset()
		return
	}

	reader := ggio.NewDelimitedReader(s, inet.MessageSizeMax)
	for {
		received, err := gsmsg.FromPBReader(reader,
			gsnet.decodeSelectorFunc,
			gsnet.decodeSelectionResponseFunc)
		if err != nil {
			if err != io.EOF {
				s.Reset()
				go gsnet.receiver.ReceiveError(err)
				log.Debugf("graphsync net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		p := s.Conn().RemotePeer()
		ctx := context.Background()
		log.Debugf("graphsync net handleNewStream from %s", s.Conn().RemotePeer())
		gsnet.receiver.ReceiveMessage(ctx, p, received)
	}
}
