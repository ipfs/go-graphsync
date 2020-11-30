package network

import (
	"context"
	"fmt"
	"io"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/jpillora/backoff"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/message/message1_0"
)

var log = logging.Logger("data_transfer_network")

var sendMessageTimeout = time.Minute * 10

const defaultMaxStreamOpenAttempts = 5
const defaultMinAttemptDuration = 1 * time.Second
const defaultMaxAttemptDuration = 5 * time.Minute

var defaultDataTransferProtocols = []protocol.ID{datatransfer.ProtocolDataTransfer1_1, datatransfer.ProtocolDataTransfer1_0}

// Option is an option for configuring the libp2p storage market network
type Option func(*libp2pDataTransferNetwork)

// DataTransferProtocols OVERWRITES the default libp2p protocols we use for data transfer with the given protocols.
func DataTransferProtocols(protocols []protocol.ID) Option {
	return func(impl *libp2pDataTransferNetwork) {
		impl.dtProtocols = nil
		impl.dtProtocols = append(impl.dtProtocols, protocols...)
	}
}

// RetryParameters changes the default parameters around connection reopening
func RetryParameters(minDuration time.Duration, maxDuration time.Duration, attempts float64) Option {
	return func(impl *libp2pDataTransferNetwork) {
		impl.maxStreamOpenAttempts = attempts
		impl.minAttemptDuration = minDuration
		impl.maxAttemptDuration = maxDuration
	}
}

// NewFromLibp2pHost returns a GraphSyncNetwork supported by underlying Libp2p host.
func NewFromLibp2pHost(host host.Host, options ...Option) DataTransferNetwork {
	dataTransferNetwork := libp2pDataTransferNetwork{
		host: host,

		maxStreamOpenAttempts: defaultMaxStreamOpenAttempts,
		minAttemptDuration:    defaultMinAttemptDuration,
		maxAttemptDuration:    defaultMaxAttemptDuration,
		dtProtocols:           defaultDataTransferProtocols,
	}

	for _, option := range options {
		option(&dataTransferNetwork)
	}

	return &dataTransferNetwork
}

// libp2pDataTransferNetwork transforms the libp2p host interface, which sends and receives
// NetMessage objects, into the graphsync network interface.
type libp2pDataTransferNetwork struct {
	host host.Host
	// inbound messages from the network are forwarded to the receiver
	receiver Receiver

	maxStreamOpenAttempts float64
	minAttemptDuration    time.Duration
	maxAttemptDuration    time.Duration
	dtProtocols           []protocol.ID
}

func (impl *libp2pDataTransferNetwork) openStream(ctx context.Context, id peer.ID, protocols ...protocol.ID) (network.Stream, error) {
	b := &backoff.Backoff{
		Min:    impl.minAttemptDuration,
		Max:    impl.maxAttemptDuration,
		Factor: impl.maxStreamOpenAttempts,
		Jitter: true,
	}

	for {
		// will use the first among the given protocols that the remote peer supports
		s, err := impl.host.NewStream(ctx, id, protocols...)
		if err == nil {
			return s, err
		}

		nAttempts := b.Attempt()
		if nAttempts == impl.maxStreamOpenAttempts {
			return nil, xerrors.Errorf("exhausted %d attempts but failed to open stream, err: %w", impl.maxStreamOpenAttempts, err)
		}

		d := b.Duration()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d):
		}
	}
}

func (dtnet *libp2pDataTransferNetwork) SendMessage(
	ctx context.Context,
	p peer.ID,
	outgoing datatransfer.Message) error {

	s, err := dtnet.openStream(ctx, p, dtnet.dtProtocols...)
	if err != nil {
		return err
	}

	outgoing, err = outgoing.MessageForProtocol(s.Protocol())
	if err != nil {
		return xerrors.Errorf("failed to convert message for protocol: %w", err)
	}

	if err = msgToStream(ctx, s, outgoing); err != nil {
		if err2 := s.Reset(); err2 != nil {
			log.Error(err)
			return err2
		}
		return err
	}

	return s.Close()
}

func (dtnet *libp2pDataTransferNetwork) SetDelegate(r Receiver) {
	dtnet.receiver = r
	for _, p := range dtnet.dtProtocols {
		dtnet.host.SetStreamHandler(p, dtnet.handleNewStream)
	}
}

func (dtnet *libp2pDataTransferNetwork) ConnectTo(ctx context.Context, p peer.ID) error {
	return dtnet.host.Connect(ctx, peer.AddrInfo{ID: p})
}

// handleNewStream receives a new stream from the network.
func (dtnet *libp2pDataTransferNetwork) handleNewStream(s network.Stream) {
	defer s.Close() // nolint: errcheck,gosec

	if dtnet.receiver == nil {
		s.Reset() // nolint: errcheck,gosec
		return
	}

	for {
		var received datatransfer.Message
		var err error
		if s.Protocol() == datatransfer.ProtocolDataTransfer1_1 {
			received, err = message.FromNet(s)
		} else {
			received, err = message1_0.FromNet(s)
		}

		if err != nil {
			if err != io.EOF {
				s.Reset() // nolint: errcheck,gosec
				go dtnet.receiver.ReceiveError(err)
				log.Debugf("graphsync net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		p := s.Conn().RemotePeer()
		ctx := context.Background()
		log.Debugf("graphsync net handleNewStream from %s", s.Conn().RemotePeer())

		if received.IsRequest() {
			receivedRequest, ok := received.(datatransfer.Request)
			if ok {
				if receivedRequest.IsRestartExistingChannelRequest() {
					dtnet.receiver.ReceiveRestartExistingChannelRequest(ctx, p, receivedRequest)
				} else {
					dtnet.receiver.ReceiveRequest(ctx, p, receivedRequest)
				}
			}
		} else {
			receivedResponse, ok := received.(datatransfer.Response)
			if ok {
				dtnet.receiver.ReceiveResponse(ctx, p, receivedResponse)
			}
		}
	}
}

func (dtnet *libp2pDataTransferNetwork) ID() peer.ID {
	return dtnet.host.ID()
}

func (dtnet *libp2pDataTransferNetwork) Protect(id peer.ID, tag string) {
	dtnet.host.ConnManager().Protect(id, tag)
}

func (dtnet *libp2pDataTransferNetwork) Unprotect(id peer.ID, tag string) bool {
	return dtnet.host.ConnManager().Unprotect(id, tag)
}

func msgToStream(ctx context.Context, s network.Stream, msg datatransfer.Message) error {
	if msg.IsRequest() {
		log.Debugf("Outgoing request message for transfer ID: %d", msg.TransferID())
	}

	deadline := time.Now().Add(sendMessageTimeout)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}
	if err := s.SetWriteDeadline(deadline); err != nil {
		log.Warnf("error setting deadline: %s", err)
	}

	switch s.Protocol() {
	case datatransfer.ProtocolDataTransfer1_1:
	case datatransfer.ProtocolDataTransfer1_0:
	default:
		return fmt.Errorf("unrecognized protocol on remote: %s", s.Protocol())
	}

	if err := msg.ToNet(s); err != nil {
		log.Debugf("error: %s", err)
		return err
	}

	if err := s.SetWriteDeadline(time.Time{}); err != nil {
		log.Warnf("error resetting deadline: %s", err)
	}
	return nil
}
