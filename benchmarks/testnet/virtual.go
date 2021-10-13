package testnet

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
	"github.com/libp2p/go-libp2p-core/connmgr"
	"github.com/libp2p/go-libp2p-core/peer"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"google.golang.org/protobuf/proto"

	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
)

// VirtualNetwork generates a new testnet instance - a fake network that
// is used to simulate sending messages.
func VirtualNetwork(d delay.D) Network {
	return &network{
		latencies:          make(map[peer.ID]map[peer.ID]time.Duration),
		clients:            make(map[peer.ID]*receiverQueue),
		delay:              d,
		isRateLimited:      false,
		rateLimitGenerator: nil,
		conns:              make(map[string]struct{}),
	}
}

// RateLimitGenerator is an interface for generating rate limits across peers
type RateLimitGenerator interface {
	NextRateLimit() float64
}

// RateLimitedVirtualNetwork generates a testnet instance where nodes are rate
// limited in the upload/download speed.
func RateLimitedVirtualNetwork(rs mockrouting.Server, d delay.D, rateLimitGenerator RateLimitGenerator) Network {
	return &network{
		latencies:          make(map[peer.ID]map[peer.ID]time.Duration),
		rateLimiters:       make(map[peer.ID]map[peer.ID]*mocknet.RateLimiter),
		clients:            make(map[peer.ID]*receiverQueue),
		delay:              d,
		isRateLimited:      true,
		rateLimitGenerator: rateLimitGenerator,
		conns:              make(map[string]struct{}),
	}
}

type network struct {
	mu                 sync.Mutex
	latencies          map[peer.ID]map[peer.ID]time.Duration
	rateLimiters       map[peer.ID]map[peer.ID]*mocknet.RateLimiter
	clients            map[peer.ID]*receiverQueue
	delay              delay.D
	isRateLimited      bool
	rateLimitGenerator RateLimitGenerator
	conns              map[string]struct{}
}

type message struct {
	from       peer.ID
	msg        gsmsg.GraphSyncMessage
	shouldSend time.Time
}

// receiverQueue queues up a set of messages to be sent, and sends them *in
// order* with their delays respected as much as sending them in order allows
// for
type receiverQueue struct {
	receiver *networkClient
	queue    []*message
	active   bool
	lk       sync.Mutex
}

func (n *network) Adapter(p tnet.Identity) gsnet.GraphSyncNetwork {
	n.mu.Lock()
	defer n.mu.Unlock()

	client := &networkClient{
		local:   p.ID(),
		network: n,
	}
	n.clients[p.ID()] = &receiverQueue{receiver: client}
	return client
}

func (n *network) HasPeer(p peer.ID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, found := n.clients[p]
	return found
}

// TODO should this be completely asynchronous?
// TODO what does the network layer do with errors received from services?
func (n *network) SendMessage(
	ctx context.Context,
	from peer.ID,
	to peer.ID,
	mes gsmsg.GraphSyncMessage) error {

	mes = mes.Clone()

	n.mu.Lock()
	defer n.mu.Unlock()

	latencies, ok := n.latencies[from]
	if !ok {
		latencies = make(map[peer.ID]time.Duration)
		n.latencies[from] = latencies
	}

	latency, ok := latencies[to]
	if !ok {
		latency = n.delay.NextWaitTime()
		latencies[to] = latency
	}

	var bandwidthDelay time.Duration
	if n.isRateLimited {
		rateLimiters, ok := n.rateLimiters[from]
		if !ok {
			rateLimiters = make(map[peer.ID]*mocknet.RateLimiter)
			n.rateLimiters[from] = rateLimiters
		}

		rateLimiter, ok := rateLimiters[to]
		if !ok {
			rateLimiter = mocknet.NewRateLimiter(n.rateLimitGenerator.NextRateLimit())
			rateLimiters[to] = rateLimiter
		}

		pbMsg, err := mes.ToProto()
		if err != nil {
			return err
		}
		size := proto.Size(pbMsg)
		bandwidthDelay = rateLimiter.Limit(size)
	} else {
		bandwidthDelay = 0
	}

	receiver, ok := n.clients[to]
	if !ok {
		return errors.New("cannot locate peer on network")
	}

	// nb: terminate the context since the context wouldn't actually be passed
	// over the network in a real scenario

	msg := &message{
		from:       from,
		msg:        mes,
		shouldSend: time.Now().Add(latency).Add(bandwidthDelay),
	}
	receiver.enqueue(msg)

	return nil
}

type networkClient struct {
	local peer.ID
	gsnet.Receiver
	network *network
}

func (nc *networkClient) SendMessage(
	ctx context.Context,
	to peer.ID,
	message gsmsg.GraphSyncMessage) error {
	if err := nc.network.SendMessage(ctx, nc.local, to, message); err != nil {
		return err
	}
	return nil
}

type messagePasser struct {
	net    *networkClient
	target peer.ID
	local  peer.ID
	ctx    context.Context
}

func (mp *messagePasser) SendMsg(ctx context.Context, m gsmsg.GraphSyncMessage) error {
	return mp.net.SendMessage(ctx, mp.target, m)
}

func (mp *messagePasser) Close() error {
	return nil
}

func (mp *messagePasser) Reset() error {
	return nil
}

func (nc *networkClient) NewMessageSender(ctx context.Context, p peer.ID, _ gsnet.MessageSenderOpts) (gsnet.MessageSender, error) {
	return &messagePasser{
		net:    nc,
		target: p,
		local:  nc.local,
		ctx:    ctx,
	}, nil
}

func (nc *networkClient) SetDelegate(r gsnet.Receiver) {
	nc.Receiver = r
}

func (nc *networkClient) ConnectTo(_ context.Context, p peer.ID) error {
	nc.network.mu.Lock()
	otherClient, ok := nc.network.clients[p]
	if !ok {
		nc.network.mu.Unlock()
		return errors.New("no such peer in network")
	}

	tag := tagForPeers(nc.local, p)
	if _, ok := nc.network.conns[tag]; ok {
		nc.network.mu.Unlock()
		// log.Warning("ALREADY CONNECTED TO PEER (is this a reconnect? test lib needs fixing)")
		return nil
	}
	nc.network.conns[tag] = struct{}{}
	nc.network.mu.Unlock()

	otherClient.receiver.Connected(nc.local)
	nc.Receiver.Connected(p)
	return nil
}

func (nc *networkClient) DisconnectFrom(_ context.Context, p peer.ID) error {
	nc.network.mu.Lock()
	defer nc.network.mu.Unlock()

	otherClient, ok := nc.network.clients[p]
	if !ok {
		return errors.New("no such peer in network")
	}

	tag := tagForPeers(nc.local, p)
	if _, ok := nc.network.conns[tag]; !ok {
		// Already disconnected
		return nil
	}
	delete(nc.network.conns, tag)

	otherClient.receiver.Disconnected(nc.local)
	nc.Receiver.Disconnected(p)
	return nil
}

func (nc *networkClient) ConnectionManager() gsnet.ConnManager {
	return &connmgr.NullConnMgr{}
}

func (rq *receiverQueue) enqueue(m *message) {
	rq.lk.Lock()
	defer rq.lk.Unlock()
	rq.queue = append(rq.queue, m)
	if !rq.active {
		rq.active = true
		go rq.process()
	}
}

func (rq *receiverQueue) Swap(i, j int) {
	rq.queue[i], rq.queue[j] = rq.queue[j], rq.queue[i]
}

func (rq *receiverQueue) Len() int {
	return len(rq.queue)
}

func (rq *receiverQueue) Less(i, j int) bool {
	return rq.queue[i].shouldSend.UnixNano() < rq.queue[j].shouldSend.UnixNano()
}

func (rq *receiverQueue) process() {
	for {
		rq.lk.Lock()
		sort.Sort(rq)
		if len(rq.queue) == 0 {
			rq.active = false
			rq.lk.Unlock()
			return
		}
		m := rq.queue[0]
		if time.Until(m.shouldSend).Seconds() < 0.1 {
			rq.queue = rq.queue[1:]
			rq.lk.Unlock()
			time.Sleep(time.Until(m.shouldSend))
			rq.receiver.ReceiveMessage(context.TODO(), m.from, m.msg)
		} else {
			rq.lk.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func tagForPeers(a, b peer.ID) string {
	if a < b {
		return string(a + b)
	}
	return string(b + a)
}
