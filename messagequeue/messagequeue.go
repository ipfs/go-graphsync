package messagequeue

import (
	"context"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"

	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("graphsync")

const maxRetries = 10

// MessageNetwork is any network that can connect peers and generate a message
// sender.
type MessageNetwork interface {
	NewMessageSender(context.Context, peer.ID) (gsnet.MessageSender, error)
	ConnectTo(context.Context, peer.ID) error
}

// MessageQueue implements queue of want messages to send to peers.
type MessageQueue struct {
	p       peer.ID
	network MessageNetwork
	ctx     context.Context

	outgoingWork chan struct{}
	done         chan struct{}

	// internal do not touch outside go routines
	nextMessage        gsmsg.GraphSyncMessage
	nextMessageLk      sync.RWMutex
	processedNotifiers []chan struct{}
	sender             gsnet.MessageSender
}

// New creats a new MessageQueue.
func New(ctx context.Context, p peer.ID, network MessageNetwork) *MessageQueue {
	return &MessageQueue{
		ctx:          ctx,
		network:      network,
		p:            p,
		outgoingWork: make(chan struct{}, 1),
		done:         make(chan struct{}),
	}
}

// AddRequest adds an outgoing request to the message queue.
func (mq *MessageQueue) AddRequest(graphSyncRequest gsmsg.GraphSyncRequest) {

	if mq.mutateNextMessage(func(nextMessage gsmsg.GraphSyncMessage) {
		nextMessage.AddRequest(graphSyncRequest)
	}, nil) {
		mq.signalWork()
	}
}

// AddResponses adds the given blocks and responses to the next message and
// returns a channel that sends a notification when sending initiates. If ignored by the consumer
// sending will not block.
func (mq *MessageQueue) AddResponses(responses []gsmsg.GraphSyncResponse, blks []blocks.Block) <-chan struct{} {
	notificationChannel := make(chan struct{}, 1)
	if mq.mutateNextMessage(func(nextMessage gsmsg.GraphSyncMessage) {
		for _, response := range responses {
			nextMessage.AddResponse(response)
		}
		for _, block := range blks {
			nextMessage.AddBlock(block)
		}
	}, notificationChannel) {
		mq.signalWork()
	}
	return notificationChannel
}

// Startup starts the processing of messages, and creates an initial message
// based on the given initial wantlist.
func (mq *MessageQueue) Startup() {
	go mq.runQueue()
}

// Shutdown stops the processing of messages for a message queue.
func (mq *MessageQueue) Shutdown() {
	close(mq.done)
}

func (mq *MessageQueue) runQueue() {
	for {
		select {
		case <-mq.outgoingWork:
			mq.sendMessage()
		case <-mq.done:
			if mq.sender != nil {
				mq.sender.Close()
			}
			return
		case <-mq.ctx.Done():
			if mq.sender != nil {
				mq.sender.Reset()
			}
			return
		}
	}
}

func (mq *MessageQueue) mutateNextMessage(mutator func(gsmsg.GraphSyncMessage), processedNotifier chan struct{}) bool {
	mq.nextMessageLk.Lock()
	defer mq.nextMessageLk.Unlock()
	if mq.nextMessage == nil {
		mq.nextMessage = gsmsg.New()
	}
	mutator(mq.nextMessage)
	if processedNotifier != nil {
		mq.processedNotifiers = append(mq.processedNotifiers, processedNotifier)
	}
	return !mq.nextMessage.Empty()
}

func (mq *MessageQueue) signalWork() {
	select {
	case mq.outgoingWork <- struct{}{}:
	default:
	}
}

func (mq *MessageQueue) extractOutgoingMessage() gsmsg.GraphSyncMessage {
	// grab outgoing message
	mq.nextMessageLk.Lock()
	message := mq.nextMessage
	mq.nextMessage = nil
	for _, processedNotifier := range mq.processedNotifiers {
		select {
		case processedNotifier <- struct{}{}:
		default:
		}
		close(processedNotifier)
	}
	mq.processedNotifiers = nil
	mq.nextMessageLk.Unlock()
	return message
}

func (mq *MessageQueue) sendMessage() {
	message := mq.extractOutgoingMessage()
	if message == nil || message.Empty() {
		return
	}

	err := mq.initializeSender()
	if err != nil {
		log.Infof("cant open message sender to peer %s: %s", mq.p, err)
		// TODO: cant connect, what now?
		return
	}

	for i := 0; i < maxRetries; i++ { // try to send this message until we fail.
		if mq.attemptSendAndRecovery(message) {
			return
		}
	}
}

func (mq *MessageQueue) initializeSender() error {
	if mq.sender != nil {
		return nil
	}
	nsender, err := openSender(mq.ctx, mq.network, mq.p)
	if err != nil {
		return err
	}
	mq.sender = nsender
	return nil
}

func (mq *MessageQueue) attemptSendAndRecovery(message gsmsg.GraphSyncMessage) bool {
	err := mq.sender.SendMsg(mq.ctx, message)
	if err == nil {
		return true
	}

	log.Infof("graphsync send error: %s", err)
	mq.sender.Reset()
	mq.sender = nil

	select {
	case <-mq.done:
		return true
	case <-mq.ctx.Done():
		return true
	case <-time.After(time.Millisecond * 100):
		// wait 100ms in case disconnect notifications are still propogating
		log.Warning("SendMsg errored but neither 'done' nor context.Done() were set")
	}

	err = mq.initializeSender()
	if err != nil {
		log.Infof("couldnt open sender again after SendMsg(%s) failed: %s", mq.p, err)
		// TODO(why): what do we do now?
		// I think the *right* answer is to probably put the message we're
		// trying to send back, and then return to waiting for new work or
		// a disconnect.
		return true
	}

	return false
}

func openSender(ctx context.Context, network MessageNetwork, p peer.ID) (gsnet.MessageSender, error) {
	// allow ten minutes for connections this includes looking them up in the
	// dht dialing them, and handshaking
	conctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()

	err := network.ConnectTo(conctx, p)
	if err != nil {
		return nil, err
	}

	nsender, err := network.NewMessageSender(ctx, p)
	if err != nil {
		return nil, err
	}

	return nsender, nil
}
