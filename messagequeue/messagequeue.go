package messagequeue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ipfs/go-graphsync"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"

	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/notifications"
)

var log = logging.Logger("graphsync")

// max block size is the maximum size for batching blocks in a single payload
const maxBlockSize uint64 = 512 * 1024

type EventName uint64

const (
	Queued EventName = iota
	Sent
	Error
)

type Event struct {
	Name EventName
	Err  error
}

// MessageNetwork is any network that can connect peers and generate a message
// sender.
type MessageNetwork interface {
	NewMessageSender(context.Context, peer.ID, gsnet.MessageSenderOpts) (gsnet.MessageSender, error)
	ConnectTo(context.Context, peer.ID) error
}

type Allocator interface {
	AllocateBlockMemory(p peer.ID, amount uint64) <-chan error
	ReleasePeerMemory(p peer.ID) error
	ReleaseBlockMemory(p peer.ID, amount uint64) error
}

// MessageQueue implements queue of want messages to send to peers.
type MessageQueue struct {
	p       peer.ID
	network MessageNetwork
	ctx     context.Context

	outgoingWork chan struct{}
	done         chan struct{}

	// internal do not touch outside go routines
	sender             gsnet.MessageSender
	eventPublisher     notifications.Publisher
	buildersLk         sync.RWMutex
	builders           []*gsmsg.Builder
	nextBuilderTopic   gsmsg.Topic
	allocator          Allocator
	maxRetries         int
	sendMessageTimeout time.Duration
}

// New creats a new MessageQueue.
func New(ctx context.Context, p peer.ID, network MessageNetwork, allocator Allocator, maxRetries int, sendMessageTimeout time.Duration) *MessageQueue {
	return &MessageQueue{
		ctx:                ctx,
		network:            network,
		p:                  p,
		outgoingWork:       make(chan struct{}, 1),
		done:               make(chan struct{}),
		eventPublisher:     notifications.NewPublisher(),
		allocator:          allocator,
		maxRetries:         maxRetries,
		sendMessageTimeout: sendMessageTimeout,
	}
}

// AllocateAndBuildMessage allows you to work modify the next message that is sent in the queue.
// If blkSize > 0, message building may block until enough memory has been freed from the queues to allocate the message.
func (mq *MessageQueue) AllocateAndBuildMessage(size uint64, buildMessageFn func(*gsmsg.Builder), notifees []notifications.Notifee) {
	if size > 0 {
		select {
		case <-mq.allocator.AllocateBlockMemory(mq.p, size):
		case <-mq.ctx.Done():
			return
		}
	}
	if mq.buildMessage(size, buildMessageFn, notifees) {
		mq.signalWork()
	}
}

type responseStream struct {
	requestID graphsync.RequestID
	closed    bool
	closedLk  sync.RWMutex
}

func (r *responseStream) close() {
	r.closedLk.Lock()
	r.closed = true
	r.closedLk.Unlock()
}

func (r *responseStream) isClosed() bool {
	r.closedLk.RLock()
	defer r.closedLk.RUnlock()
	return r.closed
}

func (mq *MessageQueue) buildMessage(size uint64, buildMessageFn func(*gsmsg.Builder), notifees []notifications.Notifee) bool {
	mq.buildersLk.Lock()
	defer mq.buildersLk.Unlock()
	if shouldBeginNewResponse(mq.builders, size) {
		topic := mq.nextBuilderTopic
		mq.nextBuilderTopic++
		mq.builders = append(mq.builders, gsmsg.NewBuilder(topic))
	}
	builder := mq.builders[len(mq.builders)-1]
	buildMessageFn(builder)
	for _, notifee := range notifees {
		notifications.SubscribeWithData(mq.eventPublisher, builder.Topic(), notifee)
	}
	return !builder.Empty()
}

func shouldBeginNewResponse(builders []*gsmsg.Builder, blkSize uint64) bool {
	if len(builders) == 0 {
		return true
	}
	if blkSize == 0 {
		return false
	}
	return builders[len(builders)-1].BlockSize()+blkSize > maxBlockSize
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
	defer func() {
		_ = mq.allocator.ReleasePeerMemory(mq.p)
		mq.eventPublisher.Shutdown()
	}()
	mq.eventPublisher.Startup()
	for {
		select {
		case <-mq.outgoingWork:
			mq.sendMessage()
		case <-mq.done:
			select {
			case <-mq.outgoingWork:
				for {
					_, metadata, err := mq.extractOutgoingMessage()
					if err == nil {
						mq.publishError(metadata, fmt.Errorf("message queue shutdown"))
						mq.eventPublisher.Close(metadata.topic)
					} else {
						break
					}
				}
			default:
			}
			if mq.sender != nil {
				mq.sender.Close()
			}
			return
		case <-mq.ctx.Done():
			if mq.sender != nil {
				_ = mq.sender.Reset()
			}
			return
		}
	}
}

func (mq *MessageQueue) signalWork() {
	select {
	case mq.outgoingWork <- struct{}{}:
	default:
	}
}

var errEmptyMessage = errors.New("empty Message")

func (mq *MessageQueue) extractOutgoingMessage() (gsmsg.GraphSyncMessage, messageMetadata, error) {
	// grab outgoing message
	mq.buildersLk.Lock()
	if len(mq.builders) == 0 {
		mq.buildersLk.Unlock()
		return gsmsg.GraphSyncMessage{}, messageMetadata{}, errEmptyMessage
	}
	builder := mq.builders[0]
	mq.builders = mq.builders[1:]
	// if there are more queued messages, signal we still have more work
	if len(mq.builders) > 0 {
		select {
		case mq.outgoingWork <- struct{}{}:
		default:
		}
	}
	mq.buildersLk.Unlock()
	if builder.Empty() {
		return gsmsg.GraphSyncMessage{}, messageMetadata{}, errEmptyMessage
	}
	message, err := builder.Build()
	return message, messageMetadata{builder.Topic(), builder.ResponseStreams(), builder.BlockSize()}, err
}

func (mq *MessageQueue) sendMessage() {
	message, metadata, err := mq.extractOutgoingMessage()
	if err != nil {
		if err != errEmptyMessage {
			log.Errorf("Unable to assemble GraphSync message: %s", err.Error())
		}
		return
	}
	mq.publishQueued(metadata)
	defer mq.eventPublisher.Close(metadata.topic)

	err = mq.initializeSender()
	if err != nil {
		log.Infof("cant open message sender to peer %s: %s", mq.p, err)
		// TODO: cant connect, what now?
		mq.publishError(metadata, fmt.Errorf("cant open message sender to peer %s: %w", mq.p, err))
		return
	}

	for i := 0; i < mq.maxRetries; i++ { // try to send this message until we fail.
		if mq.attemptSendAndRecovery(message, metadata) {
			return
		}
	}
	mq.publishError(metadata, fmt.Errorf("expended retries on SendMsg(%s)", mq.p))
}

func (mq *MessageQueue) scrubResponseStreams(responseStreams map[graphsync.RequestID]io.Closer) {
	requestIDs := make([]graphsync.RequestID, 0, len(responseStreams))
	for requestID, responseStream := range responseStreams {
		_ = responseStream.Close()
		requestIDs = append(requestIDs, requestID)
	}
	totalFreed := mq.scrubResponses(requestIDs)
	if totalFreed > 0 {
		err := mq.allocator.ReleaseBlockMemory(mq.p, totalFreed)
		if err != nil {
			log.Error(err)
		}
	}
}

// ScrubResponses removes the given response and associated blocks
// from all pending messages in the queue
func (mq *MessageQueue) scrubResponses(requestIDs []graphsync.RequestID) uint64 {
	mq.buildersLk.Lock()
	newBuilders := make([]*gsmsg.Builder, 0, len(mq.builders))
	totalFreed := uint64(0)
	for _, builder := range mq.builders {
		totalFreed = builder.ScrubResponses(requestIDs)
		if !builder.Empty() {
			newBuilders = append(newBuilders, builder)
		}
	}
	mq.builders = newBuilders
	mq.buildersLk.Unlock()
	return totalFreed
}

func (mq *MessageQueue) initializeSender() error {
	if mq.sender != nil {
		return nil
	}
	nsender, err := openSender(mq.ctx, mq.network, mq.p, mq.sendMessageTimeout)
	if err != nil {
		return err
	}
	mq.sender = nsender
	return nil
}

func (mq *MessageQueue) attemptSendAndRecovery(message gsmsg.GraphSyncMessage, metadata messageMetadata) bool {
	err := mq.sender.SendMsg(mq.ctx, message)
	if err == nil {
		mq.publishSent(metadata)
		return true
	}

	log.Infof("graphsync send error: %s", err)
	_ = mq.sender.Reset()
	mq.sender = nil

	select {
	case <-mq.done:
		mq.publishError(metadata, errors.New("queue shutdown"))
		return true
	case <-mq.ctx.Done():
		mq.publishError(metadata, errors.New("context cancelled"))
		return true
	case <-time.After(time.Millisecond * 100):
		// wait 100ms in case disconnect notifications are still propogating
		log.Warn("SendMsg errored but neither 'done' nor context.Done() were set")
	}

	err = mq.initializeSender()
	if err != nil {
		log.Infof("couldnt open sender again after SendMsg(%s) failed: %s", mq.p, err)
		// TODO(why): what do we do now?
		// I think the *right* answer is to probably put the message we're
		// trying to send back, and then return to waiting for new work or
		// a disconnect.
		mq.publishError(metadata, fmt.Errorf("couldnt open sender again after SendMsg(%s) failed: %w", mq.p, err))
		return true
	}

	return false
}

func openSender(ctx context.Context, network MessageNetwork, p peer.ID, sendTimeout time.Duration) (gsnet.MessageSender, error) {
	// allow ten minutes for connections this includes looking them up in the
	// dht dialing them, and handshaking
	conctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()

	err := network.ConnectTo(conctx, p)
	if err != nil {
		return nil, err
	}

	nsender, err := network.NewMessageSender(ctx, p, gsnet.MessageSenderOpts{SendTimeout: sendTimeout})
	if err != nil {
		return nil, err
	}

	return nsender, nil
}

type messageMetadata struct {
	topic           gsmsg.Topic
	responseStreams map[graphsync.RequestID]io.Closer
	msgSize         uint64
}

func (mq *MessageQueue) publishQueued(metadata messageMetadata) {
	mq.eventPublisher.Publish(metadata.topic, Event{Name: Queued})
}

func (mq *MessageQueue) publishSent(metadata messageMetadata) {
	mq.eventPublisher.Publish(metadata.topic, Event{Name: Sent})
	_ = mq.allocator.ReleaseBlockMemory(mq.p, metadata.msgSize)
}

func (mq *MessageQueue) publishError(metadata messageMetadata, err error) {
	mq.scrubResponseStreams(metadata.responseStreams)
	mq.eventPublisher.Publish(metadata.topic, Event{Name: Error, Err: err})
	_ = mq.allocator.ReleaseBlockMemory(mq.p, metadata.msgSize)
}

func (mq *MessageQueue) close(metadata messageMetadata) {
	mq.eventPublisher.Close(metadata.topic)
}
