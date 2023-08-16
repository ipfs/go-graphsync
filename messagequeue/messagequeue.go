package messagequeue

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-protocolnetwork/pkg/messagequeue"
	"github.com/ipfs/go-protocolnetwork/pkg/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-protocolnetwork/pkg/notifications"
)

var log = logging.Logger("graphsync")

// max block size is the maximum size for batching blocks in a single payload
const maxBlockSize uint64 = 512 * 1024

type Topic uint64

type EventName uint64

const (
	Queued EventName = iota
	Sent
	Error
)

type Metadata struct {
	BlockData     map[graphsync.RequestID][]graphsync.BlockData
	ResponseCodes map[graphsync.RequestID]graphsync.ResponseStatusCode
}

type Event struct {
	Name     EventName
	Err      error
	Metadata Metadata
}

type Allocator interface {
	AllocateBlockMemory(p peer.ID, amount uint64) <-chan error
	ReleasePeerMemory(p peer.ID) error
	ReleaseBlockMemory(p peer.ID, amount uint64) error
}

// MessageParams are parameters sent to build messages
type MessageParams struct {
	Size           uint64
	BuildMessageFn func(*Builder)
}

// messageBuilder implements queue of want messages to send to peers.
type messageBuilder struct {
	ctx context.Context
	p   peer.ID
	// internal do not touch outside go routines
	eventPublisher   notifications.Publisher[Topic, Event]
	buildersLk       sync.RWMutex
	builders         []*Builder
	nextBuilderTopic Topic
	allocator        Allocator
}

// New creats a new MessageQueue.
func New(ctx context.Context, p peer.ID, messageNetwork messagequeue.MessageNetwork[gsmsg.GraphSyncMessage], allocator Allocator, maxRetries int, sendMessageTimeout time.Duration, sendErrorBackoff time.Duration, onShutdown func(peer.ID)) *messagequeue.MessageQueue[gsmsg.GraphSyncMessage, MessageParams] {
	eventPublisher := notifications.NewPublisher[Topic, Event]()
	messageBuilder := &messageBuilder{
		ctx:            ctx,
		p:              p,
		eventPublisher: eventPublisher,
		allocator:      allocator,
	}
	return messagequeue.New[gsmsg.GraphSyncMessage, MessageParams](ctx, p, messageNetwork, messageBuilder, &network.MessageSenderOpts{
		MaxRetries:       maxRetries,
		SendTimeout:      sendMessageTimeout,
		SendErrorBackoff: sendErrorBackoff,
	}, eventPublisher.Startup, func() {
		allocator.ReleasePeerMemory(p)
		eventPublisher.Shutdown()
		onShutdown(p)
	})

}

// AllocateAndBuildMessage allows you to work modify the next message that is sent in the queue.
// If blkSize > 0, message building may block until enough memory has been freed from the queues to allocate the message.
func (mb *messageBuilder) BuildMessage(params MessageParams) bool {
	if params.Size > 0 {
		select {
		case <-mb.allocator.AllocateBlockMemory(mb.p, params.Size):
		case <-mb.ctx.Done():
			return false
		}
	}
	return mb.buildMessage(params.Size, params.BuildMessageFn)
}

func (mb *messageBuilder) buildMessage(size uint64, buildMessageFn func(*Builder)) bool {
	mb.buildersLk.Lock()
	defer mb.buildersLk.Unlock()
	if shouldBeginNewResponse(mb.builders, size) {
		topic := mb.nextBuilderTopic
		mb.nextBuilderTopic++
		ctx, _ := otel.Tracer("graphsync").Start(mb.ctx, "message", trace.WithAttributes(
			attribute.Int64("topic", int64(topic)),
		))
		mb.builders = append(mb.builders, NewBuilder(ctx, topic))
	}
	builder := mb.builders[len(mb.builders)-1]
	buildMessageFn(builder)
	return !builder.Empty()
}

func shouldBeginNewResponse(builders []*Builder, blkSize uint64) bool {
	if len(builders) == 0 {
		return true
	}
	if blkSize == 0 {
		return false
	}
	return builders[len(builders)-1].BlockSize()+blkSize > maxBlockSize
}

var errEmptyMessage = errors.New("empty Message")

func (mb *messageBuilder) NextMessage() (messagequeue.MessageSpec[gsmsg.GraphSyncMessage], bool, error) {
	// grab outgoing message
	mb.buildersLk.Lock()
	defer mb.buildersLk.Unlock()
	if len(mb.builders) == 0 {
		return nil, false, errEmptyMessage
	}
	b := mb.builders[0]
	mb.builders = mb.builders[1:]
	if b.Empty() {
		return nil, len(mb.builders) > 0, errEmptyMessage
	}
	return func() (gsmsg.GraphSyncMessage, messagequeue.Notifier, error) {
		message, err := b.Build()
		if err != nil {
			return gsmsg.GraphSyncMessage{}, &internalMetadata{}, err
		}
		for _, subscriber := range b.subscribers {
			mb.eventPublisher.Subscribe(b.topic, subscriber)
		}
		return message, &internalMetadata{
			builder: mb,
			span:    trace.SpanFromContext(b.ctx),
			public: Metadata{
				BlockData:     b.blockData,
				ResponseCodes: message.ResponseCodes(),
			},
			ctx:             b.ctx,
			topic:           b.topic,
			msgSize:         b.BlockSize(),
			responseStreams: b.responseStreams,
		}, nil
	}, len(mb.builders) > 0, nil
}

func (mb *messageBuilder) scrubResponseStreams(responseStreams map[graphsync.RequestID]io.Closer) {
	requestIDs := make([]graphsync.RequestID, 0, len(responseStreams))
	for requestID, responseStream := range responseStreams {
		_ = responseStream.Close()
		requestIDs = append(requestIDs, requestID)
	}
	totalFreed := mb.scrubResponses(requestIDs)
	if totalFreed > 0 {
		err := mb.allocator.ReleaseBlockMemory(mb.p, totalFreed)
		if err != nil {
			log.Error(err)
		}
	}
}

// ScrubResponses removes the given response and associated blocks
// from all pending messages in the queue
func (mb *messageBuilder) scrubResponses(requestIDs []graphsync.RequestID) uint64 {
	mb.buildersLk.Lock()
	newBuilders := make([]*Builder, 0, len(mb.builders))
	totalFreed := uint64(0)
	for _, builder := range mb.builders {
		totalFreed = builder.ScrubResponses(requestIDs)
		if !builder.Empty() {
			newBuilders = append(newBuilders, builder)
		}
	}
	mb.builders = newBuilders
	mb.buildersLk.Unlock()
	return totalFreed
}

type internalMetadata struct {
	builder         *messageBuilder
	span            trace.Span
	sendSpan        trace.Span
	ctx             context.Context
	public          Metadata
	topic           Topic
	msgSize         uint64
	responseStreams map[graphsync.RequestID]io.Closer
}

func (metadata *internalMetadata) HandleQueued() {
	_, metadata.sendSpan = otel.Tracer("graphsync").Start(metadata.ctx, "sendMessage", trace.WithAttributes(
		attribute.Int64("topic", int64(metadata.topic)),
		attribute.Int64("size", int64(metadata.msgSize)),
	))
	metadata.builder.eventPublisher.Publish(metadata.topic, Event{Name: Queued, Metadata: metadata.public})
}

func (metadata *internalMetadata) HandleSent() {
	metadata.builder.eventPublisher.Publish(metadata.topic, Event{Name: Sent, Metadata: metadata.public})
	_ = metadata.builder.allocator.ReleaseBlockMemory(metadata.builder.p, metadata.msgSize)
}

func (metadata *internalMetadata) HandleError(err error) {
	metadata.builder.scrubResponseStreams(metadata.responseStreams)
	metadata.builder.eventPublisher.Publish(metadata.topic, Event{Name: Error, Err: err, Metadata: metadata.public})
	_ = metadata.builder.allocator.ReleaseBlockMemory(metadata.builder.p, metadata.msgSize)
	metadata.span.RecordError(err)
	metadata.span.SetStatus(codes.Error, err.Error())
}

func (metadata *internalMetadata) HandleFinished() {
	metadata.builder.eventPublisher.Close(metadata.topic)
	if metadata.sendSpan != nil {
		metadata.sendSpan.End()
	}
	metadata.span.End()
}
