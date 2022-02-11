package reconciledloader

import (
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"go.opentelemetry.io/otel/trace"
)

var linkedRemoteItemPool = sync.Pool{
	New: func() interface{} {
		return new(remotedLinkedItem)
	},
}

type remoteItem struct {
	link      cid.Cid
	action    graphsync.LinkAction
	block     []byte
	traceLink trace.Link
}

type remotedLinkedItem struct {
	remoteItem
	next *remotedLinkedItem
}

func newRemote() *remotedLinkedItem {
	newItem := linkedRemoteItemPool.Get().(*remotedLinkedItem)
	// need to reset next value to nil we're pulling out of a pool of potentially
	// old objects
	newItem.next = nil
	return newItem
}

func freeList(remoteItems []*remotedLinkedItem) {
	for _, ri := range remoteItems {
		ri.block = nil
		linkedRemoteItemPool.Put(ri)
	}
}

type remoteQueue struct {
	head     *remotedLinkedItem
	tail     *remotedLinkedItem
	dataSize uint64
}

func (rq *remoteQueue) empty() bool {
	return rq.head == nil
}

func (rq *remoteQueue) first() remoteItem {
	if rq.head == nil {
		return remoteItem{}
	}

	return rq.head.remoteItem
}

func (rq *remoteQueue) consume() uint64 {
	// update our total data size buffered
	rq.dataSize -= uint64(len(rq.head.block))
	rq.head.block = nil
	next := rq.head.next
	linkedRemoteItemPool.Put(rq.head)
	rq.head = next
	return rq.dataSize
}

func (rq *remoteQueue) clear() {
	for rq.head != nil {
		rq.consume()
	}
}

func (rq *remoteQueue) queue(newItems []*remotedLinkedItem) uint64 {
	for _, newItem := range newItems {
		// update total size buffered

		// TODO: this is a good place to hold off on accepting data
		// to let the local traversal catch up
		// a second enqueue/dequeue signal would allow us
		// to make this call block until datasize dropped below a certain amount
		rq.dataSize += uint64(len(newItem.block))
		if rq.head == nil {
			rq.tail = newItem
			rq.head = rq.tail
		} else {
			rq.tail.next = newItem
			rq.tail = rq.tail.next
		}
	}
	return rq.dataSize
}
