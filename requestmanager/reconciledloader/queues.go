package reconciledloader

import (
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"go.opentelemetry.io/otel/trace"
)

var linkedRemoteItemPool = sync.Pool{
	New: func() interface{} {
		return new(remotedLinkedItem)
	},
}

var linkedLocalItemPool = sync.Pool{
	New: func() interface{} {
		return new(linkedLocalItem)
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

type localItem struct {
	link cid.Cid
	path datamodel.Path
}

type linkedLocalItem struct {
	localItem
	next *linkedLocalItem
}

func newLocal() *linkedLocalItem {
	newItem := linkedLocalItemPool.Get().(*linkedLocalItem)
	// need to reset next value to nil we're pulling out of a pool of potentially
	// old objects
	newItem.next = nil
	return newItem
}

func newRemote() *remotedLinkedItem {
	newItem := linkedRemoteItemPool.Get().(*remotedLinkedItem)
	// need to reset next value to nil we're pulling out of a pool of potentially
	// old objects
	newItem.next = nil
	return newItem
}

type remoteQueue struct {
	head   *remotedLinkedItem
	tail   *remotedLinkedItem
	open   bool
	lock   *sync.Mutex
	signal *sync.Cond
}

func (lrl *remoteQueue) first() remoteItem {
	lrl.lock.Lock()
	defer lrl.lock.Unlock()
	if lrl.head == nil {
		return remoteItem{}
	}

	return lrl.head.remoteItem
}

func (lrl *remoteQueue) consume() {
	lrl.lock.Lock()
	defer lrl.lock.Unlock()
	lrl.head.block = nil
	next := lrl.head.next
	linkedRemoteItemPool.Put(lrl.head)
	lrl.head = next
}

func (lrl *remoteQueue) clear() {
	lrl.lock.Lock()
	defer lrl.lock.Unlock()
	for lrl.head != nil {
		lrl.head.block = nil
		next := lrl.head.next
		linkedRemoteItemPool.Put(lrl.head)
		lrl.head = next
	}
	lrl.open = false
}

func (lrl *remoteQueue) setOpen(openStatus bool) {
	lrl.lock.Lock()
	defer lrl.lock.Unlock()
	wasOpen := lrl.open
	lrl.open = openStatus
	// if the queue is closing, trigger any expecting new items
	if !lrl.open && wasOpen {
		lrl.signal.Signal()
	}
}

func (lrl *remoteQueue) waitItems() bool {
	lrl.lock.Lock()
	defer lrl.lock.Unlock()

	for {
		// Case 1 item is  waiting
		if lrl.head != nil {
			return true
		}

		// Case 2 no available item and channel is closed
		if !lrl.open {
			return false
		}

		// Case 3 nothing available, wait for more items
		lrl.signal.Wait()
	}
}

func (lrl *remoteQueue) queue(newItems []*remotedLinkedItem) {
	lrl.lock.Lock()
	defer lrl.lock.Unlock()
	if !lrl.open {
		return
	}

	for _, newItem := range newItems {
		if lrl.head == nil {
			lrl.tail = newItem
			lrl.head = lrl.tail
		} else {
			lrl.tail.next = newItem
			lrl.tail = lrl.tail.next
		}
	}
	lrl.signal.Signal()
}

type offlineLoadQueue struct {
	head *linkedLocalItem
	tail *linkedLocalItem
}

func (olq *offlineLoadQueue) empty() bool {
	return olq.head == nil
}

func (olq *offlineLoadQueue) first() localItem {
	if olq.empty() {
		return localItem{}
	}

	return olq.head.localItem
}

func (olq *offlineLoadQueue) consume() {
	olq.head.path = datamodel.NewPath(nil)
	next := olq.head.next
	linkedLocalItemPool.Put(olq.head)
	olq.head = next
}

func (olq *offlineLoadQueue) store(lr loadAttempt) {
	local := newLocal()
	local.link = lr.link.(cidlink.Link).Cid
	local.path = lr.linkContext.LinkPath
	if olq.head == nil {
		olq.tail = local
		olq.head = olq.tail
	} else {
		olq.tail.next = local
		olq.tail = olq.tail.next
	}
}
