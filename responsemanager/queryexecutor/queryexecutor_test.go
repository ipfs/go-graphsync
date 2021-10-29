package queryexecutor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/ipfs/go-graphsync/listeners"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestEmptyTask(t *testing.T) {
	td, qe := newTestData(t, 0, 0)
	defer td.cancel()
	td.manager.responseTask = ResponseTask{Empty: true}
	require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
}

func TestOneBlockTask(t *testing.T) {
	td, qe := newTestData(t, 1, 1)
	defer td.cancel()
	notifeeExpect(t, td, 1, td.responseCode)
	require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
	require.Equal(t, 0, td.clearRequestCalls)
	require.Equal(t, 0, td.cancelledCalls)
}

func TestSmallGraphTask(t *testing.T) {
	blockHookExpect := func(t *testing.T, td *testData, triggerAt int, triggerCb func(graphsync.OutgoingBlockHookActions), limit int) {
		var hookCalls int
		td.blockHooks.Register(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			if hookCalls == triggerAt {
				triggerCb(hookActions)
			}
			// complete the current block we have on hand once we have a pause signal
			require.LessOrEqual(t, hookCalls, limit, "called block hook too many times")
			hookCalls++
		})
	}

	transactionExpect := func(t *testing.T, td *testData, errorAt []int, errorStr string) {
		var transactionCalls int
		td.responseAssembler.transactionCb = func(e error) {
			var erroredAt bool
			for _, i := range errorAt {
				if transactionCalls == i {
					require.EqualError(t, e, errorStr)
					erroredAt = true
				}
			}
			if !erroredAt {
				require.NoError(t, e)
			}
			transactionCalls++
		}
	}

	t.Run("full graph", func(t *testing.T) {
		td, qe := newTestData(t, 10, 10)
		defer td.cancel()
		notifeeExpect(t, td, 10, td.responseCode) // AddNotifee called on all blocks
		require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
		require.Equal(t, 0, td.clearRequestCalls)
		require.Equal(t, 0, td.cancelledCalls)
	})

	t.Run("paused by hook", func(t *testing.T) {
		td, qe := newTestData(t, 10, 7)
		defer td.cancel()
		notifeeExpect(t, td, 7, nil) // AddNotifee called on first 7 blocks
		blockHookExpect(t, td, 6, func(hookActions graphsync.OutgoingBlockHookActions) {
			hookActions.PauseResponse()
		}, 6)
		transactionExpect(t, td, []int{6, 7}, hooks.ErrPaused{}.Error()) // last 2 transactions are ErrPaused

		require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
		require.Equal(t, 1, td.pauseCalls)
		require.Equal(t, 0, td.clearRequestCalls)
		require.Equal(t, 0, td.cancelledCalls)
	})

	t.Run("paused by signal", func(t *testing.T) {
		td, qe := newTestData(t, 10, 7)
		defer td.cancel()
		notifeeExpect(t, td, 7, nil) // AddNotifee called on first 7 blocks
		blockHookExpect(t, td, 5, func(hookActions graphsync.OutgoingBlockHookActions) {
			select {
			case td.signals.PauseSignal <- struct{}{}:
			default:
				require.Fail(t, "failed to send pause signal")
			}
		}, 7)
		transactionExpect(t, td, []int{6, 7}, hooks.ErrPaused{}.Error()) // last 2 transactions are ErrPaused

		require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
		require.Equal(t, 1, td.pauseCalls)
		require.Equal(t, 0, td.clearRequestCalls)
		require.Equal(t, 0, td.cancelledCalls)
	})

	t.Run("partial cancelled by hook", func(t *testing.T) {
		td, qe := newTestData(t, 10, 7)
		defer td.cancel()
		notifeeExpect(t, td, 7, nil) // AddNotifee called on first 7 blocks
		blockHookExpect(t, td, 6, func(hookActions graphsync.OutgoingBlockHookActions) {
			hookActions.TerminateWithError(ipldutil.ContextCancelError{})
		}, 6)
		transactionExpect(t, td, []int{6, 7}, ipldutil.ContextCancelError{}.Error()) // last 2 transactions are ContextCancelled

		require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
		require.Equal(t, 1, td.cancelledCalls)
		require.Equal(t, 1, td.clearRequestCalls)
	})

	t.Run("partial cancelled by signal", func(t *testing.T) {
		// we load 7 blocks, by don't send the final one because cancel interrupts it,
		// unlike via blockhooks which is run after the block is sent
		td, qe := newTestData(t, 10, 7)
		defer td.cancel()
		notifeeExpect(t, td, 6, graphsync.RequestCancelled) // AddNotifee called on first 6 blocks
		blockHookExpect(t, td, 5, func(hookActions graphsync.OutgoingBlockHookActions) {
			select {
			case td.signals.ErrSignal <- ErrCancelledByCommand:
			default:
				require.Fail(t, "failed to send error signal")
			}
		}, 6)
		transactionExpect(t, td, []int{6, 7}, ErrCancelledByCommand.Error())

		require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
		require.Equal(t, 0, td.clearRequestCalls)
		// cancelled by signal doesn't mean we get a cancelled call here
		// ErrCancelledByCommand is treated differently to a context cancellation error
		require.Equal(t, 0, td.cancelledCalls)
	})

	t.Run("unknown error by hook", func(t *testing.T) {
		td, qe := newTestData(t, 10, 7)
		defer td.cancel()
		expectedErr := fmt.Errorf("derp")
		notifeeExpect(t, td, 7, graphsync.RequestFailedUnknown) // AddNotifee called on first 7 blocks
		blockHookExpect(t, td, 6, func(hookActions graphsync.OutgoingBlockHookActions) {
			hookActions.TerminateWithError(expectedErr)
		}, 6)
		transactionExpect(t, td, []int{6, 7}, expectedErr.Error())

		require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
		require.Equal(t, 0, td.clearRequestCalls)
		require.Equal(t, 0, td.cancelledCalls)
	})

	t.Run("unknown error by signal", func(t *testing.T) {
		// we load 7 blocks, by don't send the final one because error interrupts it,
		// unlike via blockhooks which is run after the block is sent
		td, qe := newTestData(t, 10, 7)
		defer td.cancel()
		expectedErr := fmt.Errorf("derp")
		notifeeExpect(t, td, 6, graphsync.RequestFailedUnknown) // AddNotifee called on first 6 blocks
		blockHookExpect(t, td, 5, func(hookActions graphsync.OutgoingBlockHookActions) {
			select {
			case td.signals.ErrSignal <- expectedErr:
			default:
				require.Fail(t, "failed to send error signal")
			}
		}, 6)
		transactionExpect(t, td, []int{6, 7}, expectedErr.Error())

		require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
		require.Equal(t, 0, td.clearRequestCalls)
		require.Equal(t, 0, td.cancelledCalls)
	})

	t.Run("network error by hook", func(t *testing.T) {
		td, qe := newTestData(t, 10, 7)
		defer td.cancel()
		expectedErr := ErrNetworkError
		notifeeExpect(t, td, 7, nil) // AddNotifee called on first 6 blocks
		blockHookExpect(t, td, 6, func(hookActions graphsync.OutgoingBlockHookActions) {
			hookActions.TerminateWithError(expectedErr)
		}, 6)
		transactionExpect(t, td, []int{6, 7}, expectedErr.Error())

		require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
		require.Equal(t, 1, td.clearRequestCalls)
		require.Equal(t, 0, td.cancelledCalls)
	})

	t.Run("network error by signal", func(t *testing.T) {
		// we load 7 blocks, by don't send the final one because error interrupts it,
		// unlike via blockhooks which is run after the block is sent
		td, qe := newTestData(t, 10, 7)
		defer td.cancel()
		expectedErr := ErrNetworkError
		notifeeExpect(t, td, 6, nil) // AddNotifee called on first 6 blocks
		blockHookExpect(t, td, 5, func(hookActions graphsync.OutgoingBlockHookActions) {
			select {
			case td.signals.ErrSignal <- expectedErr:
			default:
				require.Fail(t, "failed to send error signal")
			}
		}, 6)
		transactionExpect(t, td, []int{6, 7}, expectedErr.Error())

		require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
		require.Equal(t, 1, td.clearRequestCalls)
		require.Equal(t, 0, td.cancelledCalls)
	})

	t.Run("first block wont load", func(t *testing.T) {
		td, qe := newTestData(t, 10, 7)
		defer td.cancel()
		notifeeExpect(t, td, 0, graphsync.RequestFailedContentNotFound) // AddNotifee only called with error
		td.manager.responseTask.Traverser = &skipMeTraverser{}
		blockHookExpect(t, td, 0, func(hookActions graphsync.OutgoingBlockHookActions) {}, 0)
		transactionExpect(t, td, []int{0}, ErrFirstBlockLoad.Error())

		require.Equal(t, false, qe.ExecuteTask(td.ctx, td.peer, td.task))
		require.Equal(t, 0, td.clearRequestCalls)
		require.Equal(t, 0, td.cancelledCalls)
	})
}

func notifeeExpect(t *testing.T, td *testData, expectedCalls int, expectedFinalData notifications.TopicData) {
	notifeeCount := 1
	td.responseBuilder.notifeeCb = func(n notifications.Notifee) {
		require.Same(t, td.subscriber, n.Subscriber)
		if notifeeCount <= expectedCalls {
			require.Same(t, td.expectedBlocks[notifeeCount-1], n.Data)
		} else if notifeeCount == expectedCalls+1 {
			// may not reach here in some cases
			require.Equal(t, expectedFinalData, n.Data)
		} else {
			require.Fail(t, "too many notifee calls")
		}
		notifeeCount++
	}
}

func newRandomBlock(index int64) *blockData {
	digest := make([]byte, 32)
	_, err := rand.Read(digest)
	if err != nil {
		panic(err)
	}
	mh, _ := multihash.Encode(digest, multihash.SHA2_256)
	c := cid.NewCidV1(cid.DagCBOR, mh)
	link := &cidlink.Link{Cid: c}
	data := make([]byte, rand.Intn(64)+1)
	_, err = rand.Read(data)
	if err != nil {
		panic(err)
	}
	return &blockData{link, data, index}
}

type testData struct {
	ctx                context.Context
	t                  *testing.T
	cancel             func()
	task               *peertask.Task
	blockStore         map[ipld.Link][]byte
	persistence        ipld.LinkSystem
	manager            *fauxManager
	responseAssembler  *fauxResponseAssembler
	responseBuilder    *fauxResponseBuilder
	connManager        *testutil.TestConnManager
	blockHooks         *hooks.OutgoingBlockHooks
	updateHooks        *hooks.RequestUpdatedHooks
	cancelledListeners *listeners.RequestorCancelledListeners
	workSignal         chan struct{}
	extensionData      []byte
	extensionName      graphsync.ExtensionName
	extension          graphsync.ExtensionData
	requestID          graphsync.RequestID
	requestCid         cid.Cid
	requestSelector    datamodel.Node
	requests           []gsmsg.GraphSyncRequest
	signals            *ResponseSignals
	pauseCalls         int
	clearRequestCalls  int
	cancelledCalls     int
	expectedBlocks     []*blockData
	responseCode       graphsync.ResponseStatusCode
	peer               peer.ID
	subscriber         *notifications.TopicDataSubscriber
}

func newTestData(t *testing.T, blockCount int, expectedTraverse int) (*testData, *QueryExecutor) {
	ctx := context.Background()
	td := &testData{}
	td.t = t
	td.ctx, td.cancel = context.WithTimeout(ctx, 10*time.Second)
	td.blockStore = make(map[ipld.Link][]byte)
	td.persistence = testutil.NewTestStore(td.blockStore)
	td.task = &peertask.Task{}
	td.manager = &fauxManager{ctx: ctx, t: t, expectedStartTask: td.task}
	td.responseAssembler = &fauxResponseAssembler{}
	td.connManager = testutil.NewTestConnManager()
	td.blockHooks = hooks.NewBlockHooks()
	td.updateHooks = hooks.NewUpdateHooks()
	td.cancelledListeners = listeners.NewRequestorCancelledListeners()
	td.workSignal = make(chan struct{}, 1)
	td.requestID = graphsync.RequestID(rand.Int31())
	td.requestCid, _ = cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	td.requestSelector = basicnode.NewInt(rand.Int63())
	td.extensionData = testutil.RandomBytes(100)
	td.extensionName = graphsync.ExtensionName("AppleSauce/McGee")
	td.responseCode = graphsync.ResponseStatusCode(101)
	td.peer = testutil.GeneratePeers(1)[0]
	td.subscriber = &notifications.TopicDataSubscriber{}

	td.extension = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionData,
	}
	td.requests = []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(td.requestID, td.requestCid, td.requestSelector, graphsync.Priority(0), td.extension),
	}
	td.signals = &ResponseSignals{
		PauseSignal: make(chan struct{}, 1),
		ErrSignal:   make(chan error, 1),
	}

	td.expectedBlocks = make([]*blockData, 0)
	links := make([]ipld.Link, 0)
	for i := 0; i < blockCount; i++ {
		td.expectedBlocks = append(td.expectedBlocks, newRandomBlock(int64(i)))
		links = append(links, td.expectedBlocks[i].link)
	}

	var sentCount int
	sendResponseCb := func(actualLink ipld.Link, actualData []byte) graphsync.BlockData {
		require.Same(t, td.expectedBlocks[sentCount].link, actualLink)
		require.Equal(t, td.expectedBlocks[sentCount].data, actualData)
		sentCount++
		return td.expectedBlocks[sentCount-1]
	}

	td.responseBuilder = &fauxResponseBuilder{
		t:              t,
		finishRequest:  td.responseCode,
		sendResponseCb: sendResponseCb,
		clearRequestCb: func() {
			td.clearRequestCalls++
		},
		pauseCb: func() {
			require.Fail(t, "should not have called ResponseBuilder#PauseRequest()")
		},
	}

	td.responseAssembler = &fauxResponseAssembler{
		t:               t,
		responseBuilder: td.responseBuilder,
	}

	loads := 1
	loader := func(_ linking.LinkContext, _ datamodel.Link) (io.Reader, error) {
		require.LessOrEqual(t, loads, expectedTraverse, "loaded more blocks than expected")
		loads++
		return bytes.NewReader(td.expectedBlocks[loads-2].data), nil
	}
	expectedTraverser := &fauxTraverser{
		links: links,
		advanceCb: func(curLink int, actualData []byte) error {
			require.Less(t, loads-2, len(td.expectedBlocks), "should not have loaded more than the blocks we have")
			require.NotSame(t, td.expectedBlocks[curLink].data, actualData) // a copy has to happen
			require.Equal(t, td.expectedBlocks[curLink].data, actualData)
			return nil
		},
	}

	td.manager.responseTask = ResponseTask{
		Request:    td.requests[0],
		Loader:     loader,
		Traverser:  expectedTraverser,
		Signals:    *td.signals,
		Subscriber: td.subscriber,
	}
	td.responseAssembler.responseBuilder.pauseCb = func() {
		td.pauseCalls++
	}
	td.cancelledListeners.Register(func(p peer.ID, request graphsync.RequestData) {
		td.cancelledCalls++
	})

	qe := New(
		td.ctx,
		td.manager,
		td.blockHooks,
		td.updateHooks,
		td.cancelledListeners,
		td.responseAssembler,
		td.workSignal,
		td.connManager,
	)
	return td, qe
}

type fauxManager struct {
	ctx               context.Context
	t                 *testing.T
	responseTask      ResponseTask
	expectedStartTask *peertask.Task
}

func (fm *fauxManager) StartTask(task *peertask.Task, responseTaskChan chan<- ResponseTask) {
	require.Same(fm.t, fm.expectedStartTask, task)
	go func() {
		select {
		case <-fm.ctx.Done():
		case responseTaskChan <- fm.responseTask:
		}
	}()
}

func (fm *fauxManager) GetUpdates(p peer.ID, requestID graphsync.RequestID, updatesChan chan<- []gsmsg.GraphSyncRequest) {
}

func (fm *fauxManager) FinishTask(task *peertask.Task, err error) {
}

type fauxResponseAssembler struct {
	t               *testing.T
	responseBuilder *fauxResponseBuilder
	transactionCb   func(error)
}

func (fra *fauxResponseAssembler) Transaction(p peer.ID, requestID graphsync.RequestID, transaction responseassembler.Transaction) error {
	var err error
	if fra.responseBuilder != nil {
		err = transaction(fra.responseBuilder)
	}
	if fra.transactionCb != nil {
		fra.transactionCb(err)
	} else {
		require.NoError(fra.t, err)
	}
	return err
}

type fauxResponseBuilder struct {
	t              *testing.T
	sendResponseCb func(ipld.Link, []byte) graphsync.BlockData
	finishRequest  graphsync.ResponseStatusCode
	notifeeCb      func(n notifications.Notifee)
	pauseCb        func()
	clearRequestCb func()
}

func (rb fauxResponseBuilder) SendResponse(link ipld.Link, data []byte) graphsync.BlockData {
	return rb.sendResponseCb(link, data)
}

func (rb fauxResponseBuilder) SendExtensionData(ed graphsync.ExtensionData) {
}

func (rb fauxResponseBuilder) ClearRequest() {
	if rb.clearRequestCb != nil {
		rb.clearRequestCb()
	}
}

func (rb fauxResponseBuilder) FinishRequest() graphsync.ResponseStatusCode {
	return rb.finishRequest
}

func (rb fauxResponseBuilder) FinishWithError(status graphsync.ResponseStatusCode) {
}

func (rb fauxResponseBuilder) PauseRequest() {
	if rb.pauseCb != nil {
		rb.pauseCb()
	}
}

func (rb fauxResponseBuilder) AddNotifee(n notifications.Notifee) {
	if rb.notifeeCb != nil {
		rb.notifeeCb(n)
	}
}

var _ responseassembler.ResponseBuilder = &fauxResponseBuilder{}

type blockData struct {
	link  ipld.Link
	data  []byte
	index int64
}

func (bd blockData) Link() ipld.Link {
	return bd.link
}

func (bd blockData) BlockSize() uint64 {
	return uint64(len(bd.data))
}

func (bd blockData) BlockSizeOnWire() uint64 {
	return uint64(len(bd.data))
}

func (bd blockData) Index() int64 {
	return bd.index
}

type fauxTraverser struct {
	t          *testing.T
	lnkCtx     ipld.LinkContext
	links      []ipld.Link
	curLink    int
	advanceCb  func(int, []byte) error
	errorCb    func(error)
	shutdownCb func(context.Context)
}

func (t fauxTraverser) IsComplete() (bool, error) {
	return t.curLink >= len(t.links), nil
}

func (t fauxTraverser) CurrentRequest() (ipld.Link, ipld.LinkContext) {
	if t.curLink >= len(t.links) {
		require.Fail(t.t, "CurrentRequest called after complete")
	}
	return t.links[t.curLink], t.lnkCtx
}

func (t *fauxTraverser) Advance(reader io.Reader) error {
	t.curLink++
	if t.advanceCb != nil {
		buf := new(bytes.Buffer)
		buf.ReadFrom(reader)
		return t.advanceCb(t.curLink-1, buf.Bytes())
	}
	return nil
}

func (t fauxTraverser) Error(err error) {
	if t.errorCb != nil {
		t.errorCb(err)
	}
}

func (t fauxTraverser) Shutdown(ctx context.Context) {
	if t.shutdownCb != nil {
		t.shutdownCb(ctx)
	}
}

func (t fauxTraverser) NBlocksTraversed() int {
	return t.curLink
}

type skipMeTraverser struct {
	fauxTraverser
}

func (t skipMeTraverser) IsComplete() (bool, error) {
	return true, traversal.SkipMe{}
}
