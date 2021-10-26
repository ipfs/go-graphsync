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
	td := newTestData(t)
	defer td.cancel()

	peer := testutil.GeneratePeers(1)[0]
	task := &peertask.Task{}
	td.manager.expectedStartTask = task
	td.manager.toSendResponseTaskData = ResponseTask{Empty: true}

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

	// should panic if we sent anything other than Empty:true
	require.Equal(t, false, qe.ExecuteTask(td.ctx, peer, task))
}

func TestOneBlockTask(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()

	peer := testutil.GeneratePeers(1)[0]
	task := &peertask.Task{}
	sub := &notifications.TopicDataSubscriber{}
	expectedResponseCode := graphsync.ResponseStatusCode(101)
	expectedBlock := newRandomBlock(101)

	var notifeeCount int
	notifeeCb := func(n notifications.Notifee) {
		require.Same(t, sub, n.Subscriber)
		switch notifeeCount {
		case 0:
			require.Same(t, expectedBlock, n.Data)
		case 1:
			require.Equal(t, expectedResponseCode, n.Data)
		default:
			require.Fail(t, "too many calls")
		}
		notifeeCount++
	}
	sendResponseCb := func(actualLink ipld.Link, actualData []byte) graphsync.BlockData {
		require.Same(t, expectedBlock.link, actualLink)
		require.Equal(t, expectedBlock.data, actualData)
		return expectedBlock
	}

	rb := &fauxResponseBuilder{
		t:                   t,
		toSendFinishRequest: expectedResponseCode,
		notifeeCb:           notifeeCb,
		sendResponseCb:      sendResponseCb,
	}
	td.responseAssembler = &fauxResponseAssembler{
		t:                     t,
		toSendResponseBuilder: rb,
	}

	var loads int
	loader := func(_ linking.LinkContext, _ datamodel.Link) (io.Reader, error) {
		require.Equal(t, 0, loads, "should not have loaded more than one block")
		loads++
		return bytes.NewReader(expectedBlock.data), nil
	}
	expectedTraverser := &fauxTraverser{
		links: []ipld.Link{expectedBlock.link},
		advanceCb: func(curLink int, b []byte) error {
			if curLink != 0 {
				require.Fail(t, "should not have advanced beyond first block")
			}
			return nil
		},
	}

	td.manager.expectedStartTask = task
	td.manager.toSendResponseTaskData = ResponseTask{
		Request:    td.requests[0],
		Loader:     loader,
		Traverser:  expectedTraverser,
		Signals:    *td.signals,
		Subscriber: sub,
	}

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

	require.Equal(t, false, qe.ExecuteTask(td.ctx, peer, task))
}

func TestSmallGraphTask(t *testing.T) {
	setup := func(t *testing.T, blockCount int, expectedTraverse int) (*testData, *QueryExecutor) {
		td := newTestData(t)

		task := &peertask.Task{}
		sub := &notifications.TopicDataSubscriber{}
		expectedResponseCode := graphsync.ResponseStatusCode(101)
		expectedBlocks := make([]*blockData, 0)
		links := make([]ipld.Link, 0)
		for i := 0; i < blockCount; i++ {
			expectedBlocks = append(expectedBlocks, newRandomBlock(int64(i)))
			links = append(links, expectedBlocks[i].link)
		}

		var notifeeCount int
		notifeeCb := func(n notifications.Notifee) {
			require.Same(t, sub, n.Subscriber)
			expMax := blockCount
			if expectedTraverse != blockCount {
				expMax = expectedTraverse + 1 // we load one before than we use
			}
			if notifeeCount < expMax {
				require.Same(t, expectedBlocks[notifeeCount], n.Data)
			} else if notifeeCount == blockCount {
				require.Equal(t, expectedResponseCode, n.Data)
			} else {
				require.Fail(t, "too many notifee calls")
			}
			notifeeCount++
		}
		var sentCount int
		sendResponseCb := func(actualLink ipld.Link, actualData []byte) graphsync.BlockData {
			require.Same(t, expectedBlocks[sentCount].link, actualLink)
			require.Equal(t, expectedBlocks[sentCount].data, actualData)
			sentCount++
			return expectedBlocks[sentCount-1]
		}

		rb := &fauxResponseBuilder{
			t:                   t,
			toSendFinishRequest: expectedResponseCode,
			notifeeCb:           notifeeCb,
			sendResponseCb:      sendResponseCb,
			clearRequestCb: func() {
				require.Fail(t, "should not have called ResponseBuilder#ClearRequest()")
			},
			pauseCb: func() {
				require.Fail(t, "should not have called ResponseBuilder#PauseRequest()")
			},
		}

		td.responseAssembler = &fauxResponseAssembler{
			t:                     t,
			toSendResponseBuilder: rb,
		}

		var loads int
		loader := func(_ linking.LinkContext, _ datamodel.Link) (io.Reader, error) {
			if expectedTraverse == blockCount {
				require.Less(t, loads, blockCount, "should not have loaded more than the blocks we have")
			} else {
				// we load one before than we use
				require.Less(t, loads, expectedTraverse+1, "should not have loaded more than one extra block")
			}
			loads++
			return bytes.NewReader(expectedBlocks[loads-1].data), nil
		}
		expectedTraverser := &fauxTraverser{
			links: links,
			advanceCb: func(curLink int, actualData []byte) error {
				require.Less(t, loads-1, len(expectedBlocks), "should not have loaded more than the blocks we have")
				require.NotSame(t, expectedBlocks[curLink].data, actualData) // a copy has to happen
				require.Equal(t, expectedBlocks[curLink].data, actualData)
				return nil
			},
		}

		td.manager.expectedStartTask = task
		td.manager.toSendResponseTaskData = ResponseTask{
			Request:    td.requests[0],
			Loader:     loader,
			Traverser:  expectedTraverser,
			Signals:    *td.signals,
			Subscriber: sub,
		}

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

	noCancel := func(t *testing.T, td *testData) {
		td.cancelledListeners.Register(func(p peer.ID, request graphsync.RequestData) {
			require.Fail(t, "should not have called a cancel listener")
		})
	}

	t.Run("full graph", func(t *testing.T) {
		td, qe := setup(t, 10, 10)
		defer td.cancel()
		noCancel(t, td)

		require.Equal(t, false, qe.ExecuteTask(td.ctx, testutil.GeneratePeers(1)[0], td.manager.expectedStartTask))
	})

	t.Run("paused by hook", func(t *testing.T) {
		td, qe := setup(t, 10, 6)
		defer td.cancel()
		noCancel(t, td)

		var hookCalls int
		td.blockHooks.Register(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			if hookCalls == 6 {
				hookActions.PauseResponse()
			} else {
				require.Less(t, hookCalls, 6, "have only called block hook once per block before pausing")
			}
			hookCalls++
		})
		var pauseCalls int
		td.responseAssembler.toSendResponseBuilder.pauseCb = func() {
			pauseCalls++
		}

		require.Equal(t, false, qe.ExecuteTask(td.ctx, testutil.GeneratePeers(1)[0], td.manager.expectedStartTask))
		require.Equal(t, 1, pauseCalls)
	})

	t.Run("paused by signal", func(t *testing.T) {
		td, qe := setup(t, 10, 6)
		defer td.cancel()
		noCancel(t, td)

		var hookCalls int
		td.blockHooks.Register(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			if hookCalls == 5 {
				select {
				case td.signals.PauseSignal <- struct{}{}:
				default:
					require.Fail(t, "failed to send pause signal")
				}
			}
			// on a pause, we continue to send the current block, so this is signal+2, whereas other error types are signal+1
			require.Less(t, hookCalls, 7, "have only called block hook once per block before pausing (with padding for signals)")
			hookCalls++
		})
		var pauseCalls int
		td.responseAssembler.toSendResponseBuilder.pauseCb = func() {
			pauseCalls++
		}

		require.Equal(t, false, qe.ExecuteTask(td.ctx, testutil.GeneratePeers(1)[0], td.manager.expectedStartTask))
		require.Equal(t, 1, pauseCalls)
	})

	t.Run("partial cancelled by hook", func(t *testing.T) {
		td, qe := setup(t, 10, 5)
		defer td.cancel()

		var hookCalls int
		td.blockHooks.Register(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			if hookCalls == 5 {
				hookActions.TerminateWithError(ipldutil.ContextCancelError{})
			} else {
				require.Less(t, hookCalls, 5, "have only called block hook once per block before cancelling")
			}
			hookCalls++
		})

		var cancelledCalls int
		td.cancelledListeners.Register(func(p peer.ID, request graphsync.RequestData) {
			cancelledCalls++
		})

		var clearRequestCalls int
		td.responseAssembler.toSendResponseBuilder.clearRequestCb = func() {
			clearRequestCalls++
		}

		require.Equal(t, false, qe.ExecuteTask(td.ctx, testutil.GeneratePeers(1)[0], td.manager.expectedStartTask))
		require.Equal(t, 1, clearRequestCalls)
		require.Equal(t, 1, cancelledCalls)
	})

	t.Run("partial cancelled by signal", func(t *testing.T) {
		td, qe := setup(t, 10, 6)
		defer td.cancel()

		var hookCalls int
		td.blockHooks.Register(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			if hookCalls == 5 {
				select {
				case td.signals.ErrSignal <- ErrCancelledByCommand:
				default:
					require.Fail(t, "failed to send error signal")
				}
			}
			// on a pause, we continue to send the current block, so this is signal+2, whereas other error types are signal+1
			require.Less(t, hookCalls, 6, "have only called block hook once per block before cancelling (with padding for signals)")
			hookCalls++
		})

		// the standard AddNotifee call checker won't work here, we need to check for a custom code at block 6
		stdNotifeeCb := td.responseAssembler.toSendResponseBuilder.notifeeCb
		var notifeeCount int
		td.responseAssembler.toSendResponseBuilder.notifeeCb = func(n notifications.Notifee) {
			if notifeeCount < 6 {
				stdNotifeeCb(n)
			} else if notifeeCount == 6 {
				require.Equal(t, graphsync.RequestCancelled, n.Data)
			} else {
				require.Fail(t, "too many notifee calls")
			}
			notifeeCount++
		}

		/* TODO(rv): this is not called because `ErrCancelledByCommand` isn't a `isContextError()`
		var cancelledCalls int
		td.cancelledListeners.Register(func(p peer.ID, request graphsync.RequestData) {
			cancelledCalls++
		})
		*/

		td.responseAssembler.toSendResponseBuilder.clearRequestCb = func() {
			require.Fail(t, "unexpected clear request call")
		}

		require.Equal(t, false, qe.ExecuteTask(td.ctx, testutil.GeneratePeers(1)[0], td.manager.expectedStartTask))
		// require.Equal(t, 1, cancelledCalls)
	})

	t.Run("unknown error by hook", func(t *testing.T) {
		td, qe := setup(t, 10, 6)
		defer td.cancel()
		noCancel(t, td)

		expectedErr := fmt.Errorf("derp")

		var hookCalls int
		td.blockHooks.Register(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			if hookCalls == 6 {
				hookActions.TerminateWithError(expectedErr)
			} else {
				require.Less(t, hookCalls, 6, "have only called block hook once per block before erroring")
			}
			hookCalls++
		})
		// the standard AddNotifee call checker won't work here, we need to check for a custom code at block 6
		stdNotifeeCb := td.responseAssembler.toSendResponseBuilder.notifeeCb
		var notifeeCount int
		td.responseAssembler.toSendResponseBuilder.notifeeCb = func(n notifications.Notifee) {
			if notifeeCount < 7 {
				stdNotifeeCb(n)
			} else if notifeeCount == 7 {
				require.Equal(t, graphsync.RequestFailedUnknown, n.Data)
			} else {
				require.Fail(t, "too many notifee calls")
			}
			notifeeCount++
		}
		require.Equal(t, false, qe.ExecuteTask(td.ctx, testutil.GeneratePeers(1)[0], td.manager.expectedStartTask))
	})

	t.Run("unknown error by signal", func(t *testing.T) {
		td, qe := setup(t, 10, 6)
		defer td.cancel()
		noCancel(t, td)

		expectedErr := fmt.Errorf("derp")

		var hookCalls int
		td.blockHooks.Register(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			if hookCalls == 5 {
				select {
				case td.signals.ErrSignal <- expectedErr:
				default:
					require.Fail(t, "failed to send error signal")
				}
			}
			require.Less(t, hookCalls, 6, "have only called block hook once per block before erroring (with padding for signals)")
			hookCalls++
		})
		// the standard AddNotifee call checker won't work here, we need to check for a custom code at block 6
		stdNotifeeCb := td.responseAssembler.toSendResponseBuilder.notifeeCb
		var notifeeCount int
		td.responseAssembler.toSendResponseBuilder.notifeeCb = func(n notifications.Notifee) {
			if notifeeCount < 6 {
				stdNotifeeCb(n)
			} else if notifeeCount == 6 {
				require.Equal(t, graphsync.RequestFailedUnknown, n.Data)
			} else {
				require.Fail(t, "too many notifee calls")
			}
			notifeeCount++
		}
		require.Equal(t, false, qe.ExecuteTask(td.ctx, testutil.GeneratePeers(1)[0], td.manager.expectedStartTask))
	})

	t.Run("network error by hook", func(t *testing.T) {
		td, qe := setup(t, 10, 6)
		defer td.cancel()
		noCancel(t, td)

		expectedErr := ErrNetworkError

		var hookCalls int
		td.blockHooks.Register(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			if hookCalls == 6 {
				hookActions.TerminateWithError(expectedErr)
			} else {
				require.Less(t, hookCalls, 6, "have only called block hook once per block before erroring")
			}
			hookCalls++
		})
		// the standard AddNotifee call checker won't work here, we need to check for a custom code at block 6
		stdNotifeeCb := td.responseAssembler.toSendResponseBuilder.notifeeCb
		var notifeeCount int
		td.responseAssembler.toSendResponseBuilder.notifeeCb = func(n notifications.Notifee) {
			if notifeeCount < 7 {
				stdNotifeeCb(n)
			} else if notifeeCount == 7 {
				require.Equal(t, graphsync.RequestFailedUnknown, n.Data)
			} else {
				require.Fail(t, "too many notifee calls")
			}
			notifeeCount++
		}
		var clearRequestCalls int
		td.responseAssembler.toSendResponseBuilder.clearRequestCb = func() {
			clearRequestCalls++
		}

		require.Equal(t, false, qe.ExecuteTask(td.ctx, testutil.GeneratePeers(1)[0], td.manager.expectedStartTask))
		require.Equal(t, 1, clearRequestCalls)
	})

	t.Run("network error by signal", func(t *testing.T) {
		td, qe := setup(t, 10, 6)
		defer td.cancel()
		noCancel(t, td)

		expectedErr := ErrNetworkError

		var hookCalls int
		td.blockHooks.Register(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			if hookCalls == 5 {
				select {
				case td.signals.ErrSignal <- expectedErr:
				default:
					require.Fail(t, "failed to send error signal")
				}
			}
			require.Less(t, hookCalls, 6, "have only called block hook once per block before erroring (with padding for signals)")
			hookCalls++
		})
		// the standard AddNotifee call checker won't work here, we need to check for a custom code at block 6
		stdNotifeeCb := td.responseAssembler.toSendResponseBuilder.notifeeCb
		var notifeeCount int
		td.responseAssembler.toSendResponseBuilder.notifeeCb = func(n notifications.Notifee) {
			if notifeeCount < 6 {
				stdNotifeeCb(n)
			} else if notifeeCount == 6 {
				require.Equal(t, graphsync.RequestFailedUnknown, n.Data)
			} else {
				require.Fail(t, "too many notifee calls")
			}
			notifeeCount++
		}
		var clearRequestCalls int
		td.responseAssembler.toSendResponseBuilder.clearRequestCb = func() {
			clearRequestCalls++
		}

		require.Equal(t, false, qe.ExecuteTask(td.ctx, testutil.GeneratePeers(1)[0], td.manager.expectedStartTask))
		require.Equal(t, 1, clearRequestCalls)
	})

	t.Run("first block wont load", func(t *testing.T) {
		td, qe := setup(t, 10, 6)
		defer td.cancel()
		noCancel(t, td)

		td.manager.toSendResponseTaskData.Traverser = &skipMeTraverser{}

		td.blockHooks.Register(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			require.Fail(t, "unexpected hook call")
		})
		td.responseAssembler.toSendResponseBuilder.notifeeCb = func(n notifications.Notifee) {
			require.Equal(t, graphsync.RequestFailedContentNotFound, n.Data)
		}
		td.responseAssembler.toSendResponseBuilder.clearRequestCb = func() {
			require.Fail(t, "unexpected clear request call")
		}

		require.Equal(t, false, qe.ExecuteTask(td.ctx, testutil.GeneratePeers(1)[0], td.manager.expectedStartTask))
	})

	// TODO: test extensions, and update signals
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
	blockStore         map[ipld.Link][]byte
	persistence        ipld.LinkSystem
	manager            *fauxManager
	responseAssembler  *fauxResponseAssembler
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
}

func newTestData(t *testing.T) *testData {
	ctx := context.Background()
	td := &testData{}
	td.t = t
	td.ctx, td.cancel = context.WithTimeout(ctx, 10*time.Second)
	td.blockStore = make(map[ipld.Link][]byte)
	td.persistence = testutil.NewTestStore(td.blockStore)
	td.manager = &fauxManager{ctx: ctx, t: t}
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

	return td
}

type fauxManager struct {
	ctx                    context.Context
	t                      *testing.T
	toSendResponseTaskData ResponseTask
	expectedStartTask      *peertask.Task
}

func (fm *fauxManager) StartTask(task *peertask.Task, responseTaskChan chan<- ResponseTask) {
	require.Same(fm.t, fm.expectedStartTask, task)
	go func() {
		select {
		case <-fm.ctx.Done():
		case responseTaskChan <- fm.toSendResponseTaskData:
		}
	}()
}

func (fm *fauxManager) GetUpdates(p peer.ID, requestID graphsync.RequestID, updatesChan chan<- []gsmsg.GraphSyncRequest) {
}

func (fm *fauxManager) FinishTask(task *peertask.Task, err error) {
}

type fauxResponseAssembler struct {
	t                     *testing.T
	toSendResponseBuilder *fauxResponseBuilder
}

func (fra *fauxResponseAssembler) Transaction(p peer.ID, requestID graphsync.RequestID, transaction responseassembler.Transaction) error {
	if fra.toSendResponseBuilder != nil {
		require.NoError(fra.t, transaction(fra.toSendResponseBuilder))
	}
	return nil
}

type fauxResponseBuilder struct {
	t                   *testing.T
	sendResponseCb      func(ipld.Link, []byte) graphsync.BlockData
	toSendFinishRequest graphsync.ResponseStatusCode
	notifeeCb           func(n notifications.Notifee)
	pauseCb             func()
	clearRequestCb      func()
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
	return rb.toSendFinishRequest
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
