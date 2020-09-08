package executor_test

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/requestmanager/executor"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/requestmanager/testloader"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipfs/go-graphsync/testutil"
)

type configureLoaderFn func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, fal *testloader.FakeAsyncLoader, startStop [2]int)

func TestRequestExecutionBlockChain(t *testing.T) {
	testCases := map[string]struct {
		configureLoader           configureLoaderFn
		configureRequestExecution func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv)
		verifyResults             func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error)
	}{
		"simple success case": {
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, 0, ree.currentWaitForResumeResult)
				require.Equal(t, []requestSent{{ree.p, ree.request}}, ree.requestsSent)
				require.Len(t, ree.blookHooksCalled, 10)
				require.Equal(t, ree.request.ID(), ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
		"error at block hook": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = errors.New("something went wrong")
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 5)
				require.Len(t, receivedErrors, 1)
				require.Regexp(t, "something went wrong", receivedErrors[0].Error())
				require.Equal(t, 0, ree.currentWaitForResumeResult)
				require.Equal(t, []requestSent{{ree.p, ree.request}}, ree.requestsSent)
				require.Len(t, ree.blookHooksCalled, 6)
				require.Equal(t, ree.request.ID(), ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
		"context cancelled": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = ipldutil.ContextCancelError{}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 5)
				require.Empty(t, receivedErrors)
				require.Equal(t, 0, ree.currentWaitForResumeResult)
				require.Equal(t, []requestSent{{ree.p, ree.request}}, ree.requestsSent)
				require.Len(t, ree.blookHooksCalled, 6)
				require.Equal(t, ree.request.ID(), ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
		"simple pause": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = hooks.ErrPaused{}
				ree.waitForResumeResults = append(ree.waitForResumeResults, nil)
				ree.loaderRanges = [][2]int{{0, 6}, {6, 10}}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, 1, ree.currentWaitForResumeResult)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				doNotSendCidsExt, has := ree.requestsSent[1].request.Extension(graphsync.ExtensionDoNotSendCIDs)
				require.True(t, has)
				cidSet, err := cidset.DecodeCidSet(doNotSendCidsExt)
				require.NoError(t, err)
				require.Equal(t, 6, cidSet.Len())
				require.Len(t, ree.blookHooksCalled, 10)
				require.Equal(t, ree.request.ID(), ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
		"multiple pause": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = hooks.ErrPaused{}
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(7)}] = hooks.ErrPaused{}
				ree.waitForResumeResults = append(ree.waitForResumeResults, nil, nil)
				ree.loaderRanges = [][2]int{{0, 6}, {6, 8}, {8, 10}}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, 2, ree.currentWaitForResumeResult)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				doNotSendCidsExt, has := ree.requestsSent[1].request.Extension(graphsync.ExtensionDoNotSendCIDs)
				require.True(t, has)
				cidSet, err := cidset.DecodeCidSet(doNotSendCidsExt)
				require.NoError(t, err)
				require.Equal(t, 6, cidSet.Len())
				doNotSendCidsExt, has = ree.requestsSent[2].request.Extension(graphsync.ExtensionDoNotSendCIDs)
				require.True(t, has)
				cidSet, err = cidset.DecodeCidSet(doNotSendCidsExt)
				require.NoError(t, err)
				require.Equal(t, 8, cidSet.Len())
				require.Len(t, ree.blookHooksCalled, 10)
				require.Equal(t, ree.request.ID(), ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
		"multiple pause with extensions": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = hooks.ErrPaused{}
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(7)}] = hooks.ErrPaused{}
				ree.waitForResumeResults = append(ree.waitForResumeResults, []graphsync.ExtensionData{
					{
						Name: graphsync.ExtensionName("applesauce"),
						Data: []byte("cheese 1"),
					},
				}, []graphsync.ExtensionData{
					{
						Name: graphsync.ExtensionName("applesauce"),
						Data: []byte("cheese 2"),
					},
				})
				ree.loaderRanges = [][2]int{{0, 6}, {6, 8}, {8, 10}}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, 2, ree.currentWaitForResumeResult)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				testExtData, has := ree.requestsSent[1].request.Extension(graphsync.ExtensionName("applesauce"))
				require.True(t, has)
				require.Equal(t, "cheese 1", string(testExtData))
				doNotSendCidsExt, has := ree.requestsSent[1].request.Extension(graphsync.ExtensionDoNotSendCIDs)
				require.True(t, has)
				cidSet, err := cidset.DecodeCidSet(doNotSendCidsExt)
				require.NoError(t, err)
				require.Equal(t, 6, cidSet.Len())
				testExtData, has = ree.requestsSent[2].request.Extension(graphsync.ExtensionName("applesauce"))
				require.True(t, has)
				require.Equal(t, "cheese 2", string(testExtData))
				doNotSendCidsExt, has = ree.requestsSent[2].request.Extension(graphsync.ExtensionDoNotSendCIDs)
				require.True(t, has)
				cidSet, err = cidset.DecodeCidSet(doNotSendCidsExt)
				require.NoError(t, err)
				require.Equal(t, 8, cidSet.Len())
				require.Len(t, ree.blookHooksCalled, 10)
				require.Equal(t, ree.request.ID(), ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
		"preexisting do not send cids": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.doNotSendCids.Add(tbc.GenisisLink.(cidlink.Link).Cid)
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = hooks.ErrPaused{}
				ree.waitForResumeResults = append(ree.waitForResumeResults, nil)
				ree.loaderRanges = [][2]int{{0, 6}, {6, 10}}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, 1, ree.currentWaitForResumeResult)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				doNotSendCidsExt, has := ree.requestsSent[1].request.Extension(graphsync.ExtensionDoNotSendCIDs)
				require.True(t, has)
				cidSet, err := cidset.DecodeCidSet(doNotSendCidsExt)
				require.NoError(t, err)
				require.Equal(t, 7, cidSet.Len())
				require.Len(t, ree.blookHooksCalled, 10)
				require.Equal(t, ree.request.ID(), ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
		"pause but request is cancelled": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = hooks.ErrPaused{}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 5)
				require.Empty(t, receivedErrors)
				require.Equal(t, 0, ree.currentWaitForResumeResult)
				require.Equal(t, []requestSent{{ree.p, ree.request}}, ree.requestsSent)
				require.Len(t, ree.blookHooksCalled, 6)
				require.Equal(t, ree.request.ID(), ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
		"pause externally": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.externalPauses = append(ree.externalPauses, pauseKey{requestID, tbc.LinkTipIndex(5)})
				ree.waitForResumeResults = append(ree.waitForResumeResults, nil)
				ree.loaderRanges = [][2]int{{0, 6}, {6, 10}}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, 1, ree.currentPauseResult)
				require.Equal(t, 1, ree.currentWaitForResumeResult)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				require.True(t, ree.requestsSent[1].request.IsCancel())
				doNotSendCidsExt, has := ree.requestsSent[2].request.Extension(graphsync.ExtensionDoNotSendCIDs)
				require.True(t, has)
				cidSet, err := cidset.DecodeCidSet(doNotSendCidsExt)
				require.NoError(t, err)
				require.Equal(t, 6, cidSet.Len())
				require.Len(t, ree.blookHooksCalled, 10)
				require.Equal(t, ree.request.ID(), ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
		"pause externally multiple": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.externalPauses = append(ree.externalPauses, pauseKey{requestID, tbc.LinkTipIndex(5)}, pauseKey{requestID, tbc.LinkTipIndex(7)})
				ree.waitForResumeResults = append(ree.waitForResumeResults, nil, nil)
				ree.loaderRanges = [][2]int{{0, 6}, {6, 8}, {8, 10}}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, 2, ree.currentPauseResult)
				require.Equal(t, 2, ree.currentWaitForResumeResult)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				require.True(t, ree.requestsSent[1].request.IsCancel())
				doNotSendCidsExt, has := ree.requestsSent[2].request.Extension(graphsync.ExtensionDoNotSendCIDs)
				require.True(t, has)
				cidSet, err := cidset.DecodeCidSet(doNotSendCidsExt)
				require.NoError(t, err)
				require.Equal(t, 6, cidSet.Len())
				require.True(t, ree.requestsSent[3].request.IsCancel())
				doNotSendCidsExt, has = ree.requestsSent[4].request.Extension(graphsync.ExtensionDoNotSendCIDs)
				require.True(t, has)
				cidSet, err = cidset.DecodeCidSet(doNotSendCidsExt)
				require.NoError(t, err)
				require.Equal(t, 8, cidSet.Len())
				require.Len(t, ree.blookHooksCalled, 10)
				require.Equal(t, ree.request.ID(), ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			loader, storer := testutil.NewTestStore(make(map[ipld.Link][]byte))
			tbc := testutil.SetupBlockChain(ctx, t, loader, storer, 100, 10)
			fal := testloader.NewFakeAsyncLoader()
			requestID := graphsync.RequestID(rand.Int31())
			p := testutil.GeneratePeers(1)[0]
			configureLoader := data.configureLoader
			if configureLoader == nil {
				configureLoader = func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, fal *testloader.FakeAsyncLoader, startStop [2]int) {
					fal.SuccessResponseOn(requestID, tbc.Blocks(startStop[0], startStop[1]))
				}
			}
			requestCtx, requestCancel := context.WithCancel(ctx)
			ree := &requestExecutionEnv{
				ctx:              requestCtx,
				cancelFn:         requestCancel,
				p:                p,
				resumeMessages:   make(chan []graphsync.ExtensionData, 1),
				pauseMessages:    make(chan struct{}, 1),
				blockHookResults: make(map[blockHookKey]error),
				doNotSendCids:    cid.NewSet(),
				request:          gsmsg.NewRequest(requestID, tbc.TipLink.(cidlink.Link).Cid, tbc.Selector(), graphsync.Priority(rand.Int31())),
				fal:              fal,
				tbc:              tbc,
				configureLoader:  configureLoader,
			}
			fal.OnAsyncLoad(ree.checkPause)
			if data.configureRequestExecution != nil {
				data.configureRequestExecution(p, requestID, tbc, ree)
			}
			if len(ree.loaderRanges) == 0 {
				ree.loaderRanges = [][2]int{{0, 10}}
			}
			inProgress, inProgressErr := ree.requestExecution()
			var responsesReceived []graphsync.ResponseProgress
			var errorsReceived []error
			var inProgressDone, inProgressErrDone bool
			for !inProgressDone || !inProgressErrDone {
				select {
				case response, ok := <-inProgress:
					if !ok {
						inProgress = nil
						inProgressDone = true
					} else {
						responsesReceived = append(responsesReceived, response)
					}
				case err, ok := <-inProgressErr:
					if !ok {
						inProgressErr = nil
						inProgressErrDone = true
					} else {
						errorsReceived = append(errorsReceived, err)
					}
				case <-ctx.Done():
					t.Fatal("did not complete request")
				}
			}
			data.verifyResults(t, tbc, ree, responsesReceived, errorsReceived)
		})
	}
}

type requestSent struct {
	p       peer.ID
	request gsmsg.GraphSyncRequest
}

type blockHookKey struct {
	p         peer.ID
	requestID graphsync.RequestID
	link      ipld.Link
}

type pauseKey struct {
	requestID graphsync.RequestID
	link      ipld.Link
}

type requestExecutionEnv struct {
	// params
	ctx                  context.Context
	cancelFn             func()
	request              gsmsg.GraphSyncRequest
	p                    peer.ID
	blockHookResults     map[blockHookKey]error
	doNotSendCids        *cid.Set
	waitForResumeResults [][]graphsync.ExtensionData
	resumeMessages       chan []graphsync.ExtensionData
	pauseMessages        chan struct{}
	externalPauses       []pauseKey
	loaderRanges         [][2]int

	// results
	currentPauseResult         int
	currentWaitForResumeResult int
	requestsSent               []requestSent
	blookHooksCalled           []blockHookKey
	terminateRequested         graphsync.RequestID
	nodeStyleChooserCalled     bool

	// deps
	configureLoader configureLoaderFn
	tbc             *testutil.TestBlockChain
	fal             *testloader.FakeAsyncLoader
}

func (ree *requestExecutionEnv) terminateRequest(requestID graphsync.RequestID) {
	ree.terminateRequested = requestID
}

func (ree *requestExecutionEnv) waitForResume() ([]graphsync.ExtensionData, error) {
	if len(ree.waitForResumeResults) <= ree.currentWaitForResumeResult {
		return nil, ipldutil.ContextCancelError{}
	}
	extensions := ree.waitForResumeResults[ree.currentWaitForResumeResult]
	ree.currentWaitForResumeResult++
	return extensions, nil
}

func (ree *requestExecutionEnv) sendRequest(p peer.ID, request gsmsg.GraphSyncRequest) {
	ree.requestsSent = append(ree.requestsSent, requestSent{p, request})
	if ree.currentWaitForResumeResult < len(ree.loaderRanges) && !request.IsCancel() {
		ree.configureLoader(ree.p, ree.request.ID(), ree.tbc, ree.fal, ree.loaderRanges[ree.currentWaitForResumeResult])
	}
}

func (ree *requestExecutionEnv) nodeStyleChooser(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
	ree.nodeStyleChooserCalled = true
	return basicnode.Prototype.Any, nil
}

func (ree *requestExecutionEnv) checkPause(requestID graphsync.RequestID, link ipld.Link, result <-chan types.AsyncLoadResult) {
	if ree.currentPauseResult >= len(ree.externalPauses) {
		return
	}
	currentPause := ree.externalPauses[ree.currentPauseResult]
	if currentPause.link == link && currentPause.requestID == requestID {
		ree.currentPauseResult++
		ree.pauseMessages <- struct{}{}
		extensions, err := ree.waitForResume()
		if err != nil {
			ree.cancelFn()
		} else {
			ree.resumeMessages <- extensions
		}
	}
}

func (ree *requestExecutionEnv) runBlockHooks(p peer.ID, response graphsync.ResponseData, blk graphsync.BlockData) error {
	bhk := blockHookKey{p, response.RequestID(), blk.Link()}
	ree.blookHooksCalled = append(ree.blookHooksCalled, bhk)
	err := ree.blockHookResults[bhk]
	if _, ok := err.(hooks.ErrPaused); ok {
		extensions, err := ree.waitForResume()
		if err != nil {
			ree.cancelFn()
		} else {
			ree.resumeMessages <- extensions
		}
	}
	return err
}

func (ree *requestExecutionEnv) requestExecution() (chan graphsync.ResponseProgress, chan error) {
	var lastResponse atomic.Value
	lastResponse.Store(gsmsg.NewResponse(ree.request.ID(), graphsync.RequestAcknowledged))
	return executor.ExecutionEnv{
		SendRequest:      ree.sendRequest,
		RunBlockHooks:    ree.runBlockHooks,
		TerminateRequest: ree.terminateRequest,
		Loader:           ree.fal.AsyncLoad,
	}.Start(executor.RequestExecution{
		Ctx:              ree.ctx,
		P:                ree.p,
		LastResponse:     &lastResponse,
		Request:          ree.request,
		DoNotSendCids:    ree.doNotSendCids,
		NodePrototypeChooser: ree.nodeStyleChooser,
		ResumeMessages:   ree.resumeMessages,
		PauseMessages:    ree.pauseMessages,
	})
}
