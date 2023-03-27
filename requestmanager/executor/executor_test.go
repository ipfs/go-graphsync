package executor_test

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-peertaskqueue/peertask"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	graphsync "github.com/filecoin-project/boost-graphsync"
	"github.com/filecoin-project/boost-graphsync/donotsendfirstblocks"
	"github.com/filecoin-project/boost-graphsync/ipldutil"
	gsmsg "github.com/filecoin-project/boost-graphsync/message"
	"github.com/filecoin-project/boost-graphsync/requestmanager/executor"
	"github.com/filecoin-project/boost-graphsync/requestmanager/hooks"
	"github.com/filecoin-project/boost-graphsync/requestmanager/types"
	"github.com/filecoin-project/boost-graphsync/testutil"
)

func TestRequestExecutionBlockChain(t *testing.T) {
	testCases := map[string]struct {
		configureRequestExecution func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv)
		verifyResults             func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error)
	}{
		"simple success case": {
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, []requestSent{{ree.p, ree.request}}, ree.requestsSent)
				require.Len(t, ree.blookHooksCalled, 10)
				require.NoError(t, ree.terminalError)
			},
		},
		"missing block case": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.customRemoteBehavior = func() {
					// pretend the remote sent five blocks before encountering a missing block
					ree.reconciledLoader.successResponseOn(tbc.Blocks(0, 5))
					missingCid := cidlink.Link{Cid: tbc.Blocks(5, 6)[0].Cid()}
					ree.reconciledLoader.responseOn(missingCid, types.AsyncLoadResult{Err: graphsync.RemoteMissingBlockErr{Link: missingCid, Path: tbc.PathTipIndex(5)}})
				}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 5)
				require.Len(t, receivedErrors, 1)
				require.Equal(t, receivedErrors[0], graphsync.RemoteMissingBlockErr{Link: cidlink.Link{Cid: tbc.Blocks(5, 6)[0].Cid()}, Path: tbc.PathTipIndex(5)})
				require.Equal(t, []requestSent{{ree.p, ree.request}}, ree.requestsSent)
				// we should only call block hooks for blocks we actually received
				require.Len(t, ree.blookHooksCalled, 5)
				require.NoError(t, ree.terminalError)
			},
		},

		"error at block hook": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = hooks.UpdateResult{Err: errors.New("something went wrong")}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 6)
				require.Len(t, receivedErrors, 1)
				require.Regexp(t, "something went wrong", receivedErrors[0].Error())
				require.Len(t, ree.requestsSent, 2)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				require.Equal(t, ree.requestsSent[1].request.Type(), graphsync.RequestTypeCancel)
				require.Len(t, ree.blookHooksCalled, 6)
				require.EqualError(t, ree.terminalError, "something went wrong")
			},
		},
		"context cancelled": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = hooks.UpdateResult{Err: ipldutil.ContextCancelError{}}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 6)
				require.Empty(t, receivedErrors)
				require.Equal(t, []requestSent{{ree.p, ree.request}}, ree.requestsSent)
				require.Len(t, ree.blookHooksCalled, 6)
				require.EqualError(t, ree.terminalError, ipldutil.ContextCancelError{}.Error())
			},
		},
		"simple pause": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = hooks.UpdateResult{Err: hooks.ErrPaused{}}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 6)
				require.Empty(t, receivedErrors)
				require.Len(t, ree.requestsSent, 2)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				require.Equal(t, ree.requestsSent[1].request.Type(), graphsync.RequestTypeCancel)
				require.Len(t, ree.blookHooksCalled, 6)
				require.EqualError(t, ree.terminalError, hooks.ErrPaused{}.Error())
			},
		},
		"preexisting do not send firstBlocks": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.doNotSendFirstBlocks = 1
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, ree.request.ID(), ree.requestsSent[0].request.ID())
				require.Equal(t, ree.request.Root(), ree.requestsSent[0].request.Root())
				require.Equal(t, ree.request.Selector(), ree.requestsSent[0].request.Selector())
				doNotSendFirstBlocksData, has := ree.requestsSent[0].request.Extension(graphsync.ExtensionsDoNotSendFirstBlocks)
				require.True(t, has)
				doNotSendFirstBlocks, err := donotsendfirstblocks.DecodeDoNotSendFirstBlocks(doNotSendFirstBlocksData)
				require.NoError(t, err)
				require.Equal(t, int64(1), doNotSendFirstBlocks)
				require.Len(t, ree.blookHooksCalled, 10)
				require.NoError(t, ree.terminalError)
			},
		},
		"pause externally": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.externalPause = pauseKey{requestID, tbc.LinkTipIndex(5)}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 6)
				require.Empty(t, receivedErrors)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				require.Equal(t, ree.requestsSent[1].request.Type(), graphsync.RequestTypeCancel)
				require.Len(t, ree.blookHooksCalled, 6)
				require.EqualError(t, ree.terminalError, hooks.ErrPaused{}.Error())
			},
		},
		"resume request": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.initialRequest = false
				ree.loadLocallyUntil = 6
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, ree.request.ID(), ree.requestsSent[0].request.ID())
				require.Equal(t, ree.request.Root(), ree.requestsSent[0].request.Root())
				require.Equal(t, ree.request.Selector(), ree.requestsSent[0].request.Selector())
				doNotSendFirstBlocksData, has := ree.requestsSent[0].request.Extension(graphsync.ExtensionsDoNotSendFirstBlocks)
				require.True(t, has)
				doNotSendFirstBlocks, err := donotsendfirstblocks.DecodeDoNotSendFirstBlocks(doNotSendFirstBlocksData)
				require.NoError(t, err)
				require.Equal(t, int64(6), doNotSendFirstBlocks)
				require.Len(t, ree.blookHooksCalled, 10)
				require.NoError(t, ree.terminalError)
			},
		},
		"error at block hook has precedence over pause": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = hooks.UpdateResult{Err: errors.New("something went wrong")}
				ree.externalPause = pauseKey{requestID, tbc.LinkTipIndex(5)}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 6)
				require.Len(t, receivedErrors, 1)
				require.Regexp(t, "something went wrong", receivedErrors[0].Error())
				require.Len(t, ree.requestsSent, 2)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				require.Equal(t, ree.requestsSent[1].request.Type(), graphsync.RequestTypeCancel)
				require.Len(t, ree.blookHooksCalled, 6)
				require.EqualError(t, ree.terminalError, "something went wrong")
			},
		},
		"sending updates": {
			configureRequestExecution: func(p peer.ID, requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[blockHookKey{p, requestID, tbc.LinkTipIndex(5)}] = hooks.UpdateResult{Extensions: []graphsync.ExtensionData{{Name: "something", Data: basicnode.NewString("applesauce")}}}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Len(t, ree.requestsSent, 2)
				require.Equal(t, ree.request, ree.requestsSent[0].request)
				require.Equal(t, ree.requestsSent[1].request.Type(), graphsync.RequestTypeUpdate)
				data, has := ree.requestsSent[1].request.Extension("something")
				require.True(t, has)
				str, _ := data.AsString()
				require.Equal(t, str, "applesauce")
				require.Len(t, ree.blookHooksCalled, 10)
				require.NoError(t, ree.terminalError)
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			persistence := testutil.NewTestStore(make(map[datamodel.Link][]byte))
			tbc := testutil.SetupBlockChain(ctx, t, persistence, 100, 10)
			reconciledLoader := &fakeReconciledLoader{
				responses: make(map[datamodel.Link]chan types.AsyncLoadResult),
			}
			requestID := graphsync.NewRequestID()
			p := testutil.GeneratePeers(1)[0]
			requestCtx, requestCancel := context.WithCancel(ctx)
			defer requestCancel()
			var responsesReceived []graphsync.ResponseProgress
			ree := &requestExecutionEnv{
				ctx:                  requestCtx,
				p:                    p,
				pauseMessages:        make(chan struct{}, 1),
				blockHookResults:     make(map[blockHookKey]hooks.UpdateResult),
				doNotSendFirstBlocks: 0,
				request:              gsmsg.NewRequest(requestID, tbc.TipLink.(cidlink.Link).Cid, tbc.Selector(), graphsync.Priority(rand.Int31())),
				tbc:                  tbc,
				initialRequest:       true,
				inProgressErr:        make(chan error, 1),
				traverser: ipldutil.TraversalBuilder{
					Root:     tbc.TipLink,
					Selector: tbc.Selector(),
					Visitor: func(tp traversal.Progress, node datamodel.Node, tr traversal.VisitReason) error {
						responsesReceived = append(responsesReceived, graphsync.ResponseProgress{
							Node:      node,
							Path:      tp.Path,
							LastBlock: tp.LastBlock,
						})
						return nil
					},
				}.Start(requestCtx),
				reconciledLoader: reconciledLoader,
			}
			reconciledLoader.onAsyncLoad(ree.checkPause)
			if data.configureRequestExecution != nil {
				data.configureRequestExecution(p, requestID, tbc, ree)
			}
			reconciledLoader.successResponseOn(tbc.Blocks(0, ree.loadLocallyUntil))
			reconciledLoader.responseOn(tbc.LinkTipIndex(ree.loadLocallyUntil), types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{Link: tbc.LinkTipIndex(ree.loadLocallyUntil)}})
			var errorsReceived []error
			errCollectionErr := make(chan error, 1)
			go func() {
				for {
					select {
					case err, ok := <-ree.inProgressErr:
						if !ok {
							errCollectionErr <- nil
						} else {
							errorsReceived = append(errorsReceived, err)
						}
					case <-ctx.Done():
						errCollectionErr <- ctx.Err()
					}
				}
			}()
			executor.NewExecutor(ree, ree).ExecuteTask(ctx, ree.p, &peertask.Task{})
			require.NoError(t, <-errCollectionErr)
			ree.traverser.Shutdown(ctx)
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
	link      datamodel.Link
}

type pauseKey struct {
	requestID graphsync.RequestID
	link      datamodel.Link
}

type requestExecutionEnv struct {
	// params
	ctx                  context.Context
	request              gsmsg.GraphSyncRequest
	p                    peer.ID
	blockHookResults     map[blockHookKey]hooks.UpdateResult
	doNotSendFirstBlocks int64
	pauseMessages        chan struct{}
	externalPause        pauseKey
	loadLocallyUntil     int
	traverser            ipldutil.Traverser
	reconciledLoader     *fakeReconciledLoader
	inProgressErr        chan error
	initialRequest       bool
	customRemoteBehavior func()
	// results
	requestsSent     []requestSent
	blookHooksCalled []blockHookKey
	terminalError    error

	// deps
	tbc *testutil.TestBlockChain
}

type fakeReconciledLoader struct {
	responsesLk sync.Mutex
	responses   map[datamodel.Link]chan types.AsyncLoadResult
	lastLoad    datamodel.Link
	online      bool
	cb          func(datamodel.Link)
}

func (frl *fakeReconciledLoader) onAsyncLoad(cb func(datamodel.Link)) {
	frl.cb = cb
}

func (frl *fakeReconciledLoader) responseOn(link datamodel.Link, result types.AsyncLoadResult) {
	response := frl.asyncLoad(link, true)
	response <- result
	close(response)
}

func (frl *fakeReconciledLoader) successResponseOn(blks []blocks.Block) {

	for _, block := range blks {
		frl.responseOn(cidlink.Link{Cid: block.Cid()}, types.AsyncLoadResult{Data: block.RawData(), Local: false, Err: nil})
	}
}

func (frl *fakeReconciledLoader) asyncLoad(link datamodel.Link, force bool) chan types.AsyncLoadResult {
	frl.responsesLk.Lock()
	response, ok := frl.responses[link]
	if !ok || force {
		response = make(chan types.AsyncLoadResult, 1)
		frl.responses[link] = response
	}
	frl.responsesLk.Unlock()
	return response
}

func (frl *fakeReconciledLoader) BlockReadOpener(_ linking.LinkContext, link datamodel.Link) types.AsyncLoadResult {
	frl.lastLoad = link
	if frl.cb != nil {
		frl.cb(link)
	}
	return <-frl.asyncLoad(link, false)
}

func (frl *fakeReconciledLoader) RetryLastLoad() types.AsyncLoadResult {
	if frl.cb != nil {
		frl.cb(frl.lastLoad)
	}
	return <-frl.asyncLoad(frl.lastLoad, false)
}

func (frl *fakeReconciledLoader) SetRemoteOnline(online bool) {
	frl.online = true
}
func (ree *requestExecutionEnv) ReleaseRequestTask(_ peer.ID, _ *peertask.Task, err error) {
	ree.terminalError = err
	close(ree.inProgressErr)
}

func (ree *requestExecutionEnv) GetRequestTask(_ peer.ID, _ *peertask.Task, requestExecutionChan chan executor.RequestTask) {
	var lastResponse atomic.Value
	lastResponse.Store(gsmsg.NewResponse(ree.request.ID(), graphsync.RequestAcknowledged, nil))

	requestExecution := executor.RequestTask{
		Ctx:                  ree.ctx,
		Request:              ree.request,
		LastResponse:         &lastResponse,
		DoNotSendFirstBlocks: ree.doNotSendFirstBlocks,
		PauseMessages:        ree.pauseMessages,
		Traverser:            ree.traverser,
		P:                    ree.p,
		InProgressErr:        ree.inProgressErr,
		Empty:                false,
		ReconciledLoader:     ree.reconciledLoader,
	}
	go func() {
		select {
		case <-ree.ctx.Done():
		case requestExecutionChan <- requestExecution:
		}
	}()
}

func (ree *requestExecutionEnv) SendRequest(p peer.ID, request gsmsg.GraphSyncRequest) {
	ree.requestsSent = append(ree.requestsSent, requestSent{p, request})
	if request.Type() == graphsync.RequestTypeNew {
		if ree.customRemoteBehavior == nil {
			ree.reconciledLoader.successResponseOn(ree.tbc.Blocks(ree.loadLocallyUntil, len(ree.tbc.AllBlocks())))
		} else {
			ree.customRemoteBehavior()
		}
	}
}

func (ree *requestExecutionEnv) ProcessBlockHooks(p peer.ID, response graphsync.ResponseData, blk graphsync.BlockData) hooks.UpdateResult {
	bhk := blockHookKey{p, response.RequestID(), blk.Link()}
	ree.blookHooksCalled = append(ree.blookHooksCalled, bhk)
	return ree.blockHookResults[bhk]
}

func (ree *requestExecutionEnv) checkPause(link datamodel.Link) {
	if ree.externalPause.link == link {
		ree.externalPause = pauseKey{}
		ree.pauseMessages <- struct{}{}
	}
}
