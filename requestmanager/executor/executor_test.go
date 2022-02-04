package executor_test

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/donotsendfirstblocks"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/requestmanager/executor"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/requestmanager/testloader"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipfs/go-graphsync/testutil"
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
					ree.fal.SuccessResponseOn(p, requestID, tbc.Blocks(0, 5))
					missingCid := cidlink.Link{Cid: tbc.Blocks(5, 6)[0].Cid()}
					ree.fal.ResponseOn(p, requestID, missingCid, types.AsyncLoadResult{Err: graphsync.RemoteMissingBlockErr{Link: missingCid}})
				}
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 5)
				require.Len(t, receivedErrors, 1)
				require.Equal(t, receivedErrors[0], graphsync.RemoteMissingBlockErr{Link: cidlink.Link{Cid: tbc.Blocks(5, 6)[0].Cid()}})
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
			persistence := testutil.NewTestStore(make(map[ipld.Link][]byte))
			tbc := testutil.SetupBlockChain(ctx, t, persistence, 100, 10)
			fal := testloader.NewFakeAsyncLoader()
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
				fal:                  fal,
				tbc:                  tbc,
				initialRequest:       true,
				inProgressErr:        make(chan error, 1),
				traverser: ipldutil.TraversalBuilder{
					Root:     tbc.TipLink,
					Selector: tbc.Selector(),
					Visitor: func(tp traversal.Progress, node ipld.Node, tr traversal.VisitReason) error {
						responsesReceived = append(responsesReceived, graphsync.ResponseProgress{
							Node:      node,
							Path:      tp.Path,
							LastBlock: tp.LastBlock,
						})
						return nil
					},
				}.Start(requestCtx),
			}
			fal.OnAsyncLoad(ree.checkPause)
			if data.configureRequestExecution != nil {
				data.configureRequestExecution(p, requestID, tbc, ree)
			}
			ree.fal.SuccessResponseOn(p, requestID, tbc.Blocks(0, ree.loadLocallyUntil))
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
			executor.NewExecutor(ree, ree, fal.AsyncLoad).ExecuteTask(ctx, ree.p, &peertask.Task{})
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
	link      ipld.Link
}

type pauseKey struct {
	requestID graphsync.RequestID
	link      ipld.Link
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
	inProgressErr        chan error
	initialRequest       bool
	customRemoteBehavior func()
	// results
	requestsSent     []requestSent
	blookHooksCalled []blockHookKey
	terminalError    error

	// deps
	tbc *testutil.TestBlockChain
	fal *testloader.FakeAsyncLoader
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
		InitialRequest:       ree.initialRequest,
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
			ree.fal.SuccessResponseOn(p, request.ID(), ree.tbc.Blocks(ree.loadLocallyUntil, len(ree.tbc.AllBlocks())))
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

func (ree *requestExecutionEnv) checkPause(requestID graphsync.RequestID, link ipld.Link, result <-chan types.AsyncLoadResult) {
	if ree.externalPause.link == link && ree.externalPause.requestID == requestID {
		ree.externalPause = pauseKey{}
		ree.pauseMessages <- struct{}{}
	}
}
