package executor_test

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/requestmanager/executor"
	"github.com/ipfs/go-graphsync/requestmanager/loader"
	"github.com/ipfs/go-graphsync/requestmanager/testloader"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/stretchr/testify/require"
)

func TestRequestExecutionBlockChain(t *testing.T) {
	testCases := map[string]struct {
		configureLoader           func(requestID graphsync.RequestID, tbc *testutil.TestBlockChain, fal *testloader.FakeAsyncLoader)
		configureRequestExecution func(requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv)
		verifyResults             func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error)
	}{
		"simple success case": {
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyWholeChainSync(responses)
				require.Empty(t, receivedErrors)
				require.Equal(t, 0, ree.currentWaitForResumeResult)
				require.Equal(t, []gsmsg.GraphSyncRequest{ree.request}, ree.requestsSent)
				require.Len(t, ree.blookHooksCalled, 10)
				require.True(t, ree.terminateRequested)
				require.True(t, ree.nodeStyleChooserCalled)
			},
		},
		"error at block hook": {
			configureRequestExecution: func(requestID graphsync.RequestID, tbc *testutil.TestBlockChain, ree *requestExecutionEnv) {
				ree.blockHookResults[tbc.LinkTipIndex(5)] = errors.New("something went wrong")
			},
			verifyResults: func(t *testing.T, tbc *testutil.TestBlockChain, ree *requestExecutionEnv, responses []graphsync.ResponseProgress, receivedErrors []error) {
				tbc.VerifyResponseRangeSync(responses, 0, 5)
				require.Len(t, receivedErrors, 1)
				require.True(t, errors.Is(receivedErrors[0], errors.New("something went wrong")))
				require.Equal(t, 0, ree.currentWaitForResumeResult)
				require.Equal(t, []gsmsg.GraphSyncRequest{ree.request}, ree.requestsSent)
				require.Len(t, ree.blookHooksCalled, 5)
				require.True(t, ree.terminateRequested)
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
			if data.configureLoader == nil {
				fal.SuccessResponseOn(requestID, tbc.AllBlocks())
			} else {
				data.configureLoader(requestID, tbc, fal)
			}
			ree := &requestExecutionEnv{
				blockHookResults: make(map[ipld.Link]error),
				doNotSendCids:    cid.NewSet(),
				request:          gsmsg.NewRequest(requestID, tbc.TipLink.(cidlink.Link).Cid, tbc.Selector(), graphsync.Priority(rand.Int31())),
				loader:           fal.AsyncLoad,
			}
			if data.configureRequestExecution != nil {
				data.configureRequestExecution(requestID, tbc, ree)
			}
			inProgress, inProgressErr := ree.requestExecution().Start(ctx)
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

type requestExecutionEnv struct {
	// params
	request              gsmsg.GraphSyncRequest
	blockHookResults     map[ipld.Link]error
	doNotSendCids        *cid.Set
	waitForResumeResults [][]graphsync.ExtensionData

	// results
	currentWaitForResumeResult int
	requestsSent               []gsmsg.GraphSyncRequest
	blookHooksCalled           []ipld.Link
	terminateRequested         bool
	nodeStyleChooserCalled     bool

	// loader tracks seperately
	loader loader.AsyncLoadFn
}

func (ree *requestExecutionEnv) terminateRequest() {
	ree.terminateRequested = true
}

func (ree *requestExecutionEnv) waitForResume() ([]graphsync.ExtensionData, error) {
	if len(ree.waitForResumeResults) <= ree.currentWaitForResumeResult {
		return nil, loader.ContextCancelError{}
	}
	extensions := ree.waitForResumeResults[ree.currentWaitForResumeResult]
	ree.currentWaitForResumeResult++
	return extensions, nil
}

func (ree *requestExecutionEnv) sendRequest(request gsmsg.GraphSyncRequest) {
	ree.requestsSent = append(ree.requestsSent, request)
}

func (ree *requestExecutionEnv) nodeStyleChooser(ipld.Link, ipld.LinkContext) (ipld.NodeStyle, error) {
	ree.nodeStyleChooserCalled = true
	return basicnode.Style.Any, nil
}

func (ree *requestExecutionEnv) runBlockHooks(blk graphsync.BlockData) error {
	ree.blookHooksCalled = append(ree.blookHooksCalled, blk.Link())
	return ree.blockHookResults[blk.Link()]
}

func (ree *requestExecutionEnv) requestExecution() executor.RequestExecution {
	return executor.RequestExecution{
		Request:          ree.request,
		SendRequest:      ree.sendRequest,
		Loader:           ree.loader,
		RunBlockHooks:    ree.runBlockHooks,
		DoNotSendCids:    ree.doNotSendCids,
		TerminateRequest: ree.terminateRequest,
		WaitForResume:    ree.waitForResume,
		NodeStyleChooser: ree.nodeStyleChooser,
	}
}
