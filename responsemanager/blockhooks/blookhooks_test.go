package blockhooks_test

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/blockhooks"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"
)

type fakeBlkData struct {
	link ipld.Link
	size uint64
}

func (fbd fakeBlkData) Link() ipld.Link {
	return fbd.link
}

func (fbd fakeBlkData) BlockSize() uint64 {
	return fbd.size
}

func (fbd fakeBlkData) BlockSizeOnWire() uint64 {
	return fbd.size
}

func TestBlockHookProcessing(t *testing.T) {
	extensionData := testutil.RandomBytes(100)
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionData,
	}
	extensionResponseData := testutil.RandomBytes(100)
	extensionResponse := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionResponseData,
	}

	root := testutil.GenerateCids(1)[0]
	requestID := graphsync.RequestID(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	request := gsmsg.NewRequest(requestID, root, ssb.Matcher().Node(), graphsync.Priority(0), extension)
	p := testutil.GeneratePeers(1)[0]
	blockData := &fakeBlkData{
		link: cidlink.Link{Cid: testutil.GenerateCids(1)[0]},
		size: rand.Uint64(),
	}
	testCases := map[string]struct {
		configure func(t *testing.T, blockHooks *blockhooks.OutgoingBlockHooks)
		assert    func(t *testing.T, result blockhooks.Result)
	}{
		"no hooks": {
			assert: func(t *testing.T, result blockhooks.Result) {
				require.Empty(t, result.Extensions)
				require.NoError(t, result.Err)
			},
		},
		"send extension data": {
			configure: func(t *testing.T, blockHooks *blockhooks.OutgoingBlockHooks) {
				blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					hookActions.SendExtensionData(extensionResponse)
				})
			},
			assert: func(t *testing.T, result blockhooks.Result) {
				require.Len(t, result.Extensions, 1)
				require.Contains(t, result.Extensions, extensionResponse)
				require.NoError(t, result.Err)
			},
		},
		"terminate with error": {
			configure: func(t *testing.T, blockHooks *blockhooks.OutgoingBlockHooks) {
				blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					hookActions.TerminateWithError(errors.New("failed"))
				})
			},
			assert: func(t *testing.T, result blockhooks.Result) {
				require.Empty(t, result.Extensions)
				require.EqualError(t, result.Err, "failed")
			},
		},
		"pause response": {
			configure: func(t *testing.T, blockHooks *blockhooks.OutgoingBlockHooks) {
				blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					hookActions.PauseResponse()
				})
			},
			assert: func(t *testing.T, result blockhooks.Result) {
				require.Empty(t, result.Extensions)
				require.EqualError(t, result.Err, blockhooks.ErrPaused.Error())
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			blockHooks := blockhooks.New()
			if data.configure != nil {
				data.configure(t, blockHooks)
			}
			result := blockHooks.ProcessBlockHooks(p, request, blockData)
			if data.assert != nil {
				data.assert(t, result)
			}
		})
	}
}

/*

	t.Run("test block hook processing", func(t *testing.T) {
		t.Run("can send extension data", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks)
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				hookActions.SendExtensionData(td.extensionResponse)
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			var lastRequest completedRequest
			testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
			require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
			for i := 0; i < td.blockChainLength; i++ {
				var receivedExtension sentExtension
				testutil.AssertReceive(td.ctx, t, td.sentExtensions, &receivedExtension, "should send extension response")
				require.Equal(t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")
			}
		})

		t.Run("can send errors", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks)
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				hookActions.TerminateWithError(errors.New("failed"))
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			var lastRequest completedRequest
			testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
			require.True(t, gsmsg.IsTerminalFailureCode(lastRequest.result), "request should succeed")
		})

		t.Run("can pause/unpause", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := New(td.ctx, td.loader, td.peerManager, td.queryQueue, td.requestHooks, td.blockHooks)
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			blkIndex := 1
			blockCount := 3
			var hasPaused bool
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				if blkIndex >= blockCount && !hasPaused {
					hookActions.PauseResponse()
					hasPaused = true
				}
				blkIndex++
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			timer := time.NewTimer(500 * time.Millisecond)
			testutil.AssertDoesReceiveFirst(t, timer.C, "should not complete request while paused", td.completedRequestChan)
			var sentResponses []sentResponse
		nomoreresponses:
			for {
				select {
				case sentResponse := <-td.sentResponses:
					sentResponses = append(sentResponses, sentResponse)
				default:
					break nomoreresponses
				}
			}
			require.LessOrEqual(t, len(sentResponses), blockCount)
			err := responseManager.UnpauseResponse(td.p, td.requestID)
			require.NoError(t, err)
			var lastRequest completedRequest
			testutil.AssertReceive(td.ctx, t, td.completedRequestChan, &lastRequest, "should complete request")
			require.True(t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
		})

	})
*/
