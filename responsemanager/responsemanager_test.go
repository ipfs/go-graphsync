package responsemanager

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-peertaskqueue/peertask"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/dedupkey"
	"github.com/ipfs/go-graphsync/listeners"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/messagequeue"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/persistenceoptions"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
	"github.com/ipfs/go-graphsync/selectorvalidator"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestIncomingQuery(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	blks := td.blockChain.AllBlocks()

	responseManager := td.newResponseManager()
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	responseManager.Startup()

	responseManager.ProcessRequests(td.ctx, td.p, td.requests)
	testutil.AssertDoesReceive(td.ctx, t, td.completedRequestChan, "Should have completed request but didn't")
	for i := 0; i < len(blks); i++ {
		td.assertSendBlock()
	}
}

func TestCancellationQueryInProgress(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	responseManager := td.newResponseManager()
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	cancelledListenerCalled := make(chan struct{}, 1)
	td.cancelledListeners.Register(func(p peer.ID, request graphsync.RequestData) {
		cancelledListenerCalled <- struct{}{}
	})
	responseManager.Startup()
	responseManager.ProcessRequests(td.ctx, td.p, td.requests)

	// read one block
	td.assertSendBlock()

	// send a cancellation
	cancelRequests := []gsmsg.GraphSyncRequest{
		gsmsg.CancelRequest(td.requestID),
	}
	responseManager.ProcessRequests(td.ctx, td.p, cancelRequests)
	responseManager.synchronize()

	testutil.AssertDoesReceive(td.ctx, t, cancelledListenerCalled, "should call cancelled listener")

	td.assertRequestCleared()
}

func TestCancellationViaCommand(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	responseManager := td.newResponseManager()
	td.requestHooks.Register(selectorvalidator.SelectorValidator(100))
	responseManager.Startup()
	responseManager.ProcessRequests(td.ctx, td.p, td.requests)

	// read one block
	td.assertSendBlock()

	// send a cancellation
	err := responseManager.CancelResponse(td.p, td.requestID)
	require.NoError(t, err)

	td.assertCompleteRequestWithFailure()
}

func TestEarlyCancellation(t *testing.T) {
	td := newTestData(t)
	defer td.cancel()
	td.queryQueue.popWait.Add(1)
	responseManager := td.newResponseManager()
	responseManager.Startup()
	responseManager.ProcessRequests(td.ctx, td.p, td.requests)

	// send a cancellation
	cancelRequests := []gsmsg.GraphSyncRequest{
		gsmsg.CancelRequest(td.requestID),
	}
	responseManager.ProcessRequests(td.ctx, td.p, cancelRequests)

	responseManager.synchronize()

	// unblock popping from queue
	td.queryQueue.popWait.Done()

	td.assertNoResponses()
}

func TestValidationAndExtensions(t *testing.T) {
	t.Run("on its own, should fail validation", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWithFailure()
	})

	t.Run("if non validating hook succeeds, does not pass validation", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.SendExtensionData(td.extensionResponse)
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWithFailure()
		td.assertReceiveExtensionResponse()
	})

	t.Run("if validating hook succeeds, should pass validation", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
			hookActions.SendExtensionData(td.extensionResponse)
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWithSuccess()
		td.assertReceiveExtensionResponse()
	})

	t.Run("if any hook fails, should fail", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.SendExtensionData(td.extensionResponse)
			hookActions.TerminateWithError(errors.New("everything went to crap"))
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWithFailure()
		td.assertReceiveExtensionResponse()
	})

	t.Run("hooks can be unregistered", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		unregister := td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
			hookActions.SendExtensionData(td.extensionResponse)
		})

		// hook validates request
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWithSuccess()
		td.assertReceiveExtensionResponse()

		// unregister
		unregister()

		// now same request should fail
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWithFailure()
	})

	t.Run("hooks can alter the loader", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.alternateLoaderResponseManager()
		responseManager.Startup()
		// add validating hook -- so the request SHOULD succeed
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})

		// request fails with base loader reading from block store that's missing data
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWithFailure()

		err := td.peristenceOptions.Register("chainstore", td.loader)
		require.NoError(t, err)
		// register hook to use different loader
		_ = td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			if _, found := requestData.Extension(td.extensionName); found {
				hookActions.UsePersistenceOption("chainstore")
				hookActions.SendExtensionData(td.extensionResponse)
			}
		})
		// hook uses different loader that should make request succeed
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWithSuccess()
		td.assertReceiveExtensionResponse()
	})

	t.Run("hooks can alter the node builder chooser", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()

		customChooserCallCount := 0
		customChooser := func(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
			customChooserCallCount++
			return basicnode.Prototype.Any, nil
		}

		// add validating hook -- so the request SHOULD succeed
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})

		// with default chooser, customer chooser not called
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWithSuccess()
		require.Equal(t, 0, customChooserCallCount)

		// register hook to use custom chooser
		_ = td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			if _, found := requestData.Extension(td.extensionName); found {
				hookActions.UseLinkTargetNodePrototypeChooser(customChooser)
				hookActions.SendExtensionData(td.extensionResponse)
			}
		})

		// verify now that request succeeds and uses custom chooser
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertCompleteRequestWithSuccess()
		td.assertReceiveExtensionResponse()
		require.Equal(t, 5, customChooserCallCount)
	})

	t.Run("do-not-send-cids extension", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		set := cid.NewSet()
		blks := td.blockChain.Blocks(0, 5)
		for _, blk := range blks {
			set.Add(blk.Cid())
		}
		data, err := cidset.EncodeCidSet(set)
		require.NoError(t, err)
		requests := []gsmsg.GraphSyncRequest{
			gsmsg.NewRequest(td.requestID, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0),
				graphsync.ExtensionData{
					Name: graphsync.ExtensionDoNotSendCIDs,
					Data: data,
				}),
		}
		responseManager.ProcessRequests(td.ctx, td.p, requests)
		td.assertCompleteRequestWithSuccess()
		td.assertIgnoredCids(set)
	})
	t.Run("dedup-by-key extension", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		data, err := dedupkey.EncodeDedupKey("applesauce")
		require.NoError(t, err)
		requests := []gsmsg.GraphSyncRequest{
			gsmsg.NewRequest(td.requestID, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0),
				graphsync.ExtensionData{
					Name: graphsync.ExtensionDeDupByKey,
					Data: data,
				}),
		}
		responseManager.ProcessRequests(td.ctx, td.p, requests)
		td.assertCompleteRequestWithSuccess()
		td.assertDedupKey("applesauce")
	})
	t.Run("no-blocks extension", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		requests := []gsmsg.GraphSyncRequest{
			gsmsg.NewRequest(td.requestID, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0),
				graphsync.ExtensionData{
					Name: graphsync.ExtensionNoBlocks,
					Data: nil,
				}),
		}
		responseManager.ProcessRequests(td.ctx, td.p, requests)
		td.assertCompleteRequestWithSuccess()
		td.assertNoBlocks()
	})
	t.Run("test pause/resume", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
			hookActions.PauseResponse()
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertPausedRequest()
		td.assertRequestDoesNotCompleteWhilePaused()
		testutil.AssertChannelEmpty(t, td.sentResponses, "should not send more blocks")
		err := responseManager.UnpauseResponse(td.p, td.requestID)
		require.NoError(t, err)
		td.assertCompleteRequestWithSuccess()
	})
	t.Run("test block hook processing", func(t *testing.T) {
		t.Run("can send extension data", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				hookActions.SendExtensionData(td.extensionResponse)
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertCompleteRequestWithSuccess()
			for i := 0; i < td.blockChainLength; i++ {
				td.assertReceiveExtensionResponse()
			}
		})

		t.Run("can send errors", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				hookActions.TerminateWithError(errors.New("failed"))
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertCompleteRequestWithFailure()
		})

		t.Run("can pause/unpause", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			blkIndex := 0
			blockCount := 3
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				blkIndex++
				if blkIndex == blockCount {
					hookActions.PauseResponse()
				}
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertRequestDoesNotCompleteWhilePaused()
			td.verifyNResponses(blockCount)
			td.assertPausedRequest()
			err := responseManager.UnpauseResponse(td.p, td.requestID, td.extensionResponse)
			require.NoError(t, err)
			td.assertReceiveExtensionResponse()
			td.assertCompleteRequestWithSuccess()
		})

		t.Run("can pause/unpause externally", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			blkIndex := 0
			blockCount := 3
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				blkIndex++
				if blkIndex == blockCount {
					err := responseManager.PauseResponse(p, requestData.ID())
					require.NoError(t, err)
				}
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertRequestDoesNotCompleteWhilePaused()
			td.verifyNResponses(blockCount + 1)
			td.assertPausedRequest()
			err := responseManager.UnpauseResponse(td.p, td.requestID)
			require.NoError(t, err)
			td.verifyNResponses(td.blockChainLength - (blockCount + 1))
			td.assertCompleteRequestWithSuccess()
		})

	})

	t.Run("test update hook processing", func(t *testing.T) {

		t.Run("can pause/unpause", func(t *testing.T) {
			td := newTestData(t)
			defer td.cancel()
			responseManager := td.newResponseManager()
			responseManager.Startup()
			td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
				hookActions.ValidateRequest()
			})
			blkIndex := 0
			blockCount := 3
			td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
				blkIndex++
				if blkIndex == blockCount {
					hookActions.PauseResponse()
				}
			})
			td.updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
				if _, found := updateData.Extension(td.extensionName); found {
					hookActions.UnpauseResponse()
				}
			})
			responseManager.ProcessRequests(td.ctx, td.p, td.requests)
			td.assertRequestDoesNotCompleteWhilePaused()
			td.verifyNResponses(blockCount)
			td.assertPausedRequest()

			responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)
			td.assertCompleteRequestWithSuccess()
		})

		t.Run("can send extension data", func(t *testing.T) {
			t.Run("when unpaused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := td.newResponseManager()
				responseManager.Startup()
				td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
				})
				blkIndex := 0
				blockCount := 3
				wait := make(chan struct{})
				sent := make(chan struct{})
				td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					blkIndex++
					if blkIndex == blockCount {
						close(sent)
						<-wait
					}
				})
				td.updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					if _, found := updateData.Extension(td.extensionName); found {
						hookActions.SendExtensionData(td.extensionResponse)
					}
				})
				responseManager.ProcessRequests(td.ctx, td.p, td.requests)
				testutil.AssertDoesReceive(td.ctx, t, sent, "sends blocks")
				responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)
				responseManager.synchronize()
				close(wait)
				td.assertCompleteRequestWithSuccess()
				td.assertReceiveExtensionResponse()
			})

			t.Run("when paused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := td.newResponseManager()
				responseManager.Startup()
				td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
				})
				blkIndex := 0
				blockCount := 3
				td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					blkIndex++
					if blkIndex == blockCount {
						hookActions.PauseResponse()
					}
				})
				td.updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					if _, found := updateData.Extension(td.extensionName); found {
						hookActions.SendExtensionData(td.extensionResponse)
					}
				})
				responseManager.ProcessRequests(td.ctx, td.p, td.requests)
				td.verifyNResponses(blockCount)
				td.assertPausedRequest()

				// send update
				responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)

				// receive data
				td.assertReceiveExtensionResponse()

				// should still be paused
				td.assertRequestDoesNotCompleteWhilePaused()
			})
		})

		t.Run("can send errors", func(t *testing.T) {
			t.Run("when unpaused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := td.newResponseManager()
				responseManager.Startup()
				td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
				})
				blkIndex := 0
				blockCount := 3
				wait := make(chan struct{})
				sent := make(chan struct{})
				td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					blkIndex++
					if blkIndex == blockCount {
						close(sent)
						<-wait
					}
				})
				td.updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					if _, found := updateData.Extension(td.extensionName); found {
						hookActions.TerminateWithError(errors.New("something went wrong"))
					}
				})
				responseManager.ProcessRequests(td.ctx, td.p, td.requests)
				testutil.AssertDoesReceive(td.ctx, t, sent, "sends blocks")
				responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)
				responseManager.synchronize()
				close(wait)
				td.assertCompleteRequestWithFailure()
			})

			t.Run("when paused", func(t *testing.T) {
				td := newTestData(t)
				defer td.cancel()
				responseManager := td.newResponseManager()
				responseManager.Startup()
				td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
				})
				blkIndex := 0
				blockCount := 3
				td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					blkIndex++
					if blkIndex == blockCount {
						hookActions.PauseResponse()
					}
				})
				td.updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					if _, found := updateData.Extension(td.extensionName); found {
						hookActions.TerminateWithError(errors.New("something went wrong"))
					}
				})
				responseManager.ProcessRequests(td.ctx, td.p, td.requests)
				td.verifyNResponses(blockCount)
				td.assertPausedRequest()

				// send update
				responseManager.ProcessRequests(td.ctx, td.p, td.updateRequests)
				td.assertCompleteRequestWithFailure()

				// cannot unpause
				err := responseManager.UnpauseResponse(td.p, td.requestID)
				require.Error(t, err)
			})
		})

	})
}

func TestNetworkErrors(t *testing.T) {
	t.Run("network error final status - success", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.verifyNResponses(td.blockChainLength)
		td.assertOnlyCompleteProcessingWithSuccess()
		err := errors.New("something went wrong")
		td.notifyStatusMessagesNetworkError(err)
		td.assertNetworkErrors(err, 1)
		td.assertNoCompletedResponseStatuses()
	})
	t.Run("network error final status - failure", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertOnlyCompleteProcessingWithFailure()
		err := errors.New("something went wrong")
		td.notifyStatusMessagesNetworkError(err)
		td.assertNetworkErrors(err, 1)
		td.assertNoCompletedResponseStatuses()
	})
	t.Run("network error block send", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertSendBlock()
		err := errors.New("something went wrong")
		td.notifyBlockSendsNetworkError(err)
		td.assertHasNetworkErrors(err)
		td.assertNoCompletedResponseStatuses()
		td.assertRequestCleared()
	})
	t.Run("network error while paused", func(t *testing.T) {
		td := newTestData(t)
		defer td.cancel()
		responseManager := td.newResponseManager()
		responseManager.Startup()
		td.requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			hookActions.ValidateRequest()
		})
		blkIndex := 0
		blockCount := 3
		td.blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			blkIndex++
			if blkIndex == blockCount {
				hookActions.PauseResponse()
			}
		})
		responseManager.ProcessRequests(td.ctx, td.p, td.requests)
		td.assertRequestDoesNotCompleteWhilePaused()
		td.verifyNResponsesOnlyProcessing(blockCount)
		td.assertPausedRequest()
		err := errors.New("something went wrong")
		td.notifyBlockSendsNetworkError(err)
		td.assertNetworkErrors(err, blockCount)
		td.assertRequestCleared()
		err = responseManager.UnpauseResponse(td.p, td.requestID, td.extensionResponse)
		require.Error(t, err)
	})
}

type fakeQueryQueue struct {
	popWait   sync.WaitGroup
	queriesLk sync.RWMutex
	queries   []*peertask.QueueTask
}

func (fqq *fakeQueryQueue) PushTasks(to peer.ID, tasks ...peertask.Task) {
	fqq.queriesLk.Lock()

	// This isn't quite right as the queue should deduplicate requests, but
	// it's good enough.
	for _, task := range tasks {
		fqq.queries = append(fqq.queries, peertask.NewQueueTask(task, to, time.Now()))
	}
	fqq.queriesLk.Unlock()
}

func (fqq *fakeQueryQueue) PopTasks(targetWork int) (peer.ID, []*peertask.Task, int) {
	fqq.popWait.Wait()
	fqq.queriesLk.Lock()
	defer fqq.queriesLk.Unlock()
	if len(fqq.queries) == 0 {
		return "", nil, -1
	}
	// We're not bothering to implement "work"
	task := fqq.queries[0]
	fqq.queries = fqq.queries[1:]
	return task.Target, []*peertask.Task{&task.Task}, 0
}

func (fqq *fakeQueryQueue) Remove(topic peertask.Topic, p peer.ID) {
	fqq.queriesLk.Lock()
	defer fqq.queriesLk.Unlock()
	for i, query := range fqq.queries {
		if query.Target == p && query.Topic == topic {
			fqq.queries = append(fqq.queries[:i], fqq.queries[i+1:]...)
		}
	}
}

func (fqq *fakeQueryQueue) TasksDone(to peer.ID, tasks ...*peertask.Task) {
	// We don't track active tasks so this is a no-op
}

func (fqq *fakeQueryQueue) ThawRound() {

}

type fakeResponseAssembler struct {
	sentResponses        chan sentResponse
	sentExtensions       chan sentExtension
	lastCompletedRequest chan completedRequest
	pausedRequests       chan pausedRequest
	clearedRequests      chan clearedRequest
	ignoredLinks         chan []ipld.Link
	noBlocks             chan struct{}
	notifeePublisher     *testutil.MockPublisher
	dedupKeys            chan string
}

func (fra *fakeResponseAssembler) Transaction(p peer.ID, requestID graphsync.RequestID, transaction responseassembler.Transaction) error {
	frb := &fakeResponseBuilder{requestID, fra,
		fra.notifeePublisher}
	return transaction(frb)
}

func (fra *fakeResponseAssembler) IgnoreBlocks(p peer.ID, requestID graphsync.RequestID, links []ipld.Link) {
	fra.ignoredLinks <- links
}

func (fra *fakeResponseAssembler) DedupKey(p peer.ID, requestID graphsync.RequestID, key string) {
	fra.dedupKeys <- key
}

func (fra *fakeResponseAssembler) IgnoreAllBlocks(p peer.ID, requestID graphsync.RequestID) {
	fra.noBlocks <- struct{}{}
}

type sentResponse struct {
	requestID graphsync.RequestID
	link      ipld.Link
	data      []byte
}

type sentExtension struct {
	requestID graphsync.RequestID
	extension graphsync.ExtensionData
}

type completedRequest struct {
	requestID graphsync.RequestID
	result    graphsync.ResponseStatusCode
}
type pausedRequest struct {
	requestID graphsync.RequestID
}

type clearedRequest struct {
	requestID graphsync.RequestID
}

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

func (fra *fakeResponseAssembler) sendResponse(
	requestID graphsync.RequestID,
	link ipld.Link,
	data []byte,
) graphsync.BlockData {
	fra.sentResponses <- sentResponse{requestID, link, data}

	return fakeBlkData{link, uint64(len(data))}
}

func (fra *fakeResponseAssembler) sendExtensionData(
	requestID graphsync.RequestID,
	extension graphsync.ExtensionData,
) {
	fra.sentExtensions <- sentExtension{requestID, extension}
}

func (fra *fakeResponseAssembler) finishRequest(requestID graphsync.RequestID) graphsync.ResponseStatusCode {
	fra.lastCompletedRequest <- completedRequest{requestID, graphsync.RequestCompletedFull}
	return graphsync.RequestCompletedFull
}

func (fra *fakeResponseAssembler) finishWithError(requestID graphsync.RequestID, status graphsync.ResponseStatusCode) {
	fra.lastCompletedRequest <- completedRequest{requestID, status}
}

func (fra *fakeResponseAssembler) pauseRequest(requestID graphsync.RequestID) {
	fra.pausedRequests <- pausedRequest{requestID}
}

func (fra *fakeResponseAssembler) clearRequest(requestID graphsync.RequestID) {
	fra.clearedRequests <- clearedRequest{requestID}
}

type fakeResponseBuilder struct {
	requestID        graphsync.RequestID
	fra              *fakeResponseAssembler
	notifeePublisher *testutil.MockPublisher
}

func (frb *fakeResponseBuilder) SendResponse(link ipld.Link, data []byte) graphsync.BlockData {
	return frb.fra.sendResponse(frb.requestID, link, data)
}

func (frb *fakeResponseBuilder) SendExtensionData(extension graphsync.ExtensionData) {
	frb.fra.sendExtensionData(frb.requestID, extension)
}

func (frb *fakeResponseBuilder) FinishRequest() graphsync.ResponseStatusCode {
	return frb.fra.finishRequest(frb.requestID)
}

func (frb *fakeResponseBuilder) FinishWithError(status graphsync.ResponseStatusCode) {
	frb.fra.finishWithError(frb.requestID, status)
}

func (frb *fakeResponseBuilder) PauseRequest() {
	frb.fra.pauseRequest(frb.requestID)
}

func (frb *fakeResponseBuilder) ClearRequest() {
	frb.fra.clearRequest(frb.requestID)
}

func (frb *fakeResponseBuilder) AddNotifee(notifee notifications.Notifee) {
	frb.notifeePublisher.AddNotifees([]notifications.Notifee{notifee})
}

type testData struct {
	ctx                       context.Context
	t                         *testing.T
	cancel                    func()
	blockStore                map[ipld.Link][]byte
	loader                    ipld.Loader
	storer                    ipld.Storer
	blockChainLength          int
	blockChain                *testutil.TestBlockChain
	completedRequestChan      chan completedRequest
	sentResponses             chan sentResponse
	sentExtensions            chan sentExtension
	pausedRequests            chan pausedRequest
	clearedRequests           chan clearedRequest
	ignoredLinks              chan []ipld.Link
	dedupKeys                 chan string
	noBlocks                  chan struct{}
	responseAssembler         *fakeResponseAssembler
	queryQueue                *fakeQueryQueue
	extensionData             []byte
	extensionName             graphsync.ExtensionName
	extension                 graphsync.ExtensionData
	extensionResponseData     []byte
	extensionResponse         graphsync.ExtensionData
	extensionUpdateData       []byte
	extensionUpdate           graphsync.ExtensionData
	requestID                 graphsync.RequestID
	requests                  []gsmsg.GraphSyncRequest
	updateRequests            []gsmsg.GraphSyncRequest
	p                         peer.ID
	peristenceOptions         *persistenceoptions.PersistenceOptions
	requestHooks              *hooks.IncomingRequestHooks
	blockHooks                *hooks.OutgoingBlockHooks
	updateHooks               *hooks.RequestUpdatedHooks
	completedListeners        *listeners.CompletedResponseListeners
	cancelledListeners        *listeners.RequestorCancelledListeners
	blockSentListeners        *listeners.BlockSentListeners
	networkErrorListeners     *listeners.NetworkErrorListeners
	notifeePublisher          *testutil.MockPublisher
	blockSends                chan graphsync.BlockData
	completedResponseStatuses chan graphsync.ResponseStatusCode
	networkErrorChan          chan error
	allBlocks                 []blocks.Block
}

func newTestData(t *testing.T) testData {
	ctx := context.Background()
	td := testData{}
	td.t = t
	td.ctx, td.cancel = context.WithTimeout(ctx, 10*time.Second)

	td.blockStore = make(map[ipld.Link][]byte)
	td.loader, td.storer = testutil.NewTestStore(td.blockStore)
	td.blockChainLength = 5
	td.blockChain = testutil.SetupBlockChain(ctx, t, td.loader, td.storer, 100, td.blockChainLength)

	td.completedRequestChan = make(chan completedRequest, 1)
	td.sentResponses = make(chan sentResponse, td.blockChainLength*2)
	td.sentExtensions = make(chan sentExtension, td.blockChainLength*2)
	td.pausedRequests = make(chan pausedRequest, 1)
	td.clearedRequests = make(chan clearedRequest, 1)
	td.ignoredLinks = make(chan []ipld.Link, 1)
	td.dedupKeys = make(chan string, 1)
	td.noBlocks = make(chan struct{}, 1)
	td.blockSends = make(chan graphsync.BlockData, td.blockChainLength*2)
	td.completedResponseStatuses = make(chan graphsync.ResponseStatusCode, 1)
	td.networkErrorChan = make(chan error, td.blockChainLength*2)
	td.notifeePublisher = testutil.NewMockPublisher()
	td.responseAssembler = &fakeResponseAssembler{
		lastCompletedRequest: td.completedRequestChan,
		sentResponses:        td.sentResponses,
		sentExtensions:       td.sentExtensions,
		pausedRequests:       td.pausedRequests,
		clearedRequests:      td.clearedRequests,
		ignoredLinks:         td.ignoredLinks,
		dedupKeys:            td.dedupKeys,
		notifeePublisher:     td.notifeePublisher,
		noBlocks:             td.noBlocks,
	}
	td.queryQueue = &fakeQueryQueue{}

	td.extensionData = testutil.RandomBytes(100)
	td.extensionName = graphsync.ExtensionName("AppleSauce/McGee")
	td.extension = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionData,
	}
	td.extensionResponseData = testutil.RandomBytes(100)
	td.extensionResponse = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionResponseData,
	}
	td.extensionUpdateData = testutil.RandomBytes(100)
	td.extensionUpdate = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionUpdateData,
	}
	td.requestID = graphsync.RequestID(rand.Int31())
	td.requests = []gsmsg.GraphSyncRequest{
		gsmsg.NewRequest(td.requestID, td.blockChain.TipLink.(cidlink.Link).Cid, td.blockChain.Selector(), graphsync.Priority(0), td.extension),
	}
	td.updateRequests = []gsmsg.GraphSyncRequest{
		gsmsg.UpdateRequest(td.requestID, td.extensionUpdate),
	}
	td.p = testutil.GeneratePeers(1)[0]
	td.peristenceOptions = persistenceoptions.New()
	td.requestHooks = hooks.NewRequestHooks(td.peristenceOptions)
	td.blockHooks = hooks.NewBlockHooks()
	td.updateHooks = hooks.NewUpdateHooks()
	td.completedListeners = listeners.NewCompletedResponseListeners()
	td.cancelledListeners = listeners.NewRequestorCancelledListeners()
	td.blockSentListeners = listeners.NewBlockSentListeners()
	td.networkErrorListeners = listeners.NewNetworkErrorListeners()
	td.completedListeners.Register(func(p peer.ID, requestID graphsync.RequestData, status graphsync.ResponseStatusCode) {
		select {
		case td.completedResponseStatuses <- status:
		default:
		}
	})
	td.blockSentListeners.Register(func(p peer.ID, requestID graphsync.RequestData, blockData graphsync.BlockData) {
		select {
		case td.blockSends <- blockData:
		default:
		}
	})
	td.networkErrorListeners.Register(func(p peer.ID, requestID graphsync.RequestData, err error) {
		select {
		case td.networkErrorChan <- err:
		default:
		}
	})
	return td
}

func (td *testData) newResponseManager() *ResponseManager {
	return New(td.ctx, td.loader, td.responseAssembler, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners, td.blockSentListeners, td.networkErrorListeners, 6)
}

func (td *testData) alternateLoaderResponseManager() *ResponseManager {
	obs := make(map[ipld.Link][]byte)
	oloader, _ := testutil.NewTestStore(obs)
	return New(td.ctx, oloader, td.responseAssembler, td.queryQueue, td.requestHooks, td.blockHooks, td.updateHooks, td.completedListeners, td.cancelledListeners, td.blockSentListeners, td.networkErrorListeners, 6)
}

func (td *testData) assertPausedRequest() {
	var pausedRequest pausedRequest
	testutil.AssertReceive(td.ctx, td.t, td.pausedRequests, &pausedRequest, "should pause request")
}

func (td *testData) getAllBlocks() []blocks.Block {
	if td.allBlocks == nil {
		td.allBlocks = td.blockChain.AllBlocks()
	}
	return td.allBlocks
}

func (td *testData) verifyResponse(sentResponse sentResponse) {
	k := sentResponse.link.(cidlink.Link)
	blks := td.getAllBlocks()
	blockIndex := testutil.IndexOf(blks, k.Cid)
	require.NotEqual(td.t, blockIndex, -1, "sent incorrect link")
	require.Equal(td.t, blks[blockIndex].RawData(), sentResponse.data, "sent incorrect data")
	require.Equal(td.t, td.requestID, sentResponse.requestID, "has incorrect response id")
}

func (td *testData) assertSendBlock() {
	var sentResponse sentResponse
	testutil.AssertReceive(td.ctx, td.t, td.sentResponses, &sentResponse, "did not send response")
	td.verifyResponse(sentResponse)
}

func (td *testData) assertNoResponses() {
	timer := time.NewTimer(200 * time.Millisecond)
	// verify no responses processed
	testutil.AssertDoesReceiveFirst(td.t, timer.C, "should not process more responses", td.sentResponses, td.completedRequestChan)
}

func (td *testData) assertCompleteRequestWithFailure() {
	td.assertOnlyCompleteProcessingWithFailure()
	td.notifyStatusMessagesSent()
	var status graphsync.ResponseStatusCode
	testutil.AssertReceive(td.ctx, td.t, td.completedResponseStatuses, &status, "should receive status")
	require.True(td.t, gsmsg.IsTerminalFailureCode(status), "request should succeed")
}

func (td *testData) assertCompleteRequestWithSuccess() {
	td.assertOnlyCompleteProcessingWithSuccess()
	td.notifyStatusMessagesSent()
	var status graphsync.ResponseStatusCode
	testutil.AssertReceive(td.ctx, td.t, td.completedResponseStatuses, &status, "should receive status")
	require.True(td.t, gsmsg.IsTerminalSuccessCode(status), "request should succeed")
}

func (td *testData) assertOnlyCompleteProcessingWithSuccess() {
	var lastRequest completedRequest
	testutil.AssertReceive(td.ctx, td.t, td.completedRequestChan, &lastRequest, "should complete request")
	require.True(td.t, gsmsg.IsTerminalSuccessCode(lastRequest.result), "request should succeed")
}

func (td *testData) assertRequestCleared() {
	testutil.AssertDoesReceive(td.ctx, td.t, td.clearedRequests, "should clear request")
}

func (td *testData) assertOnlyCompleteProcessingWithFailure() {
	var lastRequest completedRequest
	testutil.AssertReceive(td.ctx, td.t, td.completedRequestChan, &lastRequest, "should complete request")
	require.True(td.t, gsmsg.IsTerminalFailureCode(lastRequest.result), "should terminate with failure")
}

func (td *testData) assertRequestDoesNotCompleteWhilePaused() {
	timer := time.NewTimer(100 * time.Millisecond)
	testutil.AssertDoesReceiveFirst(td.t, timer.C, "should not complete request while paused", td.completedRequestChan)
}

func (td *testData) assertReceiveExtensionResponse() {
	var receivedExtension sentExtension
	testutil.AssertReceive(td.ctx, td.t, td.sentExtensions, &receivedExtension, "should send extension response")
	require.Equal(td.t, td.extensionResponse, receivedExtension.extension, "incorrect extension response sent")
}

func (td *testData) verifyNResponsesOnlyProcessing(blockCount int) {
	for i := 0; i < blockCount; i++ {
		testutil.AssertDoesReceive(td.ctx, td.t, td.sentResponses, "should sent block")
	}
	testutil.AssertChannelEmpty(td.t, td.sentResponses, "should not send more blocks")
}

func (td *testData) verifyNResponses(blockCount int) {
	td.verifyNResponsesOnlyProcessing(blockCount)
	td.notifyBlockSendsSent()
	for i := 0; i < blockCount; i++ {
		testutil.AssertDoesReceive(td.ctx, td.t, td.blockSends, "should sent block")
	}
	testutil.AssertChannelEmpty(td.t, td.blockSends, "should not send more blocks")
}

func (td *testData) assertDedupKey(key string) {
	var dedupKey string
	testutil.AssertReceive(td.ctx, td.t, td.dedupKeys, &dedupKey, "should dedup by key")
	require.Equal(td.t, key, dedupKey)
}

func (td *testData) assertNoBlocks() {
	testutil.AssertDoesReceive(td.ctx, td.t, td.noBlocks, "should ignore sending all blocks")
}

func (td *testData) assertIgnoredCids(set *cid.Set) {
	var lastLinks []ipld.Link
	testutil.AssertReceive(td.ctx, td.t, td.ignoredLinks, &lastLinks, "should send ignored links")
	require.Len(td.t, lastLinks, set.Len())
	for _, link := range lastLinks {
		require.True(td.t, set.Has(link.(cidlink.Link).Cid))
	}
}

func (td *testData) notifyStatusMessagesSent() {
	td.notifeePublisher.PublishMatchingEvents(func(data notifications.TopicData) bool {
		_, isSn := data.(graphsync.ResponseStatusCode)
		return isSn
	}, []notifications.Event{messagequeue.Event{Name: messagequeue.Sent}})
}

func (td *testData) notifyBlockSendsSent() {
	td.notifeePublisher.PublishMatchingEvents(func(data notifications.TopicData) bool {
		_, isBsn := data.(graphsync.BlockData)
		return isBsn
	}, []notifications.Event{messagequeue.Event{Name: messagequeue.Sent}})
}

func (td *testData) notifyStatusMessagesNetworkError(err error) {
	td.notifeePublisher.PublishMatchingEvents(func(data notifications.TopicData) bool {
		_, isSn := data.(graphsync.ResponseStatusCode)
		return isSn
	}, []notifications.Event{messagequeue.Event{Name: messagequeue.Error, Err: err}})
}

func (td *testData) notifyBlockSendsNetworkError(err error) {
	td.notifeePublisher.PublishMatchingEvents(func(data notifications.TopicData) bool {
		_, isBsn := data.(graphsync.BlockData)
		return isBsn
	}, []notifications.Event{messagequeue.Event{Name: messagequeue.Error, Err: err}})
}

func (td *testData) assertNoCompletedResponseStatuses() {
	testutil.AssertChannelEmpty(td.t, td.completedResponseStatuses, "should not send a complete notification")
}

func (td *testData) assertNetworkErrors(err error, count int) {
	for i := 0; i < count; i++ {
		td.assertHasNetworkErrors(err)
	}
	testutil.AssertChannelEmpty(td.t, td.networkErrorChan, "should not send more blocks")
}

func (td *testData) assertHasNetworkErrors(err error) {
	var receivedErr error
	testutil.AssertReceive(td.ctx, td.t, td.networkErrorChan, &receivedErr, "should sent block")
	require.EqualError(td.t, receivedErr, err.Error())
}
