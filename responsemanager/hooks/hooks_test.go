package hooks_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/testutil"
)

type fakePersistenceOptions struct {
	po map[string]ipld.LinkSystem
}

func (fpo *fakePersistenceOptions) GetLinkSystem(name string) (ipld.LinkSystem, bool) {
	loader, ok := fpo.po[name]
	return loader, ok
}

type contextKey struct{}

func TestRequestHookProcessing(t *testing.T) {
	fakeChooser := func(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
		return basicnode.Prototype.Any, nil
	}
	fakeSystem := cidlink.DefaultLinkSystem()
	fakeSystem.StorageReadOpener = func(lnkCtx ipld.LinkContext, link ipld.Link) (io.Reader, error) {
		return nil, nil
	}
	fpo := &fakePersistenceOptions{
		po: map[string]ipld.LinkSystem{
			"chainstore": fakeSystem,
		},
	}
	extensionData := basicnode.NewBytes(testutil.RandomBytes(100))
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionData,
	}
	extensionResponseData := basicnode.NewBytes(testutil.RandomBytes(100))
	extensionResponse := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionResponseData,
	}

	root := testutil.GenerateCids(1)[0]
	requestID := graphsync.NewRequestID()
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	request := gsmsg.NewRequest(requestID, root, ssb.Matcher().Node(), graphsync.Priority(0), extension)
	p := testutil.GeneratePeers(1)[0]
	ctx := context.Background()
	testCases := map[string]struct {
		configure func(t *testing.T, requestHooks *hooks.IncomingRequestHooks)
		assert    func(t *testing.T, result hooks.RequestResult)
	}{
		"no hooks": {
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.False(t, result.IsValidated)
				require.Empty(t, result.Extensions)
				require.Nil(t, result.CustomChooser)
				require.Nil(t, result.CustomLinkSystem.StorageReadOpener)
				require.NoError(t, result.Err)
			},
		},
		"sending extension data, no validation": {
			configure: func(t *testing.T, requestHooks *hooks.IncomingRequestHooks) {
				requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.SendExtensionData(extensionResponse)
				})
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.False(t, result.IsValidated)
				require.Len(t, result.Extensions, 1)
				require.Contains(t, result.Extensions, extensionResponse)
				require.Nil(t, result.CustomChooser)
				require.Nil(t, result.CustomLinkSystem.StorageReadOpener)
				require.NoError(t, result.Err)
			},
		},
		"sending extension data, with validation": {
			configure: func(t *testing.T, requestHooks *hooks.IncomingRequestHooks) {
				requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
					hookActions.SendExtensionData(extensionResponse)
				})
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.True(t, result.IsValidated)
				require.Len(t, result.Extensions, 1)
				require.Contains(t, result.Extensions, extensionResponse)
				require.Nil(t, result.CustomChooser)
				require.Nil(t, result.CustomLinkSystem.StorageReadOpener)
				require.NoError(t, result.Err)
			},
		},
		"short circuit on error": {
			configure: func(t *testing.T, requestHooks *hooks.IncomingRequestHooks) {
				requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.TerminateWithError(errors.New("something went wrong"))
				})
				requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
					hookActions.SendExtensionData(extensionResponse)
				})
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.False(t, result.IsValidated)
				require.Empty(t, result.Extensions)
				require.Nil(t, result.CustomChooser)
				require.Nil(t, result.CustomLinkSystem.StorageReadOpener)
				require.EqualError(t, result.Err, "something went wrong")
			},
		},
		"hooks unregistered": {
			configure: func(t *testing.T, requestHooks *hooks.IncomingRequestHooks) {
				unregister := requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.ValidateRequest()
					hookActions.SendExtensionData(extensionResponse)
				})
				unregister()
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.False(t, result.IsValidated)
				require.Empty(t, result.Extensions)
				require.Nil(t, result.CustomChooser)
				require.Nil(t, result.CustomLinkSystem.StorageReadOpener)
				require.NoError(t, result.Err)
			},
		},
		"hooks alter the loader": {
			configure: func(t *testing.T, requestHooks *hooks.IncomingRequestHooks) {
				requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					if _, found := requestData.Extension(extensionName); found {
						hookActions.UsePersistenceOption("chainstore")
						hookActions.SendExtensionData(extensionResponse)
					}
				})
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.False(t, result.IsValidated)
				require.Len(t, result.Extensions, 1)
				require.Contains(t, result.Extensions, extensionResponse)
				require.Nil(t, result.CustomChooser)
				require.NotNil(t, result.CustomLinkSystem.StorageReadOpener)
				require.NoError(t, result.Err)
			},
		},
		"hooks alter to non-existent loader": {
			configure: func(t *testing.T, requestHooks *hooks.IncomingRequestHooks) {
				requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					if _, found := requestData.Extension(extensionName); found {
						hookActions.UsePersistenceOption("applesauce")
						hookActions.SendExtensionData(extensionResponse)
					}
				})
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.False(t, result.IsValidated)
				require.Len(t, result.Extensions, 1)
				require.Contains(t, result.Extensions, extensionResponse)
				require.Nil(t, result.CustomChooser)
				require.Nil(t, result.CustomLinkSystem.StorageReadOpener)
				require.EqualError(t, result.Err, "unknown loader option")
			},
		},
		"hooks alter the node builder chooser": {
			configure: func(t *testing.T, requestHooks *hooks.IncomingRequestHooks) {
				requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.PauseResponse()
					hookActions.ValidateRequest()
				})
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.True(t, result.IsValidated)
				require.True(t, result.IsPaused)
				require.NoError(t, result.Err)
			},
		},
		"hooks start request paused": {
			configure: func(t *testing.T, requestHooks *hooks.IncomingRequestHooks) {
				requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					if _, found := requestData.Extension(extensionName); found {
						hookActions.UseLinkTargetNodePrototypeChooser(fakeChooser)
						hookActions.SendExtensionData(extensionResponse)
					}
				})
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.False(t, result.IsValidated)
				require.Len(t, result.Extensions, 1)
				require.Contains(t, result.Extensions, extensionResponse)
				require.NotNil(t, result.CustomChooser)
				require.Nil(t, result.CustomLinkSystem.StorageReadOpener)
				require.NoError(t, result.Err)
			},
		},
		"altering context": {
			configure: func(t *testing.T, requestHooks *hooks.IncomingRequestHooks) {
				requestHooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
					hookActions.AugmentContext(func(ctx context.Context) context.Context {
						return context.WithValue(ctx, contextKey{}, "apples")
					})
				})
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.Equal(t, "apples", result.Ctx.Value(contextKey{}))
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			requestHooks := hooks.NewRequestHooks(fpo)
			if data.configure != nil {
				data.configure(t, requestHooks)
			}
			result := requestHooks.ProcessRequestHooks(p, request, ctx)
			if data.assert != nil {
				data.assert(t, result)
			}
		})
	}
}

func TestBlockHookProcessing(t *testing.T) {
	extensionData := basicnode.NewBytes(testutil.RandomBytes(100))
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionData,
	}
	extensionResponseData := basicnode.NewBytes(testutil.RandomBytes(100))
	extensionResponse := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionResponseData,
	}

	root := testutil.GenerateCids(1)[0]
	requestID := graphsync.NewRequestID()
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	request := gsmsg.NewRequest(requestID, root, ssb.Matcher().Node(), graphsync.Priority(0), extension)
	p := testutil.GeneratePeers(1)[0]
	blockData := testutil.NewFakeBlockData()
	testCases := map[string]struct {
		configure func(t *testing.T, blockHooks *hooks.OutgoingBlockHooks)
		assert    func(t *testing.T, result hooks.BlockResult)
	}{
		"no hooks": {
			assert: func(t *testing.T, result hooks.BlockResult) {
				require.Empty(t, result.Extensions)
				require.NoError(t, result.Err)
			},
		},
		"send extension data": {
			configure: func(t *testing.T, blockHooks *hooks.OutgoingBlockHooks) {
				blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					hookActions.SendExtensionData(extensionResponse)
				})
			},
			assert: func(t *testing.T, result hooks.BlockResult) {
				require.Len(t, result.Extensions, 1)
				require.Contains(t, result.Extensions, extensionResponse)
				require.NoError(t, result.Err)
			},
		},
		"terminate with error": {
			configure: func(t *testing.T, blockHooks *hooks.OutgoingBlockHooks) {
				blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					hookActions.TerminateWithError(errors.New("failed"))
				})
			},
			assert: func(t *testing.T, result hooks.BlockResult) {
				require.Empty(t, result.Extensions)
				require.EqualError(t, result.Err, "failed")
			},
		},
		"pause response": {
			configure: func(t *testing.T, blockHooks *hooks.OutgoingBlockHooks) {
				blockHooks.Register(func(p peer.ID, requestData graphsync.RequestData, blockData graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
					hookActions.PauseResponse()
				})
			},
			assert: func(t *testing.T, result hooks.BlockResult) {
				require.Empty(t, result.Extensions)
				require.EqualError(t, result.Err, hooks.ErrPaused{}.Error())
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			blockHooks := hooks.NewBlockHooks()
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

func TestUpdateHookProcessing(t *testing.T) {
	extensionData := basicnode.NewBytes(testutil.RandomBytes(100))
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionData,
	}
	extensionUpdateData := basicnode.NewBytes(testutil.RandomBytes(100))
	extensionUpdate := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionUpdateData,
	}
	extensionResponseData := basicnode.NewBytes(testutil.RandomBytes(100))
	extensionResponse := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionResponseData,
	}

	root := testutil.GenerateCids(1)[0]
	requestID := graphsync.NewRequestID()
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	request := gsmsg.NewRequest(requestID, root, ssb.Matcher().Node(), graphsync.Priority(0), extension)
	update := gsmsg.NewUpdateRequest(requestID, extensionUpdate)
	p := testutil.GeneratePeers(1)[0]
	testCases := map[string]struct {
		configure func(t *testing.T, updateHooks *hooks.RequestUpdatedHooks)
		assert    func(t *testing.T, result hooks.UpdateResult)
	}{
		"no hooks": {
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Empty(t, result.Extensions)
				require.NoError(t, result.Err)
				require.False(t, result.Unpause)
			},
		},
		"send extension data": {
			configure: func(t *testing.T, updateHooks *hooks.RequestUpdatedHooks) {
				updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					_, found := requestData.Extension(extensionName)
					_, updateFound := updateData.Extension(extensionName)
					if found && updateFound {
						hookActions.SendExtensionData(extensionResponse)
					}
				})
			},
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Len(t, result.Extensions, 1)
				require.Contains(t, result.Extensions, extensionResponse)
				require.NoError(t, result.Err)
				require.False(t, result.Unpause)

			},
		},
		"terminate with error": {
			configure: func(t *testing.T, updateHooks *hooks.RequestUpdatedHooks) {
				updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					hookActions.TerminateWithError(errors.New("failed"))
				})
			},
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Empty(t, result.Extensions)
				require.EqualError(t, result.Err, "failed")
				require.False(t, result.Unpause)

			},
		},
		"unpause response": {
			configure: func(t *testing.T, updateHooks *hooks.RequestUpdatedHooks) {
				updateHooks.Register(func(p peer.ID, requestData graphsync.RequestData, updateData graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
					hookActions.UnpauseResponse()
				})
			},
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Empty(t, result.Extensions)
				require.NoError(t, result.Err)
				require.True(t, result.Unpause)
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			updateHooks := hooks.NewUpdateHooks()
			if data.configure != nil {
				data.configure(t, updateHooks)
			}
			result := updateHooks.ProcessUpdateHooks(p, request, update)
			if data.assert != nil {
				data.assert(t, result)
			}
		})
	}
}
