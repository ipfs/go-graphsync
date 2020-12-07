package hooks_test

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/requestmanager/hooks"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestRequestHookProcessing(t *testing.T) {
	fakeChooser := func(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
		return basicnode.Prototype.Any, nil
	}
	extensionData := testutil.RandomBytes(100)
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionData,
	}

	root := testutil.GenerateCids(1)[0]
	requestID := graphsync.RequestID(rand.Int31())
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	request := gsmsg.NewRequest(requestID, root, ssb.Matcher().Node(), graphsync.Priority(0), extension)
	p := testutil.GeneratePeers(1)[0]
	testCases := map[string]struct {
		configure func(t *testing.T, hooks *hooks.OutgoingRequestHooks)
		assert    func(t *testing.T, result hooks.RequestResult)
	}{
		"no hooks": {
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.Nil(t, result.CustomChooser)
				require.Empty(t, result.PersistenceOption)
			},
		},
		"hooks alter chooser": {
			configure: func(t *testing.T, hooks *hooks.OutgoingRequestHooks) {
				hooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
					if _, found := requestData.Extension(extensionName); found {
						hookActions.UseLinkTargetNodePrototypeChooser(fakeChooser)
					}
				})
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.NotNil(t, result.CustomChooser)
				require.Empty(t, result.PersistenceOption)
			},
		},
		"hooks alter persistence option": {
			configure: func(t *testing.T, hooks *hooks.OutgoingRequestHooks) {
				hooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
					if _, found := requestData.Extension(extensionName); found {
						hookActions.UsePersistenceOption("chainstore")
					}
				})
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.Nil(t, result.CustomChooser)
				require.Equal(t, "chainstore", result.PersistenceOption)
			},
		},
		"hooks unregistered": {
			configure: func(t *testing.T, hooks *hooks.OutgoingRequestHooks) {
				unregister := hooks.Register(func(p peer.ID, requestData graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
					if _, found := requestData.Extension(extensionName); found {
						hookActions.UsePersistenceOption("chainstore")
					}
				})
				unregister()
			},
			assert: func(t *testing.T, result hooks.RequestResult) {
				require.Nil(t, result.CustomChooser)
				require.Empty(t, result.PersistenceOption)
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			hooks := hooks.NewRequestHooks()
			if data.configure != nil {
				data.configure(t, hooks)
			}
			result := hooks.ProcessRequestHooks(p, request)
			if data.assert != nil {
				data.assert(t, result)
			}
		})
	}
}

func TestBlockHookProcessing(t *testing.T) {

	extensionResponseData := testutil.RandomBytes(100)
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extensionResponse := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionResponseData,
	}
	extensionUpdateData := testutil.RandomBytes(100)
	extensionUpdate := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionUpdateData,
	}
	requestID := graphsync.RequestID(rand.Int31())
	response := gsmsg.NewResponse(requestID, graphsync.PartialResponse, extensionResponse)

	p := testutil.GeneratePeers(1)[0]
	blockData := testutil.NewFakeBlockData()

	testCases := map[string]struct {
		configure func(t *testing.T, hooks *hooks.IncomingBlockHooks)
		assert    func(t *testing.T, result hooks.UpdateResult)
	}{
		"no hooks": {
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Empty(t, result.Extensions)
				require.NoError(t, result.Err)
			},
		},
		"short circuit on error": {
			configure: func(t *testing.T, hooks *hooks.IncomingBlockHooks) {
				hooks.Register(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
					hookActions.TerminateWithError(errors.New("something went wrong"))
				})
				hooks.Register(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
					hookActions.UpdateRequestWithExtensions(extensionUpdate)
				})
			},
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Empty(t, result.Extensions)
				require.EqualError(t, result.Err, "something went wrong")
			},
		},
		"pause request": {
			configure: func(t *testing.T, hooks *hooks.IncomingBlockHooks) {
				hooks.Register(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
					hookActions.PauseRequest()
				})
			},
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Empty(t, result.Extensions)
				require.EqualError(t, result.Err, hooks.ErrPaused{}.Error())
			},
		},
		"hooks update with extensions": {
			configure: func(t *testing.T, hooks *hooks.IncomingBlockHooks) {
				hooks.Register(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
					if _, found := responseData.Extension(extensionName); found {
						hookActions.UpdateRequestWithExtensions(extensionUpdate)
					}
				})
			},
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Len(t, result.Extensions, 1)
				require.Equal(t, extensionUpdate, result.Extensions[0])
				require.NoError(t, result.Err)
			},
		},
		"hooks unregistered": {
			configure: func(t *testing.T, hooks *hooks.IncomingBlockHooks) {
				unregister := hooks.Register(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
					if _, found := responseData.Extension(extensionName); found {
						hookActions.UpdateRequestWithExtensions(extensionUpdate)
					}
				})
				unregister()
			},
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Empty(t, result.Extensions)
				require.NoError(t, result.Err)
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			hooks := hooks.NewBlockHooks()
			if data.configure != nil {
				data.configure(t, hooks)
			}
			result := hooks.ProcessBlockHooks(p, response, blockData)
			if data.assert != nil {
				data.assert(t, result)
			}
		})
	}
}

func TestResponseHookProcessing(t *testing.T) {

	extensionResponseData := testutil.RandomBytes(100)
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extensionResponse := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionResponseData,
	}
	extensionUpdateData := testutil.RandomBytes(100)
	extensionUpdate := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionUpdateData,
	}
	requestID := graphsync.RequestID(rand.Int31())
	response := gsmsg.NewResponse(requestID, graphsync.PartialResponse, extensionResponse)

	p := testutil.GeneratePeers(1)[0]
	testCases := map[string]struct {
		configure func(t *testing.T, hooks *hooks.IncomingResponseHooks)
		assert    func(t *testing.T, result hooks.UpdateResult)
	}{
		"no hooks": {
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Empty(t, result.Extensions)
				require.NoError(t, result.Err)
			},
		},
		"short circuit on error": {
			configure: func(t *testing.T, hooks *hooks.IncomingResponseHooks) {
				hooks.Register(func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
					hookActions.TerminateWithError(errors.New("something went wrong"))
				})
				hooks.Register(func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
					hookActions.UpdateRequestWithExtensions(extensionUpdate)
				})
			},
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Empty(t, result.Extensions)
				require.EqualError(t, result.Err, "something went wrong")
			},
		},
		"hooks update with extensions": {
			configure: func(t *testing.T, hooks *hooks.IncomingResponseHooks) {
				hooks.Register(func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
					if _, found := responseData.Extension(extensionName); found {
						hookActions.UpdateRequestWithExtensions(extensionUpdate)
					}
				})
			},
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Len(t, result.Extensions, 1)
				require.Equal(t, extensionUpdate, result.Extensions[0])
				require.NoError(t, result.Err)
			},
		},
		"hooks unregistered": {
			configure: func(t *testing.T, hooks *hooks.IncomingResponseHooks) {
				unregister := hooks.Register(func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
					if _, found := responseData.Extension(extensionName); found {
						hookActions.UpdateRequestWithExtensions(extensionUpdate)
					}
				})
				unregister()
			},
			assert: func(t *testing.T, result hooks.UpdateResult) {
				require.Empty(t, result.Extensions)
				require.NoError(t, result.Err)
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			hooks := hooks.NewResponseHooks()
			if data.configure != nil {
				data.configure(t, hooks)
			}
			result := hooks.ProcessResponseHooks(p, response)
			if data.assert != nil {
				data.assert(t, result)
			}
		})
	}
}
