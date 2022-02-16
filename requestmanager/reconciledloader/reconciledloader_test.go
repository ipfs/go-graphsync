package reconciledloader_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/requestmanager/reconciledloader"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestReconciledLoader(t *testing.T) {
	ctx := context.Background()
	testBCStorage := make(map[datamodel.Link][]byte)
	bcLinkSys := testutil.NewTestStore(testBCStorage)
	testChain := testutil.SetupBlockChain(ctx, t, bcLinkSys, 100, 100)
	testTree := testutil.NewTestIPLDTree()
	testCases := map[string]struct {
		root                cid.Cid
		baseStore           map[datamodel.Link][]byte
		presentRemoteBlocks []blocks.Block
		presentLocalBlocks  []blocks.Block
		remoteSeq           []message.GraphSyncLinkMetadatum
		steps               []step
	}{
		"load entirely from local store": {
			root:               testChain.TipLink.(cidlink.Link).Cid,
			baseStore:          testBCStorage,
			presentLocalBlocks: testChain.AllBlocks(),
			steps:              syncLoadRange(testChain, 0, 100, true),
		},
		"load entirely from remote store": {
			root:                testChain.TipLink.(cidlink.Link).Cid,
			baseStore:           testBCStorage,
			presentRemoteBlocks: testChain.AllBlocks(),
			remoteSeq:           metadataRange(testChain, 0, 100, false),
			steps: append([]step{
				goOnline{},
				injest{metadataStart: 0, metadataEnd: 100},
			}, syncLoadRange(testChain, 0, 100, false)...),
		},
		"load from local store, then go online": {
			root:                testChain.TipLink.(cidlink.Link).Cid,
			baseStore:           testBCStorage,
			presentLocalBlocks:  testChain.Blocks(0, 50),
			presentRemoteBlocks: testChain.Blocks(50, 100),
			remoteSeq:           metadataRange(testChain, 0, 100, false),
			steps: append(append(
				// load first 50 locally
				syncLoadRange(testChain, 0, 50, true),
				[]step{
					// should fail next because it's not stored locally
					syncLoad{
						loadSeq:        50,
						expectedResult: types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{Link: testChain.LinkTipIndex(50), Path: testChain.PathTipIndex(50)}},
					},
					// go online
					goOnline{},
					// retry now that we're online -- note this won't return until we injest responses
					asyncRetry{},
					// injest responses from remote peer
					injest{
						metadataStart: 0,
						metadataEnd:   100,
					},
					// verify the retry worked
					verifyAsyncResult{
						expectedResult: types.AsyncLoadResult{Local: false, Data: testChain.Blocks(50, 51)[0].RawData()},
					},
				}...),
				// verify we can load the remaining items from the remote
				syncLoadRange(testChain, 51, 100, false)...),
		},
		"retry while offline": {
			root:               testChain.TipLink.(cidlink.Link).Cid,
			baseStore:          testBCStorage,
			presentLocalBlocks: testChain.Blocks(0, 50),
			steps: append(
				// load first 50 locally
				syncLoadRange(testChain, 0, 50, true),
				[]step{
					// should fail next because it's not stored locally
					syncLoad{
						loadSeq:        50,
						expectedResult: types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{Link: testChain.LinkTipIndex(50), Path: testChain.PathTipIndex(50)}},
					},
					retry{
						expectedResult: types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{Link: testChain.LinkTipIndex(50), Path: testChain.PathTipIndex(50)}},
					},
				}...),
		},
		"retry while online": {
			root:                testChain.TipLink.(cidlink.Link).Cid,
			baseStore:           testBCStorage,
			presentRemoteBlocks: testChain.AllBlocks(),
			remoteSeq:           metadataRange(testChain, 0, 100, false),
			steps: append(append([]step{
				goOnline{},
				injest{metadataStart: 0, metadataEnd: 100},
			},
				syncLoadRange(testChain, 0, 50, false)...),
				retry{
					expectedResult: types.AsyncLoadResult{Data: testChain.Blocks(49, 50)[0].RawData(), Local: true},
				}),
		},
		"retry online load after going offline": {
			root:                testChain.TipLink.(cidlink.Link).Cid,
			baseStore:           testBCStorage,
			presentRemoteBlocks: testChain.AllBlocks(),
			remoteSeq:           metadataRange(testChain, 0, 100, false),
			steps: append(append([]step{
				goOnline{},
				injest{metadataStart: 0, metadataEnd: 100},
			},
				syncLoadRange(testChain, 0, 50, false)...),
				goOffline{},
				retry{
					expectedResult: types.AsyncLoadResult{Data: testChain.Blocks(49, 50)[0].RawData(), Local: true},
				}),
		},
		"error reconciling local results": {
			root:                testChain.TipLink.(cidlink.Link).Cid,
			baseStore:           testBCStorage,
			presentLocalBlocks:  testChain.Blocks(0, 50),
			presentRemoteBlocks: testChain.Blocks(50, 100),
			remoteSeq: append(append(metadataRange(testChain, 0, 30, false),
				message.GraphSyncLinkMetadatum{
					Link:   testChain.LinkTipIndex(53).(cidlink.Link).Cid,
					Action: graphsync.LinkActionPresent,
				}),
				metadataRange(testChain, 31, 100, false)...),
			steps: append(
				// load first 50 locally
				syncLoadRange(testChain, 0, 50, true),
				[]step{
					// should fail next because it's not stored locally
					syncLoad{
						loadSeq:        50,
						expectedResult: types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{Link: testChain.LinkTipIndex(50), Path: testChain.PathTipIndex(50)}},
					},
					// go online
					goOnline{},
					// retry now that we're online -- note this won't return until we injest responses
					asyncRetry{},
					// injest responses from remote peer
					injest{
						metadataStart: 0,
						metadataEnd:   100,
					},
					// we should get an error cause of issues reconciling against previous local log
					verifyAsyncResult{
						expectedResult: types.AsyncLoadResult{Local: false, Err: graphsync.RemoteIncorrectResponseError{
							LocalLink:  testChain.LinkTipIndex(30),
							RemoteLink: testChain.LinkTipIndex(53),
							Path:       testChain.PathTipIndex(30),
						}},
					},
				}...),
		},
		"remote sends out of order block": {
			root:                testChain.TipLink.(cidlink.Link).Cid,
			baseStore:           testBCStorage,
			presentRemoteBlocks: testChain.AllBlocks(),
			remoteSeq: append(append(metadataRange(testChain, 0, 30, false),
				message.GraphSyncLinkMetadatum{
					Link:   testChain.LinkTipIndex(53).(cidlink.Link).Cid,
					Action: graphsync.LinkActionPresent,
				}),
				metadataRange(testChain, 31, 100, false)...),
			steps: append(append([]step{
				goOnline{},
				injest{metadataStart: 0, metadataEnd: 100},
			}, syncLoadRange(testChain, 0, 30, false)...),
				// we should get an error cause the remote sent and incorrect response
				syncLoad{
					loadSeq: 30,
					expectedResult: types.AsyncLoadResult{Local: false, Err: graphsync.RemoteIncorrectResponseError{
						LocalLink:  testChain.LinkTipIndex(30),
						RemoteLink: testChain.LinkTipIndex(53),
						Path:       testChain.PathTipIndex(30),
					}},
				},
			),
		},
		"remote missing block": {
			root:                testChain.TipLink.(cidlink.Link).Cid,
			baseStore:           testBCStorage,
			presentRemoteBlocks: testChain.AllBlocks(),
			remoteSeq: append(metadataRange(testChain, 0, 30, false),
				message.GraphSyncLinkMetadatum{
					Link:   testChain.LinkTipIndex(30).(cidlink.Link).Cid,
					Action: graphsync.LinkActionMissing,
				}),
			steps: append(append([]step{
				goOnline{},
				injest{metadataStart: 0, metadataEnd: 31},
			}, syncLoadRange(testChain, 0, 30, false)...),
				// we should get an error that we're missing a block for our response
				syncLoad{
					loadSeq: 30,
					expectedResult: types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{
						Link: testChain.LinkTipIndex(30),
						Path: testChain.PathTipIndex(30),
					}},
				},
			),
		},
		"remote missing chain that local has": {
			root:                testChain.TipLink.(cidlink.Link).Cid,
			baseStore:           testBCStorage,
			presentRemoteBlocks: testChain.AllBlocks(),
			presentLocalBlocks:  testChain.Blocks(30, 100),
			remoteSeq: append(metadataRange(testChain, 0, 30, false),
				message.GraphSyncLinkMetadatum{
					Link:   testChain.LinkTipIndex(30).(cidlink.Link).Cid,
					Action: graphsync.LinkActionMissing,
				}),
			steps: append(append(append(
				[]step{
					goOnline{},
					injest{metadataStart: 0, metadataEnd: 31},
				},
				// load the blocks the remote has
				syncLoadRange(testChain, 0, 30, false)...),
				[]step{
					// load the block the remote missing says it's missing locally
					syncLoadRange(testChain, 30, 31, true)[0],
					asyncLoad{loadSeq: 31},
					// at this point we have no more remote responses, since it's a linear chain
					verifyNoAsyncResult{},
					// we'd expect the remote would terminate here, since we've sent the last missing block
					goOffline{},
					// this will cause us to start loading locally only again
					verifyAsyncResult{
						expectedResult: types.AsyncLoadResult{Local: true, Data: testChain.Blocks(31, 32)[0].RawData()},
					},
				}...),
				syncLoadRange(testChain, 30, 100, true)...,
			),
		},
		"remote missing chain that local has partial": {
			root:                testChain.TipLink.(cidlink.Link).Cid,
			baseStore:           testBCStorage,
			presentRemoteBlocks: testChain.AllBlocks(),
			presentLocalBlocks:  testChain.Blocks(30, 50),
			remoteSeq: append(metadataRange(testChain, 0, 30, false),
				message.GraphSyncLinkMetadatum{
					Link:   testChain.LinkTipIndex(30).(cidlink.Link).Cid,
					Action: graphsync.LinkActionMissing,
				}),
			steps: append(append(append(append([]step{
				goOnline{},
				injest{metadataStart: 0, metadataEnd: 31},
			},
				// load the blocks the remote has
				syncLoadRange(testChain, 0, 30, false)...),
				[]step{
					// load the block the remote missing says it's missing locally
					syncLoadRange(testChain, 30, 31, true)[0],
					asyncLoad{loadSeq: 31},
					// at this point we have no more remote responses, since it's a linear chain
					verifyNoAsyncResult{},
					// we'd expect the remote would terminate here, since we've sent the last missing block
					goOffline{},
					// this will cause us to start loading locally only
					verifyAsyncResult{
						expectedResult: types.AsyncLoadResult{Local: true, Data: testChain.Blocks(31, 32)[0].RawData()},
					},
				}...),
				// should follow up to the end of the local chain
				syncLoadRange(testChain, 32, 50, true)...),
				// but then it should return missing
				syncLoad{
					loadSeq: 50,
					expectedResult: types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{
						Link: testChain.LinkTipIndex(50),
						Path: testChain.PathTipIndex(50),
					}},
				},
			),
		},
		"remote duplicate blocks can load from local": {
			root:      testTree.RootBlock.Cid(),
			baseStore: testTree.Storage,
			presentRemoteBlocks: []blocks.Block{
				testTree.RootBlock,
				testTree.MiddleListBlock,
				testTree.MiddleMapBlock,
				testTree.LeafAlphaBlock,
				testTree.LeafBetaBlock,
			},
			presentLocalBlocks: nil,
			remoteSeq: []message.GraphSyncLinkMetadatum{
				{Link: testTree.RootBlock.Cid(), Action: graphsync.LinkActionPresent},
				{Link: testTree.MiddleListBlock.Cid(), Action: graphsync.LinkActionPresent},
				{Link: testTree.LeafAlphaBlock.Cid(), Action: graphsync.LinkActionPresent},
				{Link: testTree.LeafAlphaBlock.Cid(), Action: graphsync.LinkActionPresent},
				{Link: testTree.LeafBetaBlock.Cid(), Action: graphsync.LinkActionPresent},
				{Link: testTree.LeafAlphaBlock.Cid(), Action: graphsync.LinkActionPresent},
				{Link: testTree.MiddleMapBlock.Cid(), Action: graphsync.LinkActionPresent},
				{Link: testTree.LeafAlphaBlock.Cid(), Action: graphsync.LinkActionPresent},
			},
			steps: []step{
				goOnline{},
				injest{metadataStart: 0, metadataEnd: 8},
				syncLoad{loadSeq: 0, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.RootBlock.RawData()}},
				syncLoad{loadSeq: 1, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.MiddleListBlock.RawData()}},
				syncLoad{loadSeq: 2, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.LeafAlphaBlock.RawData()}},
				syncLoad{loadSeq: 3, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.LeafAlphaBlock.RawData()}},
				syncLoad{loadSeq: 4, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.LeafBetaBlock.RawData()}},
				syncLoad{loadSeq: 5, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.LeafAlphaBlock.RawData()}},
				syncLoad{loadSeq: 6, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.MiddleMapBlock.RawData()}},
				syncLoad{loadSeq: 7, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.LeafAlphaBlock.RawData()}},
			},
		},
		"remote missing branch finishes to end": {
			root:      testTree.RootBlock.Cid(),
			baseStore: testTree.Storage,
			presentRemoteBlocks: []blocks.Block{
				testTree.RootBlock,
				testTree.MiddleMapBlock,
				testTree.LeafAlphaBlock,
			},
			presentLocalBlocks: nil,
			remoteSeq: []message.GraphSyncLinkMetadatum{
				{Link: testTree.RootBlock.Cid(), Action: graphsync.LinkActionPresent},
				// missing the whole list tree
				{Link: testTree.MiddleListBlock.Cid(), Action: graphsync.LinkActionMissing},
				{Link: testTree.MiddleMapBlock.Cid(), Action: graphsync.LinkActionPresent},
				{Link: testTree.LeafAlphaBlock.Cid(), Action: graphsync.LinkActionPresent},
			},
			steps: []step{
				goOnline{},
				injest{metadataStart: 0, metadataEnd: 4},
				syncLoad{loadSeq: 0, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.RootBlock.RawData()}},
				syncLoad{loadSeq: 1, expectedResult: types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{Link: testTree.MiddleListNodeLnk, Path: datamodel.ParsePath("linkedList")}}},
				syncLoad{loadSeq: 6, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.MiddleMapBlock.RawData()}},
				syncLoad{loadSeq: 7, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.LeafAlphaBlock.RawData()}},
			},
		},
		"remote missing branch with partial local": {
			root:      testTree.RootBlock.Cid(),
			baseStore: testTree.Storage,
			presentLocalBlocks: []blocks.Block{
				testTree.MiddleListBlock,
				testTree.LeafAlphaBlock,
			},
			presentRemoteBlocks: []blocks.Block{
				testTree.RootBlock,
				testTree.MiddleMapBlock,
				testTree.LeafAlphaBlock,
			},
			remoteSeq: []message.GraphSyncLinkMetadatum{
				{Link: testTree.RootBlock.Cid(), Action: graphsync.LinkActionPresent},
				// missing the whole list tree
				{Link: testTree.MiddleListBlock.Cid(), Action: graphsync.LinkActionMissing},
				{Link: testTree.MiddleMapBlock.Cid(), Action: graphsync.LinkActionPresent},
				{Link: testTree.LeafAlphaBlock.Cid(), Action: graphsync.LinkActionPresent},
			},
			steps: []step{
				goOnline{},
				injest{metadataStart: 0, metadataEnd: 4},
				syncLoad{loadSeq: 0, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.RootBlock.RawData()}},
				syncLoad{loadSeq: 1, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.MiddleListBlock.RawData()}},
				syncLoad{loadSeq: 2, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.LeafAlphaBlock.RawData()}},
				syncLoad{loadSeq: 3, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.LeafAlphaBlock.RawData()}},
				syncLoad{
					loadSeq: 4,
					expectedResult: types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{Link: testTree.LeafBetaLnk, Path: datamodel.NewPath([]datamodel.PathSegment{
						datamodel.PathSegmentOfString("linkedList"),
						datamodel.PathSegmentOfInt(2),
					})}},
				},
				syncLoad{loadSeq: 5, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.LeafAlphaBlock.RawData()}},
				syncLoad{loadSeq: 6, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.MiddleMapBlock.RawData()}},
				syncLoad{loadSeq: 7, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.LeafAlphaBlock.RawData()}},
			},
		},
		"remote missing branch during reconciliation": {
			root:      testTree.RootBlock.Cid(),
			baseStore: testTree.Storage,
			presentLocalBlocks: []blocks.Block{
				testTree.RootBlock,
				testTree.MiddleListBlock,
				testTree.LeafAlphaBlock,
			},
			presentRemoteBlocks: []blocks.Block{
				testTree.RootBlock,
				testTree.MiddleMapBlock,
				testTree.LeafAlphaBlock,
			},
			remoteSeq: []message.GraphSyncLinkMetadatum{
				{Link: testTree.RootBlock.Cid(), Action: graphsync.LinkActionPresent},
				// missing the whole list tree
				{Link: testTree.MiddleListBlock.Cid(), Action: graphsync.LinkActionMissing},
				{Link: testTree.MiddleMapBlock.Cid(), Action: graphsync.LinkActionPresent},
				{Link: testTree.LeafAlphaBlock.Cid(), Action: graphsync.LinkActionPresent},
			},
			steps: []step{
				syncLoad{loadSeq: 0, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.RootBlock.RawData()}},
				syncLoad{loadSeq: 1, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.MiddleListBlock.RawData()}},
				syncLoad{loadSeq: 2, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.LeafAlphaBlock.RawData()}},
				syncLoad{loadSeq: 3, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.LeafAlphaBlock.RawData()}},
				// here we have an offline load that is missing the local beta block
				syncLoad{
					loadSeq: 4,
					expectedResult: types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{Link: testTree.LeafBetaLnk, Path: datamodel.NewPath([]datamodel.PathSegment{
						datamodel.PathSegmentOfString("linkedList"),
						datamodel.PathSegmentOfInt(2),
					})}},
				},
				goOnline{},
				injest{metadataStart: 0, metadataEnd: 4},
				// what we want to verify here is that when we retry loading, the reconciliation still works,
				// even though the remote is missing a brnach that's farther up the tree
				retry{
					expectedResult: types.AsyncLoadResult{Local: true, Err: graphsync.RemoteMissingBlockErr{Link: testTree.LeafBetaLnk, Path: datamodel.NewPath([]datamodel.PathSegment{
						datamodel.PathSegmentOfString("linkedList"),
						datamodel.PathSegmentOfInt(2),
					})}},
				},
				syncLoad{loadSeq: 5, expectedResult: types.AsyncLoadResult{Local: true, Data: testTree.LeafAlphaBlock.RawData()}},
				syncLoad{loadSeq: 6, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.MiddleMapBlock.RawData()}},
				syncLoad{loadSeq: 7, expectedResult: types.AsyncLoadResult{Local: false, Data: testTree.LeafAlphaBlock.RawData()}},
			},
		},
	}

	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			localStorage := make(map[datamodel.Link][]byte)
			for _, lb := range data.presentLocalBlocks {
				localStorage[cidlink.Link{Cid: lb.Cid()}] = lb.RawData()
			}
			localLsys := testutil.NewTestStore(localStorage)
			requestID := graphsync.NewRequestID()

			remoteStorage := make(map[cid.Cid][]byte)
			for _, rb := range data.presentRemoteBlocks {
				remoteStorage[rb.Cid()] = rb.RawData()
			}

			// collect sequence of an explore all
			var loadSeq []loadRequest
			traverser := ipldutil.TraversalBuilder{
				Root:     cidlink.Link{Cid: data.root},
				Selector: selectorparse.CommonSelector_ExploreAllRecursively,
			}.Start(ctx)
			for {
				isComplete, err := traverser.IsComplete()
				require.NoError(t, err)
				if isComplete {
					break
				}
				lnk, linkCtx := traverser.CurrentRequest()
				loadSeq = append(loadSeq, loadRequest{linkCtx: linkCtx, link: lnk})
				traverser.Advance(bytes.NewReader(data.baseStore[lnk]))
			}
			ts := &testState{
				ctx:          ctx,
				remoteBlocks: remoteStorage,
				remoteSeq:    data.remoteSeq,
				loadSeq:      loadSeq,
				asyncLoad:    nil,
			}

			rl := reconciledloader.NewReconciledLoader(requestID, &localLsys)
			for _, step := range data.steps {
				step.execute(t, ts, rl)
			}
		})
	}
}

type loadRequest struct {
	linkCtx ipld.LinkContext
	link    ipld.Link
}

type testState struct {
	ctx          context.Context
	remoteBlocks map[cid.Cid][]byte
	remoteSeq    []message.GraphSyncLinkMetadatum
	loadSeq      []loadRequest
	asyncLoad    <-chan types.AsyncLoadResult
}

type step interface {
	execute(t *testing.T, ts *testState, rl *reconciledloader.ReconciledLoader)
}

type goOffline struct{}

func (goOffline) execute(t *testing.T, ts *testState, rl *reconciledloader.ReconciledLoader) {
	rl.SetRemoteOnline(false)
}

type goOnline struct{}

func (goOnline) execute(t *testing.T, ts *testState, rl *reconciledloader.ReconciledLoader) {
	rl.SetRemoteOnline(true)
}

type syncLoad struct {
	loadSeq        int
	expectedResult types.AsyncLoadResult
}

func (s syncLoad) execute(t *testing.T, ts *testState, rl *reconciledloader.ReconciledLoader) {
	require.Nil(t, ts.asyncLoad)
	result := rl.BlockReadOpener(ts.loadSeq[s.loadSeq].linkCtx, ts.loadSeq[s.loadSeq].link)
	require.Equal(t, s.expectedResult, result)
}

type retry struct {
	expectedResult types.AsyncLoadResult
}

func (s retry) execute(t *testing.T, ts *testState, rl *reconciledloader.ReconciledLoader) {
	require.Nil(t, ts.asyncLoad)
	result := rl.RetryLastLoad()
	require.Equal(t, s.expectedResult, result)
}

type asyncLoad struct {
	loadSeq int
}

func (s asyncLoad) execute(t *testing.T, ts *testState, rl *reconciledloader.ReconciledLoader) {
	require.Nil(t, ts.asyncLoad)
	asyncLoad := make(chan types.AsyncLoadResult, 1)
	ts.asyncLoad = asyncLoad
	go func() {
		result := rl.BlockReadOpener(ts.loadSeq[s.loadSeq].linkCtx, ts.loadSeq[s.loadSeq].link)
		asyncLoad <- result
	}()
}

type asyncRetry struct {
}

func (s asyncRetry) execute(t *testing.T, ts *testState, rl *reconciledloader.ReconciledLoader) {
	require.Nil(t, ts.asyncLoad)
	asyncLoad := make(chan types.AsyncLoadResult, 1)
	ts.asyncLoad = asyncLoad
	go func() {
		result := rl.RetryLastLoad()
		asyncLoad <- result
	}()
}

type verifyNoAsyncResult struct{}

func (verifyNoAsyncResult) execute(t *testing.T, ts *testState, rl *reconciledloader.ReconciledLoader) {
	require.NotNil(t, ts.asyncLoad)
	time.Sleep(20 * time.Millisecond)
	select {
	case <-ts.asyncLoad:
		require.FailNow(t, "should have no async load result but does")
	default:
	}
}

type verifyAsyncResult struct {
	expectedResult types.AsyncLoadResult
}

func (v verifyAsyncResult) execute(t *testing.T, ts *testState, rl *reconciledloader.ReconciledLoader) {
	require.NotNil(t, ts.asyncLoad)
	select {
	case <-ts.ctx.Done():
		require.FailNow(t, "expected async load but failed")
	case result := <-ts.asyncLoad:
		ts.asyncLoad = nil
		require.Equal(t, v.expectedResult, result)
	}
}

type injest struct {
	metadataStart int
	metadataEnd   int
	traceLink     trace.Link
}

func (i injest) execute(t *testing.T, ts *testState, rl *reconciledloader.ReconciledLoader) {
	linkMetadata := ts.remoteSeq[i.metadataStart:i.metadataEnd]
	rl.IngestResponse(message.NewLinkMetadata(linkMetadata), i.traceLink, ts.remoteBlocks)
	// simulate no dub blocks
	for _, lmd := range linkMetadata {
		delete(ts.remoteBlocks, lmd.Link)
	}
}

func syncLoadRange(tbc *testutil.TestBlockChain, from int, to int, local bool) []step {
	blocks := tbc.Blocks(from, to)
	steps := make([]step, 0, len(blocks))
	for i := from; i < to; i++ {
		steps = append(steps, syncLoad{i, types.AsyncLoadResult{Data: blocks[i-from].RawData(), Local: local}})
	}
	return steps
}

func metadataRange(tbc *testutil.TestBlockChain, from int, to int, missing bool) []message.GraphSyncLinkMetadatum {
	tm := make([]message.GraphSyncLinkMetadatum, 0, to-from)
	for i := from; i < to; i++ {
		action := graphsync.LinkActionPresent
		if missing {
			action = graphsync.LinkActionMissing
		}
		tm = append(tm, message.GraphSyncLinkMetadatum{Link: tbc.LinkTipIndex(i).(cidlink.Link).Cid, Action: action})
	}
	return tm
}
