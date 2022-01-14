package impl_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	dss "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipldformat "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channelmonitor"
	"github.com/filecoin-project/go-data-transfer/encoding"
	. "github.com/filecoin-project/go-data-transfer/impl"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/network"
	"github.com/filecoin-project/go-data-transfer/testutil"
	tp "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/go-data-transfer/transport/graphsync/extension"
)

const loremFile = "lorem.txt"
const loremFileTransferBytes = 20439

const loremLargeFile = "lorem_large.txt"
const loremLargeFileTransferBytes = 217452

// nil means use the default protocols
// tests data transfer for the following protocol combinations:
// default protocol -> default protocols
// old protocol -> default protocols
// default protocols -> old protocol
var protocolsForTest = map[string]struct {
	host1Protocols []protocol.ID
	host2Protocols []protocol.ID
}{
	"(v1.2 -> v1.2)": {nil, nil},
	"(v1.0 -> v1.2)": {[]protocol.ID{datatransfer.ProtocolDataTransfer1_0}, nil},
	"(v1.2 -> v1.0)": {nil, []protocol.ID{datatransfer.ProtocolDataTransfer1_0}},
	"(v1.1 -> v1.2)": {[]protocol.ID{datatransfer.ProtocolDataTransfer1_1}, nil},
	"(v1.2 -> v1.1)": {nil, []protocol.ID{datatransfer.ProtocolDataTransfer1_1}},
}

// tests data transfer for the protocol combinations that support restart messages
var protocolsForRestartTest = map[string]struct {
	host1Protocols []protocol.ID
	host2Protocols []protocol.ID
}{
	"(v1.2 -> v1.2)": {nil, nil},
	"(v1.1 -> v1.2)": {[]protocol.ID{datatransfer.ProtocolDataTransfer1_1}, nil},
	"(v1.2 -> v1.1)": {nil, []protocol.ID{datatransfer.ProtocolDataTransfer1_1}},
}

func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	testCases := map[string]struct {
		isPull            bool
		customSourceStore bool
		customTargetStore bool
		expectedTraces    []string
	}{
		"roundtrip for push requests": {
			expectedTraces: []string{
				// initiator: send push request
				"transfer(0)->sendMessage(0)",
				// initiator: receive GS request and execute response
				"transfer(0)->response(0)->executeTask(0)",
				// initiator: receive completion message from responder that they got all the data
				"transfer(0)->receiveResponse(0)",
				// responder: receive dt request, execute graphsync request in response
				"transfer(1)->receiveRequest(0)->request(0)",
				// responder: send message indicating we received all data
				"transfer(1)->sendMessage(0)",
			},
		},
		"roundtrip for pull requests": {
			isPull: true,
			expectedTraces: []string{
				// initiator: execute outgoing graphsync request
				"transfer(0)->request(0)->executeTask(0)",
				// initiator: receive completion message from responder that they sent all the data
				"transfer(0)->receiveResponse(0)",
				// responder: receive GS request and execute response
				"transfer(1)->response(0)->executeTask(0)",
				// responder: send message indicating we sent all data
				"transfer(1)->sendMessage(0)",
			},
		},
		"custom source, push": {
			customSourceStore: true,
		},
		"custom source, pull": {
			isPull:            true,
			customSourceStore: true,
		},
		"custom dest, push": {
			customTargetStore: true,
		},
		"custom dest, pull": {
			isPull:            true,
			customTargetStore: true,
		},
		"custom both sides, push": {
			customSourceStore: true,
			customTargetStore: true,
		},
		"custom both sides, pull": {
			isPull:            true,
			customSourceStore: true,
			customTargetStore: true,
		},
	}
	for testCase, data := range testCases {
		for pname, ps := range protocolsForTest {
			t.Run(testCase+pname, func(t *testing.T) {
				ctx, collectTracing := testutil.SetupTracing(ctx)
				ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()

				gsData := testutil.NewGraphsyncTestingData(ctx, t, ps.host1Protocols, ps.host2Protocols)
				host1 := gsData.Host1 // initiator, data sender
				host2 := gsData.Host2 // data recipient

				tp1 := gsData.SetupGSTransportHost1()
				tp2 := gsData.SetupGSTransportHost2()

				dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
				require.NoError(t, err)
				testutil.StartAndWaitForReady(ctx, t, dt1)
				dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
				require.NoError(t, err)
				testutil.StartAndWaitForReady(ctx, t, dt2)

				finished := make(chan struct{}, 2)
				errChan := make(chan struct{}, 2)
				opened := make(chan struct{}, 2)
				sent := make(chan uint64, 21)
				received := make(chan uint64, 21)
				var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
					if event.Code == datatransfer.DataQueued {
						if channelState.Queued() > 0 {
							sent <- channelState.Queued()
						}
					}

					if event.Code == datatransfer.DataReceived {
						if channelState.Received() > 0 {
							received <- channelState.Received()
						}
					}

					if channelState.Status() == datatransfer.Completed {
						finished <- struct{}{}
					}
					if event.Code == datatransfer.Error {
						errChan <- struct{}{}
					}
					if event.Code == datatransfer.Open {
						opened <- struct{}{}
					}
				}
				dt1.SubscribeToEvents(subscriber)
				dt2.SubscribeToEvents(subscriber)
				voucher := testutil.FakeDTType{Data: "applesauce"}
				sv := testutil.NewStubbedValidator()

				var sourceDagService ipldformat.DAGService
				if data.customSourceStore {
					ds := dss.MutexWrap(datastore.NewMapDatastore())
					bs := bstore.NewBlockstore(namespace.Wrap(ds, datastore.NewKey("blockstore")))
					lsys := storeutil.LinkSystemForBlockstore(bs)
					sourceDagService = merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
					err := dt1.RegisterTransportConfigurer(&testutil.FakeDTType{}, func(channelID datatransfer.ChannelID, testVoucher datatransfer.Voucher, transport datatransfer.Transport) {
						fv, ok := testVoucher.(*testutil.FakeDTType)
						if ok && fv.Data == voucher.Data {
							gsTransport, ok := transport.(*tp.Transport)
							if ok {
								err := gsTransport.UseStore(channelID, lsys)
								require.NoError(t, err)
							}
						}
					})
					require.NoError(t, err)
				} else {
					sourceDagService = gsData.DagService1
				}
				root, origBytes := testutil.LoadUnixFSFile(ctx, t, sourceDagService, loremFile)
				rootCid := root.(cidlink.Link).Cid

				var destDagService ipldformat.DAGService
				if data.customTargetStore {
					ds := dss.MutexWrap(datastore.NewMapDatastore())
					bs := bstore.NewBlockstore(namespace.Wrap(ds, datastore.NewKey("blockstore")))
					lsys := storeutil.LinkSystemForBlockstore(bs)
					destDagService = merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
					err := dt2.RegisterTransportConfigurer(&testutil.FakeDTType{}, func(channelID datatransfer.ChannelID, testVoucher datatransfer.Voucher, transport datatransfer.Transport) {
						fv, ok := testVoucher.(*testutil.FakeDTType)
						if ok && fv.Data == voucher.Data {
							gsTransport, ok := transport.(*tp.Transport)
							if ok {
								err := gsTransport.UseStore(channelID, lsys)
								require.NoError(t, err)
							}
						}
					})
					require.NoError(t, err)
				} else {
					destDagService = gsData.DagService2
				}

				var chid datatransfer.ChannelID
				if data.isPull {
					sv.ExpectSuccessPull()
					require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))
					chid, err = dt2.OpenPullDataChannel(ctx, host1.ID(), &voucher, rootCid, gsData.AllSelector)
				} else {
					sv.ExpectSuccessPush()
					require.NoError(t, dt2.RegisterVoucherType(&testutil.FakeDTType{}, sv))
					chid, err = dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, rootCid, gsData.AllSelector)
				}
				require.NoError(t, err)
				opens := 0
				completes := 0
				sentIncrements := make([]uint64, 0, 21)
				receivedIncrements := make([]uint64, 0, 21)
				for opens < 2 || completes < 2 || len(sentIncrements) < 21 || len(receivedIncrements) < 21 {
					select {
					case <-ctx.Done():
						t.Fatal("Did not complete successful data transfer")
					case <-finished:
						completes++
					case <-opened:
						opens++
					case sentIncrement := <-sent:
						sentIncrements = append(sentIncrements, sentIncrement)
					case receivedIncrement := <-received:
						receivedIncrements = append(receivedIncrements, receivedIncrement)
					case <-errChan:
						t.Fatal("received error on data transfer")
					}
				}
				require.Equal(t, sentIncrements, receivedIncrements)
				testutil.VerifyHasFile(ctx, t, destDagService, root, origBytes)
				if data.isPull {
					assert.Equal(t, chid.Initiator, host2.ID())
				} else {
					assert.Equal(t, chid.Initiator, host1.ID())
				}
				traces := collectTracing(t).TracesToStrings(3)
				for _, expectedTrace := range data.expectedTraces {
					require.Contains(t, traces, expectedTrace)
				}
			})
		}
	} //
}

func TestRoundTripMissingBlocks(t *testing.T) {
	ctx := context.Background()
	testCases := map[string]struct {
		isPull bool
	}{
		"roundtrip for push requests": {},
		"roundtrip for pull requests": {
			isPull: true,
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
			host1 := gsData.Host1 // initiator, data sender
			host2 := gsData.Host2 // data recipient

			tp1 := gsData.SetupGSTransportHost1()
			tp2 := gsData.SetupGSTransportHost2()

			dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt1)
			dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt2)

			finished := make(chan struct{}, 2)
			errChan := make(chan struct{}, 2)
			opened := make(chan struct{}, 2)
			sent := make(chan uint64, 21)
			received := make(chan uint64, 21)
			var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if event.Code == datatransfer.DataQueued {
					if channelState.Queued() > 0 {
						sent <- channelState.Queued()
					}
				}

				if event.Code == datatransfer.DataReceived {
					if channelState.Received() > 0 {
						received <- channelState.Received()
					}
				}

				if channelState.Status() == datatransfer.Completed || channelState.Status() == datatransfer.PartiallyCompleted {
					finished <- struct{}{}
				}
				if event.Code == datatransfer.Error {
					fmt.Println(channelState.Message())
					errChan <- struct{}{}
				}
				if event.Code == datatransfer.Open {
					opened <- struct{}{}
				}
			}
			dt1.SubscribeToEvents(subscriber)
			dt2.SubscribeToEvents(subscriber)
			voucher := testutil.FakeDTType{Data: "applesauce"}
			sv := testutil.NewStubbedValidator()

			partialTree := testutil.NewPartialTree(t, gsData.Bs1)
			var chid datatransfer.ChannelID
			if data.isPull {
				sv.ExpectSuccessPull()
				require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				chid, err = dt2.OpenPullDataChannel(ctx, host1.ID(), &voucher, partialTree.PresentRootLink.(cidlink.Link).Cid, gsData.AllSelector)
			} else {
				sv.ExpectSuccessPush()
				require.NoError(t, dt2.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				chid, err = dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, partialTree.PresentRootLink.(cidlink.Link).Cid, gsData.AllSelector)
			}
			require.NoError(t, err)
			opens := 0
			completes := 0
			sentIncrements := make([]uint64, 0, 21)
			receivedIncrements := make([]uint64, 0, 21)
			for opens < 2 || completes < 2 {
				select {
				case <-ctx.Done():
					t.Fatal("Did not complete successful data transfer")
				case <-finished:
					completes++
				case <-opened:
					opens++
				case sentIncrement := <-sent:
					sentIncrements = append(sentIncrements, sentIncrement)
				case receivedIncrement := <-received:
					receivedIncrements = append(receivedIncrements, receivedIncrement)
				case <-errChan:
					t.Fatal("received error on data transfer")
				}
			}
			require.Equal(t, sentIncrements, receivedIncrements)
			if data.isPull {
				assert.Equal(t, chid.Initiator, host2.ID())
			} else {
				assert.Equal(t, chid.Initiator, host1.ID())
			}
			cs, err := dt2.ChannelState(ctx, chid)
			require.NoError(t, err)
			require.Equal(t, cs.Status(), datatransfer.PartiallyCompleted)
			missingCids := cs.MissingCids()
			require.Len(t, missingCids, 2)
			require.Contains(t, missingCids, partialTree.MissingLeafLink.(cidlink.Link).Cid)
			require.Contains(t, missingCids, partialTree.MissingMiddleLink.(cidlink.Link).Cid)
			require.NotContains(t, missingCids, partialTree.PresentMiddleLink.(cidlink.Link).Cid)
			require.NotContains(t, missingCids, partialTree.PresentRootLink.(cidlink.Link).Cid)

			// The missing leaf is not included cause it's hidden completely underneath another missing link
			require.NotContains(t, missingCids, partialTree.HiddenMissingLeafLink.(cidlink.Link).Cid)
		})

	} //
}

func TestMultipleRoundTripMultipleStores(t *testing.T) {
	ctx := context.Background()
	testCases := map[string]struct {
		isPull       bool
		requestCount int
	}{
		"multiple roundtrip for push requests": {
			requestCount: 2,
		},
		"multiple roundtrip for pull requests": {
			isPull:       true,
			requestCount: 2,
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
			host1 := gsData.Host1 // initiator, data sender
			host2 := gsData.Host2 // data recipient

			tp1 := gsData.SetupGSTransportHost1()
			tp2 := gsData.SetupGSTransportHost2()

			dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt1)
			dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt2)

			finished := make(chan struct{}, 2*data.requestCount)
			errChan := make(chan string, 2*data.requestCount)
			opened := make(chan struct{}, 2*data.requestCount)
			var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if channelState.Status() == datatransfer.Completed {
					finished <- struct{}{}
				}
				if event.Code == datatransfer.Error {
					errChan <- event.Message
				}
				if event.Code == datatransfer.Open {
					opened <- struct{}{}
				}
			}
			dt1.SubscribeToEvents(subscriber)
			dt2.SubscribeToEvents(subscriber)
			vouchers := make([]datatransfer.Voucher, 0, data.requestCount)
			for i := 0; i < data.requestCount; i++ {
				vouchers = append(vouchers, testutil.NewFakeDTType())
			}
			sv := testutil.NewStubbedValidator()

			root, origBytes := testutil.LoadUnixFSFile(ctx, t, gsData.DagService1, loremFile)
			rootCid := root.(cidlink.Link).Cid

			destDagServices := make([]ipldformat.DAGService, 0, data.requestCount)
			linkSystems := make([]ipld.LinkSystem, 0, data.requestCount)
			for i := 0; i < data.requestCount; i++ {
				ds := dss.MutexWrap(datastore.NewMapDatastore())
				bs := bstore.NewBlockstore(namespace.Wrap(ds, datastore.NewKey("blockstore")))
				lsys := storeutil.LinkSystemForBlockstore(bs)
				destDagService := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

				destDagServices = append(destDagServices, destDagService)
				linkSystems = append(linkSystems, lsys)
			}

			err = dt2.RegisterTransportConfigurer(&testutil.FakeDTType{}, func(channelID datatransfer.ChannelID, testVoucher datatransfer.Voucher, transport datatransfer.Transport) {
				fv, ok := testVoucher.(*testutil.FakeDTType)
				if ok {
					for i, voucher := range vouchers {
						if fv.Data == voucher.(*testutil.FakeDTType).Data {
							gsTransport, ok := transport.(*tp.Transport)
							if ok {
								err := gsTransport.UseStore(channelID, linkSystems[i])
								require.NoError(t, err)
							}
						}
					}
				}
			})
			require.NoError(t, err)

			if data.isPull {
				sv.ExpectSuccessPull()
				require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				for i := 0; i < data.requestCount; i++ {
					_, err = dt2.OpenPullDataChannel(ctx, host1.ID(), vouchers[i], rootCid, gsData.AllSelector)
					require.NoError(t, err)
				}
			} else {
				sv.ExpectSuccessPush()
				require.NoError(t, dt2.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				for i := 0; i < data.requestCount; i++ {
					_, err = dt1.OpenPushDataChannel(ctx, host2.ID(), vouchers[i], rootCid, gsData.AllSelector)
					require.NoError(t, err)
				}
			}
			opens := 0
			completes := 0
			for opens < 2*data.requestCount || completes < 2*data.requestCount {
				select {
				case <-ctx.Done():
					t.Fatal("Did not complete successful data transfer")
				case <-finished:
					completes++
				case <-opened:
					opens++
				case err := <-errChan:
					t.Fatalf("received error on data transfer: %s", err)
				}
			}
			for _, destDagService := range destDagServices {
				testutil.VerifyHasFile(ctx, t, destDagService, root, origBytes)
			}
		})
	}
}

func TestManyReceiversAtOnce(t *testing.T) {
	ctx := context.Background()
	testCases := map[string]struct {
		isPull        bool
		receiverCount int
	}{
		"multiple receivers for push requests": {
			receiverCount: 10,
		},
		"multiple receivers for pull requests": {
			isPull:        true,
			receiverCount: 10,
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
			host1 := gsData.Host1 // initiator, data sender

			tp1 := gsData.SetupGSTransportHost1()
			dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt1)

			destDagServices := make([]ipldformat.DAGService, 0, data.receiverCount)
			receivers := make([]datatransfer.Manager, 0, data.receiverCount)
			hosts := make([]host.Host, 0, data.receiverCount)
			for i := 0; i < data.receiverCount; i++ {
				host, err := gsData.Mn.GenPeer()
				require.NoError(t, err, "error generating host")
				gsnet := gsnet.NewFromLibp2pHost(host)
				dtnet := network.NewFromLibp2pHost(host)
				ds := dss.MutexWrap(datastore.NewMapDatastore())
				bs := bstore.NewBlockstore(namespace.Wrap(ds, datastore.NewKey("blockstore")))
				altBs := bstore.NewBlockstore(namespace.Wrap(ds, datastore.NewKey("altstore")))

				lsys := storeutil.LinkSystemForBlockstore(bs)
				altLinkSystem := storeutil.LinkSystemForBlockstore(altBs)

				destDagService := merkledag.NewDAGService(blockservice.New(altBs, offline.Exchange(altBs)))

				gs := gsimpl.New(gsData.Ctx, gsnet, lsys)
				gsTransport := tp.NewTransport(host.ID(), gs, dtnet)

				dtDs := namespace.Wrap(ds, datastore.NewKey("datatransfer"))

				receiver, err := NewDataTransfer(dtDs, os.TempDir(), dtnet, gsTransport)
				require.NoError(t, err)
				err = receiver.Start(gsData.Ctx)
				require.NoError(t, err)

				err = receiver.RegisterTransportConfigurer(&testutil.FakeDTType{}, func(channelID datatransfer.ChannelID, testVoucher datatransfer.Voucher, transport datatransfer.Transport) {
					_, isFv := testVoucher.(*testutil.FakeDTType)
					gsTransport, isGs := transport.(*tp.Transport)
					if isFv && isGs {
						err := gsTransport.UseStore(channelID, altLinkSystem)
						require.NoError(t, err)
					}
				})
				require.NoError(t, err)

				destDagServices = append(destDagServices, destDagService)
				receivers = append(receivers, receiver)
				hosts = append(hosts, host)
			}
			err = gsData.Mn.LinkAll()
			require.NoError(t, err, "error linking hosts")

			finished := make(chan struct{}, 2*data.receiverCount)
			errChan := make(chan string, 2*data.receiverCount)
			opened := make(chan struct{}, 2*data.receiverCount)
			var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if channelState.Status() == datatransfer.Completed {
					finished <- struct{}{}
				}
				if event.Code == datatransfer.Error {
					errChan <- event.Message
				}
				if event.Code == datatransfer.Open {
					opened <- struct{}{}
				}
			}
			dt1.SubscribeToEvents(subscriber)
			for _, receiver := range receivers {
				receiver.SubscribeToEvents(subscriber)
			}
			vouchers := make([]datatransfer.Voucher, 0, data.receiverCount)
			for i := 0; i < data.receiverCount; i++ {
				vouchers = append(vouchers, testutil.NewFakeDTType())
			}
			sv := testutil.NewStubbedValidator()

			root, origBytes := testutil.LoadUnixFSFile(ctx, t, gsData.DagService1, loremFile)
			rootCid := root.(cidlink.Link).Cid

			if data.isPull {
				sv.ExpectSuccessPull()
				require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				for i, receiver := range receivers {
					_, err = receiver.OpenPullDataChannel(ctx, host1.ID(), vouchers[i], rootCid, gsData.AllSelector)
					require.NoError(t, err)
				}
			} else {
				sv.ExpectSuccessPush()
				for i, receiver := range receivers {
					require.NoError(t, receiver.RegisterVoucherType(&testutil.FakeDTType{}, sv))
					_, err = dt1.OpenPushDataChannel(ctx, hosts[i].ID(), vouchers[i], rootCid, gsData.AllSelector)
					require.NoError(t, err)
				}
			}
			opens := 0
			completes := 0
			for opens < 2*data.receiverCount || completes < 2*data.receiverCount {
				select {
				case <-ctx.Done():
					t.Fatal("Did not complete successful data transfer")
				case <-finished:
					completes++
				case <-opened:
					opens++
				case err := <-errChan:
					t.Fatalf("received error on data transfer: %s", err)
				}
			}
			for _, destDagService := range destDagServices {
				testutil.VerifyHasFile(ctx, t, destDagService, root, origBytes)
			}
		})
	}
}

// disconnectCoordinator is used by TestAutoRestart to allow
// test cases to signal when a disconnect should start, and whether
// to wait for the disconnect to take effect before continuing
type disconnectCoordinator struct {
	readyForDisconnect chan struct{}
	disconnected       chan struct{}
}

func newDisconnectCoordinator() *disconnectCoordinator {
	return &disconnectCoordinator{
		readyForDisconnect: make(chan struct{}),
		disconnected:       make(chan struct{}),
	}
}

func (dc *disconnectCoordinator) signalReadyForDisconnect(awaitDisconnect bool) {
	dc.readyForDisconnect <- struct{}{}
	if awaitDisconnect {
		<-dc.disconnected
	}
}

func (dc *disconnectCoordinator) onDisconnect() {
	close(dc.disconnected)
}

type restartRevalidator struct {
	*testutil.StubbedRevalidator
	pullDataSent map[datatransfer.ChannelID][]uint64
	pushDataRcvd map[datatransfer.ChannelID][]uint64
}

func newRestartRevalidator() *restartRevalidator {
	return &restartRevalidator{
		StubbedRevalidator: testutil.NewStubbedRevalidator(),
		pullDataSent:       make(map[datatransfer.ChannelID][]uint64),
		pushDataRcvd:       make(map[datatransfer.ChannelID][]uint64),
	}
}

func (r *restartRevalidator) OnPullDataSent(chid datatransfer.ChannelID, additionalBytesSent uint64) (bool, datatransfer.VoucherResult, error) {
	chSent, ok := r.pullDataSent[chid]
	if !ok {
		chSent = []uint64{}
	}
	chSent = append(chSent, additionalBytesSent)
	r.pullDataSent[chid] = chSent

	return true, nil, nil
}

func (r *restartRevalidator) pullDataSum(chid datatransfer.ChannelID) uint64 {
	pullDataSent, ok := r.pullDataSent[chid]
	var total uint64
	if !ok {
		return total
	}
	for _, sent := range pullDataSent {
		total += sent
	}
	return total
}

func (r *restartRevalidator) OnPushDataReceived(chid datatransfer.ChannelID, additionalBytesReceived uint64) (bool, datatransfer.VoucherResult, error) {
	chRcvd, ok := r.pushDataRcvd[chid]
	if !ok {
		chRcvd = []uint64{}
	}
	chRcvd = append(chRcvd, additionalBytesReceived)
	r.pushDataRcvd[chid] = chRcvd

	return true, nil, nil
}

func (r *restartRevalidator) pushDataSum(chid datatransfer.ChannelID) uint64 {
	pushDataRcvd, ok := r.pushDataRcvd[chid]
	var total uint64
	if !ok {
		return total
	}
	for _, rcvd := range pushDataRcvd {
		total += rcvd
	}
	return total
}

// TestAutoRestart tests that if the connection for a push or pull request
// goes down, it will automatically restart (given the right config options)
func TestAutoRestart(t *testing.T) {
	//SetDTLogLevelDebug()

	testCases := []struct {
		name                        string
		isPush                      bool
		expectInitiatorDTFail       bool
		disconnectOnRequestComplete bool
		registerResponder           func(responder datatransfer.Manager, dc *disconnectCoordinator)
	}{{
		// Push: Verify that the client fires an error event when the disconnect
		// occurs right when the responder receives the open channel request
		// (ie the responder doesn't get a chance to respond to the open
		// channel request)
		name:                  "push: when responder receives incoming request",
		isPush:                true,
		expectInitiatorDTFail: true,
		registerResponder: func(responder datatransfer.Manager, dc *disconnectCoordinator) {
			subscriber := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if event.Code == datatransfer.Open {
					dc.signalReadyForDisconnect(true)
				}
			}
			responder.SubscribeToEvents(subscriber)
		},
	}, {
		// Pull: Verify that the client fires an error event when the disconnect
		// occurs right when the responder receives the open channel request
		// (ie the responder doesn't get a chance to respond to the open
		// channel request)
		name:                  "pull: when responder receives incoming request",
		isPush:                false,
		expectInitiatorDTFail: true,
		registerResponder: func(responder datatransfer.Manager, dc *disconnectCoordinator) {
			subscriber := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if event.Code == datatransfer.Open {
					dc.signalReadyForDisconnect(true)
				}
			}
			responder.SubscribeToEvents(subscriber)
		},
	}, {
		// Push: Verify that if a disconnect happens right after the responder
		// receives the first block, the transfer will complete automatically
		// when the link comes back up
		name:   "push: when responder receives first block",
		isPush: true,
		registerResponder: func(responder datatransfer.Manager, dc *disconnectCoordinator) {
			rcvdCount := 0
			subscriber := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				//t.Logf("resp: %s / %s\n", datatransfer.Events[event.Code], datatransfer.Statuses[channelState.Status()])
				if event.Code == datatransfer.DataReceived {
					rcvdCount++
					if rcvdCount == 1 {
						dc.signalReadyForDisconnect(false)
					}
				}
			}
			responder.SubscribeToEvents(subscriber)
		},
	}, {
		// Pull: Verify that if a disconnect happens right after the responder
		// enqueues the first block, the transfer will complete automatically
		// when the link comes back up
		name:   "pull: when responder sends first block",
		isPush: false,
		registerResponder: func(responder datatransfer.Manager, dc *disconnectCoordinator) {
			sentCount := 0
			subscriber := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if event.Code == datatransfer.DataSent {
					sentCount++
					if sentCount == 1 {
						dc.signalReadyForDisconnect(false)
					}
				}
			}
			responder.SubscribeToEvents(subscriber)
		},
	}, {
		// Push: Verify that the client fires an error event when disconnect occurs
		// right before the responder sends the complete message (ie the responder
		// has received all blocks but the responder doesn't get a chance to tell
		// the initiator before the disconnect)
		name:                        "push: before requester sends complete message",
		isPush:                      true,
		expectInitiatorDTFail:       true,
		disconnectOnRequestComplete: true,
	}, {
		// Pull: Verify that the client fires an error event when disconnect occurs
		// right before the responder sends the complete message (ie responder sent
		// all blocks, but the responder doesn't get a chance to tell the initiator
		// before the disconnect)
		name:                        "pull: before responder sends complete message",
		isPush:                      false,
		expectInitiatorDTFail:       true,
		disconnectOnRequestComplete: true,
	}}
	for _, tc := range testCases {
		expectFailMsg := ""
		if tc.expectInitiatorDTFail {
			expectFailMsg = " (expect failure)"
		}

		// Test for different combinations of protocol versions on client
		// and provider
		for pname, ps := range protocolsForRestartTest {
			t.Run(tc.name+pname+expectFailMsg, func(t *testing.T) {
				ctx := context.Background()
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				// Create an object to coordinate disconnect events
				dc := newDisconnectCoordinator()

				// If the test should disconnect just before the responder sends
				// the Complete message, add a hook to do so
				var responderTransportOpts []tp.Option
				if tc.disconnectOnRequestComplete {
					if tc.isPush {
						responderTransportOpts = []tp.Option{
							tp.RegisterCompletedRequestListener(func(chid datatransfer.ChannelID) {
								dc.signalReadyForDisconnect(true)
							}),
						}
					} else {
						responderTransportOpts = []tp.Option{
							tp.RegisterCompletedResponseListener(func(chid datatransfer.ChannelID) {
								dc.signalReadyForDisconnect(true)
							}),
						}
					}
				}

				// The retry config for the network layer: make 5 attempts, backing off by 1s each time
				netRetry := network.RetryParameters(time.Second, time.Second, 5, 1)
				gsData := testutil.NewGraphsyncTestingData(ctx, t, ps.host1Protocols, ps.host2Protocols)
				gsData.DtNet1 = network.NewFromLibp2pHost(gsData.Host1, netRetry)
				initiatorHost := gsData.Host1 // initiator, data sender
				responderHost := gsData.Host2 // data recipient

				initiatorGSTspt := gsData.SetupGSTransportHost1()
				responderGSTspt := gsData.SetupGSTransportHost2(responderTransportOpts...)

				// Set up
				restartConf := ChannelRestartConfig(channelmonitor.Config{
					AcceptTimeout:          100 * time.Millisecond,
					RestartDebounce:        500 * time.Millisecond,
					RestartBackoff:         500 * time.Millisecond,
					MaxConsecutiveRestarts: 10,
					CompleteTimeout:        100 * time.Millisecond,
				})
				initiator, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, initiatorGSTspt, restartConf)
				require.NoError(t, err)
				testutil.StartAndWaitForReady(ctx, t, initiator)
				defer initiator.Stop(ctx)

				responder, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, responderGSTspt)
				require.NoError(t, err)
				testutil.StartAndWaitForReady(ctx, t, responder)
				defer responder.Stop(ctx)

				//initiator.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				//	t.Logf("clnt: evt %s / status %s", datatransfer.Events[event.Code], datatransfer.Statuses[channelState.Status()])
				//})

				// Watch for successful completion
				finished := make(chan struct{}, 2)
				var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
					if channelState.Status() == datatransfer.Completed {
						finished <- struct{}{}
					}
				}
				initiator.SubscribeToEvents(subscriber)
				responder.SubscribeToEvents(subscriber)
				voucher := testutil.FakeDTType{Data: "applesauce"}
				sv := testutil.NewStubbedValidator()

				var sourceDagService, destDagService ipldformat.DAGService
				if tc.isPush {
					sourceDagService = gsData.DagService1
					destDagService = gsData.DagService2
				} else {
					sourceDagService = gsData.DagService2
					destDagService = gsData.DagService1
				}

				root, origBytes := testutil.LoadUnixFSFile(ctx, t, sourceDagService, loremFile)
				rootCid := root.(cidlink.Link).Cid

				require.NoError(t, initiator.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				require.NoError(t, responder.RegisterVoucherType(&testutil.FakeDTType{}, sv))

				// Register a revalidator that records calls to OnPullDataSent and OnPushDataReceived
				srv := newRestartRevalidator()
				require.NoError(t, responder.RegisterRevalidator(testutil.NewFakeDTType(), srv))

				// If the test case needs to subscribe to response events, provide
				// the test case with the responder
				if tc.registerResponder != nil {
					tc.registerResponder(responder, dc)
				}

				// If the initiator is expected to fail, watch for the Failed event
				initiatorFailed := make(chan struct{})
				if tc.expectInitiatorDTFail {
					initiator.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
						if channelState.Status() == datatransfer.Failed {
							close(initiatorFailed)
						}
					})
				}

				var chid datatransfer.ChannelID
				if tc.isPush {
					// Open a push channel
					chid, err = initiator.OpenPushDataChannel(ctx, responderHost.ID(), &voucher, rootCid, gsData.AllSelector)
				} else {
					// Open a pull channel
					chid, err = initiator.OpenPullDataChannel(ctx, responderHost.ID(), &voucher, rootCid, gsData.AllSelector)
				}
				require.NoError(t, err)

				// Wait for the moment at which the test case should experience a disconnect
				select {
				case <-time.After(time.Second):
					t.Fatal("Timed out waiting for point at which to break connection")
				case <-dc.readyForDisconnect:
				}

				// Break connection
				t.Logf("Breaking connection to peer")
				require.NoError(t, gsData.Mn.UnlinkPeers(initiatorHost.ID(), responderHost.ID()))
				require.NoError(t, gsData.Mn.DisconnectPeers(initiatorHost.ID(), responderHost.ID()))

				// Inform the coordinator that the disconnect has occurred
				dc.onDisconnect()

				t.Logf("Sleep for a second")
				time.Sleep(1 * time.Second)

				// Restore link
				t.Logf("Restore link")
				require.NoError(t, gsData.Mn.LinkAll())
				time.Sleep(200 * time.Millisecond)

				// If we're expecting a Failed event, verify that it occurs
				if tc.expectInitiatorDTFail {
					select {
					case <-ctx.Done():
						t.Fatal("Initiator data-transfer did not fail as expected")
						return
					case <-initiatorFailed:
						t.Logf("Initiator data-transfer failed as expected")
						return
					}
				}

				// We're not expecting a failure event, wait for the transfer to
				// complete
				t.Logf("Waiting for auto-restart on push channel %s", chid)

				(func() {
					finishedCount := 0
					for {
						select {
						case <-ctx.Done():
							t.Fatal("Did not complete successful data transfer")
							return
						case <-finished:
							finishedCount++
							if finishedCount == 2 {
								return
							}
						}
					}
				})()

				// Verify that the total amount of data sent / received that was
				// reported to the revalidator is correct
				if tc.isPush {
					require.EqualValues(t, loremFileTransferBytes, srv.pushDataSum(chid))
				} else {
					require.EqualValues(t, loremFileTransferBytes, srv.pullDataSum(chid))
				}

				// Verify that the file was transferred to the destination node
				testutil.VerifyHasFile(ctx, t, destDagService, root, origBytes)
			})
		}
	}
}

// TestAutoRestartAfterBouncingInitiator verifies correct behaviour in the
// following scenario:
// 1. An "initiator" opens a push / pull channel to a "responder"
// 2. The initiator is shut down when the first block is received
// 3. The initiator is brought back up
// 4. The initiator restarts the data transfer with RestartDataTransferChannel
// 5. The connection is broken when the first block is received
// 6. The connection is automatically re-established and the transfer completes
func TestAutoRestartAfterBouncingInitiator(t *testing.T) {
	t.Skip("flaky test")
	SetDTLogLevelDebug()

	runTest := func(t *testing.T, isPush bool) {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		// The retry config for the network layer: make 5 attempts, backing off by 1s each time
		netRetry := network.RetryParameters(time.Second, time.Second, 5, 1)
		gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
		gsData.DtNet1 = network.NewFromLibp2pHost(gsData.Host1, netRetry)
		initiatorHost := gsData.Host1 // initiator, data sender
		responderHost := gsData.Host2 // data recipient

		initiatorGSTspt := gsData.SetupGSTransportHost1()
		responderGSTspt := gsData.SetupGSTransportHost2()

		// Set up
		restartConf := ChannelRestartConfig(channelmonitor.Config{
			AcceptTimeout:          10 * time.Second,
			RestartDebounce:        500 * time.Millisecond,
			RestartBackoff:         500 * time.Millisecond,
			MaxConsecutiveRestarts: 10,
			CompleteTimeout:        100 * time.Millisecond,
		})
		initiator, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, initiatorGSTspt, restartConf)
		require.NoError(t, err)
		testutil.StartAndWaitForReady(ctx, t, initiator)
		defer initiator.Stop(ctx)

		responder, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, responderGSTspt)
		require.NoError(t, err)
		testutil.StartAndWaitForReady(ctx, t, responder)
		defer responder.Stop(ctx)

		// Watch for the Completed event on the responder.
		// (below we watch for the Completed event on the initiator)
		finished := make(chan struct{}, 2)
		var completeSubscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
			if channelState.Status() == datatransfer.Completed {
				finished <- struct{}{}
			}
		}
		responder.SubscribeToEvents(completeSubscriber)

		// onDataReceivedChan watches for the first DataReceived event
		dataReceiver := initiator
		if isPush {
			dataReceiver = responder
		}
		onDataReceivedChan := func(dataRcvr datatransfer.Manager) chan struct{} {
			dataReceived := make(chan struct{}, 1)
			rcvdCount := 0
			dataRcvdSubscriber := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				//t.Logf("resp: %s / %s\n", datatransfer.Events[event.Code], datatransfer.Statuses[channelState.Status()])
				if event.Code == datatransfer.DataReceived {
					rcvdCount++
					if rcvdCount == 1 {
						dataReceived <- struct{}{}
					}
				}
			}
			dataRcvr.SubscribeToEvents(dataRcvdSubscriber)
			return dataReceived
		}
		dataReceived := onDataReceivedChan(dataReceiver)

		voucher := testutil.FakeDTType{Data: "applesauce"}
		sv := testutil.NewStubbedValidator()

		var sourceDagService, destDagService ipldformat.DAGService
		if isPush {
			sourceDagService = gsData.DagService1
			destDagService = gsData.DagService2
		} else {
			sourceDagService = gsData.DagService2
			destDagService = gsData.DagService1
		}

		root, origBytes := testutil.LoadUnixFSFile(ctx, t, sourceDagService, loremLargeFile)
		rootCid := root.(cidlink.Link).Cid

		require.NoError(t, initiator.RegisterVoucherType(&testutil.FakeDTType{}, sv))
		require.NoError(t, responder.RegisterVoucherType(&testutil.FakeDTType{}, sv))

		// Register a revalidator that records calls to OnPullDataSent and OnPushDataReceived
		srv := newRestartRevalidator()
		require.NoError(t, responder.RegisterRevalidator(testutil.NewFakeDTType(), srv))

		var chid datatransfer.ChannelID
		if isPush {
			// Open a push channel
			chid, err = initiator.OpenPushDataChannel(ctx, responderHost.ID(), &voucher, rootCid, gsData.AllSelector)
		} else {
			// Open a pull channel
			chid, err = initiator.OpenPullDataChannel(ctx, responderHost.ID(), &voucher, rootCid, gsData.AllSelector)
		}
		require.NoError(t, err)

		// Wait for the first block to be received
		select {
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for point at which to break connection")
		case <-dataReceived:
		}

		// Break connection
		t.Logf("Breaking connection to peer")
		require.NoError(t, gsData.Mn.UnlinkPeers(initiatorHost.ID(), responderHost.ID()))
		require.NoError(t, gsData.Mn.DisconnectPeers(initiatorHost.ID(), responderHost.ID()))

		time.Sleep(100 * time.Millisecond)

		// We want to simulate shutting down and restarting the initiator of
		// the data transfer:
		// 1. Shut down the initiator of the data transfer
		t.Logf("Stopping initiator")
		err = initiator.Stop(ctx)
		require.NoError(t, err)

		t.Logf("Sleep for a moment")
		time.Sleep(500 * time.Millisecond)

		// 2. Create a new initiator
		initiator2GSTspt := gsData.SetupGSTransportHost1()
		initiator2, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, initiator2GSTspt, restartConf)
		require.NoError(t, err)
		require.NoError(t, initiator2.RegisterVoucherType(&testutil.FakeDTType{}, sv))
		initiator2.SubscribeToEvents(completeSubscriber)

		testutil.StartAndWaitForReady(ctx, t, initiator2)
		defer initiator2.Stop(ctx)

		t.Logf("Sleep for a second")
		time.Sleep(1 * time.Second)

		// Restore link
		t.Logf("Restore link")
		require.NoError(t, gsData.Mn.LinkAll())
		time.Sleep(200 * time.Millisecond)

		// Watch for data received event
		dataReceiver = initiator2
		if isPush {
			dataReceiver = responder
		}
		dataReceivedAfterRestart := onDataReceivedChan(dataReceiver)

		// Restart the data transfer on the new initiator.
		// (this is equivalent to shutting down and restarting a node running
		// the initiator)
		err = initiator2.RestartDataTransferChannel(ctx, chid)
		require.NoError(t, err)

		// Wait for the first block to be received
		select {
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for point at which to break connection")
		case <-dataReceivedAfterRestart:
		}

		// Break connection
		t.Logf("Breaking connection to peer")
		require.NoError(t, gsData.Mn.UnlinkPeers(initiatorHost.ID(), responderHost.ID()))
		require.NoError(t, gsData.Mn.DisconnectPeers(initiatorHost.ID(), responderHost.ID()))

		t.Logf("Sleep for a second")
		time.Sleep(1 * time.Second)

		// Restore link
		t.Logf("Restore link")
		require.NoError(t, gsData.Mn.LinkAll())
		time.Sleep(200 * time.Millisecond)

		// Wait for the transfer to complete
		t.Logf("Waiting for auto-restart on push channel %s", chid)

		(func() {
			finishedCount := 0
			for {
				select {
				case <-ctx.Done():
					t.Fatal("Did not complete successful data transfer")
					return
				case <-finished:
					finishedCount++
					if finishedCount == 2 {
						return
					}
				}
			}
		})()

		// Verify that the total amount of data sent / received that was
		// reported to the revalidator is correct
		if isPush {
			require.EqualValues(t, loremLargeFileTransferBytes, srv.pushDataSum(chid))
		} else {
			require.EqualValues(t, loremLargeFileTransferBytes, srv.pullDataSum(chid))
		}

		// Verify that the file was transferred to the destination node
		testutil.VerifyHasFile(ctx, t, destDagService, root, origBytes)
	}

	t.Run("push", func(t *testing.T) {
		runTest(t, true)
	})
	t.Run("pull", func(t *testing.T) {
		runTest(t, false)
	})
}

func TestRoundTripCancelledRequest(t *testing.T) {
	ctx := context.Background()
	testCases := map[string]struct {
		isPull bool
	}{
		"cancelled push request": {},
		"cancelled pull request": {
			isPull: true,
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
			host1 := gsData.Host1 // initiator, data sender
			host2 := gsData.Host2

			tp1 := gsData.SetupGSTransportHost1()
			tp2 := gsData.SetupGSTransportHost2()

			dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt1)
			dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt2)

			finished := make(chan struct{}, 2)
			errChan := make(chan string, 2)
			cancelled := make(chan struct{}, 2)
			accepted := make(chan struct{}, 2)
			opened := make(chan struct{}, 2)
			var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if channelState.Status() == datatransfer.Completed {
					finished <- struct{}{}
				}
				if event.Code == datatransfer.Accept {
					accepted <- struct{}{}
				}
				if event.Code == datatransfer.Error {
					errChan <- event.Message
				}
				if event.Code == datatransfer.Cancel {
					cancelled <- struct{}{}
				}
				if event.Code == datatransfer.Open {
					opened <- struct{}{}
				}
			}
			dt1.SubscribeToEvents(subscriber)
			dt2.SubscribeToEvents(subscriber)
			voucher := testutil.FakeDTType{Data: "applesauce"}
			sv := testutil.NewStubbedValidator()
			root, _ := testutil.LoadUnixFSFile(ctx, t, gsData.DagService1, loremFile)
			rootCid := root.(cidlink.Link).Cid

			var chid datatransfer.ChannelID
			if data.isPull {
				sv.ExpectPausePull()
				require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				chid, err = dt2.OpenPullDataChannel(ctx, host1.ID(), &voucher, rootCid, gsData.AllSelector)
			} else {
				sv.ExpectPausePush()
				require.NoError(t, dt2.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				chid, err = dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, rootCid, gsData.AllSelector)
			}
			require.NoError(t, err)
			opens := 0
			cancels := 0
			accepts := 0
			for opens < 2 || cancels < 2 {
				select {
				case <-ctx.Done():
					t.Fatal("Did not finish data transfer")
				case <-finished:
					t.Fatal("request completed succussfully but should have been cancelled")
				case <-opened:
					opens++
				case <-cancelled:
					cancels++
				case <-accepted:
					if accepts == 0 {
						timer := time.NewTimer(10 * time.Millisecond)
						go func() {
							select {
							case <-ctx.Done():
							case <-timer.C:
								if data.isPull {
									_ = dt1.CloseDataTransferChannel(ctx, chid)
								} else {
									_ = dt2.CloseDataTransferChannel(ctx, chid)
								}
							}
						}()
					}
					accepts++
				case err := <-errChan:
					t.Fatalf("received error on data transfer: %s", err)
				}
			}
		})
	}
}

type retrievalRevalidator struct {
	*testutil.StubbedRevalidator
	dataSoFar          uint64
	providerPausePoint int
	pausePoints        []uint64
	finalVoucher       datatransfer.VoucherResult
	revalVouchers      []datatransfer.VoucherResult
}

func (r *retrievalRevalidator) OnPullDataSent(chid datatransfer.ChannelID, additionalBytesSent uint64) (bool, datatransfer.VoucherResult, error) {
	r.dataSoFar += additionalBytesSent
	if r.providerPausePoint < len(r.pausePoints) &&
		r.dataSoFar >= r.pausePoints[r.providerPausePoint] {
		var v datatransfer.VoucherResult = testutil.NewFakeDTType()
		if len(r.revalVouchers) > r.providerPausePoint {
			v = r.revalVouchers[r.providerPausePoint]
		}
		r.providerPausePoint++
		return true, v, datatransfer.ErrPause
	}
	return true, nil, nil
}

func (r *retrievalRevalidator) OnPushDataReceived(chid datatransfer.ChannelID, additionalBytesReceived uint64) (bool, datatransfer.VoucherResult, error) {
	return false, nil, nil
}
func (r *retrievalRevalidator) OnComplete(chid datatransfer.ChannelID) (bool, datatransfer.VoucherResult, error) {
	return true, r.finalVoucher, datatransfer.ErrPause
}

func TestSimulatedRetrievalFlow(t *testing.T) {
	ctx := context.Background()
	testCases := map[string]struct {
		unpauseRequestorDelay time.Duration
		unpauseResponderDelay time.Duration
		pausePoints           []uint64
		expectedTraces        []string
	}{
		"fast unseal, payment channel ready": {
			pausePoints: []uint64{1000, 3000, 6000, 10000, 15000},
			expectedTraces: []string{
				// initiator: execute outgoing graphsync request
				"transfer(0)->request(0)->executeTask(0)",
				// initiator: send first voucher
				"transfer(0)->sendVoucher(0)->sendMessage(0)",
				// initiator: send second voucher
				"transfer(0)->sendVoucher(1)->sendMessage(0)",
				// initiator: send third voucher
				"transfer(0)->sendVoucher(2)->sendMessage(0)",
				// initiator: send fourth voucher
				"transfer(0)->sendVoucher(3)->sendMessage(0)",
				// initiator: send fifth voucher
				"transfer(0)->sendVoucher(4)->sendMessage(0)",
				// initiator: receive completion message from responder with final voucher request
				"transfer(0)->receiveResponse(0)",
				// initiator: send final voucher
				"transfer(0)->sendVoucher(5)->sendMessage(0)",
				// initiator: receive confirmation of final voucher
				"transfer(0)->receiveResponse(1)",
				// responder: receive GS request and execute response up to pause
				"transfer(1)->response(0)->executeTask(0)",
				// responder: execute GS request up to second pause after first voucher
				"transfer(1)->response(0)->executeTask(1)",
				// responder: execute GS request up to third pause after second voucher
				"transfer(1)->response(0)->executeTask(2)",
				// responder: execute GS request up to fourth pause after third voucher
				"transfer(1)->response(0)->executeTask(3)",
				// responder: execute GS request up to fifth pause after fourth voucher
				"transfer(1)->response(0)->executeTask(4)",
				// responder: execute GS request to finish after fifth voucher
				"transfer(1)->response(0)->executeTask(5)",
				// responder: receive first voucher
				"transfer(1)->receiveRequest(0)",
				// responder: receive second voucher
				"transfer(1)->receiveRequest(1)",
				// responder: receive third voucher
				"transfer(1)->receiveRequest(2)",
				// responder: receive fourth voucher
				"transfer(1)->receiveRequest(3)",
				// responder: receive fifth voucher
				"transfer(1)->receiveRequest(4)",
				// responder: send message that we sent all data along with final voucher request
				"transfer(1)->sendMessage(0)",
				// responder: receive final voucher and send acceptance message
				"transfer(1)->receiveRequest(5)->sendMessage(0)",
			},
		},
		"fast unseal, payment channel not ready": {
			unpauseRequestorDelay: 100 * time.Millisecond,
			pausePoints:           []uint64{1000, 3000, 6000, 10000, 15000},
		},
		"slow unseal, payment channel ready": {
			unpauseResponderDelay: 200 * time.Millisecond,
			pausePoints:           []uint64{1000, 3000, 6000, 10000, 15000},
		},
	}
	for testCase, config := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, collectTracing := testutil.SetupTracing(ctx)
			ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
			defer cancel()

			gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
			host1 := gsData.Host1 // initiator, data sender

			root := gsData.LoadUnixFSFile(t, false)
			rootCid := root.(cidlink.Link).Cid
			tp1 := gsData.SetupGSTransportHost1()
			tp2 := gsData.SetupGSTransportHost2()

			dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt1)
			dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt2)
			var chid datatransfer.ChannelID
			errChan := make(chan struct{}, 2)
			clientPausePoint := 0
			clientFinished := make(chan struct{}, 1)
			finalVoucherResult := testutil.NewFakeDTType()
			encodedFVR, err := encoding.Encode(finalVoucherResult)
			require.NoError(t, err)
			var clientSubscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if event.Code == datatransfer.Error {
					errChan <- struct{}{}
				}
				if event.Code == datatransfer.NewVoucherResult {
					lastVoucherResult := channelState.LastVoucherResult()
					encodedLVR, err := encoding.Encode(lastVoucherResult)
					require.NoError(t, err)
					if bytes.Equal(encodedLVR, encodedFVR) {
						_ = dt2.SendVoucher(ctx, chid, testutil.NewFakeDTType())
					}
				}

				if event.Code == datatransfer.DataReceived &&
					clientPausePoint < len(config.pausePoints) &&
					channelState.Received() > config.pausePoints[clientPausePoint] {
					_ = dt2.SendVoucher(ctx, chid, testutil.NewFakeDTType())
					clientPausePoint++
				}
				if channelState.Status() == datatransfer.Completed {
					clientFinished <- struct{}{}
				}
			}
			dt2.SubscribeToEvents(clientSubscriber)
			providerFinished := make(chan struct{}, 1)
			providerAccepted := false
			var providerSubscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if event.Code == datatransfer.PauseResponder {
					if !providerAccepted {
						providerAccepted = true
						timer := time.NewTimer(config.unpauseResponderDelay)
						go func() {
							<-timer.C
							_ = dt1.ResumeDataTransferChannel(ctx, chid)
						}()
					}
				}
				if event.Code == datatransfer.Error {
					errChan <- struct{}{}
				}
				if channelState.Status() == datatransfer.Completed {
					providerFinished <- struct{}{}
				}
			}
			dt1.SubscribeToEvents(providerSubscriber)
			voucher := testutil.FakeDTType{Data: "applesauce"}
			sv := testutil.NewStubbedValidator()
			sv.ExpectPausePull()
			require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))

			srv := &retrievalRevalidator{
				testutil.NewStubbedRevalidator(), 0, 0, config.pausePoints, finalVoucherResult, []datatransfer.VoucherResult{},
			}
			srv.ExpectSuccessErrResume()
			require.NoError(t, dt1.RegisterRevalidator(testutil.NewFakeDTType(), srv))

			require.NoError(t, dt2.RegisterVoucherResultType(testutil.NewFakeDTType()))
			chid, err = dt2.OpenPullDataChannel(ctx, host1.ID(), &voucher, rootCid, gsData.AllSelector)
			require.NoError(t, err)

			for providerFinished != nil || clientFinished != nil {
				select {
				case <-ctx.Done():
					t.Fatal("Did not complete successful data transfer")
				case <-providerFinished:
					providerFinished = nil
				case <-clientFinished:
					clientFinished = nil
				case <-errChan:
					t.Fatal("received unexpected error")
				}
			}
			sv.VerifyExpectations(t)
			srv.VerifyExpectations(t)
			gsData.VerifyFileTransferred(t, root, true)
			require.Equal(t, srv.providerPausePoint, len(config.pausePoints))
			require.Equal(t, clientPausePoint, len(config.pausePoints))
			traces := collectTracing(t).TracesToStrings(3)
			for _, expectedTrace := range config.expectedTraces {
				require.Contains(t, traces, expectedTrace)
			}
		})
	}
}

func TestPauseAndResume(t *testing.T) {
	ctx := context.Background()
	testCases := map[string]bool{
		"pause and resume works for push requests": false,
		"pause and resume works for pull requests": true,
	}
	for testCase, isPull := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
			host1 := gsData.Host1 // initiator, data sender
			host2 := gsData.Host2 // data recipient

			root := gsData.LoadUnixFSFile(t, false)
			rootCid := root.(cidlink.Link).Cid
			tp1 := gsData.SetupGSTransportHost1()
			tp2 := gsData.SetupGSTransportHost2()

			dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt1)
			dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt2)
			finished := make(chan struct{}, 2)
			errChan := make(chan struct{}, 2)
			opened := make(chan struct{}, 2)
			sent := make(chan uint64, 100)
			received := make(chan uint64, 100)
			pauseInitiator := make(chan struct{}, 2)
			resumeInitiator := make(chan struct{}, 2)
			pauseResponder := make(chan struct{}, 2)
			resumeResponder := make(chan struct{}, 2)
			var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {

				if event.Code == datatransfer.DataQueued {
					if channelState.Queued() > 0 {
						sent <- channelState.Queued()
					}
				}

				if event.Code == datatransfer.DataReceived {
					if channelState.Received() > 0 {
						received <- channelState.Received()
					}
				}

				if event.Code == datatransfer.PauseInitiator {
					pauseInitiator <- struct{}{}
				}
				if event.Code == datatransfer.ResumeInitiator {
					resumeInitiator <- struct{}{}
				}
				if event.Code == datatransfer.PauseResponder {
					pauseResponder <- struct{}{}
				}
				if event.Code == datatransfer.ResumeResponder {
					resumeResponder <- struct{}{}
				}
				if channelState.Status() == datatransfer.Completed {
					finished <- struct{}{}
				}
				if event.Code == datatransfer.Error {
					errChan <- struct{}{}
				}
				if event.Code == datatransfer.Open {
					opened <- struct{}{}
				}
			}
			dt1.SubscribeToEvents(subscriber)
			dt2.SubscribeToEvents(subscriber)
			voucher := testutil.FakeDTType{Data: "applesauce"}
			sv := testutil.NewStubbedValidator()

			var chid datatransfer.ChannelID
			if isPull {
				sv.ExpectSuccessPull()
				require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				chid, err = dt2.OpenPullDataChannel(ctx, host1.ID(), &voucher, rootCid, gsData.AllSelector)
			} else {
				sv.ExpectSuccessPush()
				require.NoError(t, dt2.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				chid, err = dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, rootCid, gsData.AllSelector)
			}
			require.NoError(t, err)
			opens := 0
			completes := 0
			pauseInitiators := 0
			pauseResponders := 0
			resumeInitiators := 0
			resumeResponders := 0
			sentIncrements := make([]uint64, 0, 21)
			receivedIncrements := make([]uint64, 0, 21)
			for opens < 2 || completes < 2 || len(sentIncrements) < 21 || len(receivedIncrements) < 21 ||
				pauseInitiators < 1 || pauseResponders < 1 || resumeInitiators < 1 || resumeResponders < 1 {
				select {
				case <-ctx.Done():
					t.Fatal("Did not complete successful data transfer")
				case <-finished:
					completes++
				case <-opened:
					opens++
				case <-pauseInitiator:
					pauseInitiators++
				case <-resumeInitiator:
					resumeInitiators++
				case <-pauseResponder:
					pauseResponders++
				case <-resumeResponder:
					resumeResponders++
				case sentIncrement := <-sent:
					sentIncrements = append(sentIncrements, sentIncrement)
					if len(sentIncrements) == 5 {
						require.NoError(t, dt1.PauseDataTransferChannel(ctx, chid))
						time.Sleep(100 * time.Millisecond)
						require.NoError(t, dt1.ResumeDataTransferChannel(ctx, chid))
					}
				case receivedIncrement := <-received:
					receivedIncrements = append(receivedIncrements, receivedIncrement)
					if len(receivedIncrements) == 10 {
						require.NoError(t, dt2.PauseDataTransferChannel(ctx, chid))
						time.Sleep(100 * time.Millisecond)
						require.NoError(t, dt2.ResumeDataTransferChannel(ctx, chid))
					}
				case <-errChan:
					t.Fatal("received error on data transfer")
				}
			}
			require.Equal(t, sentIncrements, receivedIncrements)
			gsData.VerifyFileTransferred(t, root, true)
			if isPull {
				assert.Equal(t, chid.Initiator, host2.ID())
			} else {
				assert.Equal(t, chid.Initiator, host1.ID())
			}
		})
	}
}

func TestUnrecognizedVoucherRoundTrip(t *testing.T) {
	ctx := context.Background()
	testCases := map[string]bool{
		"push requests": false,
		"pull requests": true,
	}
	for testCase, isPull := range testCases {
		t.Run(testCase, func(t *testing.T) {
			//	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			//	defer cancel()

			gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
			host1 := gsData.Host1 // initiator, data sender
			host2 := gsData.Host2 // data recipient

			tp1 := gsData.SetupGSTransportHost1()
			tp2 := gsData.SetupGSTransportHost2()

			dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt1)
			dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
			require.NoError(t, err)
			testutil.StartAndWaitForReady(ctx, t, dt2)

			finished := make(chan struct{}, 2)
			errChan := make(chan string, 2)
			opened := make(chan struct{}, 2)
			var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if channelState.Status() == datatransfer.Failed {
					finished <- struct{}{}
				}
				if event.Code == datatransfer.Error {
					errChan <- channelState.Message()
				}
				if event.Code == datatransfer.Open {
					opened <- struct{}{}
				}
			}
			dt1.SubscribeToEvents(subscriber)
			dt2.SubscribeToEvents(subscriber)
			voucher := testutil.FakeDTType{Data: "applesauce"}

			root, _ := testutil.LoadUnixFSFile(ctx, t, gsData.DagService1, loremFile)
			rootCid := root.(cidlink.Link).Cid

			if isPull {
				_, err = dt2.OpenPullDataChannel(ctx, host1.ID(), &voucher, rootCid, gsData.AllSelector)
			} else {
				_, err = dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, rootCid, gsData.AllSelector)
			}
			require.NoError(t, err)
			opens := 0
			var errMessages []string
			finishes := 0
			for opens < 1 || finishes < 1 {
				select {
				case <-ctx.Done():
					t.Fatal("Did not complete successful data transfer")
				case <-finished:
					finishes++
				case <-opened:
					opens++
				case errMessage := <-errChan:
					require.Equal(t, errMessage, datatransfer.ErrRejected.Error())
					errMessages = append(errMessages, errMessage)
					if len(errMessages) > 1 {
						t.Fatal("too many errors")
					}
				}
			}
		})
	}
}

func TestDataTransferSubscribing(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
	host2 := gsData.Host2

	tp1 := gsData.SetupGSTransportHost1()
	tp2 := gsData.SetupGSTransportHost2()
	sv := testutil.NewStubbedValidator()
	sv.StubErrorPull()
	sv.StubErrorPush()
	dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
	require.NoError(t, err)
	testutil.StartAndWaitForReady(ctx, t, dt2)
	require.NoError(t, dt2.RegisterVoucherType(&testutil.FakeDTType{}, sv))
	voucher := testutil.FakeDTType{Data: "applesauce"}
	baseCid := testutil.GenerateCids(1)[0]

	dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
	require.NoError(t, err)
	testutil.StartAndWaitForReady(ctx, t, dt1)
	subscribe1Calls := make(chan struct{}, 1)
	subscribe1 := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			subscribe1Calls <- struct{}{}
		}
	}
	subscribe2Calls := make(chan struct{}, 1)
	subscribe2 := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			subscribe2Calls <- struct{}{}
		}
	}
	unsub1 := dt1.SubscribeToEvents(subscribe1)
	unsub2 := dt1.SubscribeToEvents(subscribe2)
	_, err = dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, baseCid, gsData.AllSelector)
	require.NoError(t, err)
	select {
	case <-ctx.Done():
		t.Fatal("subscribed events not received")
	case <-subscribe1Calls:
	}
	select {
	case <-ctx.Done():
		t.Fatal("subscribed events not received")
	case <-subscribe2Calls:
	}
	unsub1()
	unsub2()

	subscribe3Calls := make(chan struct{}, 1)
	subscribe3 := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			subscribe3Calls <- struct{}{}
		}
	}
	subscribe4Calls := make(chan struct{}, 1)
	subscribe4 := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			subscribe4Calls <- struct{}{}
		}
	}
	unsub3 := dt1.SubscribeToEvents(subscribe3)
	unsub4 := dt1.SubscribeToEvents(subscribe4)
	_, err = dt1.OpenPullDataChannel(ctx, host2.ID(), &voucher, baseCid, gsData.AllSelector)
	require.NoError(t, err)
	select {
	case <-ctx.Done():
		t.Fatal("subscribed events not received")
	case <-subscribe1Calls:
		t.Fatal("received channel that should have been unsubscribed")
	case <-subscribe2Calls:
		t.Fatal("received channel that should have been unsubscribed")
	case <-subscribe3Calls:
	}
	select {
	case <-ctx.Done():
		t.Fatal("subscribed events not received")
	case <-subscribe1Calls:
		t.Fatal("received channel that should have been unsubscribed")
	case <-subscribe2Calls:
		t.Fatal("received channel that should have been unsubscribed")
	case <-subscribe4Calls:
	}
	unsub3()
	unsub4()
}

type receivedGraphSyncMessage struct {
	message gsmsg.GraphSyncMessage
	p       peer.ID
}

type fakeGraphSyncReceiver struct {
	receivedMessages chan receivedGraphSyncMessage
}

func (fgsr *fakeGraphSyncReceiver) ReceiveMessage(ctx context.Context, sender peer.ID, incoming gsmsg.GraphSyncMessage) {
	select {
	case <-ctx.Done():
	case fgsr.receivedMessages <- receivedGraphSyncMessage{incoming, sender}:
	}
}

func (fgsr *fakeGraphSyncReceiver) ReceiveError(_ peer.ID, _ error) {
}
func (fgsr *fakeGraphSyncReceiver) Connected(p peer.ID) {
}
func (fgsr *fakeGraphSyncReceiver) Disconnected(p peer.ID) {
}

func (fgsr *fakeGraphSyncReceiver) consumeResponses(ctx context.Context, t *testing.T) graphsync.ResponseStatusCode {
	var gsMessageReceived receivedGraphSyncMessage
	for {
		select {
		case <-ctx.Done():
			t.Fail()
		case gsMessageReceived = <-fgsr.receivedMessages:
			responses := gsMessageReceived.message.Responses()
			if (len(responses) > 0) && gsmsg.IsTerminalResponseCode(responses[0].Status()) {
				return responses[0].Status()
			}
		}
	}
}

func TestRespondingToPushGraphsyncRequests(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
	host1 := gsData.Host1 // initiator and data sender
	host2 := gsData.Host2 // data recipient, makes graphsync request for data
	voucher := testutil.NewFakeDTType()
	link := gsData.LoadUnixFSFile(t, false)

	// setup receiving peer to just record message coming in
	dtnet2 := network.NewFromLibp2pHost(host2)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	dtnet2.SetDelegate(r)

	gsr := &fakeGraphSyncReceiver{
		receivedMessages: make(chan receivedGraphSyncMessage),
	}
	gsData.GsNet2.SetDelegate(gsr)

	tp1 := gsData.SetupGSTransportHost1()
	dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
	require.NoError(t, err)
	testutil.StartAndWaitForReady(ctx, t, dt1)
	voucherResult := testutil.NewFakeDTType()
	err = dt1.RegisterVoucherResultType(voucherResult)
	require.NoError(t, err)

	t.Run("when request is initiated", func(t *testing.T) {
		_, err := dt1.OpenPushDataChannel(ctx, host2.ID(), voucher, link.(cidlink.Link).Cid, gsData.AllSelector)
		require.NoError(t, err)

		var messageReceived receivedMessage
		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case messageReceived = <-r.messageReceived:
		}
		requestReceived := messageReceived.message.(datatransfer.Request)

		var buf bytes.Buffer
		response, err := message.NewResponse(requestReceived.TransferID(), true, false, voucherResult.Type(), voucherResult)
		require.NoError(t, err)
		err = response.ToNet(&buf)
		require.NoError(t, err)
		extData := buf.Bytes()

		request := gsmsg.NewRequest(graphsync.RequestID(rand.Int31()), link.(cidlink.Link).Cid, gsData.AllSelector, graphsync.Priority(rand.Int31()), graphsync.ExtensionData{
			Name: extension.ExtensionDataTransfer1_1,
			Data: extData,
		})
		builder := gsmsg.NewBuilder()
		builder.AddRequest(request)
		gsmessage, err := builder.Build()
		require.NoError(t, err)
		require.NoError(t, gsData.GsNet2.SendMessage(ctx, host1.ID(), gsmessage))

		status := gsr.consumeResponses(ctx, t)
		require.False(t, gsmsg.IsTerminalFailureCode(status))
	})

	t.Run("when no request is initiated", func(t *testing.T) {
		var buf bytes.Buffer
		response, err := message.NewResponse(datatransfer.TransferID(rand.Uint64()), true, false, voucher.Type(), voucher)
		require.NoError(t, err)
		err = response.ToNet(&buf)
		require.NoError(t, err)
		extData := buf.Bytes()

		request := gsmsg.NewRequest(graphsync.RequestID(rand.Int31()), link.(cidlink.Link).Cid, gsData.AllSelector, graphsync.Priority(rand.Int31()), graphsync.ExtensionData{
			Name: extension.ExtensionDataTransfer1_1,
			Data: extData,
		})
		builder := gsmsg.NewBuilder()
		builder.AddRequest(request)
		gsmessage, err := builder.Build()
		require.NoError(t, err)
		require.NoError(t, gsData.GsNet2.SendMessage(ctx, host1.ID(), gsmessage))

		status := gsr.consumeResponses(ctx, t)
		require.True(t, gsmsg.IsTerminalFailureCode(status))
	})
}

func TestResponseHookWhenExtensionNotFound(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
	host1 := gsData.Host1 // initiator and data sender
	host2 := gsData.Host2 // data recipient, makes graphsync request for data
	voucher := testutil.FakeDTType{Data: "applesauce"}
	link := gsData.LoadUnixFSFile(t, false)

	// setup receiving peer to just record message coming in
	dtnet2 := network.NewFromLibp2pHost(host2)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	dtnet2.SetDelegate(r)

	gsr := &fakeGraphSyncReceiver{
		receivedMessages: make(chan receivedGraphSyncMessage),
	}
	gsData.GsNet2.SetDelegate(gsr)

	gs1 := gsData.SetupGraphsyncHost1()
	tp1 := tp.NewTransport(host1.ID(), gs1, gsData.DtNet1)
	dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
	require.NoError(t, err)
	testutil.StartAndWaitForReady(ctx, t, dt1)
	t.Run("when it's not our extension, does not error and does not validate", func(t *testing.T) {
		//register a hook that validates the request so we don't fail in gs because the request
		//never gets processed
		validateHook := func(p peer.ID, req graphsync.RequestData, ha graphsync.IncomingRequestHookActions) {
			ha.ValidateRequest()
		}
		gs1.RegisterIncomingRequestHook(validateHook)

		_, err := dt1.OpenPushDataChannel(ctx, host2.ID(), &voucher, link.(cidlink.Link).Cid, gsData.AllSelector)
		require.NoError(t, err)

		select {
		case <-ctx.Done():
			t.Fatal("did not receive message sent")
		case <-r.messageReceived:
		}

		request := gsmsg.NewRequest(graphsync.RequestID(rand.Int31()), link.(cidlink.Link).Cid, gsData.AllSelector, graphsync.Priority(rand.Int31()))
		builder := gsmsg.NewBuilder()
		builder.AddRequest(request)
		gsmessage, err := builder.Build()
		require.NoError(t, err)
		require.NoError(t, gsData.GsNet2.SendMessage(ctx, host1.ID(), gsmessage))

		status := gsr.consumeResponses(ctx, t)
		assert.False(t, gsmsg.IsTerminalFailureCode(status))
	})
}

func TestRespondingToPullGraphsyncRequests(t *testing.T) {
	//create network
	ctx := context.Background()
	testCases := map[string]struct {
		test func(*testing.T, *testutil.GraphsyncTestingData, datatransfer.Transport, ipld.Link, datatransfer.TransferID, *fakeGraphSyncReceiver)
	}{
		"When a pull request is initiated and validated": {
			test: func(t *testing.T, gsData *testutil.GraphsyncTestingData, tp2 datatransfer.Transport, link ipld.Link, id datatransfer.TransferID, gsr *fakeGraphSyncReceiver) {
				sv := testutil.NewStubbedValidator()
				sv.ExpectSuccessPull()

				dt1, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
				require.NoError(t, err)
				testutil.StartAndWaitForReady(ctx, t, dt1)
				require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))

				voucher := testutil.NewFakeDTType()
				request, err := message.NewRequest(id, false, true, voucher.Type(), voucher, testutil.GenerateCids(1)[0], gsData.AllSelector)
				require.NoError(t, err)
				buf := new(bytes.Buffer)
				err = request.ToNet(buf)
				require.NoError(t, err)
				extData := buf.Bytes()

				gsRequest := gsmsg.NewRequest(graphsync.RequestID(rand.Int31()), link.(cidlink.Link).Cid, gsData.AllSelector, graphsync.Priority(rand.Int31()), graphsync.ExtensionData{
					Name: extension.ExtensionDataTransfer1_1,
					Data: extData,
				})

				// initiator requests data over graphsync network
				builder := gsmsg.NewBuilder()
				builder.AddRequest(gsRequest)
				gsmessage, err := builder.Build()
				require.NoError(t, err)
				require.NoError(t, gsData.GsNet1.SendMessage(ctx, gsData.Host2.ID(), gsmessage))
				status := gsr.consumeResponses(ctx, t)
				require.False(t, gsmsg.IsTerminalFailureCode(status))
			},
		},
		"When request is initiated, but fails validation": {
			test: func(t *testing.T, gsData *testutil.GraphsyncTestingData, tp2 datatransfer.Transport, link ipld.Link, id datatransfer.TransferID, gsr *fakeGraphSyncReceiver) {
				sv := testutil.NewStubbedValidator()
				sv.ExpectErrorPull()
				dt1, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
				require.NoError(t, err)
				testutil.StartAndWaitForReady(ctx, t, dt1)
				require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))
				voucher := testutil.NewFakeDTType()
				dtRequest, err := message.NewRequest(id, false, true, voucher.Type(), voucher, testutil.GenerateCids(1)[0], gsData.AllSelector)
				require.NoError(t, err)

				buf := new(bytes.Buffer)
				err = dtRequest.ToNet(buf)
				require.NoError(t, err)
				extData := buf.Bytes()
				request := gsmsg.NewRequest(graphsync.RequestID(rand.Int31()), link.(cidlink.Link).Cid, gsData.AllSelector, graphsync.Priority(rand.Int31()), graphsync.ExtensionData{
					Name: extension.ExtensionDataTransfer1_1,
					Data: extData,
				})
				builder := gsmsg.NewBuilder()
				builder.AddRequest(request)
				gsmessage, err := builder.Build()
				require.NoError(t, err)

				// non-initiator requests data over graphsync network, but should not get it
				// because there was no previous request
				require.NoError(t, gsData.GsNet1.SendMessage(ctx, gsData.Host2.ID(), gsmessage))
				status := gsr.consumeResponses(ctx, t)
				require.True(t, gsmsg.IsTerminalFailureCode(status))
			},
		},
	}

	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)

			// setup receiving peer to just record message coming in
			gsr := &fakeGraphSyncReceiver{
				receivedMessages: make(chan receivedGraphSyncMessage),
			}
			gsData.GsNet1.SetDelegate(gsr)

			tp2 := gsData.SetupGSTransportHost2()

			link := gsData.LoadUnixFSFile(t, true)

			id := datatransfer.TransferID(rand.Int31())

			data.test(t, gsData, tp2, link, id, gsr)
		})
	}
}

// Test the ability to attach data from multiple hooks in the same extension payload by using
// different names
func TestMultipleMessagesInExtension(t *testing.T) {
	pausePoints := []uint64{1000, 3000, 6000, 10000, 15000}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
	host1 := gsData.Host1 // initiator, data sender

	root, origBytes := LoadRandomData(ctx, t, gsData.DagService1, 256000)
	gsData.OrigBytes = origBytes
	rootCid := root.(cidlink.Link).Cid
	tp1 := gsData.SetupGSTransportHost1()
	tp2 := gsData.SetupGSTransportHost2()

	dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
	require.NoError(t, err)
	testutil.StartAndWaitForReady(ctx, t, dt1)

	dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
	require.NoError(t, err)
	testutil.StartAndWaitForReady(ctx, t, dt2)

	var chid datatransfer.ChannelID
	errChan := make(chan struct{}, 2)

	clientPausePoint := 0

	clientGotResponse := make(chan struct{}, 1)
	clientFinished := make(chan struct{}, 1)

	// In this retrieval flow we expect 2 voucher results:
	// The first one is sent as a response from the initial request telling the client
	// the provider has accepted the request and is starting to send blocks
	respVoucher := testutil.NewFakeDTType()
	encodedRVR, err := encoding.Encode(respVoucher)
	require.NoError(t, err)

	// voucher results are sent by the providers to request payment while pausing until a voucher is sent
	// to revalidate
	voucherResults := []datatransfer.VoucherResult{
		&testutil.FakeDTType{Data: "one"},
		&testutil.FakeDTType{Data: "two"},
		&testutil.FakeDTType{Data: "thr"},
		&testutil.FakeDTType{Data: "for"},
		&testutil.FakeDTType{Data: "fiv"},
	}

	// The final voucher result is sent by the provider to request a last payment voucher
	finalVoucherResult := testutil.NewFakeDTType()
	encodedFVR, err := encoding.Encode(finalVoucherResult)
	require.NoError(t, err)

	dt2.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			errChan <- struct{}{}
		}
		// Here we verify reception of voucherResults by the client
		if event.Code == datatransfer.NewVoucherResult {
			voucherResult := channelState.LastVoucherResult()
			encodedVR, err := encoding.Encode(voucherResult)
			require.NoError(t, err)

			// If this voucher result is the response voucher no action is needed
			// we just know that the provider has accepted the transfer and is sending blocks
			if bytes.Equal(encodedVR, encodedRVR) {
				// The test will fail if no response voucher is received
				clientGotResponse <- struct{}{}
			}

			// If this voucher is a revalidation request we need to send a new voucher
			// to revalidate and unpause the transfer
			if clientPausePoint < 5 {
				encodedExpected, err := encoding.Encode(voucherResults[clientPausePoint])
				require.NoError(t, err)
				if bytes.Equal(encodedVR, encodedExpected) {
					_ = dt2.SendVoucher(ctx, chid, testutil.NewFakeDTType())
					clientPausePoint++
				}
			}

			// If this voucher result is the final voucher result we need
			// to send a new voucher to unpause the provider and complete the transfer
			if bytes.Equal(encodedVR, encodedFVR) {
				_ = dt2.SendVoucher(ctx, chid, testutil.NewFakeDTType())
			}
		}

		if channelState.Status() == datatransfer.Completed {
			clientFinished <- struct{}{}
		}
	})

	providerFinished := make(chan struct{}, 1)
	dt1.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error {
			errChan <- struct{}{}
		}
		if channelState.Status() == datatransfer.Completed {
			providerFinished <- struct{}{}
		}
	})

	sv := testutil.NewStubbedValidator()
	require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))
	// Stub in the validator so it returns that exact voucher when calling ValidatePull
	// this validator will not pause transfer when accepting a transfer and will start
	// sending blocks immediately
	sv.StubResult(respVoucher)

	srv := &retrievalRevalidator{
		testutil.NewStubbedRevalidator(), 0, 0, pausePoints, finalVoucherResult, voucherResults,
	}
	// The stubbed revalidator will authorize Revalidate and return ErrResume to finisht the transfer
	srv.ExpectSuccessErrResume()
	require.NoError(t, dt1.RegisterRevalidator(testutil.NewFakeDTType(), srv))

	// Register our response voucher with the client
	require.NoError(t, dt2.RegisterVoucherResultType(respVoucher))

	voucher := testutil.FakeDTType{Data: "applesauce"}
	chid, err = dt2.OpenPullDataChannel(ctx, host1.ID(), &voucher, rootCid, gsData.AllSelector)
	require.NoError(t, err)

	// Expect the client to receive a response voucher, the provider to complete the transfer and
	// the client to finish the transfer
	for clientGotResponse != nil || providerFinished != nil || clientFinished != nil {
		select {
		case <-ctx.Done():
			t.Fatal("Did not complete successful data transfer")
		case <-clientGotResponse:
			clientGotResponse = nil
		case <-providerFinished:
			providerFinished = nil
		case <-clientFinished:
			clientFinished = nil
		case <-errChan:
			t.Fatal("received unexpected error")
		}
	}
	sv.VerifyExpectations(t)
	srv.VerifyExpectations(t)
	gsData.VerifyFileTransferred(t, root, true)
}

// completeRevalidator does not pause when sending the last voucher to confirm the deal is completed
type completeRevalidator struct {
	*retrievalRevalidator
}

func (r *completeRevalidator) OnComplete(chid datatransfer.ChannelID) (bool, datatransfer.VoucherResult, error) {
	return true, r.finalVoucher, nil
}

func TestMultipleParallelTransfers(t *testing.T) {
	SetDTLogLevelDebug()

	// Add more sizes here to trigger more transfers.
	sizes := []int{300000, 256000, 200000, 256000}

	ctx := context.Background()

	gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
	host1 := gsData.Host1 // initiator, data sender

	tp1 := gsData.SetupGSTransportHost1()
	tp2 := gsData.SetupGSTransportHost2()

	dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
	require.NoError(t, err)
	testutil.StartAndWaitForReady(ctx, t, dt1)

	dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
	require.NoError(t, err)
	testutil.StartAndWaitForReady(ctx, t, dt2)

	// In this retrieval flow we expect 2 voucher results:
	// The first one is sent as a response from the initial request telling the client
	// the provider has accepted the request and is starting to send blocks
	respVoucher := testutil.NewFakeDTType()
	encodedRVR, err := encoding.Encode(respVoucher)
	require.NoError(t, err)

	// The final voucher result is sent by the provider to let the client know the deal is completed
	finalVoucherResult := testutil.NewFakeDTType()
	encodedFVR, err := encoding.Encode(finalVoucherResult)
	require.NoError(t, err)

	sv := testutil.NewStubbedValidator()
	require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))
	// Stub in the validator so it returns that exact voucher when calling ValidatePull
	// this validator will not pause transfer when accepting a transfer and will start
	// sending blocks immediately
	sv.StubResult(respVoucher)

	// no need for intermediary voucher results
	voucherResults := []datatransfer.VoucherResult{}

	pausePoints := []uint64{}
	srv := &retrievalRevalidator{
		testutil.NewStubbedRevalidator(), 0, 0, pausePoints, finalVoucherResult, voucherResults,
	}
	srv.ExpectSuccessErrResume()
	require.NoError(t, dt1.RegisterRevalidator(testutil.NewFakeDTType(), srv))

	// Register our response voucher with the client
	require.NoError(t, dt2.RegisterVoucherResultType(respVoucher))

	// for each size we create a new random DAG of the given size and try to retrieve it
	for _, size := range sizes {
		size := size
		t.Run(fmt.Sprintf("size %d", size), func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
			defer cancel()

			errChan := make(chan struct{}, 2)

			clientGotResponse := make(chan struct{}, 1)
			clientFinished := make(chan struct{}, 1)

			var chid datatransfer.ChannelID
			chidReceived := make(chan struct{})
			dt2.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				<-chidReceived
				if chid != channelState.ChannelID() {
					return
				}
				if event.Code == datatransfer.Error {
					errChan <- struct{}{}
				}
				// Here we verify reception of voucherResults by the client
				if event.Code == datatransfer.NewVoucherResult {
					voucherResult := channelState.LastVoucherResult()
					encodedVR, err := encoding.Encode(voucherResult)
					require.NoError(t, err)

					// If this voucher result is the response voucher no action is needed
					// we just know that the provider has accepted the transfer and is sending blocks
					if bytes.Equal(encodedVR, encodedRVR) {
						// The test will fail if no response voucher is received
						clientGotResponse <- struct{}{}
					}

					// If this voucher result is the final voucher result we need
					// to send a new voucher to unpause the provider and complete the transfer
					if bytes.Equal(encodedVR, encodedFVR) {
						_ = dt2.SendVoucher(ctx, chid, testutil.NewFakeDTType())
					}
				}

				if channelState.Status() == datatransfer.Completed {
					clientFinished <- struct{}{}
				}
			})

			providerFinished := make(chan struct{}, 1)
			dt1.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				<-chidReceived
				if chid != channelState.ChannelID() {
					return
				}
				if event.Code == datatransfer.Error {
					fmt.Println(event.Message)
					errChan <- struct{}{}
				}
				if channelState.Status() == datatransfer.Completed {
					providerFinished <- struct{}{}
				}
			})

			root, origBytes := LoadRandomData(ctx, t, gsData.DagService1, size)
			rootCid := root.(cidlink.Link).Cid

			voucher := testutil.NewFakeDTType()
			chid, err = dt2.OpenPullDataChannel(ctx, host1.ID(), voucher, rootCid, gsData.AllSelector)
			require.NoError(t, err)
			close(chidReceived)
			// Expect the client to receive a response voucher, the provider to complete the transfer and
			// the client to finish the transfer
			for clientGotResponse != nil || providerFinished != nil || clientFinished != nil {
				select {
				case <-ctx.Done():
					reason := "Did not complete successful data transfer"
					switch true {
					case clientGotResponse != nil:
						reason = "client did not get initial response"
					case clientFinished != nil:
						reason = "client did not finish"
					case providerFinished != nil:
						reason = "provider did not finish"
					}
					t.Fatal(reason)
				case <-clientGotResponse:
					clientGotResponse = nil
				case <-providerFinished:
					providerFinished = nil
				case <-clientFinished:
					clientFinished = nil
				case <-errChan:
					t.Fatal("received unexpected error")
				}
			}
			sv.VerifyExpectations(t)
			srv.VerifyExpectations(t)
			testutil.VerifyHasFile(gsData.Ctx, t, gsData.DagService2, root, origBytes)
		})
	}
}

func LoadRandomData(ctx context.Context, t *testing.T, dagService ipldformat.DAGService, size int) (ipld.Link, []byte) {
	data := make([]byte, size)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)

	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dagService)

	params := ihelper.DagBuilderParams{
		Maxlinks:   1024,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	db, err := params.New(chunker.NewSizeSplitter(bytes.NewReader(data), 128000))
	require.NoError(t, err)

	nd, err := balanced.Layout(db)
	require.NoError(t, err)

	err = bufferedDS.Commit()
	require.NoError(t, err)

	// save the original files bytes
	return cidlink.Link{Cid: nd.Cid()}, data
}

type receivedMessage struct {
	message datatransfer.Message
	sender  peer.ID
}

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type receiver struct {
	messageReceived chan receivedMessage
}

func (r *receiver) ReceiveRequest(
	ctx context.Context,
	sender peer.ID,
	incoming datatransfer.Request) {

	select {
	case <-ctx.Done():
	case r.messageReceived <- receivedMessage{incoming, sender}:
	}
}

func (r *receiver) ReceiveResponse(
	ctx context.Context,
	sender peer.ID,
	incoming datatransfer.Response) {

	select {
	case <-ctx.Done():
	case r.messageReceived <- receivedMessage{incoming, sender}:
	}
}

func (r *receiver) ReceiveError(err error) {
}

func (r *receiver) ReceiveRestartExistingChannelRequest(ctx context.Context,
	sender peer.ID,
	incoming datatransfer.Request) {

}

func SetDTLogLevelDebug() {
	_ = logging.SetLogLevel("dt-impl", "debug")
	_ = logging.SetLogLevel("dt-chanmon", "debug")
	_ = logging.SetLogLevel("dt_graphsync", "debug")
	_ = logging.SetLogLevel("data-transfer", "debug")
	_ = logging.SetLogLevel("data_transfer_network", "debug")
}
