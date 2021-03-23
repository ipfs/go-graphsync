package impl_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	. "github.com/filecoin-project/go-data-transfer/impl"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

const totalIncrements = 204
const expectedTransferSize = 217452

// has 204 chunks/blocks
const largeFile = "lorem_large.txt"

type peerError struct {
	p   peer.ID
	err error
}

func TestRestartPush(t *testing.T) {
	tcs := map[string]struct {
		stopAt    int
		openPushF func(rh *restartHarness) datatransfer.ChannelID
		restartF  func(rh *restartHarness, chId datatransfer.ChannelID, subFnc datatransfer.Subscriber)
	}{
		"Restart peer create push": {
			stopAt: 20,
			openPushF: func(rh *restartHarness) datatransfer.ChannelID {
				voucher := testutil.FakeDTType{Data: "applesauce"}
				chid, err := rh.dt1.OpenPushDataChannel(rh.testCtx, rh.peer2, &voucher, rh.rootCid, rh.gsData.AllSelector)
				require.NoError(rh.t, err)
				return chid
			},
			restartF: func(rh *restartHarness, chId datatransfer.ChannelID, subscriber datatransfer.Subscriber) {
				var err error
				require.NoError(t, rh.dt1.Stop(rh.testCtx))
				time.Sleep(100 * time.Millisecond)
				tp1 := rh.gsData.SetupGSTransportHost1()
				rh.dt1, err = NewDataTransfer(rh.gsData.DtDs1, rh.gsData.TempDir1, rh.gsData.DtNet1, tp1)
				require.NoError(rh.t, err)
				require.NoError(rh.t, rh.dt1.RegisterVoucherType(&testutil.FakeDTType{}, rh.sv))
				testutil.StartAndWaitForReady(rh.testCtx, t, rh.dt1)
				rh.dt1.SubscribeToEvents(subscriber)
				require.NoError(rh.t, rh.dt1.RestartDataTransferChannel(rh.testCtx, chId))
			},
		},
		"Restart peer receive push": {
			stopAt: 20,
			openPushF: func(rh *restartHarness) datatransfer.ChannelID {
				voucher := testutil.FakeDTType{Data: "applesauce"}
				chid, err := rh.dt1.OpenPushDataChannel(rh.testCtx, rh.peer2, &voucher, rh.rootCid, rh.gsData.AllSelector)
				require.NoError(rh.t, err)
				return chid
			},
			restartF: func(rh *restartHarness, chId datatransfer.ChannelID, subscriber datatransfer.Subscriber) {
				var err error
				require.NoError(t, rh.dt2.Stop(rh.testCtx))
				time.Sleep(100 * time.Millisecond)
				tp2 := rh.gsData.SetupGSTransportHost2()
				rh.dt2, err = NewDataTransfer(rh.gsData.DtDs2, rh.gsData.TempDir2, rh.gsData.DtNet2, tp2)
				require.NoError(rh.t, err)
				require.NoError(rh.t, rh.dt2.RegisterVoucherType(&testutil.FakeDTType{}, rh.sv))
				testutil.StartAndWaitForReady(rh.testCtx, t, rh.dt2)
				rh.dt2.SubscribeToEvents(subscriber)
				require.NoError(rh.t, rh.dt2.RestartDataTransferChannel(rh.testCtx, chId))
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			// CREATE HARNESS
			rh := newRestartHarness(t)
			defer rh.cancel()

			// START DATA TRANSFER INSTANCES
			rh.sv.ExpectSuccessPush()
			testutil.StartAndWaitForReady(rh.testCtx, t, rh.dt1)
			testutil.StartAndWaitForReady(rh.testCtx, t, rh.dt2)

			// SETUP DATA TRANSFER SUBSCRIBERS AND SUBSCRIBE
			finished := make(chan peer.ID, 2)
			errChan := make(chan *peerError, 2)
			queued := make(chan uint64, totalIncrements*2)
			sent := make(chan uint64, totalIncrements*2)
			received := make(chan uint64, totalIncrements*2)
			receivedTillNow := atomic.NewInt32(0)

			// counters we will check at the end for correctness
			opens := atomic.NewInt32(0)
			var finishedPeersLk sync.Mutex
			var finishedPeers []peer.ID
			disConnChan := make(chan struct{})

			var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if event.Code == datatransfer.DataQueued {
					if channelState.Queued() > 0 {
						queued <- channelState.Queued()
					}
				}

				if event.Code == datatransfer.DataSent {
					if channelState.Sent() > 0 {
						sent <- channelState.Sent()
					}
				}

				// disconnect and unlink the peers after we've received the required number of increments
				if event.Code == datatransfer.DataReceived {
					if channelState.Received() > 0 {
						receivedTillNow.Inc()
						received <- channelState.Received()
						if receivedTillNow.Load() == int32(tc.stopAt) {
							require.NoError(t, rh.gsData.Mn.UnlinkPeers(rh.peer1, rh.peer2))
							require.NoError(t, rh.gsData.Mn.DisconnectPeers(rh.peer1, rh.peer2))
							disConnChan <- struct{}{}
						}
					}
				}
				if channelState.Status() == datatransfer.Completed {
					finishedPeersLk.Lock()
					finishedPeers = append(finishedPeers, channelState.SelfPeer())
					finishedPeersLk.Unlock()
					finished <- channelState.SelfPeer()
				}
				if event.Code == datatransfer.Error {
					err := xerrors.New(channelState.Message())
					errChan <- &peerError{channelState.SelfPeer(), err}
				}
				if event.Code == datatransfer.Open {
					opens.Inc()
				}

				if event.Code == datatransfer.Restart {
					t.Logf("got restart event for peer %s", channelState.SelfPeer().Pretty())
				}
			}
			rh.dt1.SubscribeToEvents(subscriber)
			rh.dt2.SubscribeToEvents(subscriber)

			// OPEN PUSH
			chid := tc.openPushF(rh)
			// wait for disconnection to happen
			<-disConnChan
			t.Logf("peers unlinked and disconnected, total increments received till now: %d", receivedTillNow.Load())

			// Define function to wait for data transfer to complete
			waitF := func(wait time.Duration, nCompletes int) (sentI, receivedI []uint64, err error) {
				completes := 0

				waitCtx, cancel := context.WithTimeout(rh.testCtx, wait)
				defer cancel()
				for completes < nCompletes {
					select {
					case <-waitCtx.Done():
						return sentI, receivedI, xerrors.New("context timed-out without completing data transfer")
					case p := <-finished:
						t.Logf("peer %s completed", p.Pretty())
						completes++
					case perr := <-errChan:
						t.Fatalf("\n received error on peer %s, err: %v", perr.p.Pretty(), perr.err)
					case s := <-queued:
						sentI = append(sentI, s)
					case r := <-received:
						receivedI = append(receivedI, r)
					}
				}

				return sentI, receivedI, nil
			}

			// WAIT FOR TRANSFER TO COMPLETE -> THIS SHOULD NOT HAPPEN
			sentI, receivedI, err := waitF(2*time.Second, 1)
			require.EqualError(t, err, "context timed-out without completing data transfer")
			require.True(t, len(receivedI) < totalIncrements)
			require.NotEmpty(t, sentI)
			t.Logf("not request was completed after disconnect")

			// Connect the peers and restart
			require.NoError(t, rh.gsData.Mn.LinkAll())
			// let linking take effect
			conn, err := rh.gsData.Mn.ConnectPeers(rh.peer1, rh.peer2)
			require.NoError(t, err)
			require.NotNil(t, conn)
			tc.restartF(rh, chid, subscriber)
			t.Logf("peers have been connected and datatransfer has restarted")

			// WAIT FOR DATA TRANSFER TO FINISH -> SHOULD WORK NOW
			// we should get 2 completes
			_, _, err = waitF(10*time.Second, 2)
			require.NoError(t, err)

			// verify all cids are present on the receiver
			chs, err := rh.dt2.InProgressChannels(rh.testCtx)
			require.NoError(t, err)
			require.Len(t, chs, 1)
			cids := chs[chid].ReceivedCids()
			set := cid.NewSet()
			for _, c := range cids {
				set.Add(c)
			}
			require.Equal(t, totalIncrements, set.Len())

			testutil.VerifyHasFile(rh.testCtx, t, rh.destDagService, rh.root, rh.origBytes)
			rh.sv.VerifyExpectations(t)

			// we should ONLY see two opens and two completes
			require.EqualValues(t, 2, opens.Load())
			finishedPeersLk.Lock()
			require.Len(t, finishedPeers, 2)
			require.Contains(t, finishedPeers, rh.peer1)
			require.Contains(t, finishedPeers, rh.peer2)
			finishedPeersLk.Unlock()

			sendChan, err := rh.dt1.ChannelState(context.Background(), chid)
			require.NoError(t, err)
			recvChan, err := rh.dt2.ChannelState(context.Background(), chid)
			require.NoError(t, err)
			require.Equal(t, expectedTransferSize, int(sendChan.Queued()))
			require.Equal(t, expectedTransferSize, int(sendChan.Sent()))
			require.Equal(t, expectedTransferSize, int(recvChan.Received()))
		})
	}
}

func TestRestartPull(t *testing.T) {
	tcs := map[string]struct {
		stopAt    int
		openPullF func(rh *restartHarness) datatransfer.ChannelID
		restartF  func(rh *restartHarness, chId datatransfer.ChannelID, subFnc datatransfer.Subscriber)
	}{
		"Restart peer create pull": {
			stopAt: 40,
			openPullF: func(rh *restartHarness) datatransfer.ChannelID {
				voucher := testutil.FakeDTType{Data: "applesauce"}
				chid, err := rh.dt2.OpenPullDataChannel(rh.testCtx, rh.peer1, &voucher, rh.rootCid, rh.gsData.AllSelector)
				require.NoError(rh.t, err)
				return chid
			},
			restartF: func(rh *restartHarness, chId datatransfer.ChannelID, subscriber datatransfer.Subscriber) {
				var err error
				require.NoError(t, rh.dt2.Stop(rh.testCtx))
				time.Sleep(100 * time.Millisecond)
				tp2 := rh.gsData.SetupGSTransportHost2()
				rh.dt2, err = NewDataTransfer(rh.gsData.DtDs2, rh.gsData.TempDir2, rh.gsData.DtNet2, tp2)
				require.NoError(rh.t, err)
				require.NoError(rh.t, rh.dt2.RegisterVoucherType(&testutil.FakeDTType{}, rh.sv))
				testutil.StartAndWaitForReady(rh.testCtx, t, rh.dt2)
				rh.dt2.SubscribeToEvents(subscriber)
				require.NoError(rh.t, rh.dt2.RestartDataTransferChannel(rh.testCtx, chId))
			},
		},
		"Restart peer receive pull": {
			stopAt: 40,
			openPullF: func(rh *restartHarness) datatransfer.ChannelID {
				voucher := testutil.FakeDTType{Data: "applesauce"}
				chid, err := rh.dt2.OpenPullDataChannel(rh.testCtx, rh.peer1, &voucher, rh.rootCid, rh.gsData.AllSelector)
				require.NoError(rh.t, err)
				return chid
			},
			restartF: func(rh *restartHarness, chId datatransfer.ChannelID, subscriber datatransfer.Subscriber) {
				var err error
				require.NoError(t, rh.dt1.Stop(rh.testCtx))
				time.Sleep(100 * time.Millisecond)
				tp1 := rh.gsData.SetupGSTransportHost1()
				rh.dt1, err = NewDataTransfer(rh.gsData.DtDs1, rh.gsData.TempDir1, rh.gsData.DtNet1, tp1)
				require.NoError(rh.t, err)
				require.NoError(rh.t, rh.dt1.RegisterVoucherType(&testutil.FakeDTType{}, rh.sv))
				testutil.StartAndWaitForReady(rh.testCtx, t, rh.dt1)
				rh.dt1.SubscribeToEvents(subscriber)
				require.NoError(rh.t, rh.dt1.RestartDataTransferChannel(rh.testCtx, chId))
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			// CREATE HARNESS
			rh := newRestartHarness(t)
			defer rh.cancel()

			// START DATA TRANSFER INSTANCES
			rh.sv.ExpectSuccessPull()
			testutil.StartAndWaitForReady(rh.testCtx, t, rh.dt1)
			testutil.StartAndWaitForReady(rh.testCtx, t, rh.dt2)

			// SETUP DATA TRANSFER SUBSCRIBERS AND SUBSCRIBE
			finished := make(chan peer.ID, 2)
			errChan := make(chan *peerError, 2)
			sent := make(chan uint64, totalIncrements)
			received := make(chan uint64, totalIncrements)
			receivedTillNow := atomic.NewInt32(0)

			// counters we will check at the end for correctness
			opens := atomic.NewInt32(0)
			var finishedPeersLk sync.Mutex
			var finishedPeers []peer.ID
			disConnChan := make(chan struct{})

			var subscriber datatransfer.Subscriber = func(event datatransfer.Event, channelState datatransfer.ChannelState) {
				if event.Code == datatransfer.DataQueued {
					if channelState.Queued() > 0 {
						sent <- channelState.Queued()
					}
				}

				// disconnect and unlink the peers after we've received the required number of increments
				if event.Code == datatransfer.DataReceived {
					if channelState.Received() > 0 {
						receivedTillNow.Inc()
						received <- channelState.Received()
						if receivedTillNow.Load() == int32(tc.stopAt) {
							require.NoError(t, rh.gsData.Mn.UnlinkPeers(rh.peer1, rh.peer2))
							require.NoError(t, rh.gsData.Mn.DisconnectPeers(rh.peer1, rh.peer2))
							disConnChan <- struct{}{}
						}
					}
				}

				if channelState.Status() == datatransfer.Completed {
					finishedPeersLk.Lock()
					finishedPeers = append(finishedPeers, channelState.SelfPeer())
					finishedPeersLk.Unlock()
					finished <- channelState.SelfPeer()
				}
				if event.Code == datatransfer.Error {
					err := xerrors.New(channelState.Message())
					errChan <- &peerError{channelState.SelfPeer(), err}
				}
				if event.Code == datatransfer.Open {
					opens.Inc()
				}

				if event.Code == datatransfer.Restart {
					t.Logf("got restart event for peer %s", channelState.SelfPeer().Pretty())
				}
			}
			rh.dt1.SubscribeToEvents(subscriber)
			rh.dt2.SubscribeToEvents(subscriber)

			// OPEN pull
			chid := tc.openPullF(rh)

			// wait for disconnection to happen
			select {
			case <-time.After(10 * time.Second):
				t.Fatal("did not hear a diconnection: test timed out")
			case <-disConnChan:
				t.Logf("peers unlinked and disconnected, total increments received till now: %d", receivedTillNow.Load())
			}
			// Define function to wait for data transfer to complete
			waitF := func(wait time.Duration, nCompletes int) (sentI, receivedI []uint64, err error) {
				completes := 0

				waitCtx, cancel := context.WithTimeout(rh.testCtx, wait)
				defer cancel()
				for completes < nCompletes {
					select {
					case <-waitCtx.Done():
						return sentI, receivedI, xerrors.New("context timed-out without completing data transfer")
					case p := <-finished:
						t.Logf("peer %s completed", p.Pretty())
						completes++
					case perr := <-errChan:
						t.Fatalf("\n received error on peer %s, err: %v", perr.p.Pretty(), perr.err)
					case s := <-sent:
						sentI = append(sentI, s)
					case r := <-received:
						receivedI = append(receivedI, r)
					}
				}

				return sentI, receivedI, nil
			}

			// WAIT FOR TRANSFER TO COMPLETE -> THIS SHOULD NOT HAPPEN
			sentI, receivedI, err := waitF(1*time.Second, 1)
			require.EqualError(t, err, "context timed-out without completing data transfer")
			require.True(t, len(receivedI) < totalIncrements)
			require.NotEmpty(t, sentI)

			// Connect the peers and restart
			require.NoError(t, rh.gsData.Mn.LinkAll())
			conn, err := rh.gsData.Mn.ConnectPeers(rh.peer1, rh.peer2)
			require.NoError(t, err)
			require.NotNil(t, conn)
			tc.restartF(rh, chid, subscriber)
			t.Logf("peers have been connected and datatransfer has restarted")

			// WAIT FOR DATA TRANSFER TO FINISH -> SHOULD WORK NOW
			// we should get 2 completes
			_, _, err = waitF(10*time.Second, 2)
			require.NoError(t, err)

			// verify all cids are present on the receiver
			chs, err := rh.dt2.InProgressChannels(rh.testCtx)
			require.NoError(t, err)
			require.Len(t, chs, 1)
			cids := chs[chid].ReceivedCids()
			set := cid.NewSet()
			for _, c := range cids {
				set.Add(c)
			}
			require.Equal(t, totalIncrements, set.Len())

			testutil.VerifyHasFile(rh.testCtx, t, rh.destDagService, rh.root, rh.origBytes)
			rh.sv.VerifyExpectations(t)

			// we should ONLY see two opens and two completes
			require.EqualValues(t, 2, opens.Load())
			finishedPeersLk.Lock()
			require.Len(t, finishedPeers, 2)
			require.Contains(t, finishedPeers, rh.peer1)
			require.Contains(t, finishedPeers, rh.peer2)
			finishedPeersLk.Unlock()

			sendChan, err := rh.dt1.ChannelState(context.Background(), chid)
			require.NoError(t, err)
			recvChan, err := rh.dt2.ChannelState(context.Background(), chid)
			require.NoError(t, err)
			require.Equal(t, expectedTransferSize, int(sendChan.Queued()))
			require.Equal(t, expectedTransferSize, int(sendChan.Sent()))
			require.Equal(t, expectedTransferSize, int(recvChan.Received()))
		})
	}
}

type restartHarness struct {
	t       *testing.T
	testCtx context.Context
	cancel  context.CancelFunc

	peer1 peer.ID
	peer2 peer.ID

	gsData *testutil.GraphsyncTestingData
	dt1    datatransfer.Manager
	dt2    datatransfer.Manager
	sv     *testutil.StubbedValidator

	origBytes      []byte
	root           ipld.Link
	rootCid        cid.Cid
	destDagService ipldformat.DAGService
}

func newRestartHarness(t *testing.T) *restartHarness {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)

	// Setup host
	gsData := testutil.NewGraphsyncTestingData(ctx, t, nil, nil)
	host1 := gsData.Host1 // initiator, data sender
	host2 := gsData.Host2 // data recipient
	peer1 := host1.ID()
	peer2 := host2.ID()
	t.Logf("peer1 is %s", peer1.Pretty())
	t.Logf("peer2 is %s", peer2.Pretty())

	// Setup data transfer
	tp1 := gsData.SetupGSTransportHost1()
	tp2 := gsData.SetupGSTransportHost2()

	dt1, err := NewDataTransfer(gsData.DtDs1, gsData.TempDir1, gsData.DtNet1, tp1)
	require.NoError(t, err)

	dt2, err := NewDataTransfer(gsData.DtDs2, gsData.TempDir2, gsData.DtNet2, tp2)
	require.NoError(t, err)

	sv := testutil.NewStubbedValidator()
	require.NoError(t, dt1.RegisterVoucherType(&testutil.FakeDTType{}, sv))
	require.NoError(t, dt2.RegisterVoucherType(&testutil.FakeDTType{}, sv))

	sourceDagService := gsData.DagService1
	root, origBytes := testutil.LoadUnixFSFile(ctx, t, sourceDagService, largeFile)
	rootCid := root.(cidlink.Link).Cid
	destDagService := gsData.DagService2

	return &restartHarness{
		t:       t,
		testCtx: ctx,
		cancel:  cancel,

		peer1: peer1,
		peer2: peer2,

		gsData: gsData,
		dt1:    dt1,
		dt2:    dt2,
		sv:     sv,

		origBytes:      origBytes,
		root:           root,
		rootCid:        rootCid,
		destDagService: destDagService,
	}
}
