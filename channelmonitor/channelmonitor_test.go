package channelmonitor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

var ch1 = datatransfer.ChannelID{
	Initiator: "initiator",
	Responder: "responder",
	ID:        1,
}

func TestPushChannelMonitorAutoRestart(t *testing.T) {
	type testCase struct {
		name         string
		errOnRestart bool
		dataQueued   uint64
		dataSent     uint64
		errorEvent   bool
	}
	testCases := []testCase{{
		name:         "attempt restart",
		errOnRestart: false,
		dataQueued:   10,
		dataSent:     5,
	}, {
		name:         "fail attempt restart",
		errOnRestart: true,
		dataQueued:   10,
		dataSent:     5,
	}, {
		name:         "error event",
		errOnRestart: false,
		dataQueued:   10,
		dataSent:     10,
		errorEvent:   true,
	}, {
		name:         "error event then fail attempt restart",
		errOnRestart: true,
		dataQueued:   10,
		dataSent:     10,
		errorEvent:   true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &mockChannelState{chid: ch1}
			mockAPI := newMockMonitorAPI(ch, tc.errOnRestart)

			m := NewMonitor(mockAPI, &Config{
				MonitorPushChannels:    true,
				AcceptTimeout:          time.Hour,
				Interval:               10 * time.Millisecond,
				ChecksPerInterval:      10,
				MinBytesTransferred:    1,
				MaxConsecutiveRestarts: 3,
				CompleteTimeout:        time.Hour,
			})
			m.Start()
			mch := m.AddPushChannel(ch1).(*monitoredPushChannel)

			// Simulate the responder sending Accept
			mockAPI.accept()

			// Simulate data being queued and sent
			// If sent - queued > MinBytesTransferred it should cause a restart
			mockAPI.dataQueued(tc.dataQueued)
			mockAPI.dataSent(tc.dataSent)

			if tc.errorEvent {
				// Fire an error event, should cause a restart
				mockAPI.sendDataErrorEvent()
			}

			if tc.errOnRestart {
				// If there is an error attempting to restart, just wait for
				// the push channel to be closed
				<-mockAPI.closed
				return
			}

			// Verify that channel is restarted within interval
			select {
			case <-time.After(100 * time.Millisecond):
				require.Fail(t, "failed to restart channel")
			case <-mockAPI.restarts:
			}

			// Simulate sending the remaining data
			delta := tc.dataQueued - tc.dataSent
			if delta > 0 {
				mockAPI.dataSent(delta)
			}

			// Simulate the complete event
			mockAPI.completed()

			// Verify that channel has been shutdown
			verifyChannelShutdown(t, mch.ctx)
		})
	}
}

func TestPullChannelMonitorAutoRestart(t *testing.T) {
	type testCase struct {
		name         string
		errOnRestart bool
		dataRcvd     uint64
		errorEvent   bool
	}
	testCases := []testCase{{
		name:         "attempt restart",
		errOnRestart: false,
		dataRcvd:     10,
	}, {
		name:         "fail attempt restart",
		errOnRestart: true,
		dataRcvd:     10,
	}, {
		name:         "error event",
		errOnRestart: false,
		dataRcvd:     10,
		errorEvent:   true,
	}, {
		name:         "error event then fail attempt restart",
		errOnRestart: true,
		dataRcvd:     10,
		errorEvent:   true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &mockChannelState{chid: ch1}
			mockAPI := newMockMonitorAPI(ch, tc.errOnRestart)

			m := NewMonitor(mockAPI, &Config{
				MonitorPullChannels:    true,
				AcceptTimeout:          time.Hour,
				Interval:               10 * time.Millisecond,
				ChecksPerInterval:      10,
				MinBytesTransferred:    1,
				MaxConsecutiveRestarts: 3,
				CompleteTimeout:        time.Hour,
			})
			m.Start()
			mch := m.AddPullChannel(ch1).(*monitoredPullChannel)

			// Simulate the responder sending Accept
			mockAPI.accept()

			// Simulate receiving some data
			mockAPI.dataReceived(tc.dataRcvd)

			if tc.errorEvent {
				// Fire an error event, should cause a restart
				mockAPI.sendDataErrorEvent()
			}

			if tc.errOnRestart {
				// If there is an error attempting to restart, just wait for
				// the pull channel to be closed
				<-mockAPI.closed
				return
			}

			// Verify that channel is restarted within interval
			select {
			case <-time.After(100 * time.Millisecond):
				require.Fail(t, "failed to restart channel")
			case <-mockAPI.restarts:
			}

			// Simulate sending more data
			mockAPI.dataSent(tc.dataRcvd)

			// Simulate the complete event
			mockAPI.completed()

			// Verify that channel has been shutdown
			verifyChannelShutdown(t, mch.ctx)
		})
	}
}

func TestPushChannelMonitorDataRate(t *testing.T) {
	type dataPoint struct {
		queued uint64
		sent   uint64
	}
	type testCase struct {
		name          string
		minBytesSent  uint64
		dataPoints    []dataPoint
		expectRestart bool
	}
	testCases := []testCase{{
		name:         "restart when sent (10) < pending (20)",
		minBytesSent: 1,
		dataPoints: []dataPoint{{
			queued: 20,
			sent:   10,
		}},
		expectRestart: true,
	}, {
		name:         "dont restart when sent (20) >= pending (10)",
		minBytesSent: 1,
		dataPoints: []dataPoint{{
			queued: 20,
			sent:   10,
		}, {
			queued: 20,
			sent:   20,
		}},
		expectRestart: false,
	}, {
		name:         "restart when sent (5) < pending (10)",
		minBytesSent: 10,
		dataPoints: []dataPoint{{
			queued: 20,
			sent:   10,
		}, {
			queued: 20,
			sent:   15,
		}},
		expectRestart: true,
	}, {
		name:         "dont restart when pending is zero",
		minBytesSent: 1,
		dataPoints: []dataPoint{{
			queued: 20,
			sent:   20,
		}},
		expectRestart: false,
	}, {
		name:         "dont restart when pending increases but sent also increases within interval",
		minBytesSent: 1,
		dataPoints: []dataPoint{{
			queued: 10,
			sent:   10,
		}, {
			queued: 20,
			sent:   10,
		}, {
			queued: 20,
			sent:   20,
		}},
		expectRestart: false,
	}, {
		name:         "restart when pending increases and sent doesn't increase within interval",
		minBytesSent: 1,
		dataPoints: []dataPoint{{
			queued: 10,
			sent:   10,
		}, {
			queued: 20,
			sent:   10,
		}, {
			queued: 20,
			sent:   10,
		}, {
			queued: 20,
			sent:   10,
		}, {
			queued: 20,
			sent:   20,
		}},
		expectRestart: true,
	}, {
		name:         "dont restart with typical progression",
		minBytesSent: 1,
		dataPoints: []dataPoint{{
			queued: 10,
			sent:   10,
		}, {
			queued: 20,
			sent:   10,
		}, {
			queued: 20,
			sent:   15,
		}, {
			queued: 30,
			sent:   25,
		}, {
			queued: 35,
			sent:   30,
		}, {
			queued: 35,
			sent:   35,
		}},
		expectRestart: false,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &mockChannelState{chid: ch1}
			mockAPI := newMockMonitorAPI(ch, false)

			checksPerInterval := uint32(1)
			m := NewMonitor(mockAPI, &Config{
				MonitorPushChannels:    true,
				AcceptTimeout:          time.Hour,
				Interval:               time.Hour,
				ChecksPerInterval:      checksPerInterval,
				MinBytesTransferred:    tc.minBytesSent,
				MaxConsecutiveRestarts: 3,
				CompleteTimeout:        time.Hour,
			})

			// Note: Don't start monitor, we'll call checkDataRate() manually

			m.AddPushChannel(ch1)

			totalChecks := checksPerInterval + uint32(len(tc.dataPoints))
			for i := uint32(0); i < totalChecks; i++ {
				if i < uint32(len(tc.dataPoints)) {
					dp := tc.dataPoints[i]
					mockAPI.dataQueued(dp.queued)
					mockAPI.dataSent(dp.sent)
				}
				m.checkDataRate()
			}

			// Check if channel was restarted
			select {
			case <-time.After(5 * time.Millisecond):
				if tc.expectRestart {
					require.Fail(t, "failed to restart channel")
				}
			case <-mockAPI.restarts:
				if !tc.expectRestart {
					require.Fail(t, "expected no channel restart")
				}
			}
		})
	}
}

func TestPullChannelMonitorDataRate(t *testing.T) {
	type testCase struct {
		name                string
		minBytesTransferred uint64
		dataPoints          []uint64
		expectRestart       bool
	}
	testCases := []testCase{{
		name:                "restart when received (5) < min required (10)",
		minBytesTransferred: 10,
		dataPoints:          []uint64{10, 15},
		expectRestart:       true,
	}, {
		name:                "dont restart when received (5) > min required (1)",
		minBytesTransferred: 1,
		dataPoints:          []uint64{10, 15},
		expectRestart:       false,
	}, {
		name:                "dont restart with typical progression",
		minBytesTransferred: 1,
		dataPoints:          []uint64{10, 20, 30, 40},
		expectRestart:       false,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &mockChannelState{chid: ch1}
			mockAPI := newMockMonitorAPI(ch, false)

			checksPerInterval := uint32(1)
			m := NewMonitor(mockAPI, &Config{
				MonitorPullChannels:    true,
				AcceptTimeout:          time.Hour,
				Interval:               time.Hour,
				ChecksPerInterval:      checksPerInterval,
				MinBytesTransferred:    tc.minBytesTransferred,
				MaxConsecutiveRestarts: 3,
				CompleteTimeout:        time.Hour,
			})

			// Note: Don't start monitor, we'll call checkDataRate() manually

			m.AddPullChannel(ch1)

			totalChecks := uint32(len(tc.dataPoints))
			for i := uint32(0); i < totalChecks; i++ {
				if i < uint32(len(tc.dataPoints)) {
					rcvd := tc.dataPoints[i]
					mockAPI.dataReceived(rcvd)
				}
				m.checkDataRate()
			}

			// Check if channel was restarted
			select {
			case <-time.After(5 * time.Millisecond):
				if tc.expectRestart {
					require.Fail(t, "failed to restart channel")
				}
			case <-mockAPI.restarts:
				if !tc.expectRestart {
					require.Fail(t, "expected no channel restart")
				}
			}
		})
	}
}

func TestChannelMonitorMaxConsecutiveRestarts(t *testing.T) {
	runTest := func(name string, isPush bool) {
		t.Run(name, func(t *testing.T) {
			ch := &mockChannelState{chid: ch1}
			mockAPI := newMockMonitorAPI(ch, false)

			maxConsecutiveRestarts := 3
			m := NewMonitor(mockAPI, &Config{
				MonitorPushChannels:    isPush,
				MonitorPullChannels:    !isPush,
				AcceptTimeout:          time.Hour,
				Interval:               time.Hour,
				ChecksPerInterval:      1,
				MinBytesTransferred:    2,
				MaxConsecutiveRestarts: uint32(maxConsecutiveRestarts),
				CompleteTimeout:        time.Hour,
			})

			// Note: Don't start monitor, we'll call checkDataRate() manually

			var chanCtx context.Context
			if isPush {
				mch := m.AddPushChannel(ch1).(*monitoredPushChannel)
				chanCtx = mch.ctx

				mockAPI.dataQueued(10)
				mockAPI.dataSent(5)
			} else {
				mch := m.AddPullChannel(ch1).(*monitoredPullChannel)
				chanCtx = mch.ctx

				mockAPI.dataReceived(5)
			}

			// Check once to add a data point to the queue.
			// Subsequent checks will compare against the previous data point.
			m.checkDataRate()

			// Each check should trigger a restart up to the maximum number of restarts
			triggerMaxRestarts := func() {
				for i := 0; i < maxConsecutiveRestarts; i++ {
					m.checkDataRate()

					err := mockAPI.awaitRestart()
					require.NoError(t, err)
				}
			}
			triggerMaxRestarts()

			// When data is transferred it should reset the consecutive restarts back to zero
			if isPush {
				mockAPI.dataSent(6)
			} else {
				mockAPI.dataReceived(5)
			}

			// Trigger restarts up to max again
			triggerMaxRestarts()

			// Reached max restarts, so now there should not be another restart
			// attempt.
			// Instead the channel should be closed and the monitor shut down.
			m.checkDataRate()
			err := mockAPI.awaitRestart()
			require.Error(t, err) // require error because expecting no restart
			verifyChannelShutdown(t, chanCtx)
		})
	}

	// test push channel
	runTest("push", true)
	// test pull channel
	runTest("pull", false)
}

func TestChannelMonitorTimeouts(t *testing.T) {
	type testCase struct {
		name           string
		expectAccept   bool
		expectComplete bool
	}
	testCases := []testCase{{
		name:           "accept in time",
		expectAccept:   true,
		expectComplete: true,
	}, {
		name:         "accept too late",
		expectAccept: false,
	}, {
		name:           "complete in time",
		expectAccept:   true,
		expectComplete: true,
	}, {
		name:           "complete too late",
		expectAccept:   true,
		expectComplete: false,
	}}

	runTest := func(name string, isPush bool) {
		for _, tc := range testCases {
			t.Run(name+": "+tc.name, func(t *testing.T) {
				ch := &mockChannelState{chid: ch1}
				mockAPI := newMockMonitorAPI(ch, false)

				verifyClosedAndShutdown := func(chCtx context.Context, timeout time.Duration) {
					// Verify channel has been closed
					select {
					case <-time.After(timeout):
						require.Fail(t, "failed to close channel within "+timeout.String())
					case <-mockAPI.closed:
					}

					// Verify that channel has been shutdown
					verifyChannelShutdown(t, chCtx)
				}

				verifyNotClosed := func(timeout time.Duration) {
					// Verify channel has not been closed
					select {
					case <-time.After(timeout):
					case <-mockAPI.closed:
						require.Fail(t, "expected channel not to have been closed")
					}
				}

				acceptTimeout := 10 * time.Millisecond
				completeTimeout := 10 * time.Millisecond
				m := NewMonitor(mockAPI, &Config{
					MonitorPushChannels:    isPush,
					MonitorPullChannels:    !isPush,
					AcceptTimeout:          acceptTimeout,
					Interval:               time.Hour,
					ChecksPerInterval:      1,
					MinBytesTransferred:    1,
					MaxConsecutiveRestarts: 1,
					CompleteTimeout:        completeTimeout,
				})
				m.Start()

				var chCtx context.Context
				if isPush {
					mch := m.AddPushChannel(ch1).(*monitoredPushChannel)
					chCtx = mch.ctx
				} else {
					mch := m.AddPullChannel(ch1).(*monitoredPullChannel)
					chCtx = mch.ctx
				}

				if tc.expectAccept {
					// Fire the Accept event
					mockAPI.accept()
				} else {
					// If we are expecting the test to have a timeout waiting for
					// the accept event, verify that channel was closed (because a
					// timeout error occurred)
					verifyClosedAndShutdown(chCtx, 5*acceptTimeout)
					return
				}

				// If we're not expecting the test to have a timeout waiting for
				// the accept event, verify that channel was not closed
				verifyNotClosed(2 * acceptTimeout)

				// Fire the FinishTransfer event
				mockAPI.finishTransfer()
				if tc.expectComplete {
					// Fire the Complete event
					mockAPI.completed()
				}

				if !tc.expectComplete {
					// If we are expecting the test to have a timeout waiting for
					// the complete event verify that channel was closed (because a
					// timeout error occurred)
					verifyClosedAndShutdown(chCtx, 5*completeTimeout)
					return
				}

				// If we're not expecting the test to have a timeout waiting for
				// the accept event, verify that channel was not closed
				verifyNotClosed(2 * completeTimeout)
			})
		}
	}

	// test push channel
	runTest("push", true)
	// test pull channel
	runTest("pull", false)
}

func verifyChannelShutdown(t *testing.T, shutdownCtx context.Context) {
	select {
	case <-time.After(10 * time.Millisecond):
		require.Fail(t, "failed to shutdown channel")
	case <-shutdownCtx.Done():
	}
}

type mockMonitorAPI struct {
	ch            *mockChannelState
	restartErrors chan error
	restarts      chan struct{}
	closed        chan struct{}

	lk         sync.Mutex
	subscriber datatransfer.Subscriber
}

func newMockMonitorAPI(ch *mockChannelState, errOnRestart bool) *mockMonitorAPI {
	m := &mockMonitorAPI{
		ch:            ch,
		restarts:      make(chan struct{}, 1),
		closed:        make(chan struct{}),
		restartErrors: make(chan error, 1),
	}
	var restartErr error
	if errOnRestart {
		restartErr = xerrors.Errorf("restart err")
	}
	m.restartErrors <- restartErr
	return m
}

func (m *mockMonitorAPI) SubscribeToEvents(subscriber datatransfer.Subscriber) datatransfer.Unsubscribe {
	m.lk.Lock()
	defer m.lk.Unlock()

	m.subscriber = subscriber

	return func() {
		m.lk.Lock()
		defer m.lk.Unlock()

		m.subscriber = nil
	}
}

func (m *mockMonitorAPI) callSubscriber(e datatransfer.Event, state datatransfer.ChannelState) {
	m.subscriber(e, state)
}

func (m *mockMonitorAPI) RestartDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	defer func() {
		m.restarts <- struct{}{}
	}()

	select {
	case err := <-m.restartErrors:
		return err
	default:
		return nil
	}
}

func (m *mockMonitorAPI) awaitRestart() error {
	select {
	case <-time.After(10 * time.Millisecond):
		return xerrors.Errorf("failed to restart channel")
	case <-m.restarts:
		return nil
	}
}

func (m *mockMonitorAPI) CloseDataTransferChannelWithError(ctx context.Context, chid datatransfer.ChannelID, cherr error) error {
	close(m.closed)
	return nil
}

func (m *mockMonitorAPI) accept() {
	m.callSubscriber(datatransfer.Event{Code: datatransfer.Accept}, m.ch)
}

func (m *mockMonitorAPI) dataQueued(n uint64) {
	m.ch.queued = n
	m.callSubscriber(datatransfer.Event{Code: datatransfer.DataQueued}, m.ch)
}

func (m *mockMonitorAPI) dataSent(n uint64) {
	m.ch.sent = n
	m.callSubscriber(datatransfer.Event{Code: datatransfer.DataSent}, m.ch)
}

func (m *mockMonitorAPI) dataReceived(n uint64) {
	m.ch.received = n
	m.callSubscriber(datatransfer.Event{Code: datatransfer.DataReceived}, m.ch)
}

func (m *mockMonitorAPI) finishTransfer() {
	m.callSubscriber(datatransfer.Event{Code: datatransfer.FinishTransfer}, m.ch)
}

func (m *mockMonitorAPI) completed() {
	m.ch.complete = true
	m.callSubscriber(datatransfer.Event{Code: datatransfer.Complete}, m.ch)
}

func (m *mockMonitorAPI) sendDataErrorEvent() {
	m.callSubscriber(datatransfer.Event{Code: datatransfer.SendDataError}, m.ch)
}

type mockChannelState struct {
	chid     datatransfer.ChannelID
	queued   uint64
	sent     uint64
	received uint64
	complete bool
}

func (m *mockChannelState) Queued() uint64 {
	return m.queued
}

func (m *mockChannelState) Sent() uint64 {
	return m.sent
}

func (m *mockChannelState) Received() uint64 {
	return m.received
}

func (m *mockChannelState) ChannelID() datatransfer.ChannelID {
	return m.chid
}

func (m *mockChannelState) Status() datatransfer.Status {
	if m.complete {
		return datatransfer.Completed
	}
	return datatransfer.Ongoing
}

func (m *mockChannelState) TransferID() datatransfer.TransferID {
	panic("implement me")
}

func (m *mockChannelState) BaseCID() cid.Cid {
	panic("implement me")
}

func (m *mockChannelState) Selector() ipld.Node {
	panic("implement me")
}

func (m *mockChannelState) Voucher() datatransfer.Voucher {
	panic("implement me")
}

func (m *mockChannelState) Sender() peer.ID {
	panic("implement me")
}

func (m *mockChannelState) Recipient() peer.ID {
	panic("implement me")
}

func (m *mockChannelState) TotalSize() uint64 {
	panic("implement me")
}

func (m *mockChannelState) IsPull() bool {
	panic("implement me")
}

func (m *mockChannelState) OtherPeer() peer.ID {
	panic("implement me")
}

func (m *mockChannelState) SelfPeer() peer.ID {
	panic("implement me")
}

func (m *mockChannelState) Message() string {
	panic("implement me")
}

func (m *mockChannelState) Vouchers() []datatransfer.Voucher {
	panic("implement me")
}

func (m *mockChannelState) VoucherResults() []datatransfer.VoucherResult {
	panic("implement me")
}

func (m *mockChannelState) LastVoucher() datatransfer.Voucher {
	panic("implement me")
}

func (m *mockChannelState) LastVoucherResult() datatransfer.VoucherResult {
	panic("implement me")
}

func (m *mockChannelState) ReceivedCids() []cid.Cid {
	panic("implement me")
}
