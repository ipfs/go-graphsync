package channelmonitor

import (
	"context"
	"fmt"
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

func TestChannelMonitorAutoRestart(t *testing.T) {
	type testCase struct {
		name              string
		errReconnect      bool
		errSendRestartMsg bool
	}
	testCases := []testCase{{
		name: "attempt restart",
	}, {
		name:         "fail to reconnect to peer",
		errReconnect: true,
	}, {
		name:              "fail to send restart message",
		errSendRestartMsg: true,
	}}

	runTest := func(name string, isPush bool) {
		for _, tc := range testCases {
			t.Run(name+": "+tc.name, func(t *testing.T) {
				ch := &mockChannelState{chid: ch1}
				mockAPI := newMockMonitorAPI(ch, tc.errReconnect, tc.errSendRestartMsg)

				triggerErrorEvent := func() {
					if isPush {
						mockAPI.sendDataErrorEvent()
					} else {
						mockAPI.receiveDataErrorEvent()
					}
				}

				m := NewMonitor(mockAPI, &Config{
					AcceptTimeout:          time.Hour,
					MaxConsecutiveRestarts: 1,
					CompleteTimeout:        time.Hour,
				})

				var mch *monitoredChannel
				if isPush {
					mch = m.AddPushChannel(ch1)
				} else {
					mch = m.AddPullChannel(ch1)
				}

				// Simulate the responder sending Accept
				mockAPI.accept()

				if isPush {
					// Simulate data being queued and sent
					mockAPI.dataQueued(10)
					mockAPI.dataSent(5)
				} else {
					// Simulate data being received
					mockAPI.dataReceived(10)
				}

				// Simulate error sending / receiving data
				triggerErrorEvent()

				// If there is an error attempting to restart, just wait for
				// the push channel to be closed
				if tc.errReconnect || tc.errSendRestartMsg {
					mockAPI.verifyChannelClosed(t, true)
					return
				}

				// Verify that restart message is sent
				err := mockAPI.awaitRestartSent()
				require.NoError(t, err)

				if isPush {
					// Simulate sending the remaining data
					mockAPI.dataSent(5)
				} else {
					// Simulate receiving more data
					mockAPI.dataReceived(5)
				}

				// Simulate the complete event
				mockAPI.completed()

				// Verify that channel has been shutdown
				verifyChannelShutdown(t, mch.ctx)
			})
		}
	}

	runTest("push", true)
	runTest("pull", false)
}

func TestChannelMonitorMaxConsecutiveRestarts(t *testing.T) {
	runTest := func(name string, isPush bool) {
		t.Run(name, func(t *testing.T) {
			ch := &mockChannelState{chid: ch1}
			mockAPI := newMockMonitorAPI(ch, false, false)

			triggerErrorEvent := func() {
				if isPush {
					mockAPI.sendDataErrorEvent()
				} else {
					mockAPI.receiveDataErrorEvent()
				}
			}

			maxConsecutiveRestarts := 3
			m := NewMonitor(mockAPI, &Config{
				AcceptTimeout:          time.Hour,
				MaxConsecutiveRestarts: uint32(maxConsecutiveRestarts),
				CompleteTimeout:        time.Hour,
			})

			var mch *monitoredChannel
			if isPush {
				mch = m.AddPushChannel(ch1)

				mockAPI.dataQueued(10)
				mockAPI.dataSent(5)
			} else {
				mch = m.AddPullChannel(ch1)

				mockAPI.dataReceived(5)
			}

			// Each error should trigger a restart up to the maximum number of restarts
			triggerMaxRestarts := func() {
				for i := 0; i < maxConsecutiveRestarts; i++ {
					triggerErrorEvent()

					err := mockAPI.awaitRestartSent()
					require.NoError(t, err)

					err = awaitRestartComplete(mch)
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
			triggerErrorEvent()
			err := mockAPI.awaitRestartSent()
			require.Error(t, err) // require error because expecting no restart
			verifyChannelShutdown(t, mch.ctx)
		})
	}

	// test push channel
	runTest("push", true)
	// test pull channel
	runTest("pull", false)
}

func awaitRestartComplete(mch *monitoredChannel) error {
	for i := 0; i < 10; i++ {
		if !mch.isRestarting() {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return xerrors.Errorf("restart did not complete after 10ms")
}

func TestChannelMonitorQueuedRestart(t *testing.T) {
	runTest := func(name string, isPush bool) {
		t.Run(name, func(t *testing.T) {
			ch := &mockChannelState{chid: ch1}
			mockAPI := newMockMonitorAPI(ch, false, false)

			triggerErrorEvent := func() {
				if isPush {
					mockAPI.sendDataErrorEvent()
				} else {
					mockAPI.receiveDataErrorEvent()
				}
			}

			m := NewMonitor(mockAPI, &Config{
				AcceptTimeout:          time.Hour,
				RestartDebounce:        10 * time.Millisecond,
				MaxConsecutiveRestarts: 3,
				CompleteTimeout:        time.Hour,
			})

			if isPush {
				m.AddPushChannel(ch1)

				mockAPI.dataQueued(10)
				mockAPI.dataSent(5)
			} else {
				m.AddPullChannel(ch1)

				mockAPI.dataReceived(5)
			}

			// Trigger an error event, should cause a restart
			triggerErrorEvent()
			// Wait for restart to occur
			err := mockAPI.awaitRestartSent()
			require.NoError(t, err)

			// Trigger another error event before the restart has completed
			triggerErrorEvent()

			// A second restart should be sent because of the second error
			err = mockAPI.awaitRestartSent()
			require.NoError(t, err)
		})
	}

	// test push channel
	runTest("push", true)
	// test pull channel
	runTest("pull", false)
}

func TestChannelMonitorTimeouts(t *testing.T) {
	type testCase struct {
		name                    string
		expectAccept            bool
		expectComplete          bool
		acceptTimeoutDisabled   bool
		completeTimeoutDisabled bool
	}
	testCases := []testCase{{
		name:           "accept in time",
		expectAccept:   true,
		expectComplete: true,
	}, {
		name:         "accept too late",
		expectAccept: false,
	}, {
		name:                  "disable accept timeout",
		acceptTimeoutDisabled: true,
		expectAccept:          true,
	}, {
		name:           "complete in time",
		expectAccept:   true,
		expectComplete: true,
	}, {
		name:           "complete too late",
		expectAccept:   true,
		expectComplete: false,
	}, {
		name:                    "disable complete timeout",
		completeTimeoutDisabled: true,
		expectAccept:            true,
		expectComplete:          true,
	}}

	runTest := func(name string, isPush bool) {
		for _, tc := range testCases {
			t.Run(name+": "+tc.name, func(t *testing.T) {
				ch := &mockChannelState{chid: ch1}
				mockAPI := newMockMonitorAPI(ch, false, false)

				verifyClosedAndShutdown := func(chCtx context.Context, timeout time.Duration) {
					mockAPI.verifyChannelClosed(t, true)

					// Verify that channel has been shutdown
					verifyChannelShutdown(t, chCtx)
				}

				acceptTimeout := 10 * time.Millisecond
				completeTimeout := 10 * time.Millisecond
				if tc.acceptTimeoutDisabled {
					acceptTimeout = 0
				}
				if tc.completeTimeoutDisabled {
					completeTimeout = 0
				}
				m := NewMonitor(mockAPI, &Config{
					AcceptTimeout:          acceptTimeout,
					MaxConsecutiveRestarts: 1,
					CompleteTimeout:        completeTimeout,
				})

				var chCtx context.Context
				if isPush {
					mch := m.AddPushChannel(ch1)
					chCtx = mch.ctx
				} else {
					mch := m.AddPullChannel(ch1)
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
				mockAPI.verifyChannelNotClosed(t, 2*acceptTimeout)

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
				mockAPI.verifyChannelNotClosed(t, 2*completeTimeout)
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
	ch              *mockChannelState
	connectErrors   bool
	restartErrors   bool
	restartMessages chan struct{}
	closeErr        chan error

	lk          sync.Mutex
	subscribers map[int]datatransfer.Subscriber
}

func newMockMonitorAPI(ch *mockChannelState, errOnReconnect, errOnRestart bool) *mockMonitorAPI {
	return &mockMonitorAPI{
		ch:              ch,
		connectErrors:   errOnReconnect,
		restartErrors:   errOnRestart,
		restartMessages: make(chan struct{}, 100),
		closeErr:        make(chan error, 1),
		subscribers:     make(map[int]datatransfer.Subscriber),
	}
}

func (m *mockMonitorAPI) SubscribeToEvents(subscriber datatransfer.Subscriber) datatransfer.Unsubscribe {
	m.lk.Lock()
	defer m.lk.Unlock()

	idx := len(m.subscribers)
	m.subscribers[idx] = subscriber

	return func() {
		m.lk.Lock()
		defer m.lk.Unlock()

		delete(m.subscribers, idx)
	}
}

func (m *mockMonitorAPI) fireEvent(e datatransfer.Event, state datatransfer.ChannelState) {
	m.lk.Lock()
	defer m.lk.Unlock()

	for _, subscriber := range m.subscribers {
		subscriber(e, state)
	}
}

func (m *mockMonitorAPI) ConnectTo(ctx context.Context, id peer.ID) error {
	if m.connectErrors {
		return xerrors.Errorf("connect err")
	}
	return nil
}

func (m *mockMonitorAPI) PeerID() peer.ID {
	return "p"
}

func (m *mockMonitorAPI) RestartDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	defer func() {
		m.restartMessages <- struct{}{}
	}()

	if m.restartErrors {
		return xerrors.Errorf("restart err")
	}
	return nil
}

func (m *mockMonitorAPI) awaitRestartSent() error {
	timeout := 100 * time.Millisecond
	select {
	case <-time.After(timeout):
		return xerrors.Errorf("failed to restart channel after %s", timeout)
	case <-m.restartMessages:
		return nil
	}
}

func (m *mockMonitorAPI) CloseDataTransferChannelWithError(ctx context.Context, chid datatransfer.ChannelID, cherr error) error {
	m.closeErr <- cherr
	return nil
}

func (m *mockMonitorAPI) verifyChannelClosed(t *testing.T, expectErr bool) {
	// Verify channel has been closed
	closeTimeout := 100 * time.Millisecond
	select {
	case <-time.After(closeTimeout):
		require.Fail(t, fmt.Sprintf("failed to close channel within %s", closeTimeout))
	case err := <-m.closeErr:
		if expectErr && err == nil {
			require.Fail(t, "expected error on close")
		}
		if !expectErr && err != nil {
			require.Fail(t, fmt.Sprintf("got error on close: %s", err))
		}
	}
}

func (m *mockMonitorAPI) verifyChannelNotClosed(t *testing.T, timeout time.Duration) {
	// Verify channel has not been closed
	select {
	case <-time.After(timeout):
	case <-m.closeErr:
		require.Fail(t, "expected channel not to have been closed")
	}
}

func (m *mockMonitorAPI) accept() {
	m.fireEvent(datatransfer.Event{Code: datatransfer.Accept}, m.ch)
}

func (m *mockMonitorAPI) dataQueued(n uint64) {
	m.ch.queued = n
	m.fireEvent(datatransfer.Event{Code: datatransfer.DataQueued}, m.ch)
}

func (m *mockMonitorAPI) dataSent(n uint64) {
	m.ch.sent = n
	m.fireEvent(datatransfer.Event{Code: datatransfer.DataSent}, m.ch)
}

func (m *mockMonitorAPI) dataReceived(n uint64) {
	m.ch.received = n
	m.fireEvent(datatransfer.Event{Code: datatransfer.DataReceived}, m.ch)
}

func (m *mockMonitorAPI) finishTransfer() {
	m.fireEvent(datatransfer.Event{Code: datatransfer.FinishTransfer}, m.ch)
}

func (m *mockMonitorAPI) completed() {
	m.ch.complete = true
	m.fireEvent(datatransfer.Event{Code: datatransfer.Complete}, m.ch)
}

func (m *mockMonitorAPI) sendDataErrorEvent() {
	m.fireEvent(datatransfer.Event{Code: datatransfer.SendDataError}, m.ch)
}

func (m *mockMonitorAPI) receiveDataErrorEvent() {
	m.fireEvent(datatransfer.Event{Code: datatransfer.ReceiveDataError}, m.ch)
}

type mockChannelState struct {
	chid     datatransfer.ChannelID
	queued   uint64
	sent     uint64
	received uint64
	complete bool
}

var _ datatransfer.ChannelState = (*mockChannelState)(nil)

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

func (m *mockChannelState) Stages() *datatransfer.ChannelStages {
	panic("implement me")
}

func (m *mockChannelState) ReceivedCids() []cid.Cid {
	panic("implement me")
}

func (m *mockChannelState) ReceivedCidsLen() int {
	panic("implement me")
}

func (m *mockChannelState) ReceivedCidsTotal() int64 {
	panic("implement me")
}

func (m *mockChannelState) MissingCids() []cid.Cid {
	panic("implement me")
}
