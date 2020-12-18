package pushchannelmonitor

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

	ch1 := datatransfer.ChannelID{
		Initiator: "initiator",
		Responder: "responder",
		ID:        1,
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &mockChannelState{chid: ch1}
			mockAPI := newMockMonitorAPI(ch, tc.errOnRestart)

			m := NewMonitor(mockAPI, &Config{
				Interval:               10 * time.Millisecond,
				ChecksPerInterval:      10,
				MinBytesSent:           1,
				MaxConsecutiveRestarts: 3,
			})
			m.Start()
			m.AddChannel(ch1)
			mch := getFirstMonitoredChannel(m)

			mockAPI.dataQueued(tc.dataQueued)
			mockAPI.dataSent(tc.dataSent)
			if tc.errorEvent {
				mockAPI.errorEvent()
			}

			if tc.errOnRestart {
				// If there is no recovery from restart, wait for the push
				// channel to be closed
				<-mockAPI.closed
				return
			}

			// Verify that channel was restarted
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
			verifyChannelShutdown(t, mch)
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
		name:         "restart when min sent (1) < pending (10)",
		minBytesSent: 1,
		dataPoints: []dataPoint{{
			queued: 20,
			sent:   10,
		}},
		expectRestart: true,
	}, {
		name:         "dont restart when min sent (20) >= pending (10)",
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
		name:         "restart when min sent (5) < pending (10)",
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

	ch1 := datatransfer.ChannelID{
		Initiator: "initiator",
		Responder: "responder",
		ID:        1,
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &mockChannelState{chid: ch1}
			mockAPI := newMockMonitorAPI(ch, false)

			checksPerInterval := uint32(1)
			m := NewMonitor(mockAPI, &Config{
				Interval:               time.Hour,
				ChecksPerInterval:      checksPerInterval,
				MinBytesSent:           tc.minBytesSent,
				MaxConsecutiveRestarts: 3,
			})

			// Note: Don't start monitor, we'll call checkDataRate() manually

			m.AddChannel(ch1)

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

func TestPushChannelMonitorMaxConsecutiveRestarts(t *testing.T) {
	ch1 := datatransfer.ChannelID{
		Initiator: "initiator",
		Responder: "responder",
		ID:        1,
	}
	ch := &mockChannelState{chid: ch1}
	mockAPI := newMockMonitorAPI(ch, false)

	maxConsecutiveRestarts := 3
	m := NewMonitor(mockAPI, &Config{
		Interval:               time.Hour,
		ChecksPerInterval:      1,
		MinBytesSent:           2,
		MaxConsecutiveRestarts: uint32(maxConsecutiveRestarts),
	})

	// Note: Don't start monitor, we'll call checkDataRate() manually

	m.AddChannel(ch1)
	mch := getFirstMonitoredChannel(m)

	mockAPI.dataQueued(10)
	mockAPI.dataSent(5)

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

	// When data is sent it should reset the consecutive restarts back to zero
	mockAPI.dataSent(6)

	// Trigger restarts up to max again
	triggerMaxRestarts()

	// Reached max restarts, so now there should not be another restart
	// attempt.
	// Instead the channel should be closed and the monitor shut down.
	m.checkDataRate()
	err := mockAPI.awaitRestart()
	require.Error(t, err) // require error because expecting no restart
	verifyChannelShutdown(t, mch)
}

func getFirstMonitoredChannel(m *Monitor) *monitoredChannel {
	var mch *monitoredChannel
	for mch = range m.channels {
		return mch
	}
	panic("no channels")
}

func verifyChannelShutdown(t *testing.T, mch *monitoredChannel) {
	select {
	case <-time.After(10 * time.Millisecond):
		require.Fail(t, "failed to shutdown channel")
	case <-mch.ctx.Done():
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

func (m *mockMonitorAPI) CloseDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	close(m.closed)
	return nil
}

func (m *mockMonitorAPI) dataQueued(n uint64) {
	m.ch.queued = n
	m.callSubscriber(datatransfer.Event{Code: datatransfer.DataQueued}, m.ch)
}

func (m *mockMonitorAPI) dataSent(n uint64) {
	m.ch.sent = n
	m.callSubscriber(datatransfer.Event{Code: datatransfer.DataSent}, m.ch)
}

func (m *mockMonitorAPI) completed() {
	m.ch.complete = true
	m.callSubscriber(datatransfer.Event{Code: datatransfer.Complete}, m.ch)
}

func (m *mockMonitorAPI) errorEvent() {
	m.callSubscriber(datatransfer.Event{Code: datatransfer.Error}, m.ch)
}

type mockChannelState struct {
	chid     datatransfer.ChannelID
	queued   uint64
	sent     uint64
	complete bool
}

func (m *mockChannelState) Queued() uint64 {
	return m.queued
}

func (m *mockChannelState) Sent() uint64 {
	return m.sent
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

func (m *mockChannelState) Received() uint64 {
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
