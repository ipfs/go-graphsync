package channelmonitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels"
)

var log = logging.Logger("dt-chanmon")

type monitorAPI interface {
	SubscribeToEvents(subscriber datatransfer.Subscriber) datatransfer.Unsubscribe
	RestartDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error
	CloseDataTransferChannelWithError(ctx context.Context, chid datatransfer.ChannelID, cherr error) error
}

// Monitor watches the data-rate for data transfer channels, and restarts
// a channel if the data-rate falls too low or if there are timeouts / errors
type Monitor struct {
	ctx  context.Context
	stop context.CancelFunc
	mgr  monitorAPI
	cfg  *Config

	lk       sync.RWMutex
	channels map[monitoredChan]struct{}
}

type Config struct {
	// Max time to wait for other side to accept open channel request before attempting restart
	AcceptTimeout time.Duration
	// Interval between checks of transfer rate
	Interval time.Duration
	// Min bytes that must be sent / received in interval
	MinBytesTransferred uint64
	// Number of times to check transfer rate per interval
	ChecksPerInterval uint32
	// Backoff after restarting
	RestartBackoff time.Duration
	// Number of times to try to restart before failing
	MaxConsecutiveRestarts uint32
	// Max time to wait for the responder to send a Complete message once all
	// data has been sent
	CompleteTimeout time.Duration
}

func NewMonitor(mgr monitorAPI, cfg *Config) *Monitor {
	checkConfig(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	return &Monitor{
		ctx:      ctx,
		stop:     cancel,
		mgr:      mgr,
		cfg:      cfg,
		channels: make(map[monitoredChan]struct{}),
	}
}

func checkConfig(cfg *Config) {
	if cfg == nil {
		return
	}

	prefix := "data-transfer channel monitor config "
	if cfg.AcceptTimeout <= 0 {
		panic(fmt.Sprintf(prefix+"AcceptTimeout is %s but must be > 0", cfg.AcceptTimeout))
	}
	if cfg.Interval <= 0 {
		panic(fmt.Sprintf(prefix+"Interval is %s but must be > 0", cfg.Interval))
	}
	if cfg.ChecksPerInterval == 0 {
		panic(fmt.Sprintf(prefix+"ChecksPerInterval is %d but must be > 0", cfg.ChecksPerInterval))
	}
	if cfg.MinBytesTransferred == 0 {
		panic(fmt.Sprintf(prefix+"MinBytesTransferred is %d but must be > 0", cfg.MinBytesTransferred))
	}
	if cfg.MaxConsecutiveRestarts == 0 {
		panic(fmt.Sprintf(prefix+"MaxConsecutiveRestarts is %d but must be > 0", cfg.MaxConsecutiveRestarts))
	}
	if cfg.CompleteTimeout <= 0 {
		panic(fmt.Sprintf(prefix+"CompleteTimeout is %s but must be > 0", cfg.CompleteTimeout))
	}
}

// This interface just makes it easier to abstract some methods between the
// push and pull monitor implementations
type monitoredChan interface {
	Shutdown()
	checkDataRate()
}

// AddPushChannel adds a push channel to the channel monitor
func (m *Monitor) AddPushChannel(chid datatransfer.ChannelID) monitoredChan {
	return m.addChannel(chid, true)
}

// AddPullChannel adds a pull channel to the channel monitor
func (m *Monitor) AddPullChannel(chid datatransfer.ChannelID) monitoredChan {
	return m.addChannel(chid, false)
}

// addChannel adds a channel to the channel monitor
func (m *Monitor) addChannel(chid datatransfer.ChannelID, isPush bool) monitoredChan {
	if !m.enabled() {
		return nil
	}

	m.lk.Lock()
	defer m.lk.Unlock()

	var mpc monitoredChan
	if isPush {
		mpc = newMonitoredPushChannel(m.mgr, chid, m.cfg, m.onMonitoredChannelShutdown)
	} else {
		mpc = newMonitoredPullChannel(m.mgr, chid, m.cfg, m.onMonitoredChannelShutdown)
	}
	m.channels[mpc] = struct{}{}
	return mpc
}

func (m *Monitor) Shutdown() {
	// Causes the run loop to exit
	m.stop()
}

// onShutdown shuts down all monitored channels. It is called when the run
// loop exits.
func (m *Monitor) onShutdown() {
	m.lk.RLock()
	defer m.lk.RUnlock()

	for ch := range m.channels {
		ch.Shutdown()
	}
}

// onMonitoredChannelShutdown is called when a monitored channel shuts down
func (m *Monitor) onMonitoredChannelShutdown(mpc *monitoredChannel) {
	m.lk.Lock()
	defer m.lk.Unlock()

	delete(m.channels, mpc)
}

// enabled indicates whether the channel monitor is running
func (m *Monitor) enabled() bool {
	return m.cfg != nil
}

func (m *Monitor) Start() {
	if !m.enabled() {
		return
	}

	go m.run()
}

func (m *Monitor) run() {
	defer m.onShutdown()

	// Check data-rate ChecksPerInterval times per interval
	tickInterval := m.cfg.Interval / time.Duration(m.cfg.ChecksPerInterval)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	log.Infof("Starting data-transfer channel monitor with "+
		"%d checks per %s interval (check interval %s); min bytes per interval: %d, restart backoff: %s; max consecutive restarts: %d",
		m.cfg.ChecksPerInterval, m.cfg.Interval, tickInterval, m.cfg.MinBytesTransferred, m.cfg.RestartBackoff, m.cfg.MaxConsecutiveRestarts)

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkDataRate()
		}
	}
}

// check data rate for all monitored channels
func (m *Monitor) checkDataRate() {
	m.lk.RLock()
	defer m.lk.RUnlock()

	for ch := range m.channels {
		ch.checkDataRate()
	}
}

// monitoredChannel keeps track of the data-rate for a channel, and
// restarts the channel if the rate falls below the minimum allowed
type monitoredChannel struct {
	ctx        context.Context
	cancel     context.CancelFunc
	mgr        monitorAPI
	chid       datatransfer.ChannelID
	cfg        *Config
	unsub      datatransfer.Unsubscribe
	onShutdown func(*monitoredChannel)
	onDTEvent  datatransfer.Subscriber
	shutdownLk sync.Mutex

	restartLk           sync.RWMutex
	restartedAt         time.Time
	consecutiveRestarts int
}

func newMonitoredChannel(
	mgr monitorAPI,
	chid datatransfer.ChannelID,
	cfg *Config,
	onShutdown func(*monitoredChannel),
	onDTEvent datatransfer.Subscriber,
) *monitoredChannel {
	ctx, cancel := context.WithCancel(context.Background())
	mpc := &monitoredChannel{
		ctx:        ctx,
		cancel:     cancel,
		mgr:        mgr,
		chid:       chid,
		cfg:        cfg,
		onShutdown: onShutdown,
		onDTEvent:  onDTEvent,
	}
	mpc.start()
	return mpc
}

// Overridden by sub-classes
func (mc *monitoredChannel) checkDataRate() {
}

// Cancel the context and unsubscribe from events
func (mc *monitoredChannel) Shutdown() {
	mc.shutdownLk.Lock()
	defer mc.shutdownLk.Unlock()

	// Check if the channel was already shut down
	if mc.cancel == nil {
		return
	}
	mc.cancel() // cancel context so all go-routines exit
	mc.cancel = nil

	// unsubscribe from data transfer events
	mc.unsub()

	// Inform the Manager that this channel has shut down
	go mc.onShutdown(mc)
}

func (mc *monitoredChannel) start() {
	// Prevent shutdown until after startup
	mc.shutdownLk.Lock()
	defer mc.shutdownLk.Unlock()

	log.Debugf("%s: starting channel data-rate monitoring", mc.chid)

	// Watch to make sure the responder accepts the channel in time
	cancelAcceptTimer := mc.watchForResponderAccept()

	// Watch for data rate events
	mc.unsub = mc.mgr.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if channelState.ChannelID() != mc.chid {
			return
		}

		// Once the channel completes, shut down the monitor
		state := channelState.Status()
		if channels.IsChannelCleaningUp(state) || channels.IsChannelTerminated(state) {
			log.Debugf("%s: stopping channel data-rate monitoring", mc.chid)
			go mc.Shutdown()
			return
		}

		switch event.Code {
		case datatransfer.Accept:
			// The Accept event is fired when we receive an Accept message from the responder
			cancelAcceptTimer()
		case datatransfer.SendDataError:
			// If the transport layer reports an error sending data over the wire,
			// attempt to restart the channel
			log.Warnf("%s: data transfer transport send error, restarting data transfer", mc.chid)
			go mc.restartChannel()
		case datatransfer.FinishTransfer:
			// The client has finished sending all data. Watch to make sure
			// that the responder sends a message to acknowledge that the
			// transfer is complete
			go mc.watchForResponderComplete()
		default:
			// Delegate to the push channel monitor or pull channel monitor to
			// handle the event
			mc.onDTEvent(event, channelState)
		}
	})
}

// watchForResponderAccept watches to make sure that the responder sends
// an Accept to our open channel request before the accept timeout.
// Returns a function that can be used to cancel the timer.
func (mc *monitoredChannel) watchForResponderAccept() func() {
	// Start a timer for the accept timeout
	timer := time.NewTimer(mc.cfg.AcceptTimeout)

	go func() {
		defer timer.Stop()

		select {
		case <-mc.ctx.Done():
		case <-timer.C:
			// Timer expired before we received an Accept from the responder,
			// fail the data transfer
			err := xerrors.Errorf("%s: timed out waiting %s for Accept message from remote peer",
				mc.chid, mc.cfg.AcceptTimeout)
			mc.closeChannelAndShutdown(err)
		}
	}()

	return func() { timer.Stop() }
}

// Wait up to the configured timeout for the responder to send a Complete message
func (mc *monitoredChannel) watchForResponderComplete() {
	// Start a timer for the complete timeout
	timer := time.NewTimer(mc.cfg.CompleteTimeout)
	defer timer.Stop()

	select {
	case <-mc.ctx.Done():
		// When the Complete message is received, the channel shuts down
	case <-timer.C:
		// Timer expired before we received a Complete from the responder
		err := xerrors.Errorf("%s: timed out waiting %s for Complete message from remote peer",
			mc.chid, mc.cfg.AcceptTimeout)
		mc.closeChannelAndShutdown(err)
	}
}

// clear the consecutive restart count (we do this when data is sent or
// received)
func (mc *monitoredChannel) resetConsecutiveRestarts() {
	mc.restartLk.Lock()
	defer mc.restartLk.Unlock()

	mc.consecutiveRestarts = 0
}

func (mc *monitoredChannel) restartChannel() {
	var restartCount int
	var restartedAt time.Time
	mc.restartLk.Lock()
	{
		// If the channel is not already being restarted, record the restart
		// time and increment the consecutive restart count
		restartedAt = mc.restartedAt
		if mc.restartedAt.IsZero() {
			mc.restartedAt = time.Now()
			mc.consecutiveRestarts++
			restartCount = mc.consecutiveRestarts
		}
	}
	mc.restartLk.Unlock()

	// Check if channel is already being restarted
	if !restartedAt.IsZero() {
		log.Debugf("%s: restart called but already restarting channel (for %s so far; restart backoff is %s)",
			mc.chid, time.Since(restartedAt), mc.cfg.RestartBackoff)
		return
	}

	if uint32(restartCount) > mc.cfg.MaxConsecutiveRestarts {
		// If no data has been transferred since the last transfer, and we've
		// reached the consecutive restart limit, close the channel and
		// shutdown the monitor
		err := xerrors.Errorf("%s: after %d consecutive restarts failed to reach required data transfer rate", mc.chid, restartCount)
		mc.closeChannelAndShutdown(err)
		return
	}

	// Send a restart message for the channel.
	// Note that at the networking layer there is logic to retry if a network
	// connection cannot be established, so this may take some time.
	log.Infof("%s: sending restart message (%d consecutive restarts)", mc.chid, restartCount)
	err := mc.mgr.RestartDataTransferChannel(mc.ctx, mc.chid)
	if err != nil {
		// If it wasn't possible to restart the channel, close the channel
		// and shut down the monitor
		cherr := xerrors.Errorf("%s: failed to send restart message: %s", mc.chid, err)
		mc.closeChannelAndShutdown(cherr)
	} else if mc.cfg.RestartBackoff > 0 {
		log.Infof("%s: restart message sent successfully, backing off %s before allowing any other restarts",
			mc.chid, mc.cfg.RestartBackoff)
		// Backoff a little time after a restart before attempting another
		select {
		case <-time.After(mc.cfg.RestartBackoff):
		case <-mc.ctx.Done():
		}

		log.Debugf("%s: restart back-off %s complete",
			mc.chid, mc.cfg.RestartBackoff)
	}

	// Restart complete, so clear the restart time so that another restart
	// can begin
	mc.restartLk.Lock()
	mc.restartedAt = time.Time{}
	mc.restartLk.Unlock()
}

func (mc *monitoredChannel) closeChannelAndShutdown(cherr error) {
	log.Errorf("closing data-transfer channel: %s", cherr)
	err := mc.mgr.CloseDataTransferChannelWithError(mc.ctx, mc.chid, cherr)
	if err != nil {
		log.Errorf("error closing data-transfer channel %s: %s", mc.chid, err)
	}

	mc.Shutdown()
}

// Snapshot of the pending and sent data at a particular point in time.
// The push channel monitor takes regular snapshots and compares them to
// decide if the data rate has fallen too low.
type dataRatePoint struct {
	pending uint64
	sent    uint64
}

// Keeps track of the data rate for a push channel
type monitoredPushChannel struct {
	*monitoredChannel

	statsLk        sync.RWMutex
	queued         uint64
	sent           uint64
	dataRatePoints chan *dataRatePoint
}

func newMonitoredPushChannel(
	mgr monitorAPI,
	chid datatransfer.ChannelID,
	cfg *Config,
	onShutdown func(*monitoredChannel),
) *monitoredPushChannel {
	mpc := &monitoredPushChannel{
		dataRatePoints: make(chan *dataRatePoint, cfg.ChecksPerInterval),
	}
	mpc.monitoredChannel = newMonitoredChannel(mgr, chid, cfg, onShutdown, mpc.onDTEvent)
	return mpc
}

// check if the amount of data sent in the interval was too low, and if so
// restart the channel
func (mc *monitoredPushChannel) checkDataRate() {
	mc.statsLk.Lock()
	defer mc.statsLk.Unlock()

	// Before returning, add the current data rate stats to the queue
	defer func() {
		var pending uint64
		if mc.queued > mc.sent { // should always be true but just in case
			pending = mc.queued - mc.sent
		}
		mc.dataRatePoints <- &dataRatePoint{
			pending: pending,
			sent:    mc.sent,
		}
	}()

	// Check that there are enough data points that an interval has elapsed
	if len(mc.dataRatePoints) < int(mc.cfg.ChecksPerInterval) {
		log.Debugf("%s: not enough data points to check data rate yet (%d / %d)",
			mc.chid, len(mc.dataRatePoints), mc.cfg.ChecksPerInterval)

		return
	}

	// Pop the data point from one interval ago
	atIntervalStart := <-mc.dataRatePoints

	// If there was enough pending data to cover the minimum required amount,
	// and the amount sent was lower than the minimum required, restart the
	// channel
	sentInInterval := mc.sent - atIntervalStart.sent
	log.Debugf("%s: since last check: sent: %d - %d = %d, pending: %d, required %d",
		mc.chid, mc.sent, atIntervalStart.sent, sentInInterval, atIntervalStart.pending, mc.cfg.MinBytesTransferred)
	if atIntervalStart.pending > sentInInterval && sentInInterval < mc.cfg.MinBytesTransferred {
		log.Warnf("%s: data-rate too low, restarting channel: since last check %s ago: sent: %d, required %d",
			mc.chid, mc.cfg.Interval, mc.sent, mc.cfg.MinBytesTransferred)
		go mc.restartChannel()
	}
}

// Update the queued / sent amount each time it changes
func (mc *monitoredPushChannel) onDTEvent(event datatransfer.Event, channelState datatransfer.ChannelState) {
	switch event.Code {
	case datatransfer.DataQueued:
		// Keep track of the amount of data queued
		mc.statsLk.Lock()
		mc.queued = channelState.Queued()
		mc.statsLk.Unlock()

	case datatransfer.DataSent:
		// Keep track of the amount of data sent
		mc.statsLk.Lock()
		mc.sent = channelState.Sent()
		mc.statsLk.Unlock()

		// Some data was sent so reset the consecutive restart counter
		mc.resetConsecutiveRestarts()
	}
}

// Keeps track of the data rate for a pull channel
type monitoredPullChannel struct {
	*monitoredChannel

	statsLk        sync.RWMutex
	received       uint64
	dataRatePoints chan uint64
}

func newMonitoredPullChannel(
	mgr monitorAPI,
	chid datatransfer.ChannelID,
	cfg *Config,
	onShutdown func(*monitoredChannel),
) *monitoredPullChannel {
	mpc := &monitoredPullChannel{
		dataRatePoints: make(chan uint64, cfg.ChecksPerInterval),
	}
	mpc.monitoredChannel = newMonitoredChannel(mgr, chid, cfg, onShutdown, mpc.onDTEvent)
	return mpc
}

// check if the amount of data received in the interval was too low, and if so
// restart the channel
func (mc *monitoredPullChannel) checkDataRate() {
	mc.statsLk.Lock()
	defer mc.statsLk.Unlock()

	// Before returning, add the current data rate stats to the queue
	defer func() {
		mc.dataRatePoints <- mc.received
	}()

	// Check that there are enough data points that an interval has elapsed
	if len(mc.dataRatePoints) < int(mc.cfg.ChecksPerInterval) {
		log.Debugf("%s: not enough data points to check data rate yet (%d / %d)",
			mc.chid, len(mc.dataRatePoints), mc.cfg.ChecksPerInterval)

		return
	}

	// Pop the data point from one interval ago
	atIntervalStart := <-mc.dataRatePoints

	// If the amount received was lower than the minimum required, restart the
	// channel
	rcvdInInterval := mc.received - atIntervalStart
	log.Debugf("%s: since last check: received: %d - %d = %d, required %d",
		mc.chid, mc.received, atIntervalStart, rcvdInInterval, mc.cfg.MinBytesTransferred)
	if rcvdInInterval < mc.cfg.MinBytesTransferred {
		log.Warnf("%s: data-rate too low, restarting channel: since last check %s ago: received %d, required %d",
			mc.chid, mc.cfg.Interval, rcvdInInterval, mc.cfg.MinBytesTransferred)
		go mc.restartChannel()
	}
}

// Update the received amount each time it changes
func (mc *monitoredPullChannel) onDTEvent(event datatransfer.Event, channelState datatransfer.ChannelState) {
	switch event.Code {
	case datatransfer.DataReceived:
		// Keep track of the amount of data received
		mc.statsLk.Lock()
		mc.received = channelState.Received()
		mc.statsLk.Unlock()

		// Some data was received so reset the consecutive restart counter
		mc.resetConsecutiveRestarts()
	}
}
