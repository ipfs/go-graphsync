package pushchannelmonitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channels"
)

var log = logging.Logger("dt-pushchanmon")

type monitorAPI interface {
	SubscribeToEvents(subscriber datatransfer.Subscriber) datatransfer.Unsubscribe
	RestartDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error
	CloseDataTransferChannel(ctx context.Context, chid datatransfer.ChannelID) error
}

// Monitor watches the data-rate for push channels, and restarts
// a channel if the data-rate falls too low
type Monitor struct {
	ctx  context.Context
	stop context.CancelFunc
	mgr  monitorAPI
	cfg  *Config

	lk       sync.RWMutex
	channels map[*monitoredChannel]struct{}
}

type Config struct {
	Interval          time.Duration
	MinBytesSent      uint64
	ChecksPerInterval uint32
	RestartBackoff    time.Duration
}

func NewMonitor(mgr monitorAPI, cfg *Config) *Monitor {
	checkConfig(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	return &Monitor{
		ctx:      ctx,
		stop:     cancel,
		mgr:      mgr,
		cfg:      cfg,
		channels: make(map[*monitoredChannel]struct{}),
	}
}

func checkConfig(cfg *Config) {
	if cfg == nil {
		return
	}

	prefix := "data-transfer channel push monitor config "
	if cfg.Interval <= 0 {
		panic(fmt.Sprintf(prefix+"Interval is %s but must be > 0", cfg.Interval))
	}
	if cfg.ChecksPerInterval == 0 {
		panic(fmt.Sprintf(prefix+"ChecksPerInterval is %d but must be > 0", cfg.ChecksPerInterval))
	}
	if cfg.MinBytesSent == 0 {
		panic(fmt.Sprintf(prefix+"MinBytesSent is %d but must be > 0", cfg.MinBytesSent))
	}
}

// AddChannel adds a channel to the push channel monitor
func (m *Monitor) AddChannel(chid datatransfer.ChannelID) *monitoredChannel {
	if !m.enabled() {
		return nil
	}

	m.lk.Lock()
	defer m.lk.Unlock()

	mpc := newMonitoredChannel(m.mgr, chid, m.cfg, m.onMonitoredChannelShutdown)
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

// enabled indicates whether the push channel monitor is running
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

// monitoredChannel keeps track of the data-rate for a push channel, and
// restarts the channel if the rate falls below the minimum allowed
type monitoredChannel struct {
	ctx        context.Context
	cancel     context.CancelFunc
	mgr        monitorAPI
	chid       datatransfer.ChannelID
	cfg        *Config
	unsub      datatransfer.Unsubscribe
	onShutdown func(*monitoredChannel)

	statsLk        sync.RWMutex
	queued         uint64
	sent           uint64
	dataRatePoints chan *dataRatePoint

	restartLk  sync.RWMutex
	restarting bool
}

func newMonitoredChannel(
	mgr monitorAPI,
	chid datatransfer.ChannelID,
	cfg *Config,
	onShutdown func(*monitoredChannel),
) *monitoredChannel {
	ctx, cancel := context.WithCancel(context.Background())
	mpc := &monitoredChannel{
		ctx:            ctx,
		cancel:         cancel,
		mgr:            mgr,
		chid:           chid,
		cfg:            cfg,
		onShutdown:     onShutdown,
		dataRatePoints: make(chan *dataRatePoint, cfg.ChecksPerInterval),
	}
	mpc.start()
	return mpc
}

// Cancel the context and unsubscribe from events
func (mc *monitoredChannel) Shutdown() {
	mc.cancel()
	mc.unsub()
	go mc.onShutdown(mc)
}

func (mc *monitoredChannel) start() {
	mc.unsub = mc.mgr.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if channelState.ChannelID() != mc.chid {
			return
		}

		mc.statsLk.Lock()
		defer mc.statsLk.Unlock()

		// Once the channel completes, shut down the monitor
		state := channelState.Status()
		if channels.IsChannelCleaningUp(state) || channels.IsChannelTerminated(state) {
			go mc.Shutdown()
			return
		}

		switch event.Code {
		case datatransfer.Error:
			// If there's an error, attempt to restart the channel
			go mc.restartChannel()
		case datatransfer.DataQueued:
			// Keep track of the amount of data queued
			mc.queued = channelState.Queued()
		case datatransfer.DataSent:
			// Keep track of the amount of data sent
			mc.sent = channelState.Sent()
		}
	})
}

type dataRatePoint struct {
	pending uint64
	sent    uint64
}

// check if the amount of data sent in the interval was too low, and if so
// restart the channel
func (mc *monitoredChannel) checkDataRate() {
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
		return
	}

	// Pop the data point from one interval ago
	atIntervalStart := <-mc.dataRatePoints

	// If there was enough pending data to cover the minimum required amount,
	// and the amount sent was lower than the minimum required, restart the
	// channel
	sentInInterval := mc.sent - atIntervalStart.sent
	if atIntervalStart.pending > sentInInterval && sentInInterval < mc.cfg.MinBytesSent {
		go mc.restartChannel()
	}
}

func (mc *monitoredChannel) restartChannel() {
	// Check if the channel is already being restarted
	mc.restartLk.Lock()
	alreadyRestarting := mc.restarting
	if !alreadyRestarting {
		mc.restarting = true
	}
	mc.restartLk.Unlock()

	if alreadyRestarting {
		return
	}

	defer func() {
		// Backoff a little time after a restart before attempting another
		select {
		case <-time.After(mc.cfg.RestartBackoff):
		case <-mc.ctx.Done():
		}

		mc.restartLk.Lock()
		mc.restarting = false
		mc.restartLk.Unlock()
	}()

	// Send a restart message for the channel
	log.Infof("Sending restart message for push data-channel %s", mc.chid)
	err := mc.mgr.RestartDataTransferChannel(mc.ctx, mc.chid)
	if err != nil {
		log.Warnf("closing channel after failing to send restart message for push data-channel %s: %s", mc.chid, err)

		// If it wasn't possible to restart the channel, close the channel
		// and shut down the monitor
		defer mc.Shutdown()

		err := mc.mgr.CloseDataTransferChannel(mc.ctx, mc.chid)
		if err != nil {
			log.Errorf("error closing data transfer channel %s: %w", mc.chid, err)
		}
	}
}
