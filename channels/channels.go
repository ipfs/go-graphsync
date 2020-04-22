package channels

import (
	"errors"
	"sync"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Channels is a thread safe list of channels
type Channels struct {
	channelsLk sync.RWMutex
	channels   map[datatransfer.ChannelID]datatransfer.ChannelState
}

// New returns a new thread safe list of channels
func New() *Channels {
	return &Channels{
		sync.RWMutex{},
		make(map[datatransfer.ChannelID]datatransfer.ChannelState),
	}
}

// CreateNew creates a new channel id and channel state and saves to channels.
// returns error if the channel exists already.
func (c *Channels) CreateNew(tid datatransfer.TransferID, baseCid cid.Cid, selector ipld.Node, voucher datatransfer.Voucher, initiator, dataSender, dataReceiver peer.ID) (datatransfer.ChannelID, error) {
	chid := datatransfer.ChannelID{Initiator: initiator, ID: tid}
	chst := datatransfer.ChannelState{Channel: datatransfer.NewChannel(0, baseCid, selector, voucher, dataSender, dataReceiver, 0)}
	c.channelsLk.Lock()
	defer c.channelsLk.Unlock()
	_, ok := c.channels[chid]
	if ok {
		return chid, errors.New("tried to create channel but it already exists")
	}
	c.channels[chid] = chst
	return chid, nil
}

// InProgress returns a list of in progress channels
func (c *Channels) InProgress() map[datatransfer.ChannelID]datatransfer.ChannelState {
	c.channelsLk.RLock()
	defer c.channelsLk.RUnlock()
	channelsCopy := make(map[datatransfer.ChannelID]datatransfer.ChannelState, len(c.channels))
	for channelID, channelState := range c.channels {
		channelsCopy[channelID] = channelState
	}
	return channelsCopy
}

// GetByIDAndSender searches for a channel in the slice of channels with id `chid`, with the given sender.
// Returns datatransfer.EmptyChannelState if there is no channel with that id
func (c *Channels) GetByIDAndSender(chid datatransfer.ChannelID, sender peer.ID) datatransfer.ChannelState {
	c.channelsLk.RLock()
	channelState, ok := c.channels[chid]
	c.channelsLk.RUnlock()
	if !ok || channelState.Sender() != sender {
		return datatransfer.EmptyChannelState
	}
	return channelState
}
