package graphsync

import (
	"sync"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

// Keeps track of the number of threads waiting on / holding the lock
type channelLockerInfo struct {
	lk       sync.Mutex
	refCount int
}

// Keeps a map of channel ID -> lock
type channelLocker struct {
	mapLk        sync.Mutex
	channelInfos map[datatransfer.ChannelID]*channelLockerInfo
}

func newChannelLocker() *channelLocker {
	return &channelLocker{
		channelInfos: make(map[datatransfer.ChannelID]*channelLockerInfo),
	}
}

// lock takes an exclusive lock on the given channel ID.
// It returns a function that should be called to unlock the channel ID.
func (cl *channelLocker) lock(chid datatransfer.ChannelID) func() {
	chanInfo := cl.lockForChannel(chid)

	// Acquire the lock for the channel.
	// Note: this will block until the current holder of the lock releases it.
	chanInfo.lk.Lock()

	// Lock acquired.
	// Return a function that can be used to release the lock.
	unlockChan := func() {
		// Release the channel lock
		chanInfo.lk.Unlock()

		// Garbage collect the channel if there is no one else waiting on the
		// lock
		cl.unrefChannel(chid)
	}

	return unlockChan
}

func (cl *channelLocker) lockForChannel(chid datatransfer.ChannelID) *channelLockerInfo {
	cl.mapLk.Lock()
	defer cl.mapLk.Unlock()

	// Get the info for the channel
	chanInfo, ok := cl.channelInfos[chid]
	if !ok {
		// Create channel info it doesn't already isn't
		chanInfo = &channelLockerInfo{}
		cl.channelInfos[chid] = chanInfo
	}

	// Increment the count of threads waiting on / holding the lock
	chanInfo.refCount++

	return chanInfo
}

func (cl *channelLocker) unrefChannel(chid datatransfer.ChannelID) {
	cl.mapLk.Lock()
	defer cl.mapLk.Unlock()

	chanInfo, ok := cl.channelInfos[chid]
	if !ok {
		// Sanity check - should never happen
		return
	}

	// Decrement the count of threads waiting on / holding the lock
	chanInfo.refCount--
	if chanInfo.refCount == 0 {
		// If there are no threads waiting on / holding lock, garbage
		// collect the map entry
		delete(cl.channelInfos, chid)
	}
}
