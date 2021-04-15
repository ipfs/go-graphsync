package graphsync

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

func TestChannelLocker(t *testing.T) {
	chid1 := datatransfer.ChannelID{
		Initiator: peer.ID("init"),
		Responder: peer.ID("resp"),
		ID:        1,
	}
	chid2 := datatransfer.ChannelID{
		Initiator: peer.ID("init"),
		Responder: peer.ID("resp"),
		ID:        2,
	}

	t.Run("lock / unlock separate channels independently", func(t *testing.T) {
		cl := newChannelLocker()
		unlockCh1A := cl.lock(chid1)
		unlockCh2A := cl.lock(chid2)
		unlockCh1A()
		unlockCh2A()
	})

	t.Run("lock after unlock", func(t *testing.T) {
		cl := newChannelLocker()
		unlockCh1A := cl.lock(chid1)
		unlockCh1A()
		unlockCh1B := cl.lock(chid1)
		unlockCh1B()
	})

	t.Run("lock after two lock / unlocks", func(t *testing.T) {
		cl := newChannelLocker()
		unlockCh1A := cl.lock(chid1)
		go func() {
			unlockCh1B := cl.lock(chid1)
			unlockCh1B()
		}()
		time.Sleep(10 * time.Millisecond)
		unlockCh1A()

		unlockCh1C := cl.lock(chid1)
		unlockCh1C()
	})

	t.Run("lock waits for previous locks to unlock", func(t *testing.T) {
		cl := newChannelLocker()

		chan1BTakesLock := uint64(0)
		unlockCh1A := cl.lock(chid1)
		go func() {
			cl.lock(chid1)
			atomic.AddUint64(&chan1BTakesLock, 1)
		}()

		time.Sleep(10 * time.Millisecond)
		locked := atomic.LoadUint64(&chan1BTakesLock)
		if locked == 1 {
			require.Fail(t, "expected second lock to wait for first to unlock")
		}
		unlockCh1A()

		time.Sleep(10 * time.Millisecond)
		locked = atomic.LoadUint64(&chan1BTakesLock)
		if locked == 0 {
			require.Fail(t, "expected second lock to complete after first unlocks")
		}
	})

	t.Run("multiple concurrent lock / unlock", func(t *testing.T) {
		cl := newChannelLocker()
		numLocks := 100

		var wg1 sync.WaitGroup
		var wg2 sync.WaitGroup
		//var unlocksCh2 []func()
		var count1 uint64
		var count2 uint64

		doLocks := func(wg *sync.WaitGroup, count *uint64) {
			for i := 0; i < numLocks; i++ {
				go func() {
					unlock := cl.lock(chid1)
					atomic.AddUint64(count, 1)
					time.Sleep(time.Millisecond)
					unlock()
					wg.Done()
				}()
			}
		}

		wg1.Add(numLocks)
		go doLocks(&wg1, &count1)
		wg2.Add(numLocks)
		go doLocks(&wg2, &count2)

		wg1.Wait()
		wg2.Wait()
		require.EqualValues(t, numLocks, atomic.LoadUint64(&count1))
		require.EqualValues(t, numLocks, atomic.LoadUint64(&count2))
	})
}
