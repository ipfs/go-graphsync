package impl

import (
	"sync"
	"testing"
	"time"
)

func TestTimeCounter(t *testing.T) {
	// Test that counter increases between restarts
	tc1 := newTimeCounter()
	time.Sleep(time.Millisecond)
	tc2 := newTimeCounter()
	tc1Next := tc1.next()
	tc2Next := tc2.next()
	if tc2Next <= tc1Next {
		t.Fatal("counter should increase for each new counter generator", tc1Next, tc2Next)
	}

	// Test that the counter always increases
	for i := 0; i < 100; i++ {
		first := tc1.next()
		second := tc1.next()
		if second <= first {
			t.Fatal("counter should increase monotonically", first, second)
		}
	}

	// Test that the counter is thread-safe
	count := 1000
	threads := 20
	counter := tc1.next()
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < count; i++ {
				tc1.next()
			}
			wg.Done()
		}()
	}
	wg.Wait()

	if tc1.next() != counter+uint64(threads*count+1) {
		t.Fatal("next() is not thread safe")
	}
}
