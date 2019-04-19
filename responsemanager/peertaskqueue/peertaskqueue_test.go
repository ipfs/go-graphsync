package peertaskqueue

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/ipfs/go-graphsync/responsemanager/peertaskqueue/peertask"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestPushPop(t *testing.T) {
	ptq := New()
	partner := testutil.GeneratePeers(1)[0]
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")
	vowels := strings.Split("aeiou", "")
	consonants := func() []string {
		var out []string
		for _, letter := range alphabet {
			skip := false
			for _, vowel := range vowels {
				if letter == vowel {
					skip = true
				}
			}
			if !skip {
				out = append(out, letter)
			}
		}
		return out
	}()
	sort.Strings(alphabet)
	sort.Strings(vowels)
	sort.Strings(consonants)

	// add a bunch of blocks. cancel some. drain the queue. the queue should only have the kept tasks

	for _, index := range rand.Perm(len(alphabet)) { // add blocks for all letters
		letter := alphabet[index]
		t.Log(partner.String())

		ptq.PushBlock(partner, peertask.Task{Identifier: letter, Priority: math.MaxInt32 - index})
	}
	for _, consonant := range consonants {
		ptq.Remove(consonant, partner)
	}

	ptq.FullThaw()

	var out []string
	for {
		received := ptq.PopBlock()
		if received == nil {
			break
		}

		for _, task := range received.Tasks {
			out = append(out, task.Identifier.(string))
		}
	}

	// Tasks popped should already be in correct order
	for i, expected := range vowels {
		if out[i] != expected {
			t.Fatal("received", out[i], "expected", expected)
		}
	}
}

// This test checks that peers wont starve out other peers
func TestPeerRepeats(t *testing.T) {
	ptq := New()
	peers := testutil.GeneratePeers(4)
	a := peers[0]
	b := peers[1]
	c := peers[2]
	d := peers[3]

	// Have each push some blocks

	for i := 0; i < 5; i++ {
		is := fmt.Sprint(i)
		ptq.PushBlock(a, peertask.Task{Identifier: is})
		ptq.PushBlock(b, peertask.Task{Identifier: is})
		ptq.PushBlock(c, peertask.Task{Identifier: is})
		ptq.PushBlock(d, peertask.Task{Identifier: is})
	}

	// now, pop off four tasks, there should be one from each
	var targets []string
	var tasks []*peertask.TaskBlock
	for i := 0; i < 4; i++ {
		t := ptq.PopBlock()
		targets = append(targets, t.Target.Pretty())
		tasks = append(tasks, t)
	}

	expected := []string{a.Pretty(), b.Pretty(), c.Pretty(), d.Pretty()}
	sort.Strings(expected)
	sort.Strings(targets)

	t.Log(targets)
	t.Log(expected)
	for i, s := range targets {
		if expected[i] != s {
			t.Fatal("unexpected peer", s, expected[i])
		}
	}

	// Now, if one of the tasks gets finished, the next task off the queue should
	// be for the same peer
	for blockI := 0; blockI < 4; blockI++ {
		for i := 0; i < 4; i++ {
			// its okay to mark the same task done multiple times here (JUST FOR TESTING)
			tasks[i].Done(tasks[i].Tasks)

			ntask := ptq.PopBlock()
			if ntask.Target != tasks[i].Target {
				t.Fatal("Expected task from peer with lowest active count")
			}
		}
	}
}
