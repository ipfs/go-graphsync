package testbridge

import (
	"testing"

	"github.com/ipfs/go-graphsync/ipldbridge"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestFailParseSelectorSpec(t *testing.T) {
	cids := testutil.GenerateCids(5)
	spec := NewUnparsableSelectorSpec(cids)
	bridge := ipldbridge.NewIPLDBridge()
	_, err := bridge.ParseSelector(spec)
	if err == nil {
		t.Fatal("Spec should not decompose to node and selector")
	}
}

func TestFailEncodingSelectorSpec(t *testing.T) {
	cids := testutil.GenerateCids(5)
	spec := NewUnencodableSelectorSpec(cids)
	_, err := ipldbridge.EncodeNode(spec)
	if err == nil {
		t.Fatal("Spec should not be encodable")
	}
}
