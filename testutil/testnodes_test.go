package testutil

import (
	"testing"

	"github.com/ipfs/go-graphsync/ipldutil"
)

func TestFailParseSelectorSpec(t *testing.T) {
	spec := NewUnparsableSelectorSpec()
	_, err := ipldutil.ParseSelector(spec)
	if err == nil {
		t.Fatal("Spec should not decompose to node and selector")
	}
}

func TestFailEncodingSelectorSpec(t *testing.T) {
	spec := NewUnencodableSelectorSpec()
	_, err := ipldutil.EncodeNode(spec)
	if err == nil {
		t.Fatal("Spec should not be encodable")
	}
}
