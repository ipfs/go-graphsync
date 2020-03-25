package testutil

import (
	"testing"

	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/stretchr/testify/require"
)

func TestFailParseSelectorSpec(t *testing.T) {
	spec := NewUnparsableSelectorSpec()
	_, err := ipldutil.ParseSelector(spec)
	require.NotNil(t, err, "Spec should not decompose to node and selector")
}

func TestFailEncodingSelectorSpec(t *testing.T) {
	spec := NewUnencodableSelectorSpec()
	_, err := ipldutil.EncodeNode(spec)
	require.NotNil(t, err, "Spec should not be encodable")
}
