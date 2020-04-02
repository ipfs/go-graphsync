package testutil

import (
	"testing"

	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/stretchr/testify/require"
)

func TestFailParseSelectorSpec(t *testing.T) {
	spec := NewUnparsableSelectorSpec()
	_, err := ipldutil.ParseSelector(spec)
	require.Error(t, err, "unparsable selector should not parse")
}

func TestFailEncodingSelectorSpec(t *testing.T) {
	spec := NewUnencodableSelectorSpec()
	_, err := ipldutil.EncodeNode(spec)
	require.Error(t, err, "unencodable selector should not encode")
}
