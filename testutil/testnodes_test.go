package testutil

import (
	"testing"

	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/stretchr/testify/require"
)

func TestFailParseSelectorSpec(t *testing.T) {
	spec := NewUnparsableSelectorSpec()
	_, err := selector.ParseSelector(spec)
	require.Error(t, err, "unparsable selector should not parse")
}
