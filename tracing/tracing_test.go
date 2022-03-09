package tracing_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/testutil"
	"github.com/filecoin-project/go-data-transfer/tracing"
)

func TestSpansIndex(t *testing.T) {
	ctx := context.Background()
	peers := testutil.GeneratePeers(2)
	chid1 := datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: datatransfer.TransferID(rand.Uint32())}
	chid2 := datatransfer.ChannelID{Initiator: peers[0], Responder: peers[1], ID: datatransfer.TransferID(rand.Uint32())}
	testCases := map[string]struct {
		operation      func(ctx context.Context, si *tracing.SpansIndex)
		expectedTraces []string
	}{
		"sets up new span only once": {
			operation: func(ctx context.Context, si *tracing.SpansIndex) {
				si.SpanForChannel(ctx, chid1)
				si.SpanForChannel(ctx, chid1)
				si.EndChannelSpan(chid1)
			},
			expectedTraces: []string{"transfer(0)"},
		},
		"sets up new span if previous ended": {
			operation: func(ctx context.Context, si *tracing.SpansIndex) {
				si.SpanForChannel(ctx, chid1)
				si.EndChannelSpan(chid1)
				si.SpanForChannel(ctx, chid1)
				si.EndChannelSpan(chid1)
			},
			expectedTraces: []string{"transfer(0)", "transfer(1)"},
		},
		"sets up multiple spans for different requests": {
			operation: func(ctx context.Context, si *tracing.SpansIndex) {
				si.SpanForChannel(ctx, chid1)
				si.SpanForChannel(ctx, chid2)
				si.EndAll()
			},
			expectedTraces: []string{"transfer(0)", "transfer(1)"},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, collectTracing := testutil.SetupTracing(ctx)
			si := tracing.NewSpansIndex()
			data.operation(ctx, si)
			traces := collectTracing(t).TracesToStrings(3)
			require.Equal(t, data.expectedTraces, traces)
		})
	}
}
