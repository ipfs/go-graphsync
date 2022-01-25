package message

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/message/ipldbind"
	"github.com/ipfs/go-graphsync/testutil"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
)

func BenchmarkMessageEncodingRoundtrip(b *testing.B) {
	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	bb := basicnode.Prototype.Bytes.NewBuilder()
	bb.AssignBytes(testutil.RandomBytes(100))
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: bb.Build(),
	}
	id := graphsync.NewRequestID()
	priority := graphsync.Priority(rand.Int31())
	status := graphsync.RequestAcknowledged

	builder := NewBuilder()
	builder.AddRequest(NewRequest(id, root, selector, priority, extension))
	builder.AddRequest(NewRequest(id, root, selector, priority))
	builder.AddResponseCode(id, status)
	builder.AddExtensionData(id, extension)
	builder.AddBlock(blocks.NewBlock([]byte("W")))
	builder.AddBlock(blocks.NewBlock([]byte("E")))
	builder.AddBlock(blocks.NewBlock([]byte("F")))
	builder.AddBlock(blocks.NewBlock([]byte("M")))

	gsm, err := builder.Build()
	require.NoError(b, err)

	b.Run("Protobuf", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			buf := new(bytes.Buffer)
			for pb.Next() {
				buf.Reset()

				err := NewMessageHandler().ToNet(gsm, buf)
				require.NoError(b, err)

				gsm2, err := NewMessageHandler().FromNet(buf)
				require.NoError(b, err)

				// Note that require.Equal doesn't seem to handle maps well.
				// It says they are non-equal simply because their order isn't deterministic.
				if diff := cmp.Diff(gsm, gsm2, cmp.Exporter(func(reflect.Type) bool { return true })); diff != "" {
					b.Fatal(diff)
				}
			}
		})
	})

	b.Run("DagCbor", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			buf := new(bytes.Buffer)
			for pb.Next() {
				buf.Reset()

				ipldGSM, err := NewMessageHandler().toIPLD(gsm)
				require.NoError(b, err)
				node := bindnode.Wrap(ipldGSM, ipldbind.Prototype.Message.Type())
				err = dagcbor.Encode(node.Representation(), buf)
				require.NoError(b, err)

				builder := ipldbind.Prototype.Message.Representation().NewBuilder()
				err = dagcbor.Decode(builder, buf)
				require.NoError(b, err)
				node2 := builder.Build()
				ipldGSM2 := bindnode.Unwrap(node2).(*ipldbind.GraphSyncMessage)
				gsm2, err := NewMessageHandler().fromIPLD(ipldGSM2)
				require.NoError(b, err)

				// same as above.
				if diff := cmp.Diff(gsm, gsm2, cmp.Exporter(func(reflect.Type) bool { return true })); diff != "" {
					b.Fatal(diff)
				}
			}
		})
	})
}
