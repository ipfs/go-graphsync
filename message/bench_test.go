package message_test

import (
	"bytes"
	"math/rand"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/message"
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
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := message.NamedExtension{
		Name: extensionName,
		Data: basicnode.NewBytes(testutil.RandomBytes(100)),
	}
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	status := graphsync.RequestAcknowledged

	builder := message.NewBuilder()
	builder.AddRequest(message.NewRequest(id, root, selector, priority, extension))
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

				err := gsm.ToNet(buf)
				require.NoError(b, err)

				gsm2, err := message.FromNet(buf)
				require.NoError(b, err)
				require.Equal(b, gsm, gsm2)
			}
		})
	})

	b.Run("DagCbor", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			buf := new(bytes.Buffer)
			for pb.Next() {
				buf.Reset()

				node := bindnode.Wrap(&gsm, message.Prototype.Message.Type())
				err := dagcbor.Encode(node.Representation(), buf)
				require.NoError(b, err)

				builder := message.Prototype.Message.Representation().NewBuilder()
				err = dagcbor.Decode(builder, buf)
				require.NoError(b, err)
				node2 := builder.Build()
				gsm2 := *bindnode.Unwrap(node2).(*message.GraphSyncMessage)
				require.Equal(b, gsm, gsm2)
			}
		})
	})
}
