package bench

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/message"
	v2 "github.com/ipfs/go-graphsync/message/v2"
	"github.com/ipfs/go-graphsync/testutil"
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

	builder := message.NewBuilder()
	builder.AddRequest(message.NewRequest(id, root, selector, priority, extension))
	builder.AddRequest(message.NewRequest(id, root, selector, priority))
	builder.AddResponseCode(id, status)
	builder.AddExtensionData(id, extension)
	builder.AddBlock(blocks.NewBlock([]byte("W")))
	builder.AddBlock(blocks.NewBlock([]byte("E")))
	builder.AddBlock(blocks.NewBlock([]byte("F")))
	builder.AddBlock(blocks.NewBlock([]byte("M")))

	p := peer.ID("test peer")
	gsm, err := builder.Build()
	require.NoError(b, err)

	b.Run("DagCbor", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			buf := new(bytes.Buffer)
			mh := v2.NewMessageHandler()
			for pb.Next() {
				buf.Reset()

				err := mh.ToNet(p, gsm, buf)
				require.NoError(b, err)

				gsm2, err := mh.FromNet(p, buf)
				require.NoError(b, err)

				// same as above.
				if diff := cmp.Diff(gsm, gsm2,
					cmp.Exporter(func(reflect.Type) bool { return true }),
					cmp.Comparer(ipld.DeepEqual),
				); diff != "" {
					b.Fatal(diff)
				}
			}
		})
	})
}
