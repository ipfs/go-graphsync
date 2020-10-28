package graphsync_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/benchmarks/testinstance"
	tn "github.com/filecoin-project/go-data-transfer/benchmarks/testnet"
	"github.com/filecoin-project/go-data-transfer/testutil"
)

const stdBlockSize = 8000

type runStats struct {
	Time time.Duration
	Name string
}

var benchmarkLog []runStats

func BenchmarkRoundtripSuccess(b *testing.B) {
	ctx := context.Background()
	tdm, err := newTempDirMaker(b)
	require.NoError(b, err)
	b.Run("test-p2p-stress-10-128MB", func(b *testing.B) {
		p2pStrestTest(ctx, b, 10, allFilesUniformSize(128*(1<<20), 1<<20, 1024, true), tdm, false)
	})
	b.Run("test-p2p-stress-10-128MB-1KB-chunks", func(b *testing.B) {
		p2pStrestTest(ctx, b, 10, allFilesUniformSize(128*(1<<20), 1<<10, 1024, true), tdm, false)
	})
	b.Run("test-p2p-stress-1-1GB", func(b *testing.B) {
		p2pStrestTest(ctx, b, 1, allFilesUniformSize(1*(1<<30), 1<<20, 1024, true), tdm, true)
	})
	b.Run("test-p2p-stress-1-1GB-no-raw-nodes", func(b *testing.B) {
		p2pStrestTest(ctx, b, 1, allFilesUniformSize(1*(1<<30), 1<<20, 1024, false), tdm, true)
	})
}

func p2pStrestTest(ctx context.Context, b *testing.B, numfiles int, df distFunc, tdm *tempDirMaker, diskBasedDatastore bool) {
	mn := mocknet.New(ctx)
	mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 100 * time.Millisecond, Bandwidth: 16 << 20})
	net := tn.StreamNet(ctx, mn)
	ig := testinstance.NewTestInstanceGenerator(ctx, net, tdm, diskBasedDatastore)
	instances, err := ig.Instances(1 + b.N)
	require.NoError(b, err)
	var allCids []cid.Cid
	for i := 0; i < numfiles; i++ {
		thisCids := df(ctx, b, instances[:1])
		allCids = append(allCids, thisCids...)
	}
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	allSelector := ssb.ExploreRecursive(ipldselector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	runtime.GC()
	b.ResetTimer()
	b.ReportAllocs()
	pusher := instances[0]
	done := make(chan struct{}, numfiles*2)
	pusher.Manager.SubscribeToEvents(func(event datatransfer.Event, state datatransfer.ChannelState) {
		if state.Status() == datatransfer.Completed {
			done <- struct{}{}
		}
	})
	for i := 0; i < b.N; i++ {
		receiver := instances[i+1]
		receiver.Manager.SubscribeToEvents(func(event datatransfer.Event, state datatransfer.ChannelState) {
			if state.Status() == datatransfer.Completed {
				done <- struct{}{}
			}
		})
		timer := time.NewTimer(10 * time.Second)
		start := time.Now()
		for j := 0; j < numfiles; j++ {
			_, err := pusher.Manager.OpenPushDataChannel(ctx, receiver.Peer, testutil.NewFakeDTType(), allCids[j], allSelector)
			if err != nil {
				b.Fatalf("received error on request: %s", err.Error())
			}
		}
		finished := 0
		for finished < numfiles*2 {
			select {
			case <-done:
				finished++
			case <-timer.C:
				runtime.GC()
				b.Fatalf("did not complete requests in time")
			}
		}
		result := runStats{
			Time: time.Since(start),
			Name: b.Name(),
		}
		benchmarkLog = append(benchmarkLog, result)
		receiver.Close()
	}
	testinstance.Close(instances)
	ig.Close()
}

type distFunc func(ctx context.Context, b *testing.B, provs []testinstance.Instance) []cid.Cid

const defaultUnixfsChunkSize uint64 = 1 << 10
const defaultUnixfsLinksPerLevel = 1024

func loadRandomUnixFxFile(ctx context.Context, b *testing.B, bs blockstore.Blockstore, size uint64, unixfsChunkSize uint64, unixfsLinksPerLevel int, useRawNodes bool) cid.Cid {

	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(b, err)
	buf := bytes.NewReader(data)
	file := files.NewReaderFile(buf)

	dagService := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dagService)

	params := ihelper.DagBuilderParams{
		Maxlinks:   unixfsLinksPerLevel,
		RawLeaves:  useRawNodes,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	db, err := params.New(chunker.NewSizeSplitter(file, int64(unixfsChunkSize)))
	require.NoError(b, err, "unable to setup dag builder")

	nd, err := balanced.Layout(db)
	require.NoError(b, err, "unable to create unix fs node")

	err = bufferedDS.Commit()
	require.NoError(b, err, "unable to commit unix fs node")

	return nd.Cid()
}

func allFilesUniformSize(size uint64, unixfsChunkSize uint64, unixfsLinksPerLevel int, useRawNodes bool) distFunc {
	return func(ctx context.Context, b *testing.B, provs []testinstance.Instance) []cid.Cid {
		cids := make([]cid.Cid, 0, len(provs))
		for _, prov := range provs {
			c := loadRandomUnixFxFile(ctx, b, prov.BlockStore, size, unixfsChunkSize, unixfsLinksPerLevel, useRawNodes)
			cids = append(cids, c)
		}
		return cids
	}
}

type tempDirMaker struct {
	tdm        string
	tempDirSeq int32
	b          *testing.B
}

var tempDirReplacer struct {
	sync.Once
	r *strings.Replacer
}

// Cribbed from https://github.com/golang/go/blob/master/src/testing/testing.go#L890
// and modified as needed due to https://github.com/golang/go/issues/41062
func newTempDirMaker(b *testing.B) (*tempDirMaker, error) {
	c := &tempDirMaker{}
	// ioutil.TempDir doesn't like path separators in its pattern,
	// so mangle the name to accommodate subtests.
	tempDirReplacer.Do(func() {
		tempDirReplacer.r = strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	})
	pattern := tempDirReplacer.r.Replace(b.Name())

	var err error
	c.tdm, err = ioutil.TempDir("", pattern)
	if err != nil {
		return nil, err
	}
	b.Cleanup(func() {
		if err := os.RemoveAll(c.tdm); err != nil {
			b.Errorf("TempDir RemoveAll cleanup: %v", err)
		}
	})
	return c, nil
}

func (tdm *tempDirMaker) TempDir() string {
	seq := atomic.AddInt32(&tdm.tempDirSeq, 1)
	dir := fmt.Sprintf("%s%c%03d", tdm.tdm, os.PathSeparator, seq)
	if err := os.Mkdir(dir, 0777); err != nil {
		tdm.b.Fatalf("TempDir: %v", err)
	}
	return dir
}
