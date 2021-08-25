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
	delay "github.com/ipfs/go-ipfs-delay"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/ipfs/go-graphsync/benchmarks/testinstance"
	tn "github.com/ipfs/go-graphsync/benchmarks/testnet"
	graphsync "github.com/ipfs/go-graphsync/impl"
)

type runStats struct {
	Time time.Duration
	Name string
}

var benchmarkLog []runStats

func BenchmarkRoundtripSuccess(b *testing.B) {
	ctx := context.Background()
	tdm, err := newTempDirMaker(b)
	require.NoError(b, err)
	b.Run("test-20-10000", func(b *testing.B) {
		subtestDistributeAndFetch(ctx, b, 20, delay.Fixed(0), time.Duration(0), allFilesUniformSize(10000, defaultUnixfsChunkSize, defaultUnixfsLinksPerLevel, true), tdm)
	})
	b.Run("test-20-128MB", func(b *testing.B) {
		subtestDistributeAndFetch(ctx, b, 20, delay.Fixed(0), time.Duration(0), allFilesUniformSize(128*(1<<20), defaultUnixfsChunkSize, defaultUnixfsLinksPerLevel, true), tdm)
	})
	b.Run("test-p2p-stress-10-128MB", func(b *testing.B) {
		p2pStrestTest(ctx, b, 10, allFilesUniformSize(128*(1<<20), 1<<20, 1024, true), tdm, nil, false)
	})
	b.Run("test-p2p-stress-10-128MB-1KB-chunks", func(b *testing.B) {
		p2pStrestTest(ctx, b, 10, allFilesUniformSize(128*(1<<20), 1<<10, 1024, true), tdm, nil, false)
	})
	b.Run("test-p2p-stress-1-1GB-memory-pressure", func(b *testing.B) {
		p2pStrestTest(ctx, b, 1, allFilesUniformSize(1*(1<<30), 1<<20, 1024, true), tdm, []graphsync.Option{graphsync.MaxMemoryResponder(1 << 27)}, true)
	})
	b.Run("test-p2p-stress-1-1GB-memory-pressure-missing-blocks", func(b *testing.B) {
		p2pStrestTest(ctx, b, 1, allFilesMissingTopLevelBlock(1*(1<<30), 1<<20, 1024, true), tdm, []graphsync.Option{graphsync.MaxMemoryResponder(1 << 27)}, true)
	})
	b.Run("test-p2p-stress-1-1GB-memory-pressure-no-raw-nodes", func(b *testing.B) {
		p2pStrestTest(ctx, b, 1, allFilesUniformSize(1*(1<<30), 1<<20, 1024, false), tdm, []graphsync.Option{graphsync.MaxMemoryResponder(1 << 27)}, true)
	})
	b.Run("test-repeated-disconnects-20-10000", func(b *testing.B) {
		benchmarkRepeatedDisconnects(ctx, b, 20, allFilesUniformSize(10000, defaultUnixfsChunkSize, defaultUnixfsLinksPerLevel, true), tdm)
	})
}

func benchmarkRepeatedDisconnects(ctx context.Context, b *testing.B, numnodes int, df distFunc, tdm *tempDirMaker) {
	ctx, cancel := context.WithCancel(ctx)
	mn := mocknet.New(ctx)
	net := tn.StreamNet(ctx, mn)
	ig := testinstance.NewTestInstanceGenerator(ctx, net, nil, tdm, false)
	instances, err := ig.Instances(numnodes + 1)
	require.NoError(b, err)
	var allCids [][]cid.Cid
	for i := 0; i < b.N; i++ {
		thisCids := df(ctx, b, instances[1:])
		allCids = append(allCids, thisCids)
	}
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	allSelector := ssb.ExploreRecursive(ipldselector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	runtime.GC()
	b.ResetTimer()
	b.ReportAllocs()
	fetcher := instances[0]
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		require.NoError(b, err)
		start := time.Now()
		errgrp, grpctx := errgroup.WithContext(ctx)
		for j := 0; j < numnodes; j++ {
			instance := instances[j+1]
			_, errChan := fetcher.Exchange.Request(grpctx, instance.Peer, cidlink.Link{Cid: allCids[i][j]}, allSelector)
			other := instance.Peer

			errgrp.Go(func() error {
				defer func() {
					_ = mn.DisconnectPeers(fetcher.Peer, other)
				}()
				for {
					select {
					case <-grpctx.Done():
						return nil
					case err, ok := <-errChan:
						if !ok {
							return nil
						}
						return err
					}
				}
			})

		}
		if err := errgrp.Wait(); err != nil {
			b.Fatalf("received error on request: %s", err.Error())
		}
		result := runStats{
			Time: time.Since(start),
			Name: b.Name(),
		}
		benchmarkLog = append(benchmarkLog, result)

		cancel()
	}
	cancel()
	time.Sleep(100 * time.Millisecond)
	b.Logf("Number of running go-routines: %d", runtime.NumGoroutine())
	testinstance.Close(instances)
	ig.Close()
}

func p2pStrestTest(ctx context.Context, b *testing.B, numfiles int, df distFunc, tdm *tempDirMaker, options []graphsync.Option, diskBasedDatastore bool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	mn := mocknet.New(ctx)
	mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 100 * time.Millisecond, Bandwidth: 3000000})
	net := tn.StreamNet(ctx, mn)
	ig := testinstance.NewTestInstanceGenerator(ctx, net, options, tdm, diskBasedDatastore)
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
	for i := 0; i < b.N; i++ {
		fetcher := instances[i+1]
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		require.NoError(b, err)
		start := time.Now()
		errgrp, grpctx := errgroup.WithContext(ctx)
		for j := 0; j < numfiles; j++ {
			responseChan, errChan := fetcher.Exchange.Request(grpctx, instances[0].Peer, cidlink.Link{Cid: allCids[j]}, allSelector)
			errgrp.Go(func() error {
				for range responseChan {
				}
				for err := range errChan {
					return err
				}
				return nil
			})
		}
		if err := errgrp.Wait(); err != nil {
			b.Fatalf("received error on request: %s", err.Error())
		}
		result := runStats{
			Time: time.Since(start),
			Name: b.Name(),
		}
		benchmarkLog = append(benchmarkLog, result)
		cancel()
		fetcher.Close()
	}
	testinstance.Close(instances)
	ig.Close()
}
func subtestDistributeAndFetch(ctx context.Context, b *testing.B, numnodes int, d delay.D, bstoreLatency time.Duration, df distFunc, tdm *tempDirMaker) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	net := tn.VirtualNetwork(d)
	ig := testinstance.NewTestInstanceGenerator(ctx, net, nil, tdm, false)
	instances, err := ig.Instances(numnodes + b.N)
	require.NoError(b, err)
	destCids := df(ctx, b, instances[:numnodes])
	// Set the blockstore latency on seed nodes
	if bstoreLatency > 0 {
		for _, i := range instances {
			i.SetBlockstoreLatency(bstoreLatency)
		}
	}
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	allSelector := ssb.ExploreRecursive(ipldselector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	runtime.GC()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fetcher := instances[i+numnodes]
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		require.NoError(b, err)
		start := time.Now()
		errgrp, grpctx := errgroup.WithContext(ctx)
		for j := 0; j < numnodes; j++ {
			instance := instances[j]
			_, errChan := fetcher.Exchange.Request(grpctx, instance.Peer, cidlink.Link{Cid: destCids[j]}, allSelector)

			errgrp.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						return err
					case err, ok := <-errChan:
						if !ok {
							return nil
						}
						return err
					}
				}
			})
		}
		if err := errgrp.Wait(); err != nil {
			b.Fatalf("received error on request: %s", err.Error())
		}
		result := runStats{
			Time: time.Since(start),
			Name: b.Name(),
		}
		benchmarkLog = append(benchmarkLog, result)
		cancel()
		fetcher.Close()
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

func allFilesMissingTopLevelBlock(size uint64, unixfsChunkSize uint64, unixfsLinksPerLevel int, useRawNodes bool) distFunc {
	return func(ctx context.Context, b *testing.B, provs []testinstance.Instance) []cid.Cid {
		cids := make([]cid.Cid, 0, len(provs))
		for _, prov := range provs {
			c := loadRandomUnixFxFile(ctx, b, prov.BlockStore, size, unixfsChunkSize, unixfsLinksPerLevel, useRawNodes)
			ds := merkledag.NewDAGService(blockservice.New(prov.BlockStore, offline.Exchange(prov.BlockStore)))
			lnks, err := ds.GetLinks(ctx, c)
			require.NoError(b, err)
			randLink := lnks[rand.Intn(len(lnks))]
			err = ds.Remove(ctx, randLink.Cid)
			require.NoError(b, err)
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
