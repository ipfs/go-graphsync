package testinstance

import (
	"context"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	badgerds "github.com/ipfs/go-ds-badger"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/ipld/go-ipld-prime"
	peer "github.com/libp2p/go-libp2p-core/peer"
	p2ptestutil "github.com/libp2p/go-libp2p-netutil"
	tnet "github.com/libp2p/go-libp2p-testing/net"

	graphsync "github.com/ipfs/go-graphsync"
	tn "github.com/ipfs/go-graphsync/benchmarks/testnet"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
)

// TempDirGenerator is any interface that can generate temporary directories
type TempDirGenerator interface {
	TempDir() string
}

// NewTestInstanceGenerator generates a new InstanceGenerator for the given
// testnet
func NewTestInstanceGenerator(ctx context.Context, net tn.Network, gsOptions []gsimpl.Option, tempDirGenerator TempDirGenerator, diskBasedDatastore bool) InstanceGenerator {
	ctx, cancel := context.WithCancel(ctx)
	return InstanceGenerator{
		net:                net,
		seq:                0,
		ctx:                ctx, // TODO take ctx as param to Next, Instances
		cancel:             cancel,
		gsOptions:          gsOptions,
		tempDirGenerator:   tempDirGenerator,
		diskBasedDatastore: diskBasedDatastore,
	}
}

// InstanceGenerator generates new test instances of bitswap+dependencies
type InstanceGenerator struct {
	seq                int
	net                tn.Network
	ctx                context.Context
	cancel             context.CancelFunc
	gsOptions          []gsimpl.Option
	tempDirGenerator   TempDirGenerator
	diskBasedDatastore bool
}

// Close closes the clobal context, shutting down all test instances
func (g *InstanceGenerator) Close() error {
	g.cancel()
	return nil // for Closer interface
}

// Next generates a new instance of graphsync + dependencies
func (g *InstanceGenerator) Next() (Instance, error) {
	g.seq++
	p, err := p2ptestutil.RandTestBogusIdentity()
	if err != nil {
		return Instance{}, err
	}
	return NewInstance(g.ctx, g.net, p, g.gsOptions, g.tempDirGenerator.TempDir(), g.diskBasedDatastore)
}

// Instances creates N test instances of bitswap + dependencies and connects
// them to each other
func (g *InstanceGenerator) Instances(n int) ([]Instance, error) {
	var instances []Instance
	for j := 0; j < n; j++ {
		inst, err := g.Next()
		if err != nil {
			return nil, err
		}
		instances = append(instances, inst)
	}
	ConnectInstances(instances)
	return instances, nil
}

// ConnectInstances connects the given instances to each other
func ConnectInstances(instances []Instance) {
	for i, inst := range instances {
		for j := i + 1; j < len(instances); j++ {
			oinst := instances[j]
			err := inst.Adapter.ConnectTo(context.Background(), oinst.Peer)
			if err != nil {
				panic(err.Error())
			}
		}
	}
}

// Close closes multiple instances at once
func Close(instances []Instance) error {
	for _, i := range instances {
		if err := i.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Instance is a test instance of bitswap + dependencies for integration testing
type Instance struct {
	Peer            peer.ID
	LinkSystem      ipld.LinkSystem
	Exchange        graphsync.GraphExchange
	BlockStore      blockstore.Blockstore
	Adapter         gsnet.GraphSyncNetwork
	blockstoreDelay delay.D
	ds              ds.Batching
}

// Close closes the associated datastore
func (i *Instance) Close() error {
	return i.ds.Close()
}

// Blockstore returns the block store for this test instance
func (i *Instance) Blockstore() blockstore.Blockstore {
	return i.BlockStore
}

// SetBlockstoreLatency customizes the artificial delay on receiving blocks
// from a blockstore test instance.
func (i *Instance) SetBlockstoreLatency(t time.Duration) time.Duration {
	return i.blockstoreDelay.Set(t)
}

// NewInstance creates a test bitswap instance.
//
// NB: It's easy make mistakes by providing the same peer ID to two different
// instances. To safeguard, use the InstanceGenerator to generate instances. It's
// just a much better idea.
func NewInstance(ctx context.Context, net tn.Network, p tnet.Identity, gsOptions []gsimpl.Option, tempDir string, diskBasedDatastore bool) (Instance, error) {
	bsdelay := delay.Fixed(0)

	adapter := net.Adapter(p)
	var dstore ds.Batching
	var err error
	if diskBasedDatastore {
		defopts := badgerds.DefaultOptions
		defopts.SyncWrites = false
		defopts.Truncate = true
		dstore, err = badgerds.NewDatastore(tempDir, &defopts)
		if err != nil {
			return Instance{}, err
		}
	} else {
		dstore = ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))
	}
	bstore, err := blockstore.CachedBlockstore(ctx,
		blockstore.NewBlockstore(dstore),
		blockstore.DefaultCacheOpts())
	if err != nil {
		return Instance{}, err
	}

	lsys := storeutil.LinkSystemForBlockstore(bstore)
	gs := gsimpl.New(ctx, adapter, lsys, gsOptions...)
	gs.RegisterIncomingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		hookActions.ValidateRequest()
	})

	return Instance{
		Adapter:         adapter,
		Peer:            p.ID(),
		Exchange:        gs,
		LinkSystem:      lsys,
		BlockStore:      bstore,
		blockstoreDelay: bsdelay,
		ds:              dstore,
	}, nil
}
