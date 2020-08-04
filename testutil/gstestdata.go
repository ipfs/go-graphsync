package testutil

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	dss "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-storedcounter"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
)

var allSelector ipld.Node

func init() {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Style.Any)
	allSelector = ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()
}

const unixfsChunkSize uint64 = 1 << 10
const unixfsLinksPerLevel = 1024

// GraphsyncTestingData is a test harness for testing data transfer on top of
// graphsync
type GraphsyncTestingData struct {
	Ctx            context.Context
	StoredCounter1 *storedcounter.StoredCounter
	StoredCounter2 *storedcounter.StoredCounter
	DtDs1          datastore.Datastore
	DtDs2          datastore.Datastore
	Bs1            bstore.Blockstore
	Bs2            bstore.Blockstore
	DagService1    ipldformat.DAGService
	DagService2    ipldformat.DAGService
	Loader1        ipld.Loader
	Loader2        ipld.Loader
	Storer1        ipld.Storer
	Storer2        ipld.Storer
	Host1          host.Host
	Host2          host.Host
	GsNet1         gsnet.GraphSyncNetwork
	GsNet2         gsnet.GraphSyncNetwork
	DtNet1         network.DataTransferNetwork
	DtNet2         network.DataTransferNetwork
	AllSelector    ipld.Node
	OrigBytes      []byte
}

// NewGraphsyncTestingData returns a new GraphsyncTestingData instance
func NewGraphsyncTestingData(ctx context.Context, t *testing.T) *GraphsyncTestingData {

	gsData := &GraphsyncTestingData{}
	gsData.Ctx = ctx
	ds1 := dss.MutexWrap(datastore.NewMapDatastore())
	ds2 := dss.MutexWrap(datastore.NewMapDatastore())

	gsData.DtDs1 = namespace.Wrap(ds1, datastore.NewKey("datatransfer"))
	gsData.DtDs2 = namespace.Wrap(ds2, datastore.NewKey("datatransfer"))

	// make a blockstore and dag service
	gsData.Bs1 = bstore.NewBlockstore(namespace.Wrap(ds1, datastore.NewKey("blockstore")))
	gsData.Bs2 = bstore.NewBlockstore(namespace.Wrap(ds2, datastore.NewKey("blockstore")))

	// make stored counters
	gsData.StoredCounter1 = storedcounter.New(ds1, datastore.NewKey("counter"))
	gsData.StoredCounter2 = storedcounter.New(ds2, datastore.NewKey("counter"))

	gsData.DagService1 = merkledag.NewDAGService(blockservice.New(gsData.Bs1, offline.Exchange(gsData.Bs1)))
	gsData.DagService2 = merkledag.NewDAGService(blockservice.New(gsData.Bs2, offline.Exchange(gsData.Bs2)))

	// setup an IPLD loader/storer for blockstore 1
	gsData.Loader1 = storeutil.LoaderForBlockstore(gsData.Bs1)
	gsData.Storer1 = storeutil.StorerForBlockstore(gsData.Bs1)

	// setup an IPLD loader/storer for blockstore 2
	gsData.Loader2 = storeutil.LoaderForBlockstore(gsData.Bs2)
	gsData.Storer2 = storeutil.StorerForBlockstore(gsData.Bs2)

	mn := mocknet.New(ctx)

	// setup network
	var err error
	gsData.Host1, err = mn.GenPeer()
	require.NoError(t, err)

	gsData.Host2, err = mn.GenPeer()
	require.NoError(t, err)

	err = mn.LinkAll()
	require.NoError(t, err)

	gsData.GsNet1 = gsnet.NewFromLibp2pHost(gsData.Host1)
	gsData.GsNet2 = gsnet.NewFromLibp2pHost(gsData.Host2)

	gsData.DtNet1 = network.NewFromLibp2pHost(gsData.Host1)
	gsData.DtNet2 = network.NewFromLibp2pHost(gsData.Host2)

	// create a selector for the whole UnixFS dag
	gsData.AllSelector = allSelector

	return gsData
}

// SetupGraphsyncHost1 sets up a new, real graphsync instance on top of the first host
func (gsData *GraphsyncTestingData) SetupGraphsyncHost1() graphsync.GraphExchange {
	// setup graphsync
	return gsimpl.New(gsData.Ctx, gsData.GsNet1, gsData.Loader1, gsData.Storer1)
}

// SetupGSTransportHost1 sets up a new grapshync transport over real graphsync on the first host
func (gsData *GraphsyncTestingData) SetupGSTransportHost1() datatransfer.Transport {
	// setup graphsync
	gs := gsData.SetupGraphsyncHost1()
	return gstransport.NewTransport(gsData.Host1.ID(), gs)
}

// SetupGraphsyncHost2 sets up a new, real graphsync instance on top of the second host
func (gsData *GraphsyncTestingData) SetupGraphsyncHost2() graphsync.GraphExchange {
	// setup graphsync
	return gsimpl.New(gsData.Ctx, gsData.GsNet2, gsData.Loader2, gsData.Storer2)
}

// SetupGSTransportHost2 sets up a new grapshync transport over real graphsync on the second host
func (gsData *GraphsyncTestingData) SetupGSTransportHost2() datatransfer.Transport {
	// setup graphsync
	gs := gsData.SetupGraphsyncHost2()
	return gstransport.NewTransport(gsData.Host2.ID(), gs)
}

// LoadUnixFSFile loads a fixtures file we can test dag transfer with
func (gsData *GraphsyncTestingData) LoadUnixFSFile(t *testing.T, useSecondNode bool) ipld.Link {
	// import to UnixFS
	var dagService ipldformat.DAGService
	if useSecondNode {
		dagService = gsData.DagService2
	} else {
		dagService = gsData.DagService1
	}

	link, origBytes := LoadUnixFSFile(gsData.Ctx, t, dagService)
	gsData.OrigBytes = origBytes
	return link
}

// LoadUnixFSFile loads a fixtures file into the given DAG Service, returning an ipld.Link for the file
// and the original file bytes
func LoadUnixFSFile(ctx context.Context, t *testing.T, dagService ipldformat.DAGService) (ipld.Link, []byte) {
	_, curFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	// read in a fixture file
	path := filepath.Join(path.Dir(curFile), "fixtures", "lorem.txt")

	f, err := os.Open(path)
	require.NoError(t, err)

	var buf bytes.Buffer
	tr := io.TeeReader(f, &buf)
	file := files.NewReaderFile(tr)

	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dagService)

	params := ihelper.DagBuilderParams{
		Maxlinks:   unixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	db, err := params.New(chunker.NewSizeSplitter(file, int64(unixfsChunkSize)))
	require.NoError(t, err)

	nd, err := balanced.Layout(db)
	require.NoError(t, err)

	err = bufferedDS.Commit()
	require.NoError(t, err)

	// save the original files bytes
	return cidlink.Link{Cid: nd.Cid()}, buf.Bytes()
}

// VerifyFileTransferred verifies all of the file was transfer to the given node
func (gsData *GraphsyncTestingData) VerifyFileTransferred(t *testing.T, link ipld.Link, useSecondNode bool) {
	var dagService ipldformat.DAGService
	if useSecondNode {
		dagService = gsData.DagService2
	} else {
		dagService = gsData.DagService1
	}

	VerifyHasFile(gsData.Ctx, t, dagService, link, gsData.OrigBytes)
}

// VerifyHasFile verifies the presence of the given file with the given ipld.Link and file contents (fileBytes)
// exists in the given blockstore identified by dagService
func VerifyHasFile(ctx context.Context, t *testing.T, dagService ipldformat.DAGService, link ipld.Link, fileBytes []byte) {
	c := link.(cidlink.Link).Cid

	// load the root of the UnixFS DAG from the new blockstore
	otherNode, err := dagService.Get(ctx, c)
	require.NoError(t, err)

	// Setup a UnixFS file reader
	n, err := unixfile.NewUnixfsFile(ctx, dagService, otherNode)
	require.NoError(t, err)

	fn, ok := n.(files.File)
	require.True(t, ok)

	// Read the bytes for the UnixFS File
	finalBytes, err := ioutil.ReadAll(fn)
	require.NoError(t, err)

	// verify original bytes match final bytes!
	require.EqualValues(t, fileBytes, finalBytes)
}
