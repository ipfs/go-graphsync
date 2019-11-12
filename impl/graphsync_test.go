package graphsync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ipld/go-ipld-prime/fluent"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"

	"github.com/ipfs/go-graphsync"

	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	ipld "github.com/ipld/go-ipld-prime"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	mh "github.com/multiformats/go-multihash"
)

type receivedMessage struct {
	message gsmsg.GraphSyncMessage
	sender  peer.ID
}

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type receiver struct {
	messageReceived chan receivedMessage
}

func (r *receiver) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming gsmsg.GraphSyncMessage) {

	select {
	case <-ctx.Done():
	case r.messageReceived <- receivedMessage{incoming, sender}:
	}
}

func (r *receiver) ReceiveError(err error) {
}

func (r *receiver) Connected(p peer.ID) {
}

func (r *receiver) Disconnected(p peer.ID) {
}

type blockChain struct {
	genisisNode ipld.Node
	genisisLink ipld.Link
	middleNodes []ipld.Node
	middleLinks []ipld.Link
	tipNode     ipld.Node
	tipLink     ipld.Link
}

func createBlock(nb ipldbridge.NodeBuilder, parents []ipld.Link, size int64) ipld.Node {
	return nb.CreateMap(func(mb ipldbridge.MapBuilder, knb ipldbridge.NodeBuilder, vnb ipldbridge.NodeBuilder) {
		mb.Insert(knb.CreateString("Parents"), vnb.CreateList(func(lb ipldbridge.ListBuilder, vnb ipldbridge.NodeBuilder) {
			for _, parent := range parents {
				lb.Append(vnb.CreateLink(parent))
			}
		}))
		mb.Insert(knb.CreateString("Messages"), vnb.CreateList(func(lb ipldbridge.ListBuilder, vnb ipldbridge.NodeBuilder) {
			lb.Append(vnb.CreateBytes(testutil.RandomBytes(size)))
		}))
	})
}

func setupBlockChain(
	ctx context.Context,
	t *testing.T,
	storer ipldbridge.Storer,
	bridge ipldbridge.IPLDBridge,
	size int64,
	blockChainLength int) *blockChain {
	linkBuilder := cidlink.LinkBuilder{Prefix: cid.NewPrefixV1(cid.DagCBOR, mh.SHA2_256)}
	var genisisNode ipld.Node
	err := fluent.Recover(func() {
		nb := fluent.WrapNodeBuilder(ipldfree.NodeBuilder())
		genisisNode = createBlock(nb, []ipld.Link{}, size)
	})
	if err != nil {
		t.Fatal("Error creating genesis block")
	}
	genesisLink, err := linkBuilder.Build(ctx, ipldbridge.LinkContext{}, genisisNode, storer)
	if err != nil {
		t.Fatal("Error creating link to genesis block")
	}
	parent := genesisLink
	middleNodes := make([]ipld.Node, 0, blockChainLength-2)
	middleLinks := make([]ipld.Link, 0, blockChainLength-2)
	for i := 0; i < blockChainLength-2; i++ {
		var node ipld.Node
		err := fluent.Recover(func() {
			nb := fluent.WrapNodeBuilder(ipldfree.NodeBuilder())
			node = createBlock(nb, []ipld.Link{parent}, size)
		})
		if err != nil {
			t.Fatal("Error creating middle block")
		}
		middleNodes = append(middleNodes, node)
		link, err := linkBuilder.Build(ctx, ipldbridge.LinkContext{}, node, storer)
		if err != nil {
			t.Fatal("Error creating link to middle block")
		}
		middleLinks = append(middleLinks, link)
		parent = link
	}
	var tipNode ipld.Node
	err = fluent.Recover(func() {
		nb := fluent.WrapNodeBuilder(ipldfree.NodeBuilder())
		tipNode = createBlock(nb, []ipld.Link{parent}, size)
	})
	if err != nil {
		t.Fatal("Error creating tip block")
	}
	tipLink, err := linkBuilder.Build(ctx, ipldbridge.LinkContext{}, tipNode, storer)
	if err != nil {
		t.Fatal("Error creating link to tip block")
	}
	return &blockChain{genisisNode, genesisLink, middleNodes, middleLinks, tipNode, tipLink}
}

func TestMakeRequestToNetwork(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	// setup network
	host1, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	host2, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	err = mn.LinkAll()
	if err != nil {
		t.Fatal("error linking hosts")
	}

	gsnet1 := gsnet.NewFromLibp2pHost(host1)

	// setup receiving peer to just record message coming in
	gsnet2 := gsnet.NewFromLibp2pHost(host2)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	gsnet2.SetDelegate(r)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testbridge.NewMockStore(blockStore)
	bridge := ipldbridge.NewIPLDBridge()
	graphSync := New(ctx, gsnet1, bridge, loader, storer)

	blockChainLength := 100
	blockChain := setupBlockChain(ctx, t, storer, bridge, 100, blockChainLength)

	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	spec := ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(blockChainLength),
		ssb.ExploreFields(func(efsb ipldbridge.ExploreFieldsSpecBuilder) {
			efsb.Insert("Parents", ssb.ExploreAll(
				ssb.ExploreRecursiveEdge()))
		})).Node()

	extensionData := testutil.RandomBytes(100)
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionData,
	}

	requestCtx, requestCancel := context.WithCancel(ctx)
	defer requestCancel()
	graphSync.Request(requestCtx, host2.ID(), blockChain.tipLink, spec, extension)

	var message receivedMessage
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case message = <-r.messageReceived:
	}

	sender := message.sender
	if sender != host1.ID() {
		t.Fatal("received message from wrong node")
	}

	received := message.message
	receivedRequests := received.Requests()
	if len(receivedRequests) != 1 {
		t.Fatal("Did not add request to received message")
	}
	receivedRequest := receivedRequests[0]
	receivedSpec, err := bridge.DecodeNode(receivedRequest.Selector())
	if err != nil {
		t.Fatal("unable to decode transmitted selector")
	}
	if !reflect.DeepEqual(spec, receivedSpec) {
		t.Fatal("did not transmit selector spec correctly")
	}
	_, err = bridge.ParseSelector(receivedSpec)
	if err != nil {
		t.Fatal("did not receive parsible selector on other side")
	}

	returnedData, found := receivedRequest.Extension(extensionName)
	if !found || !reflect.DeepEqual(extensionData, returnedData) {
		t.Fatal("Failed to encode extension")
	}
}

func TestSendResponseToIncomingRequest(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	// setup network
	host1, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	host2, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	err = mn.LinkAll()
	if err != nil {
		t.Fatal("error linking hosts")
	}

	gsnet1 := gsnet.NewFromLibp2pHost(host1)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	gsnet1.SetDelegate(r)

	// setup receiving peer to just record message coming in
	gsnet2 := gsnet.NewFromLibp2pHost(host2)

	blockStore := make(map[ipld.Link][]byte)
	loader, storer := testbridge.NewMockStore(blockStore)
	bridge := ipldbridge.NewIPLDBridge()

	extensionData := testutil.RandomBytes(100)
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionData,
	}
	extensionResponseData := testutil.RandomBytes(100)
	extensionResponse := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionResponseData,
	}

	var receivedRequestData []byte
	// initialize graphsync on second node to response to requests
	gsnet := New(ctx, gsnet2, bridge, loader, storer)
	err = gsnet.RegisterExtension(graphsync.ExtensionConfig{
		Name: extensionName,
		OnRequestReceived: func(p peer.ID, root ipld.Link, selector ipld.Node, requestData []byte) (graphsync.ExtensionData, error) {
			receivedRequestData = requestData
			return extensionResponse, nil
		},
		PerformsValidation: false,
	})
	if err != nil {
		t.Fatal("error registering extension")
	}

	err = gsnet.RegisterExtension(graphsync.ExtensionConfig{
		Name: extensionName,
	})
	if err != graphsync.ErrExtensionAlreadyRegistered {
		t.Fatal("extension should not be able to register twice")
	}
	blockChainLength := 100
	blockChain := setupBlockChain(ctx, t, storer, bridge, 100, blockChainLength)
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	spec := ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(blockChainLength),
		ssb.ExploreFields(func(efsb ipldbridge.ExploreFieldsSpecBuilder) {
			efsb.Insert("Parents", ssb.ExploreAll(
				ssb.ExploreRecursiveEdge()))
		})).Node()

	selectorData, err := bridge.EncodeNode(spec)
	if err != nil {
		t.Fatal("could not encode selector spec")
	}
	requestID := graphsync.RequestID(rand.Int31())

	message := gsmsg.New()
	message.AddRequest(gsmsg.NewRequest(requestID, blockChain.tipLink.(cidlink.Link).Cid, selectorData, graphsync.Priority(math.MaxInt32), extension))
	// send request across network
	gsnet1.SendMessage(ctx, host2.ID(), message)
	// read the values sent back to requestor
	var received gsmsg.GraphSyncMessage
	var receivedBlocks []blocks.Block
	var receivedExtensions [][]byte
readAllMessages:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("did not receive complete response")
		case message := <-r.messageReceived:
			sender := message.sender
			if sender != host2.ID() {
				t.Fatal("received message from wrong node")
			}

			received = message.message
			receivedBlocks = append(receivedBlocks, received.Blocks()...)
			receivedResponses := received.Responses()
			receivedExtension, found := receivedResponses[0].Extension(extensionName)
			if found {
				receivedExtensions = append(receivedExtensions, receivedExtension)
			}
			if len(receivedResponses) != 1 {
				t.Fatal("Did not receive response")
			}
			if receivedResponses[0].RequestID() != requestID {
				t.Fatal("Sent response for incorrect request id")
			}
			if receivedResponses[0].Status() != graphsync.PartialResponse {
				break readAllMessages
			}
		}
	}

	if len(receivedBlocks) != blockChainLength {
		t.Fatal("Send incorrect number of blocks or there were duplicate blocks")
	}

	if !reflect.DeepEqual(extensionData, receivedRequestData) {
		t.Fatal("did not receive correct request extension data")
	}

	if len(receivedExtensions) != 1 {
		t.Fatal("should have sent extension responses but didn't")
	}

	if !reflect.DeepEqual(receivedExtensions[0], extensionResponseData) {
		t.Fatal("did not return correct extension data")
	}
}

func TestGraphsyncRoundTrip(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	// setup network
	host1, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	host2, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	err = mn.LinkAll()
	if err != nil {
		t.Fatal("error linking hosts")
	}

	gsnet1 := gsnet.NewFromLibp2pHost(host1)

	blockStore1 := make(map[ipld.Link][]byte)
	loader1, storer1 := testbridge.NewMockStore(blockStore1)
	bridge1 := ipldbridge.NewIPLDBridge()

	// initialize graphsync on first node to make requests
	requestor := New(ctx, gsnet1, bridge1, loader1, storer1)

	// setup receiving peer to just record message coming in
	gsnet2 := gsnet.NewFromLibp2pHost(host2)

	blockStore2 := make(map[ipld.Link][]byte)
	loader2, storer2 := testbridge.NewMockStore(blockStore2)
	bridge2 := ipldbridge.NewIPLDBridge()

	blockChainLength := 100
	blockChain := setupBlockChain(ctx, t, storer2, bridge2, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := New(ctx, gsnet2, bridge2, loader2, storer2)

	// setup extension handlers
	extensionData := testutil.RandomBytes(100)
	extensionName := graphsync.ExtensionName("AppleSauce/McGee")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionData,
	}
	extensionResponseData := testutil.RandomBytes(100)
	extensionResponse := graphsync.ExtensionData{
		Name: extensionName,
		Data: extensionResponseData,
	}

	var receivedResponseData []byte
	var receivedRequestData []byte

	err = requestor.RegisterExtension(graphsync.ExtensionConfig{
		Name: extensionName,
		OnResponseReceived: func(p peer.ID, responseData []byte) error {
			receivedResponseData = responseData
			return nil
		},
	})
	if err != nil {
		t.Fatal("Error setting up extension")
	}

	err = responder.RegisterExtension(graphsync.ExtensionConfig{
		Name: extensionName,
		OnRequestReceived: func(p peer.ID, root ipld.Link, selector ipld.Node, requestData []byte) (graphsync.ExtensionData, error) {
			receivedRequestData = requestData
			return extensionResponse, nil
		},
		PerformsValidation: false,
	})
	if err != nil {
		t.Fatal("Error setting up extension")
	}

	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	spec := ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(blockChainLength),
		ssb.ExploreFields(func(efsb ipldbridge.ExploreFieldsSpecBuilder) {
			efsb.Insert("Parents", ssb.ExploreAll(
				ssb.ExploreRecursiveEdge()))
		})).Node()

	progressChan, errChan := requestor.Request(ctx, host2.ID(), blockChain.tipLink, spec, extension)

	responses := testutil.CollectResponses(ctx, t, progressChan)
	errs := testutil.CollectErrors(ctx, t, errChan)

	if len(responses) != blockChainLength*2 {
		t.Fatal("did not traverse all nodes")
	}
	if len(errs) != 0 {
		t.Fatal("errors during traverse")
	}
	if len(blockStore1) != blockChainLength {
		t.Fatal("did not store all blocks")
	}

	expectedPath := ""
	for i, response := range responses {
		if response.Path.String() != expectedPath {
			t.Fatal("incorrect path")
		}
		if i%2 == 0 {
			if expectedPath == "" {
				expectedPath = "Parents"
			} else {
				expectedPath = expectedPath + "/Parents"
			}
		} else {
			expectedPath = expectedPath + "/0"
		}
	}

	// verify extension roundtrip
	if !reflect.DeepEqual(receivedRequestData, extensionData) {
		t.Fatal("did not receive correct extension request data")
	}

	if !reflect.DeepEqual(receivedResponseData, extensionResponseData) {
		t.Fatal("did not receive correct extension response data")
	}
}

// TestRoundTripLargeBlocksSlowNetwork test verifies graphsync continues to work
// under a specific of adverse conditions:
// -- large blocks being returned by a query
// -- slow network connection
// It verifies that Graphsync will properly break up network message packets
// so they can still be decoded on the client side, instead of building up a huge
// backlog of blocks and then sending them in one giant network packet that can't
// be decoded on the client side
func TestRoundTripLargeBlocksSlowNetwork(t *testing.T) {
	// create network
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	// setup network
	host1, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	host2, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 100 * time.Millisecond, Bandwidth: 3000000})
	err = mn.LinkAll()
	if err != nil {
		t.Fatal("error linking hosts")
	}

	gsnet1 := gsnet.NewFromLibp2pHost(host1)

	blockStore1 := make(map[ipld.Link][]byte)
	loader1, storer1 := testbridge.NewMockStore(blockStore1)
	bridge1 := ipldbridge.NewIPLDBridge()

	// initialize graphsync on first node to make requests
	requestor := New(ctx, gsnet1, bridge1, loader1, storer1)

	// setup receiving peer to just record message coming in
	gsnet2 := gsnet.NewFromLibp2pHost(host2)

	blockStore2 := make(map[ipld.Link][]byte)
	loader2, storer2 := testbridge.NewMockStore(blockStore2)
	bridge2 := ipldbridge.NewIPLDBridge()

	blockChainLength := 40
	blockChain := setupBlockChain(ctx, t, storer2, bridge2, 200000, blockChainLength)

	// initialize graphsync on second node to response to requests
	New(ctx, gsnet2, bridge2, loader2, storer2)

	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	spec := ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(blockChainLength),
		ssb.ExploreFields(func(efsb ipldbridge.ExploreFieldsSpecBuilder) {
			efsb.Insert("Parents", ssb.ExploreAll(
				ssb.ExploreRecursiveEdge()))
		})).Node()

	progressChan, errChan := requestor.Request(ctx, host2.ID(), blockChain.tipLink, spec)

	responses := testutil.CollectResponses(ctx, t, progressChan)
	errs := testutil.CollectErrors(ctx, t, errChan)

	if len(responses) != blockChainLength*2 {
		t.Fatal("did not traverse all nodes")
	}
	if len(errs) != 0 {
		t.Fatal("errors during traverse")
	}
}

// What this test does:
// - Construct a blockstore + dag service
// - Import a file to UnixFS v1
// - setup a graphsync request from one node to the other
// for the file
// - Load the file from the new block store on the other node
// using the
// existing UnixFS v1 file reader
// - Verify the bytes match the original
func TestUnixFSFetch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	const unixfsChunkSize uint64 = 1 << 10
	const unixfsLinksPerLevel = 1024

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	makeLoader := func(bs bstore.Blockstore) ipld.Loader {
		return func(lnk ipld.Link, lnkCtx ipld.LinkContext) (io.Reader, error) {
			c, ok := lnk.(cidlink.Link)
			if !ok {
				return nil, errors.New("Incorrect Link Type")
			}
			// read block from one store
			block, err := bs.Get(c.Cid)
			if err != nil {
				return nil, err
			}
			return bytes.NewReader(block.RawData()), nil
		}
	}

	makeStorer := func(bs bstore.Blockstore) ipld.Storer {
		return func(lnkCtx ipld.LinkContext) (io.Writer, ipld.StoreCommitter, error) {
			var buf bytes.Buffer
			var committer ipld.StoreCommitter = func(lnk ipld.Link) error {
				c, ok := lnk.(cidlink.Link)
				if !ok {
					return errors.New("Incorrect Link Type")
				}
				block, err := blocks.NewBlockWithCid(buf.Bytes(), c.Cid)
				if err != nil {
					return err
				}
				return bs.Put(block)
			}
			return &buf, committer, nil
		}
	}
	// make a blockstore and dag service
	bs1 := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))

	// make a second blockstore
	bs2 := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))
	dagService2 := merkledag.NewDAGService(blockservice.New(bs2, offline.Exchange(bs2)))

	// read in a fixture file
	path, err := filepath.Abs(filepath.Join("fixtures", "lorem.txt"))
	if err != nil {
		t.Fatal("unable to create path for fixture file")
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal("unable to open fixture file")
	}
	var buf bytes.Buffer
	tr := io.TeeReader(f, &buf)
	file := files.NewReaderFile(tr)

	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dagService2)

	params := ihelper.DagBuilderParams{
		Maxlinks:   unixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	db, err := params.New(chunker.NewSizeSplitter(file, int64(unixfsChunkSize)))
	if err != nil {
		t.Fatal("unable to setup dag builder")
	}
	nd, err := balanced.Layout(db)
	if err != nil {
		t.Fatal("unable to create unix fs node")
	}
	err = bufferedDS.Commit()
	if err != nil {
		t.Fatal("unable to commit unix fs node")
	}

	// save the original files bytes
	origBytes := buf.Bytes()

	// setup an IPLD loader/storer for blockstore 1
	loader1 := makeLoader(bs1)
	storer1 := makeStorer(bs1)

	// setup an IPLD loader/storer for blockstore 2
	loader2 := makeLoader(bs2)
	storer2 := makeStorer(bs2)

	mn := mocknet.New(ctx)

	// setup network
	host1, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	host2, err := mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 100 * time.Millisecond, Bandwidth: 3000000})
	err = mn.LinkAll()
	if err != nil {
		t.Fatal("error linking hosts")
	}

	gsnet1 := gsnet.NewFromLibp2pHost(host1)

	gsnet2 := gsnet.NewFromLibp2pHost(host2)

	// setup graphsync
	bridge1 := ipldbridge.NewIPLDBridge()
	requestor := New(ctx, gsnet1, bridge1, loader1, storer1)

	bridge2 := ipldbridge.NewIPLDBridge()
	responder := New(ctx, gsnet2, bridge2, loader2, storer2)
	extensionName := graphsync.ExtensionName("Free for all")
	responder.RegisterExtension(graphsync.ExtensionConfig{
		Name:               extensionName,
		PerformsValidation: true,
		OnRequestReceived: func(data []byte) (graphsync.ExtensionData, error) {
			return graphsync.ExtensionData{
				Name: extensionName,
				Data: nil,
			}, nil
		},
	})
	// make a go-ipld-prime link for the root UnixFS node
	clink := cidlink.Link{Cid: nd.Cid()}

	// create a selector for the whole UnixFS dag
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())

	allSelector := ssb.ExploreRecursive(ipldselector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	// execute the traversal
	progressChan, errChan := requestor.Request(ctx, host2.ID(), clink, allSelector,
		graphsync.ExtensionData{
			Name: extensionName,
			Data: nil,
		})

	_ = testutil.CollectResponses(ctx, t, progressChan)
	responseErrors := testutil.CollectErrors(ctx, t, errChan)

	// verify traversal was successful
	if len(responseErrors) != 0 {
		t.Fatal("Response should be successful but wasn't")
	}

	// setup a DagService for the second block store
	dagService1 := merkledag.NewDAGService(blockservice.New(bs1, offline.Exchange(bs1)))

	// load the root of the UnixFS DAG from the new blockstore
	otherNode, err := dagService1.Get(ctx, nd.Cid())
	if err != nil {
		t.Fatal("should have been able to read received root node but didn't")
	}

	// Setup a UnixFS file reader
	n, err := unixfile.NewUnixfsFile(ctx, dagService1, otherNode)
	if err != nil {
		t.Fatal("should have been able to setup UnixFS file but wasn't")
	}

	fn, ok := n.(files.File)
	if !ok {
		t.Fatal("file should be a regular file, but wasn't")
	}

	// Read the bytes for the UnixFS File
	finalBytes, err := ioutil.ReadAll(fn)
	if err != nil {
		t.Fatal("should have been able to read all of unix FS file but wasn't")
	}

	// verify original bytes match final bytes!
	if !reflect.DeepEqual(origBytes, finalBytes) {
		t.Fatal("should have gotten same bytes written as read but didn't")
	}

}
