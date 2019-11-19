package graphsync

import (
	"context"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"

	"github.com/ipfs/go-graphsync/ipldbridge"
	gsmsg "github.com/ipfs/go-graphsync/message"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/testbridge"
	"github.com/ipfs/go-graphsync/testutil"
	ipld "github.com/ipld/go-ipld-prime"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
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
	genisisNode, err := bridge.BuildNode(func(nb ipldbridge.NodeBuilder) ipld.Node {
		return createBlock(nb, []ipld.Link{}, size)
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
		node, err := bridge.BuildNode(func(nb ipldbridge.NodeBuilder) ipld.Node {
			return createBlock(nb, []ipld.Link{parent}, size)
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
	tipNode, err := bridge.BuildNode(func(nb ipldbridge.NodeBuilder) ipld.Node {
		return createBlock(nb, []ipld.Link{parent}, size)
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

	spec, err := bridge.BuildSelector(func(ssb ipldbridge.SelectorSpecBuilder) ipldbridge.SelectorSpec {
		return ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(blockChainLength),
			ssb.ExploreFields(func(efsb ipldbridge.ExploreFieldsSpecBuilder) {
				efsb.Insert("Parents", ssb.ExploreAll(
					ssb.ExploreRecursiveEdge()))
			}))
	})
	if err != nil {
		t.Fatal("Failed creating selector")
	}

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

	// initialize graphsync on second node to response to requests
	New(ctx, gsnet2, bridge, loader, storer)

	blockChainLength := 100
	blockChain := setupBlockChain(ctx, t, storer, bridge, 100, blockChainLength)
	spec, err := bridge.BuildSelector(func(ssb ipldbridge.SelectorSpecBuilder) ipldbridge.SelectorSpec {
		return ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(blockChainLength),
			ssb.ExploreFields(func(efsb ipldbridge.ExploreFieldsSpecBuilder) {
				efsb.Insert("Parents", ssb.ExploreAll(
					ssb.ExploreRecursiveEdge()))
			}))
	})
	if err != nil {
		t.Fatal("Failed creating selector")
	}

	selectorData, err := bridge.EncodeNode(spec)
	if err != nil {
		t.Fatal("could not encode selector spec")
	}
	requestID := graphsync.RequestID(rand.Int31())

	message := gsmsg.New()
	message.AddRequest(gsmsg.NewRequest(requestID, blockChain.tipLink.(cidlink.Link).Cid, selectorData, graphsync.Priority(math.MaxInt32)))
	// send request across network
	gsnet1.SendMessage(ctx, host2.ID(), message)
	// read the values sent back to requestor
	var received gsmsg.GraphSyncMessage
	var receivedBlocks []blocks.Block
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
	New(ctx, gsnet2, bridge2, loader2, storer2)

	spec, err := bridge1.BuildSelector(func(ssb ipldbridge.SelectorSpecBuilder) ipldbridge.SelectorSpec {
		return ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(blockChainLength),
			ssb.ExploreFields(func(efsb ipldbridge.ExploreFieldsSpecBuilder) {
				efsb.Insert("Parents", ssb.ExploreAll(
					ssb.ExploreRecursiveEdge()))
			}))
	})
	if err != nil {
		t.Fatal("Failed creating selector")
	}

	progressChan, errChan := requestor.Request(ctx, host2.ID(), blockChain.tipLink, spec)

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

	spec, err := bridge1.BuildSelector(func(ssb ipldbridge.SelectorSpecBuilder) ipldbridge.SelectorSpec {
		return ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(blockChainLength),
			ssb.ExploreFields(func(efsb ipldbridge.ExploreFieldsSpecBuilder) {
				efsb.Insert("Parents", ssb.ExploreAll(
					ssb.ExploreRecursiveEdge()))
			}))
	})
	if err != nil {
		t.Fatal("Failed creating selector")
	}

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
