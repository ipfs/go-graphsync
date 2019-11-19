package graphsync

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/ipld/go-ipld-prime/fluent"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"

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
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	mh "github.com/multiformats/go-multihash"
)

func TestMakeRequestToNetwork(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	td.gsnet2.SetDelegate(r)
	graphSync := td.GraphSyncHost1()

	blockChainLength := 100
	blockChain := setupBlockChain(ctx, t, td.storer1, td.bridge, 100, blockChainLength)

	spec := blockChainSelector(blockChainLength)

	requestCtx, requestCancel := context.WithCancel(ctx)
	defer requestCancel()
	graphSync.Request(requestCtx, td.host2.ID(), blockChain.tipLink, spec, td.extension)

	var message receivedMessage
	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case message = <-r.messageReceived:
	}

	sender := message.sender
	if sender != td.host1.ID() {
		t.Fatal("received message from wrong node")
	}

	received := message.message
	receivedRequests := received.Requests()
	if len(receivedRequests) != 1 {
		t.Fatal("Did not add request to received message")
	}
	receivedRequest := receivedRequests[0]
	receivedSpec, err := td.bridge.DecodeNode(receivedRequest.Selector())
	if err != nil {
		t.Fatal("unable to decode transmitted selector")
	}
	if !reflect.DeepEqual(spec, receivedSpec) {
		t.Fatal("did not transmit selector spec correctly")
	}
	_, err = td.bridge.ParseSelector(receivedSpec)
	if err != nil {
		t.Fatal("did not receive parsible selector on other side")
	}

	returnedData, found := receivedRequest.Extension(td.extensionName)
	if !found || !reflect.DeepEqual(td.extensionData, returnedData) {
		t.Fatal("Failed to encode extension")
	}
}

func TestSendResponseToIncomingRequest(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)
	r := &receiver{
		messageReceived: make(chan receivedMessage),
	}
	td.gsnet1.SetDelegate(r)

	var receivedRequestData []byte
	// initialize graphsync on second node to response to requests
	gsnet := td.GraphSyncHost2()
	err := gsnet.RegisterRequestReceivedHook(false,
		func(p peer.ID, requestData graphsync.RequestData) ([]graphsync.ExtensionData, error) {
			var has bool
			receivedRequestData, has = requestData.Extension(td.extensionName)
			if !has {
				t.Fatal("did not have expected extension")
			}
			return []graphsync.ExtensionData{td.extensionResponse}, nil
		},
	)
	if err != nil {
		t.Fatal("error registering extension")
	}

	blockChainLength := 100
	blockChain := setupBlockChain(ctx, t, td.storer2, td.bridge, 100, blockChainLength)

	spec := blockChainSelector(blockChainLength)

	selectorData, err := td.bridge.EncodeNode(spec)
	if err != nil {
		t.Fatal("could not encode selector spec")
	}
	requestID := graphsync.RequestID(rand.Int31())

	message := gsmsg.New()
	message.AddRequest(gsmsg.NewRequest(requestID, blockChain.tipLink.(cidlink.Link).Cid, selectorData, graphsync.Priority(math.MaxInt32), td.extension))
	// send request across network
	td.gsnet1.SendMessage(ctx, td.host2.ID(), message)
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
			if sender != td.host2.ID() {
				t.Fatal("received message from wrong node")
			}

			received = message.message
			receivedBlocks = append(receivedBlocks, received.Blocks()...)
			receivedResponses := received.Responses()
			receivedExtension, found := receivedResponses[0].Extension(td.extensionName)
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

	if !reflect.DeepEqual(td.extensionData, receivedRequestData) {
		t.Fatal("did not receive correct request extension data")
	}

	if len(receivedExtensions) != 1 {
		t.Fatal("should have sent extension responses but didn't")
	}

	if !reflect.DeepEqual(receivedExtensions[0], td.extensionResponseData) {
		t.Fatal("did not return correct extension data")
	}
}

func TestGraphsyncRoundTrip(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	td := newGsTestData(ctx, t)

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 100
	blockChain := setupBlockChain(ctx, t, td.storer2, td.bridge, 100, blockChainLength)

	// initialize graphsync on second node to response to requests
	responder := td.GraphSyncHost2()

	var receivedResponseData []byte
	var receivedRequestData []byte

	err := requestor.RegisterResponseReceivedHook(
		func(p peer.ID, responseData graphsync.ResponseData) error {
			data, has := responseData.Extension(td.extensionName)
			if has {
				receivedResponseData = data
			}
			return nil
		})
	if err != nil {
		t.Fatal("Error setting up extension")
	}

	err = responder.RegisterRequestReceivedHook(false,
		func(p peer.ID, requestData graphsync.RequestData) ([]graphsync.ExtensionData, error) {
			var has bool
			receivedRequestData, has = requestData.Extension(td.extensionName)
			if !has {
				return nil, errors.New("Missing extension")
			}
			return []graphsync.ExtensionData{td.extensionResponse}, nil
		})

	if err != nil {
		t.Fatal("Error setting up extension")
	}

	spec := blockChainSelector(blockChainLength)

	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.tipLink, spec, td.extension)

	responses := testutil.CollectResponses(ctx, t, progressChan)
	errs := testutil.CollectErrors(ctx, t, errChan)

	if len(responses) != blockChainLength*2 {
		t.Fatal("did not traverse all nodes")
	}
	if len(errs) != 0 {
		t.Fatal("errors during traverse")
	}
	if len(td.blockStore1) != blockChainLength {
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
	if !reflect.DeepEqual(receivedRequestData, td.extensionData) {
		t.Fatal("did not receive correct extension request data")
	}

	if !reflect.DeepEqual(receivedResponseData, td.extensionResponseData) {
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
	td := newGsTestData(ctx, t)
	td.mn.SetLinkDefaults(mocknet.LinkOptions{Latency: 100 * time.Millisecond, Bandwidth: 3000000})

	// initialize graphsync on first node to make requests
	requestor := td.GraphSyncHost1()

	// setup receiving peer to just record message coming in
	blockChainLength := 40
	blockChain := setupBlockChain(ctx, t, td.storer2, td.bridge, 200000, blockChainLength)

	// initialize graphsync on second node to response to requests
	td.GraphSyncHost2()

	spec := blockChainSelector(blockChainLength)
	progressChan, errChan := requestor.Request(ctx, td.host2.ID(), blockChain.tipLink, spec)

	responses := testutil.CollectResponses(ctx, t, progressChan)
	errs := testutil.CollectErrors(ctx, t, errChan)

	if len(responses) != blockChainLength*2 {
		t.Fatal("did not traverse all nodes")
	}
	if len(errs) != 0 {
		t.Fatal("errors during traverse")
	}
}

type gsTestData struct {
	mn                       mocknet.Mocknet
	ctx                      context.Context
	host1                    host.Host
	host2                    host.Host
	gsnet1                   gsnet.GraphSyncNetwork
	gsnet2                   gsnet.GraphSyncNetwork
	blockStore1, blockStore2 map[ipld.Link][]byte
	loader1, loader2         ipld.Loader
	storer1, storer2         ipld.Storer
	bridge                   ipldbridge.IPLDBridge
	extensionData            []byte
	extensionName            graphsync.ExtensionName
	extension                graphsync.ExtensionData
	extensionResponseData    []byte
	extensionResponse        graphsync.ExtensionData
}

func newGsTestData(ctx context.Context, t *testing.T) *gsTestData {
	td := &gsTestData{ctx: ctx}
	td.mn = mocknet.New(ctx)
	var err error
	// setup network
	td.host1, err = td.mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	td.host2, err = td.mn.GenPeer()
	if err != nil {
		t.Fatal("error generating host")
	}
	err = td.mn.LinkAll()
	if err != nil {
		t.Fatal("error linking hosts")
	}

	td.gsnet1 = gsnet.NewFromLibp2pHost(td.host1)
	td.gsnet2 = gsnet.NewFromLibp2pHost(td.host2)
	td.blockStore1 = make(map[ipld.Link][]byte)
	td.loader1, td.storer1 = testbridge.NewMockStore(td.blockStore1)
	td.blockStore2 = make(map[ipld.Link][]byte)
	td.loader2, td.storer2 = testbridge.NewMockStore(td.blockStore2)
	td.bridge = ipldbridge.NewIPLDBridge()
	// setup extension handlers
	td.extensionData = testutil.RandomBytes(100)
	td.extensionName = graphsync.ExtensionName("AppleSauce/McGee")
	td.extension = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionData,
	}
	td.extensionResponseData = testutil.RandomBytes(100)
	td.extensionResponse = graphsync.ExtensionData{
		Name: td.extensionName,
		Data: td.extensionResponseData,
	}

	return td
}

func (td *gsTestData) GraphSyncHost1() graphsync.GraphExchange {
	return New(td.ctx, td.gsnet1, td.bridge, td.loader1, td.storer1)
}

func (td *gsTestData) GraphSyncHost2() graphsync.GraphExchange {

	return New(td.ctx, td.gsnet2, td.bridge, td.loader2, td.storer2)
}

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

func blockChainSelector(blockChainLength int) ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	return ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(blockChainLength),
		ssb.ExploreFields(func(efsb ipldbridge.ExploreFieldsSpecBuilder) {
			efsb.Insert("Parents", ssb.ExploreAll(
				ssb.ExploreRecursiveEdge()))
		})).Node()
}
