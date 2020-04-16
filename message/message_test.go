package message

import (
	"bytes"
	"math/rand"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-graphsync"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestAppendingRequests(t *testing.T) {
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: testutil.RandomBytes(100),
	}
	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	selector := ssb.Matcher().Node()
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())

	gsm := New()
	gsm.AddRequest(NewRequest(id, root, selector, priority, extension))
	requests := gsm.Requests()
	require.Len(t, requests, 1, "did not add request to message")
	request := requests[0]
	extensionData, found := request.Extension(extensionName)
	require.Equal(t, id, request.ID())
	require.False(t, request.IsCancel())
	require.Equal(t, priority, request.Priority())
	require.Equal(t, root.String(), request.Root().String())
	require.Equal(t, selector, request.Selector())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	pbMessage, err := gsm.ToProto()
	require.NoError(t, err, "serialize to protobuf errored")
	selectorEncoded, err := ipldutil.EncodeNode(selector)
	require.NoError(t, err)

	pbRequest := pbMessage.Requests[0]
	require.Equal(t, int32(id), pbRequest.Id)
	require.Equal(t, int32(priority), pbRequest.Priority)
	require.False(t, pbRequest.Cancel)
	require.False(t, pbRequest.Update)
	require.Equal(t, root.Bytes(), pbRequest.Root)
	require.Equal(t, selectorEncoded, pbRequest.Selector)
	require.Equal(t, map[string][]byte{"graphsync/awesome": extension.Data}, pbRequest.Extensions)

	deserialized, err := newMessageFromProto(*pbMessage)
	require.NoError(t, err, "deserializing protobuf message errored")
	deserializedRequests := deserialized.Requests()
	require.Len(t, deserializedRequests, 1, "did not add request to deserialized message")

	deserializedRequest := deserializedRequests[0]
	extensionData, found = deserializedRequest.Extension(extensionName)
	require.Equal(t, id, deserializedRequest.ID())
	require.False(t, deserializedRequest.IsCancel())
	require.False(t, deserializedRequest.IsUpdate())
	require.Equal(t, priority, deserializedRequest.Priority())
	require.Equal(t, root.String(), deserializedRequest.Root().String())
	require.Equal(t, selector, deserializedRequest.Selector())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)
}

func TestAppendingResponses(t *testing.T) {
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: testutil.RandomBytes(100),
	}
	requestID := graphsync.RequestID(rand.Int31())
	status := graphsync.RequestAcknowledged

	gsm := New()
	gsm.AddResponse(NewResponse(requestID, status, extension))
	responses := gsm.Responses()
	require.Len(t, responses, 1, "did not add response to message")
	response := responses[0]
	extensionData, found := response.Extension(extensionName)
	require.Equal(t, requestID, response.RequestID())
	require.Equal(t, status, response.Status())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	pbMessage, err := gsm.ToProto()
	require.NoError(t, err, "serialize to protobuf errored")
	pbResponse := pbMessage.Responses[0]
	require.Equal(t, int32(requestID), pbResponse.Id)
	require.Equal(t, int32(status), pbResponse.Status)
	require.Equal(t, map[string][]byte{"graphsync/awesome": extension.Data}, pbResponse.Extensions)

	deserialized, err := newMessageFromProto(*pbMessage)
	require.NoError(t, err, "deserializing protobuf message errored")
	deserializedResponses := deserialized.Responses()
	require.Len(t, deserializedResponses, 1, "did not add response to deserialized message")
	deserializedResponse := deserializedResponses[0]
	extensionData, found = deserializedResponse.Extension(extensionName)
	require.Equal(t, response.RequestID(), deserializedResponse.RequestID())
	require.Equal(t, response.Status(), deserializedResponse.Status())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)
}

func TestAppendBlock(t *testing.T) {

	strs := make([]string, 2)
	strs = append(strs, "Celeritas")
	strs = append(strs, "Incendia")

	m := New()
	for _, str := range strs {
		block := blocks.NewBlock([]byte(str))
		m.AddBlock(block)
	}

	pbMessage, err := m.ToProto()
	require.NoError(t, err, "serializing to protobuf errored")

	// assert strings are in proto message
	for _, block := range pbMessage.GetData() {
		s := bytes.NewBuffer(block.GetData()).String()
		require.True(t, contains(strs, s))
	}
}

func contains(strs []string, x string) bool {
	for _, s := range strs {
		if s == x {
			return true
		}
	}
	return false
}

func TestRequestCancel(t *testing.T) {
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	selector := ssb.Matcher().Node()
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	root := testutil.GenerateCids(1)[0]

	gsm := New()
	gsm.AddRequest(NewRequest(id, root, selector, priority))

	gsm.AddRequest(CancelRequest(id))

	requests := gsm.Requests()
	require.Len(t, requests, 1, "did not add cancel request")
	request := requests[0]
	require.Equal(t, id, request.ID())
	require.True(t, request.IsCancel())

	buf := new(bytes.Buffer)
	err := gsm.ToNet(buf)
	require.NoError(t, err, "did not serialize protobuf message")
	deserialized, err := FromNet(buf)
	require.NoError(t, err, "did not deserialize protobuf message")
	deserializedRequests := deserialized.Requests()
	require.Len(t, deserializedRequests, 1, "did not add request to deserialized message")
	deserializedRequest := deserializedRequests[0]
	require.Equal(t, request.ID(), deserializedRequest.ID())
	require.Equal(t, request.IsCancel(), deserializedRequest.IsCancel())
}

func TestRequestUpdate(t *testing.T) {

	id := graphsync.RequestID(rand.Int31())
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: testutil.RandomBytes(100),
	}

	gsm := New()
	gsm.AddRequest(UpdateRequest(id, extension))

	requests := gsm.Requests()
	require.Len(t, requests, 1, "did not add cancel request")
	request := requests[0]
	require.Equal(t, id, request.ID())
	require.True(t, request.IsUpdate())
	require.False(t, request.IsCancel())
	extensionData, found := request.Extension(extensionName)
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	buf := new(bytes.Buffer)
	err := gsm.ToNet(buf)
	require.NoError(t, err, "did not serialize protobuf message")
	deserialized, err := FromNet(buf)
	require.NoError(t, err, "did not deserialize protobuf message")

	deserializedRequests := deserialized.Requests()
	require.Len(t, deserializedRequests, 1, "did not add request to deserialized message")
	deserializedRequest := deserializedRequests[0]
	extensionData, found = deserializedRequest.Extension(extensionName)
	require.Equal(t, request.ID(), deserializedRequest.ID())
	require.Equal(t, request.IsCancel(), deserializedRequest.IsCancel())
	require.Equal(t, request.IsUpdate(), deserializedRequest.IsUpdate())
	require.Equal(t, request.Priority(), deserializedRequest.Priority())
	require.Equal(t, request.Root().String(), deserializedRequest.Root().String())
	require.Equal(t, request.Selector(), deserializedRequest.Selector())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)
}

func TestToNetFromNetEquivalency(t *testing.T) {
	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(ipldfree.NodeBuilder())
	selector := ssb.Matcher().Node()
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: testutil.RandomBytes(100),
	}
	id := graphsync.RequestID(rand.Int31())
	priority := graphsync.Priority(rand.Int31())
	status := graphsync.RequestAcknowledged

	gsm := New()
	gsm.AddRequest(NewRequest(id, root, selector, priority, extension))
	gsm.AddResponse(NewResponse(id, status, extension))

	gsm.AddBlock(blocks.NewBlock([]byte("W")))
	gsm.AddBlock(blocks.NewBlock([]byte("E")))
	gsm.AddBlock(blocks.NewBlock([]byte("F")))
	gsm.AddBlock(blocks.NewBlock([]byte("M")))

	buf := new(bytes.Buffer)
	err := gsm.ToNet(buf)
	require.NoError(t, err, "did not serialize protobuf message")
	deserialized, err := FromNet(buf)
	require.NoError(t, err, "did not deserialize protobuf message")

	requests := gsm.Requests()
	require.Len(t, requests, 1, "did not add request to message")
	request := requests[0]
	deserializedRequests := deserialized.Requests()
	require.Len(t, deserializedRequests, 1, "did not add request to deserialized message")
	deserializedRequest := deserializedRequests[0]
	extensionData, found := deserializedRequest.Extension(extensionName)
	require.Equal(t, request.ID(), deserializedRequest.ID())
	require.False(t, deserializedRequest.IsCancel())
	require.False(t, deserializedRequest.IsUpdate())
	require.Equal(t, request.Priority(), deserializedRequest.Priority())
	require.Equal(t, request.Root().String(), deserializedRequest.Root().String())
	require.Equal(t, request.Selector(), deserializedRequest.Selector())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	responses := gsm.Responses()
	require.Len(t, responses, 1, "did not add response to message")
	response := responses[0]
	deserializedResponses := deserialized.Responses()
	require.Len(t, deserializedResponses, 1, "did not add response to message")
	deserializedResponse := deserializedResponses[0]
	extensionData, found = deserializedResponse.Extension(extensionName)
	require.Equal(t, response.RequestID(), deserializedResponse.RequestID())
	require.Equal(t, response.Status(), deserializedResponse.Status())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	keys := make(map[cid.Cid]bool)
	for _, b := range deserialized.Blocks() {
		keys[b.Cid()] = true
	}

	for _, b := range gsm.Blocks() {
		_, ok := keys[b.Cid()]
		require.True(t, ok)
	}
}
