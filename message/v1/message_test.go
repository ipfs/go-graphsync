package v1

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestAppendingRequests(t *testing.T) {
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: basicnode.NewBytes(testutil.RandomBytes(100)),
	}
	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	id := graphsync.NewRequestID()
	priority := graphsync.Priority(rand.Int31())

	builder := message.NewBuilder()
	builder.AddRequest(message.NewRequest(id, root, selector, priority, extension))
	gsm, err := builder.Build()
	require.NoError(t, err)
	requests := gsm.Requests()
	require.Len(t, requests, 1, "did not add request to message")
	request := requests[0]
	extensionData, found := request.Extension(extensionName)
	require.Equal(t, id, request.ID())
	require.Equal(t, request.Type(), graphsync.RequestTypeNew)
	require.Equal(t, priority, request.Priority())
	require.Equal(t, root.String(), request.Root().String())
	require.Equal(t, selector, request.Selector())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	mh := NewMessageHandler()

	pbMessage, err := mh.ToProto(peer.ID("foo"), gsm)
	require.NoError(t, err, "serialize to protobuf errored")
	selectorEncoded, err := ipldutil.EncodeNode(selector)
	require.NoError(t, err)

	pbRequest := pbMessage.Requests[0]
	require.Equal(t, int32(priority), pbRequest.Priority)
	require.False(t, pbRequest.Cancel)
	require.False(t, pbRequest.Update)
	require.Equal(t, root.Bytes(), pbRequest.Root)
	require.Equal(t, selectorEncoded, pbRequest.Selector)
	require.Equal(t, 1, len(pbRequest.Extensions))
	byts, _ := extension.Data.AsBytes()
	expectedByts := append([]byte{88, 100}, byts...)
	actualByts, ok := pbRequest.Extensions["graphsync/awesome"]
	require.True(t, ok)
	require.Equal(t, expectedByts, actualByts)

	deserialized, err := mh.fromProto(peer.ID("foo"), pbMessage)
	require.NoError(t, err, "deserializing protobuf message errored")
	deserializedRequests := deserialized.Requests()
	require.Len(t, deserializedRequests, 1, "did not add request to deserialized message")

	deserializedRequest := deserializedRequests[0]
	extensionData, found = deserializedRequest.Extension(extensionName)
	require.Equal(t, id, deserializedRequest.ID())
	require.Equal(t, deserializedRequest.Type(), graphsync.RequestTypeNew)
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
		Data: basicnode.NewString("test extension data"),
	}
	requestID := graphsync.NewRequestID()
	p := peer.ID("test peer")

	mh := NewMessageHandler()
	status := graphsync.RequestAcknowledged

	builder := message.NewBuilder()
	builder.AddResponseCode(requestID, status)
	builder.AddExtensionData(requestID, extension)
	gsm, err := builder.Build()
	require.NoError(t, err)
	responses := gsm.Responses()
	require.Len(t, responses, 1, "did not add response to message")
	response := responses[0]
	extensionData, found := response.Extension(extensionName)
	require.Equal(t, requestID, response.RequestID())
	require.Equal(t, status, response.Status())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	pbMessage, err := mh.ToProto(p, gsm)
	require.NoError(t, err, "serialize to protobuf errored")
	pbResponse := pbMessage.Responses[0]
	// no longer equal: require.Equal(t, requestID.Bytes(), pbResponse.Id)
	require.Equal(t, int32(status), pbResponse.Status)
	require.Equal(t, []byte("stest extension data"), pbResponse.Extensions["graphsync/awesome"])

	deserialized, err := mh.fromProto(p, pbMessage)
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

	builder := message.NewBuilder()
	for _, str := range strs {
		block := blocks.NewBlock([]byte(str))
		builder.AddBlock(block)
	}
	m, err := builder.Build()
	require.NoError(t, err)

	pbMessage, err := NewMessageHandler().ToProto(peer.ID("foo"), m)
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
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	id := graphsync.NewRequestID()
	priority := graphsync.Priority(rand.Int31())
	root := testutil.GenerateCids(1)[0]

	builder := message.NewBuilder()
	builder.AddRequest(message.NewRequest(id, root, selector, priority))
	builder.AddRequest(message.NewCancelRequest(id))
	gsm, err := builder.Build()
	require.NoError(t, err)

	requests := gsm.Requests()
	require.Len(t, requests, 1, "did not add cancel request")
	request := requests[0]
	require.Equal(t, id, request.ID())
	require.Equal(t, request.Type(), graphsync.RequestTypeCancel)

	mh := NewMessageHandler()

	buf := new(bytes.Buffer)
	err = mh.ToNet(peer.ID("foo"), gsm, buf)
	require.NoError(t, err, "did not serialize protobuf message")
	deserialized, err := mh.FromNet(peer.ID("foo"), buf)
	require.NoError(t, err, "did not deserialize protobuf message")
	deserializedRequests := deserialized.Requests()
	require.Len(t, deserializedRequests, 1, "did not add request to deserialized message")
	deserializedRequest := deserializedRequests[0]
	require.Equal(t, request.ID(), deserializedRequest.ID())
	require.Equal(t, request.Type(), deserializedRequest.Type())
}

func TestRequestUpdate(t *testing.T) {

	id := graphsync.NewRequestID()
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: basicnode.NewBytes(testutil.RandomBytes(100)),
	}

	builder := message.NewBuilder()
	builder.AddRequest(message.NewUpdateRequest(id, extension))
	gsm, err := builder.Build()
	require.NoError(t, err)

	requests := gsm.Requests()
	require.Len(t, requests, 1, "did not add cancel request")
	request := requests[0]
	require.Equal(t, id, request.ID())
	require.Equal(t, request.Type(), graphsync.RequestTypeUpdate)
	extensionData, found := request.Extension(extensionName)
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)

	mh := NewMessageHandler()

	buf := new(bytes.Buffer)
	err = mh.ToNet(peer.ID("foo"), gsm, buf)
	require.NoError(t, err, "did not serialize protobuf message")
	deserialized, err := mh.FromNet(peer.ID("foo"), buf)
	require.NoError(t, err, "did not deserialize protobuf message")

	deserializedRequests := deserialized.Requests()
	require.Len(t, deserializedRequests, 1, "did not add request to deserialized message")
	deserializedRequest := deserializedRequests[0]
	extensionData, found = deserializedRequest.Extension(extensionName)
	require.Equal(t, request.ID(), deserializedRequest.ID())
	require.Equal(t, request.Type(), deserializedRequest.Type())
	require.Equal(t, request.Priority(), deserializedRequest.Priority())
	require.Equal(t, request.Root().String(), deserializedRequest.Root().String())
	require.Equal(t, request.Selector(), deserializedRequest.Selector())
	require.True(t, found)
	require.Equal(t, extension.Data, extensionData)
}

func TestToNetFromNetEquivalency(t *testing.T) {
	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	extensionName := graphsync.ExtensionName("graphsync/awesome")
	extension := graphsync.ExtensionData{
		Name: extensionName,
		Data: basicnode.NewBytes(testutil.RandomBytes(100)),
	}
	id := graphsync.NewRequestID()
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
	require.NoError(t, err)

	mh := NewMessageHandler()

	buf := new(bytes.Buffer)
	err = mh.ToNet(peer.ID("foo"), gsm, buf)
	require.NoError(t, err, "did not serialize protobuf message")
	deserialized, err := mh.FromNet(peer.ID("foo"), buf)
	require.NoError(t, err, "did not deserialize protobuf message")

	requests := gsm.Requests()
	require.Len(t, requests, 1, "did not add request to message")
	request := requests[0]
	deserializedRequests := deserialized.Requests()
	require.Len(t, deserializedRequests, 1, "did not add request to deserialized message")
	deserializedRequest := deserializedRequests[0]
	extensionData, found := deserializedRequest.Extension(extensionName)
	require.Equal(t, request.ID(), deserializedRequest.ID())
	require.Equal(t, deserializedRequest.Type(), graphsync.RequestTypeNew)
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

func TestMergeExtensions(t *testing.T) {
	extensionName1 := graphsync.ExtensionName("graphsync/1")
	extensionName2 := graphsync.ExtensionName("graphsync/2")
	extensionName3 := graphsync.ExtensionName("graphsync/3")
	initialExtensions := []graphsync.ExtensionData{
		{
			Name: extensionName1,
			Data: basicnode.NewString("applesauce"),
		},
		{
			Name: extensionName2,
			Data: basicnode.NewString("hello"),
		},
	}
	replacementExtensions := []graphsync.ExtensionData{
		{
			Name: extensionName2,
			Data: basicnode.NewString("world"),
		},
		{
			Name: extensionName3,
			Data: basicnode.NewString("cheese"),
		},
	}
	defaultMergeFunc := func(name graphsync.ExtensionName, oldData datamodel.Node, newData datamodel.Node) (datamodel.Node, error) {
		os, err := oldData.AsString()
		if err != nil {
			return nil, err
		}
		ns, err := newData.AsString()
		if err != nil {
			return nil, err
		}
		return basicnode.NewString(os + " " + ns), nil
	}
	root := testutil.GenerateCids(1)[0]
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.Matcher().Node()
	id := graphsync.NewRequestID()
	priority := graphsync.Priority(rand.Int31())
	defaultRequest := message.NewRequest(id, root, selector, priority, initialExtensions...)
	t.Run("when merging into empty", func(t *testing.T) {
		emptyRequest := message.NewRequest(id, root, selector, priority)
		resultRequest, err := emptyRequest.MergeExtensions(replacementExtensions, defaultMergeFunc)
		require.NoError(t, err)
		require.Equal(t, emptyRequest.ID(), resultRequest.ID())
		require.Equal(t, emptyRequest.Priority(), resultRequest.Priority())
		require.Equal(t, emptyRequest.Root().String(), resultRequest.Root().String())
		require.Equal(t, emptyRequest.Selector(), resultRequest.Selector())
		_, has := resultRequest.Extension(extensionName1)
		require.False(t, has)
		extData2, has := resultRequest.Extension(extensionName2)
		require.True(t, has)
		require.Equal(t, basicnode.NewString("world"), extData2)
		extData3, has := resultRequest.Extension(extensionName3)
		require.True(t, has)
		require.Equal(t, basicnode.NewString("cheese"), extData3)
	})
	t.Run("when merging two requests", func(t *testing.T) {
		resultRequest, err := defaultRequest.MergeExtensions(replacementExtensions, defaultMergeFunc)
		require.NoError(t, err)
		require.Equal(t, defaultRequest.ID(), resultRequest.ID())
		require.Equal(t, defaultRequest.Priority(), resultRequest.Priority())
		require.Equal(t, defaultRequest.Root().String(), resultRequest.Root().String())
		require.Equal(t, defaultRequest.Selector(), resultRequest.Selector())
		extData1, has := resultRequest.Extension(extensionName1)
		require.True(t, has)
		require.Equal(t, basicnode.NewString("applesauce"), extData1)
		extData2, has := resultRequest.Extension(extensionName2)
		require.True(t, has)
		require.Equal(t, basicnode.NewString("hello world"), extData2)
		extData3, has := resultRequest.Extension(extensionName3)
		require.True(t, has)
		require.Equal(t, basicnode.NewString("cheese"), extData3)
	})
	t.Run("when merging errors", func(t *testing.T) {
		errorMergeFunc := func(name graphsync.ExtensionName, oldData datamodel.Node, newData datamodel.Node) (datamodel.Node, error) {
			return nil, errors.New("something went wrong")
		}
		_, err := defaultRequest.MergeExtensions(replacementExtensions, errorMergeFunc)
		require.Error(t, err)
	})
	t.Run("when merging with replace", func(t *testing.T) {
		resultRequest := defaultRequest.ReplaceExtensions(replacementExtensions)
		require.Equal(t, defaultRequest.ID(), resultRequest.ID())
		require.Equal(t, defaultRequest.Priority(), resultRequest.Priority())
		require.Equal(t, defaultRequest.Root().String(), resultRequest.Root().String())
		require.Equal(t, defaultRequest.Selector(), resultRequest.Selector())
		extData1, has := resultRequest.Extension(extensionName1)
		require.True(t, has)
		require.Equal(t, basicnode.NewString("applesauce"), extData1)
		extData2, has := resultRequest.Extension(extensionName2)
		require.True(t, has)
		require.Equal(t, basicnode.NewString("world"), extData2)
		extData3, has := resultRequest.Extension(extensionName3)
		require.True(t, has)
		require.Equal(t, basicnode.NewString("cheese"), extData3)
	})
}

func TestKnownFuzzIssues(t *testing.T) {
	inputs := []string{
		"$\x1a \x8000\x1a\x16002\xf4\xff\xff\xff\xff\xff\xff\xff\xff" +
			"00000000000000000",
		"���\x01",
		"Dخ0000000000\x12000000" +
			"00000000000000000000" +
			"000000000 0000000000" +
			"000000000",
		"�\xefĽ�\x01\"#    \n\v5 " +
			"         \n\x10\x01\x80\x01\x19@\xbf\xbd\xff " +
			"   \n\v     ",
		"\x0600\x1a\x02\x180",
	}
	p := peer.ID("test peer")

	mh := NewMessageHandler()

	for _, input := range inputs {
		//inputAsBytes, err := hex.DecodeString(input)
		///require.NoError(t, err)
		msg1, err := mh.FromNet(p, bytes.NewReader([]byte(input)))
		if err != nil {
			continue
		}
		buf2 := new(bytes.Buffer)
		err = mh.ToNet(p, msg1, buf2)
		require.NoError(t, err)

		msg2, err := mh.FromNet(p, buf2)
		require.NoError(t, err)

		require.Equal(t, msg1, msg2)
	}
}
