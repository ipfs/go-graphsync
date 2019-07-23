package message

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"

	blocks "github.com/ipfs/go-block-format"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestAppendingRequests(t *testing.T) {
	root := testutil.GenerateCids(1)[0]
	selector := testutil.RandomBytes(100)
	id := GraphSyncRequestID(rand.Int31())
	priority := GraphSyncPriority(rand.Int31())

	gsm := New()
	gsm.AddRequest(NewRequest(id, root, selector, priority))
	requests := gsm.Requests()
	if len(requests) != 1 {
		t.Fatal("Did not add request to message")
	}
	request := requests[0]
	if request.ID() != id ||
		request.IsCancel() != false ||
		request.Priority() != priority ||
		request.Root().String() != root.String() ||
		!reflect.DeepEqual(request.Selector(), selector) {
		t.Fatal("Did not properly add request to message")
	}

	pbMessage := gsm.ToProto()
	pbRequest := pbMessage.Requests[0]
	if pbRequest.Id != int32(id) ||
		pbRequest.Priority != int32(priority) ||
		pbRequest.Cancel != false ||
		!reflect.DeepEqual(pbRequest.Root, root.Bytes()) ||
		!reflect.DeepEqual(pbRequest.Selector, selector) {
		t.Fatal("Did not properly serialize message to protobuf")
	}

	deserialized, err := newMessageFromProto(*pbMessage)
	if err != nil {
		t.Fatal("Error deserializing protobuf message")
	}
	deserializedRequests := deserialized.Requests()
	if len(deserializedRequests) != 1 {
		t.Fatal("Did not add request to deserialized message")
	}
	deserializedRequest := deserializedRequests[0]
	if deserializedRequest.ID() != id ||
		deserializedRequest.IsCancel() != false ||
		deserializedRequest.Priority() != priority ||
		deserializedRequest.Root().String() != root.String() ||
		!reflect.DeepEqual(deserializedRequest.Selector(), selector) {
		t.Fatal("Did not properly deserialize protobuf messages so requests are equal")
	}
}

func TestAppendingResponses(t *testing.T) {
	extra := testutil.RandomBytes(100)
	requestID := GraphSyncRequestID(rand.Int31())
	status := RequestAcknowledged

	gsm := New()
	gsm.AddResponse(NewResponse(requestID, status, extra))
	responses := gsm.Responses()
	if len(responses) != 1 {
		t.Fatal("Did not add response to message")
	}
	response := responses[0]
	if response.RequestID() != requestID ||
		response.Status() != status ||
		!reflect.DeepEqual(response.Extra(), extra) {
		t.Fatal("Did not properly add response to message")
	}

	pbMessage := gsm.ToProto()
	pbResponse := pbMessage.Responses[0]
	if pbResponse.Id != int32(requestID) ||
		pbResponse.Status != int32(status) ||
		!reflect.DeepEqual(pbResponse.Extra, extra) {
		t.Fatal("Did not properly serialize message to protobuf")
	}

	deserialized, err := newMessageFromProto(*pbMessage)
	if err != nil {
		t.Fatal("Error deserializing protobuf message")
	}
	deserializedResponses := deserialized.Responses()
	if len(deserializedResponses) != 1 {
		t.Fatal("Did not add response to message")
	}
	deserializedResponse := deserializedResponses[0]
	if deserializedResponse.RequestID() != requestID ||
		deserializedResponse.Status() != status ||
		!reflect.DeepEqual(deserializedResponse.Extra(), extra) {
		t.Fatal("Did not properly deserialize protobuf messages so responses are equal")
	}
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

	// assert strings are in proto message
	for _, block := range m.ToProto().GetData() {
		s := bytes.NewBuffer(block.GetData()).String()
		if !contains(strs, s) {
			t.Fail()
		}
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
	selector := testutil.RandomBytes(100)
	id := GraphSyncRequestID(rand.Int31())
	priority := GraphSyncPriority(rand.Int31())
	root := testutil.GenerateCids(1)[0]

	gsm := New()
	gsm.AddRequest(NewRequest(id, root, selector, priority))

	gsm.AddRequest(CancelRequest(id))

	requests := gsm.Requests()
	if len(requests) != 1 {
		t.Fatal("Did not properly cancel request")
	}
	request := requests[0]
	if request.ID() != id ||
		request.IsCancel() != true {
		t.Fatal("Did not properly add cancel request to message")
	}
}

func TestToNetFromNetEquivalency(t *testing.T) {
	root := testutil.GenerateCids(1)[0]
	selector := testutil.RandomBytes(100)
	extra := testutil.RandomBytes(100)
	id := GraphSyncRequestID(rand.Int31())
	priority := GraphSyncPriority(rand.Int31())
	status := RequestAcknowledged

	gsm := New()
	gsm.AddRequest(NewRequest(id, root, selector, priority))
	gsm.AddResponse(NewResponse(id, status, extra))

	gsm.AddBlock(blocks.NewBlock([]byte("W")))
	gsm.AddBlock(blocks.NewBlock([]byte("E")))
	gsm.AddBlock(blocks.NewBlock([]byte("F")))
	gsm.AddBlock(blocks.NewBlock([]byte("M")))

	buf := new(bytes.Buffer)
	err := gsm.ToNet(buf)
	if err != nil {
		t.Fatal("Unable to serialize GraphSyncMessage")
	}
	deserialized, err := FromNet(buf)
	if err != nil {
		t.Fatal("Error deserializing protobuf message")
	}

	requests := gsm.Requests()
	if len(requests) != 1 {
		t.Fatal("Did not add request to message")
	}
	request := requests[0]
	deserializedRequests := deserialized.Requests()
	if len(deserializedRequests) != 1 {
		t.Fatal("Did not add request to deserialized message")
	}
	deserializedRequest := deserializedRequests[0]
	if deserializedRequest.ID() != request.ID() ||
		deserializedRequest.IsCancel() != request.IsCancel() ||
		deserializedRequest.Priority() != request.Priority() ||
		deserializedRequest.Root().String() != request.Root().String() ||
		!reflect.DeepEqual(deserializedRequest.Selector(), request.Selector()) {
		t.Fatal("Did not keep requests when writing to stream and back")
	}

	responses := gsm.Responses()
	if len(responses) != 1 {
		t.Fatal("Did not add response to message")
	}
	response := responses[0]
	deserializedResponses := deserialized.Responses()
	if len(deserializedResponses) != 1 {
		t.Fatal("Did not add response to message")
	}
	deserializedResponse := deserializedResponses[0]
	if deserializedResponse.RequestID() != response.RequestID() ||
		deserializedResponse.Status() != response.Status() ||
		!reflect.DeepEqual(deserializedResponse.Extra(), response.Extra()) {
		t.Fatal("Did not keep responses when writing to stream and back")
	}

	keys := make(map[cid.Cid]bool)
	for _, b := range deserialized.Blocks() {
		keys[b.Cid()] = true
	}

	for _, b := range gsm.Blocks() {
		if _, ok := keys[b.Cid()]; !ok {
			t.Fail()
		}
	}
}
