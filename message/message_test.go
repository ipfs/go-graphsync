package message

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"

	"github.com/ipfs/go-graphsync/testutil"
)

func TestAppendingRequests(t *testing.T) {
	selector := testutil.GenerateSelector()
	root := testutil.GenerateRootCid()
	id := GraphSyncRequestID(rand.Int31())
	priority := GraphSyncPriority(rand.Int31())

	gsm := New()
	gsm.AddRequest(id, selector, root, priority)
	requests := gsm.Requests()
	if len(requests) != 1 {
		t.Fatal("Did not add request to message")
	}
	request := requests[0]
	if request.ID() != id ||
		request.IsCancel() != false ||
		request.Priority() != priority ||
		!reflect.DeepEqual(request.Root(), root) ||
		!reflect.DeepEqual(request.Selector(), selector) {
		t.Fatal("Did not properly add request to message")
	}

	pbMessage := gsm.ToProto()
	pbRequest := pbMessage.Reqlist[0]
	if pbRequest.Id != int32(id) ||
		pbRequest.Priority != int32(priority) ||
		pbRequest.Cancel != false ||
		!reflect.DeepEqual(pbRequest.Root, root.Bytes()) ||
		!reflect.DeepEqual(pbRequest.Selector, selector.RawData()) {
		t.Fatal("Did not properly serialize message to protobuf")
	}

	deserialized, err := newMessageFromProto(*pbMessage,
		testutil.MockDecodeSelectorFunc,
		testutil.MockDecodeSelectionResponseFunc,
	)
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
		!reflect.DeepEqual(deserializedRequest.Root(), root) ||
		!reflect.DeepEqual(deserializedRequest.Selector(), selector) {
		t.Fatal("Did not properly deserialize protobuf messages so requests are equal")
	}
}

func TestAppendingResponses(t *testing.T) {
	selectionResponse := testutil.GenerateSelectionResponse()
	requestID := GraphSyncRequestID(rand.Int31())
	status := RequestAcknowledged

	gsm := New()
	gsm.AddResponse(requestID, status, selectionResponse)
	responses := gsm.Responses()
	if len(responses) != 1 {
		t.Fatal("Did not add response to message")
	}
	response := responses[0]
	if response.RequestID() != requestID ||
		response.Status() != status ||
		!reflect.DeepEqual(response.Response(), selectionResponse) {
		t.Fatal("Did not properly add response to message")
	}

	pbMessage := gsm.ToProto()
	pbResponse := pbMessage.Reslist[0]
	if pbResponse.Id != int32(requestID) ||
		pbResponse.Status != int32(status) ||
		!reflect.DeepEqual(pbResponse.Data, selectionResponse.RawData()) {
		t.Fatal("Did not properly serialize message to protobuf")
	}

	deserialized, err := newMessageFromProto(*pbMessage,
		testutil.MockDecodeSelectorFunc,
		testutil.MockDecodeSelectionResponseFunc,
	)
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
		!reflect.DeepEqual(deserializedResponse.Response(), selectionResponse) {
		t.Fatal("Did not properly deserialize protobuf messages so responses are equal")
	}
}

func TestRequestCancel(t *testing.T) {
	selector := testutil.GenerateSelector()
	root := testutil.GenerateRootCid()
	id := GraphSyncRequestID(rand.Int31())
	priority := GraphSyncPriority(rand.Int31())

	gsm := New()
	gsm.AddRequest(id, selector, root, priority)

	gsm.Cancel(id)

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
	selector := testutil.GenerateSelector()
	root := testutil.GenerateRootCid()
	selectionResponse := testutil.GenerateSelectionResponse()
	id := GraphSyncRequestID(rand.Int31())
	priority := GraphSyncPriority(rand.Int31())
	status := RequestAcknowledged

	gsm := New()
	gsm.AddRequest(id, selector, root, priority)
	gsm.AddResponse(id, status, selectionResponse)

	buf := new(bytes.Buffer)
	err := gsm.ToNet(buf)
	if err != nil {
		t.Fatal("Unable to serialize GraphSyncMessage")
	}
	deserialized, err := FromNet(buf,
		testutil.MockDecodeSelectorFunc,
		testutil.MockDecodeSelectionResponseFunc,
	)
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
		!reflect.DeepEqual(deserializedRequest.Root(), request.Root()) ||
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
		!reflect.DeepEqual(deserializedResponse.Response(), response.Response()) {
		t.Fatal("Did not keep responses when writing to stream and back")
	}
}
