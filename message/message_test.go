package message

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/ipfs/go-graphsync/testselector"
)

func TestAppendingRequests(t *testing.T) {
	selector := testselector.GenerateSelector()
	rootNode := testselector.GenerateRootNode()
	id := GraphSyncRequestID(rand.Int31())
	priority := GraphSyncPriority(rand.Int31())

	gsm := New()
	gsm.AddRequest(id, selector, rootNode, priority)
	requests := gsm.Requests()
	if len(requests) != 1 {
		t.Fatal("Did not add request to message")
	}
	request := requests[0]
	if request.ID() != id ||
		request.IsCancel() != false ||
		request.Priority() != priority ||
		!reflect.DeepEqual(request.Root(), rootNode) ||
		!reflect.DeepEqual(request.Selector(), selector) {
		t.Fatal("Did not properly add request to message")
	}

	pbMessage := gsm.ToProto()
	pbRequest := pbMessage.Reqlist[0]
	if pbRequest.Id != int32(id) ||
		pbRequest.Priority != int32(priority) ||
		pbRequest.Cancel != false ||
		!reflect.DeepEqual(pbRequest.Root, rootNode.RawData()) ||
		!reflect.DeepEqual(pbRequest.Selector, selector.RawData()) {
		t.Fatal("Did not properly serialize message to protobuf")
	}

	deserialized, err := newMessageFromProto(*pbMessage,
		testselector.MockDecodeRootNodeFunc,
		testselector.MockDecodeSelectorFunc,
		testselector.MockDecodeSelectionResponseFunc,
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
		!reflect.DeepEqual(deserializedRequest.Root(), rootNode) ||
		!reflect.DeepEqual(deserializedRequest.Selector(), selector) {
		t.Fatal("Did not properly deserialize protobuf messages so requests are equal")
	}
}

func TestAppendingResponses(t *testing.T) {
	selectionResponse := testselector.GenerateSelectionResponse()
	requestID := GraphSyncRequestID(rand.Int31())
	status := GraphSyncResponseStatusCode(RequestAcknowledged)

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
		testselector.MockDecodeRootNodeFunc,
		testselector.MockDecodeSelectorFunc,
		testselector.MockDecodeSelectionResponseFunc,
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
	selector := testselector.GenerateSelector()
	rootNode := testselector.GenerateRootNode()
	id := GraphSyncRequestID(rand.Int31())
	priority := GraphSyncPriority(rand.Int31())

	gsm := New()
	gsm.AddRequest(id, selector, rootNode, priority)

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
