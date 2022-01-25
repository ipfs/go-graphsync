// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.1
// source: message_V1_0_0.proto

package graphsync_message_pb

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Message_V1_0_0 struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// the actual data included in this message
	CompleteRequestList bool                       `protobuf:"varint,1,opt,name=completeRequestList,proto3" json:"completeRequestList,omitempty"` // This request list includes *all* requests, replacing outstanding requests.
	Requests            []*Message_V1_0_0_Request  `protobuf:"bytes,2,rep,name=requests,proto3" json:"requests,omitempty"`                        // The list of requests.
	Responses           []*Message_V1_0_0_Response `protobuf:"bytes,3,rep,name=responses,proto3" json:"responses,omitempty"`                      // The list of responses.
	Data                []*Message_V1_0_0_Block    `protobuf:"bytes,4,rep,name=data,proto3" json:"data,omitempty"`                                // Blocks related to the responses
}

func (x *Message_V1_0_0) Reset() {
	*x = Message_V1_0_0{}
	if protoimpl.UnsafeEnabled {
		mi := &file_message_V1_0_0_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Message_V1_0_0) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Message_V1_0_0) ProtoMessage() {}

func (x *Message_V1_0_0) ProtoReflect() protoreflect.Message {
	mi := &file_message_V1_0_0_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Message_V1_0_0.ProtoReflect.Descriptor instead.
func (*Message_V1_0_0) Descriptor() ([]byte, []int) {
	return file_message_V1_0_0_proto_rawDescGZIP(), []int{0}
}

func (x *Message_V1_0_0) GetCompleteRequestList() bool {
	if x != nil {
		return x.CompleteRequestList
	}
	return false
}

func (x *Message_V1_0_0) GetRequests() []*Message_V1_0_0_Request {
	if x != nil {
		return x.Requests
	}
	return nil
}

func (x *Message_V1_0_0) GetResponses() []*Message_V1_0_0_Response {
	if x != nil {
		return x.Responses
	}
	return nil
}

func (x *Message_V1_0_0) GetData() []*Message_V1_0_0_Block {
	if x != nil {
		return x.Data
	}
	return nil
}

type Message_V1_0_0_Request struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id         int32             `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`                                                                                                        // unique id set on the requester side
	Root       []byte            `protobuf:"bytes,2,opt,name=root,proto3" json:"root,omitempty"`                                                                                                     // a CID for the root node in the query
	Selector   []byte            `protobuf:"bytes,3,opt,name=selector,proto3" json:"selector,omitempty"`                                                                                             // ipld selector to retrieve
	Extensions map[string][]byte `protobuf:"bytes,4,rep,name=extensions,proto3" json:"extensions,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"` // aux information. useful for other protocols
	Priority   int32             `protobuf:"varint,5,opt,name=priority,proto3" json:"priority,omitempty"`                                                                                            // the priority (normalized). default to 1
	Cancel     bool              `protobuf:"varint,6,opt,name=cancel,proto3" json:"cancel,omitempty"`                                                                                                // whether this cancels a request
	Update     bool              `protobuf:"varint,7,opt,name=update,proto3" json:"update,omitempty"`                                                                                                // whether this requests resumes a previous request
}

func (x *Message_V1_0_0_Request) Reset() {
	*x = Message_V1_0_0_Request{}
	if protoimpl.UnsafeEnabled {
		mi := &file_message_V1_0_0_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Message_V1_0_0_Request) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Message_V1_0_0_Request) ProtoMessage() {}

func (x *Message_V1_0_0_Request) ProtoReflect() protoreflect.Message {
	mi := &file_message_V1_0_0_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Message_V1_0_0_Request.ProtoReflect.Descriptor instead.
func (*Message_V1_0_0_Request) Descriptor() ([]byte, []int) {
	return file_message_V1_0_0_proto_rawDescGZIP(), []int{0, 0}
}

func (x *Message_V1_0_0_Request) GetId() int32 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *Message_V1_0_0_Request) GetRoot() []byte {
	if x != nil {
		return x.Root
	}
	return nil
}

func (x *Message_V1_0_0_Request) GetSelector() []byte {
	if x != nil {
		return x.Selector
	}
	return nil
}

func (x *Message_V1_0_0_Request) GetExtensions() map[string][]byte {
	if x != nil {
		return x.Extensions
	}
	return nil
}

func (x *Message_V1_0_0_Request) GetPriority() int32 {
	if x != nil {
		return x.Priority
	}
	return 0
}

func (x *Message_V1_0_0_Request) GetCancel() bool {
	if x != nil {
		return x.Cancel
	}
	return false
}

func (x *Message_V1_0_0_Request) GetUpdate() bool {
	if x != nil {
		return x.Update
	}
	return false
}

type Message_V1_0_0_Response struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id         int32             `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`                                                                                                        // the request id
	Status     int32             `protobuf:"varint,2,opt,name=status,proto3" json:"status,omitempty"`                                                                                                // a status code.
	Extensions map[string][]byte `protobuf:"bytes,3,rep,name=extensions,proto3" json:"extensions,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"` // additional data
}

func (x *Message_V1_0_0_Response) Reset() {
	*x = Message_V1_0_0_Response{}
	if protoimpl.UnsafeEnabled {
		mi := &file_message_V1_0_0_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Message_V1_0_0_Response) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Message_V1_0_0_Response) ProtoMessage() {}

func (x *Message_V1_0_0_Response) ProtoReflect() protoreflect.Message {
	mi := &file_message_V1_0_0_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Message_V1_0_0_Response.ProtoReflect.Descriptor instead.
func (*Message_V1_0_0_Response) Descriptor() ([]byte, []int) {
	return file_message_V1_0_0_proto_rawDescGZIP(), []int{0, 1}
}

func (x *Message_V1_0_0_Response) GetId() int32 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *Message_V1_0_0_Response) GetStatus() int32 {
	if x != nil {
		return x.Status
	}
	return 0
}

func (x *Message_V1_0_0_Response) GetExtensions() map[string][]byte {
	if x != nil {
		return x.Extensions
	}
	return nil
}

type Message_V1_0_0_Block struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Prefix []byte `protobuf:"bytes,1,opt,name=prefix,proto3" json:"prefix,omitempty"` // CID prefix (cid version, multicodec and multihash prefix (type + length)
	Data   []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *Message_V1_0_0_Block) Reset() {
	*x = Message_V1_0_0_Block{}
	if protoimpl.UnsafeEnabled {
		mi := &file_message_V1_0_0_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Message_V1_0_0_Block) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Message_V1_0_0_Block) ProtoMessage() {}

func (x *Message_V1_0_0_Block) ProtoReflect() protoreflect.Message {
	mi := &file_message_V1_0_0_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Message_V1_0_0_Block.ProtoReflect.Descriptor instead.
func (*Message_V1_0_0_Block) Descriptor() ([]byte, []int) {
	return file_message_V1_0_0_proto_rawDescGZIP(), []int{0, 2}
}

func (x *Message_V1_0_0_Block) GetPrefix() []byte {
	if x != nil {
		return x.Prefix
	}
	return nil
}

func (x *Message_V1_0_0_Block) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

var File_message_V1_0_0_proto protoreflect.FileDescriptor

var file_message_V1_0_0_proto_rawDesc = []byte{
	0x0a, 0x14, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x56, 0x31, 0x5f, 0x30, 0x5f, 0x30,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x14, 0x67, 0x72, 0x61, 0x70, 0x68, 0x73, 0x79, 0x6e,
	0x63, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x62, 0x22, 0xd6, 0x06, 0x0a,
	0x0e, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x56, 0x31, 0x5f, 0x30, 0x5f, 0x30, 0x12,
	0x30, 0x0a, 0x13, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x4c, 0x69, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x13, 0x63, 0x6f,
	0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x4c, 0x69, 0x73,
	0x74, 0x12, 0x48, 0x0a, 0x08, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x73, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x2c, 0x2e, 0x67, 0x72, 0x61, 0x70, 0x68, 0x73, 0x79, 0x6e, 0x63, 0x2e,
	0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x62, 0x2e, 0x4d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x5f, 0x56, 0x31, 0x5f, 0x30, 0x5f, 0x30, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x52, 0x08, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x73, 0x12, 0x4b, 0x0a, 0x09, 0x72,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2d,
	0x2e, 0x67, 0x72, 0x61, 0x70, 0x68, 0x73, 0x79, 0x6e, 0x63, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x2e, 0x70, 0x62, 0x2e, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x56, 0x31,
	0x5f, 0x30, 0x5f, 0x30, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x52, 0x09, 0x72,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x73, 0x12, 0x3e, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61,
	0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x67, 0x72, 0x61, 0x70, 0x68, 0x73, 0x79,
	0x6e, 0x63, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x62, 0x2e, 0x4d, 0x65,
	0x73, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x56, 0x31, 0x5f, 0x30, 0x5f, 0x30, 0x2e, 0x42, 0x6c, 0x6f,
	0x63, 0x6b, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x1a, 0xb2, 0x02, 0x0a, 0x07, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x02, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x72, 0x6f, 0x6f, 0x74, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x04, 0x72, 0x6f, 0x6f, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x73, 0x65, 0x6c, 0x65,
	0x63, 0x74, 0x6f, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x08, 0x73, 0x65, 0x6c, 0x65,
	0x63, 0x74, 0x6f, 0x72, 0x12, 0x5c, 0x0a, 0x0a, 0x65, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f,
	0x6e, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x3c, 0x2e, 0x67, 0x72, 0x61, 0x70, 0x68,
	0x73, 0x79, 0x6e, 0x63, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x62, 0x2e,
	0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x56, 0x31, 0x5f, 0x30, 0x5f, 0x30, 0x2e, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x45, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e,
	0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x0a, 0x65, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f,
	0x6e, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x72, 0x69, 0x6f, 0x72, 0x69, 0x74, 0x79, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x70, 0x72, 0x69, 0x6f, 0x72, 0x69, 0x74, 0x79, 0x12, 0x16,
	0x0a, 0x06, 0x63, 0x61, 0x6e, 0x63, 0x65, 0x6c, 0x18, 0x06, 0x20, 0x01, 0x28, 0x08, 0x52, 0x06,
	0x63, 0x61, 0x6e, 0x63, 0x65, 0x6c, 0x12, 0x16, 0x0a, 0x06, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65,
	0x18, 0x07, 0x20, 0x01, 0x28, 0x08, 0x52, 0x06, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x1a, 0x3d,
	0x0a, 0x0f, 0x45, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e, 0x73, 0x45, 0x6e, 0x74, 0x72,
	0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03,
	0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x1a, 0xd0, 0x01,
	0x0a, 0x08, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x02, 0x69, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x12, 0x5d, 0x0a, 0x0a, 0x65, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e, 0x73,
	0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x3d, 0x2e, 0x67, 0x72, 0x61, 0x70, 0x68, 0x73, 0x79,
	0x6e, 0x63, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x62, 0x2e, 0x4d, 0x65,
	0x73, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x56, 0x31, 0x5f, 0x30, 0x5f, 0x30, 0x2e, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x45, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e, 0x73,
	0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x0a, 0x65, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e,
	0x73, 0x1a, 0x3d, 0x0a, 0x0f, 0x45, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e, 0x73, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01,
	0x1a, 0x33, 0x0a, 0x05, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x12, 0x16, 0x0a, 0x06, 0x70, 0x72, 0x65,
	0x66, 0x69, 0x78, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x70, 0x72, 0x65, 0x66, 0x69,
	0x78, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x04, 0x64, 0x61, 0x74, 0x61, 0x42, 0x18, 0x5a, 0x16, 0x2e, 0x3b, 0x67, 0x72, 0x61, 0x70, 0x68,
	0x73, 0x79, 0x6e, 0x63, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x70, 0x62, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_message_V1_0_0_proto_rawDescOnce sync.Once
	file_message_V1_0_0_proto_rawDescData = file_message_V1_0_0_proto_rawDesc
)

func file_message_V1_0_0_proto_rawDescGZIP() []byte {
	file_message_V1_0_0_proto_rawDescOnce.Do(func() {
		file_message_V1_0_0_proto_rawDescData = protoimpl.X.CompressGZIP(file_message_V1_0_0_proto_rawDescData)
	})
	return file_message_V1_0_0_proto_rawDescData
}

var file_message_V1_0_0_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_message_V1_0_0_proto_goTypes = []interface{}{
	(*Message_V1_0_0)(nil),          // 0: graphsync.message.pb.Message_V1_0_0
	(*Message_V1_0_0_Request)(nil),  // 1: graphsync.message.pb.Message_V1_0_0.Request
	(*Message_V1_0_0_Response)(nil), // 2: graphsync.message.pb.Message_V1_0_0.Response
	(*Message_V1_0_0_Block)(nil),    // 3: graphsync.message.pb.Message_V1_0_0.Block
	nil,                             // 4: graphsync.message.pb.Message_V1_0_0.Request.ExtensionsEntry
	nil,                             // 5: graphsync.message.pb.Message_V1_0_0.Response.ExtensionsEntry
}
var file_message_V1_0_0_proto_depIdxs = []int32{
	1, // 0: graphsync.message.pb.Message_V1_0_0.requests:type_name -> graphsync.message.pb.Message_V1_0_0.Request
	2, // 1: graphsync.message.pb.Message_V1_0_0.responses:type_name -> graphsync.message.pb.Message_V1_0_0.Response
	3, // 2: graphsync.message.pb.Message_V1_0_0.data:type_name -> graphsync.message.pb.Message_V1_0_0.Block
	4, // 3: graphsync.message.pb.Message_V1_0_0.Request.extensions:type_name -> graphsync.message.pb.Message_V1_0_0.Request.ExtensionsEntry
	5, // 4: graphsync.message.pb.Message_V1_0_0.Response.extensions:type_name -> graphsync.message.pb.Message_V1_0_0.Response.ExtensionsEntry
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_message_V1_0_0_proto_init() }
func file_message_V1_0_0_proto_init() {
	if File_message_V1_0_0_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_message_V1_0_0_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Message_V1_0_0); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_message_V1_0_0_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Message_V1_0_0_Request); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_message_V1_0_0_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Message_V1_0_0_Response); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_message_V1_0_0_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Message_V1_0_0_Block); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_message_V1_0_0_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_message_V1_0_0_proto_goTypes,
		DependencyIndexes: file_message_V1_0_0_proto_depIdxs,
		MessageInfos:      file_message_V1_0_0_proto_msgTypes,
	}.Build()
	File_message_V1_0_0_proto = out.File
	file_message_V1_0_0_proto_rawDesc = nil
	file_message_V1_0_0_proto_goTypes = nil
	file_message_V1_0_0_proto_depIdxs = nil
}
