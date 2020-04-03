package chaintypes

import (
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/impl/typed"
	"github.com/ipld/go-ipld-prime/schema"
)

// Code generated go-ipld-prime DO NOT EDIT.

type Bytes struct{ x []byte }

// TODO generateKindBytes.EmitNativeAccessors
// TODO generateKindBytes.EmitNativeBuilder
type MaybeBytes struct {
	Maybe typed.Maybe
	Value Bytes
}

func (m MaybeBytes) Must() Bytes {
	if m.Maybe != typed.Maybe_Value {
		panic("unbox of a maybe rejected")
	}
	return m.Value
}

var _ ipld.Node = Bytes{}
var _ typed.Node = Bytes{}

func (Bytes) Type() schema.Type {
	return nil /*TODO:typelit*/
}
func (Bytes) ReprKind() ipld.ReprKind {
	return ipld.ReprKind_Bytes
}
func (Bytes) LookupString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "LookupString", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_Bytes}
}
func (Bytes) Lookup(ipld.Node) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "Lookup", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_Bytes}
}
func (Bytes) LookupIndex(idx int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "LookupIndex", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Bytes}
}
func (Bytes) LookupSegment(seg ipld.PathSegment) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "LookupSegment", AppropriateKind: ipld.ReprKindSet_Recursive, ActualKind: ipld.ReprKind_Bytes}
}
func (Bytes) MapIterator() ipld.MapIterator {
	return mapIteratorReject{ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "MapIterator", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_Bytes}}
}
func (Bytes) ListIterator() ipld.ListIterator {
	return listIteratorReject{ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "ListIterator", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Bytes}}
}
func (Bytes) Length() int {
	return -1
}
func (Bytes) IsUndefined() bool {
	return false
}
func (Bytes) IsNull() bool {
	return false
}
func (Bytes) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "AsBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_Bytes}
}
func (Bytes) AsInt() (int, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "AsInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_Bytes}
}
func (Bytes) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "AsFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_Bytes}
}
func (Bytes) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "AsString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_Bytes}
}
func (x Bytes) AsBytes() ([]byte, error) {
	return x.x, nil
}
func (Bytes) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes", MethodName: "AsLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_Bytes}
}
func (Bytes) NodeBuilder() ipld.NodeBuilder {
	return _Bytes__NodeBuilder{}
}

type _Bytes__NodeBuilder struct{}

func Bytes__NodeBuilder() ipld.NodeBuilder {
	return _Bytes__NodeBuilder{}
}
func (_Bytes__NodeBuilder) CreateMap() (ipld.MapBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes.Builder", MethodName: "CreateMap", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_Bytes}
}
func (_Bytes__NodeBuilder) AmendMap() (ipld.MapBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes.Builder", MethodName: "AmendMap", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_Bytes}
}
func (_Bytes__NodeBuilder) CreateList() (ipld.ListBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes.Builder", MethodName: "CreateList", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Bytes}
}
func (_Bytes__NodeBuilder) AmendList() (ipld.ListBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes.Builder", MethodName: "AmendList", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Bytes}
}
func (_Bytes__NodeBuilder) CreateNull() (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes.Builder", MethodName: "CreateNull", AppropriateKind: ipld.ReprKindSet_JustNull, ActualKind: ipld.ReprKind_Bytes}
}
func (_Bytes__NodeBuilder) CreateBool(bool) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes.Builder", MethodName: "CreateBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_Bytes}
}
func (_Bytes__NodeBuilder) CreateInt(int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes.Builder", MethodName: "CreateInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_Bytes}
}
func (_Bytes__NodeBuilder) CreateFloat(float64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes.Builder", MethodName: "CreateFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_Bytes}
}
func (_Bytes__NodeBuilder) CreateString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes.Builder", MethodName: "CreateString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_Bytes}
}
func (nb _Bytes__NodeBuilder) CreateBytes(v []byte) (ipld.Node, error) {
	return Bytes{v}, nil
}
func (_Bytes__NodeBuilder) CreateLink(ipld.Link) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Bytes.Builder", MethodName: "CreateLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_Bytes}
}
func (Bytes) Representation() ipld.Node {
	panic("TODO representation")
}

type Link struct{ x ipld.Link }

// TODO generateKindLink.EmitNativeAccessors
// TODO generateKindLink.EmitNativeBuilder
type MaybeLink struct {
	Maybe typed.Maybe
	Value Link
}

func (m MaybeLink) Must() Link {
	if m.Maybe != typed.Maybe_Value {
		panic("unbox of a maybe rejected")
	}
	return m.Value
}

var _ ipld.Node = Link{}
var _ typed.Node = Link{}

func (Link) Type() schema.Type {
	return nil /*TODO:typelit*/
}
func (Link) ReprKind() ipld.ReprKind {
	return ipld.ReprKind_Link
}
func (Link) LookupString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link", MethodName: "LookupString", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_Link}
}
func (Link) Lookup(ipld.Node) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link", MethodName: "Lookup", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_Link}
}
func (Link) LookupIndex(idx int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link", MethodName: "LookupIndex", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Link}
}
func (Link) LookupSegment(seg ipld.PathSegment) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link", MethodName: "LookupSegment", AppropriateKind: ipld.ReprKindSet_Recursive, ActualKind: ipld.ReprKind_Link}
}
func (Link) MapIterator() ipld.MapIterator {
	return mapIteratorReject{ipld.ErrWrongKind{TypeName: "Link", MethodName: "MapIterator", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_Link}}
}
func (Link) ListIterator() ipld.ListIterator {
	return listIteratorReject{ipld.ErrWrongKind{TypeName: "Link", MethodName: "ListIterator", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Link}}
}
func (Link) Length() int {
	return -1
}
func (Link) IsUndefined() bool {
	return false
}
func (Link) IsNull() bool {
	return false
}
func (Link) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "Link", MethodName: "AsBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_Link}
}
func (Link) AsInt() (int, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Link", MethodName: "AsInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_Link}
}
func (Link) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Link", MethodName: "AsFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_Link}
}
func (Link) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "Link", MethodName: "AsString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_Link}
}
func (Link) AsBytes() ([]byte, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link", MethodName: "AsBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_Link}
}
func (x Link) AsLink() (ipld.Link, error) {
	return x.x, nil
}
func (Link) NodeBuilder() ipld.NodeBuilder {
	return _Link__NodeBuilder{}
}

type _Link__NodeBuilder struct{}

func Link__NodeBuilder() ipld.NodeBuilder {
	return _Link__NodeBuilder{}
}
func (_Link__NodeBuilder) CreateMap() (ipld.MapBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link.Builder", MethodName: "CreateMap", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_Link}
}
func (_Link__NodeBuilder) AmendMap() (ipld.MapBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link.Builder", MethodName: "AmendMap", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_Link}
}
func (_Link__NodeBuilder) CreateList() (ipld.ListBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link.Builder", MethodName: "CreateList", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Link}
}
func (_Link__NodeBuilder) AmendList() (ipld.ListBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link.Builder", MethodName: "AmendList", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Link}
}
func (_Link__NodeBuilder) CreateNull() (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link.Builder", MethodName: "CreateNull", AppropriateKind: ipld.ReprKindSet_JustNull, ActualKind: ipld.ReprKind_Link}
}
func (_Link__NodeBuilder) CreateBool(bool) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link.Builder", MethodName: "CreateBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_Link}
}
func (_Link__NodeBuilder) CreateInt(int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link.Builder", MethodName: "CreateInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_Link}
}
func (_Link__NodeBuilder) CreateFloat(float64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link.Builder", MethodName: "CreateFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_Link}
}
func (_Link__NodeBuilder) CreateString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link.Builder", MethodName: "CreateString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_Link}
}
func (_Link__NodeBuilder) CreateBytes([]byte) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Link.Builder", MethodName: "CreateBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_Link}
}
func (nb _Link__NodeBuilder) CreateLink(v ipld.Link) (ipld.Node, error) {
	return Link{v}, nil
}
func (Link) Representation() ipld.Node {
	panic("TODO representation")
}

type String struct{ x string }

func (x String) String() string {
	return x.x
}

type String__Content struct {
	Value string
}

func (b String__Content) Build() (String, error) {
	x := String{
		b.Value,
	}
	// FUTURE : want to support customizable validation.
	//   but 'if v, ok := x.(schema.Validatable); ok {' doesn't fly: need a way to work on concrete types.
	return x, nil
}
func (b String__Content) MustBuild() String {
	if x, err := b.Build(); err != nil {
		panic(err)
	} else {
		return x
	}
}

type MaybeString struct {
	Maybe typed.Maybe
	Value String
}

func (m MaybeString) Must() String {
	if m.Maybe != typed.Maybe_Value {
		panic("unbox of a maybe rejected")
	}
	return m.Value
}

var _ ipld.Node = String{}
var _ typed.Node = String{}

func (String) Type() schema.Type {
	return nil /*TODO:typelit*/
}
func (String) ReprKind() ipld.ReprKind {
	return ipld.ReprKind_String
}
func (String) LookupString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String", MethodName: "LookupString", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_String}
}
func (String) Lookup(ipld.Node) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String", MethodName: "Lookup", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_String}
}
func (String) LookupIndex(idx int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String", MethodName: "LookupIndex", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_String}
}
func (String) LookupSegment(seg ipld.PathSegment) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String", MethodName: "LookupSegment", AppropriateKind: ipld.ReprKindSet_Recursive, ActualKind: ipld.ReprKind_String}
}
func (String) MapIterator() ipld.MapIterator {
	return mapIteratorReject{ipld.ErrWrongKind{TypeName: "String", MethodName: "MapIterator", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_String}}
}
func (String) ListIterator() ipld.ListIterator {
	return listIteratorReject{ipld.ErrWrongKind{TypeName: "String", MethodName: "ListIterator", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_String}}
}
func (String) Length() int {
	return -1
}
func (String) IsUndefined() bool {
	return false
}
func (String) IsNull() bool {
	return false
}
func (String) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "String", MethodName: "AsBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_String}
}
func (String) AsInt() (int, error) {
	return 0, ipld.ErrWrongKind{TypeName: "String", MethodName: "AsInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_String}
}
func (String) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "String", MethodName: "AsFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_String}
}
func (x String) AsString() (string, error) {
	return x.x, nil
}
func (String) AsBytes() ([]byte, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String", MethodName: "AsBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_String}
}
func (String) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String", MethodName: "AsLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_String}
}
func (String) NodeBuilder() ipld.NodeBuilder {
	return _String__NodeBuilder{}
}

type _String__NodeBuilder struct{}

func String__NodeBuilder() ipld.NodeBuilder {
	return _String__NodeBuilder{}
}
func (_String__NodeBuilder) CreateMap() (ipld.MapBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String.Builder", MethodName: "CreateMap", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_String}
}
func (_String__NodeBuilder) AmendMap() (ipld.MapBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String.Builder", MethodName: "AmendMap", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_String}
}
func (_String__NodeBuilder) CreateList() (ipld.ListBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String.Builder", MethodName: "CreateList", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_String}
}
func (_String__NodeBuilder) AmendList() (ipld.ListBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String.Builder", MethodName: "AmendList", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_String}
}
func (_String__NodeBuilder) CreateNull() (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String.Builder", MethodName: "CreateNull", AppropriateKind: ipld.ReprKindSet_JustNull, ActualKind: ipld.ReprKind_String}
}
func (_String__NodeBuilder) CreateBool(bool) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String.Builder", MethodName: "CreateBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_String}
}
func (_String__NodeBuilder) CreateInt(int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String.Builder", MethodName: "CreateInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_String}
}
func (_String__NodeBuilder) CreateFloat(float64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String.Builder", MethodName: "CreateFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_String}
}
func (nb _String__NodeBuilder) CreateString(v string) (ipld.Node, error) {
	return String{v}, nil
}
func (_String__NodeBuilder) CreateBytes([]byte) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String.Builder", MethodName: "CreateBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_String}
}
func (_String__NodeBuilder) CreateLink(ipld.Link) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "String.Builder", MethodName: "CreateLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_String}
}
func (String) Representation() ipld.Node {
	panic("TODO representation")
}

type Parents struct {
	x []Link
}

// TODO generateKindList.EmitNativeAccessors
// TODO generateKindList.EmitNativeBuilder
type MaybeParents struct {
	Maybe typed.Maybe
	Value Parents
}

func (m MaybeParents) Must() Parents {
	if m.Maybe != typed.Maybe_Value {
		panic("unbox of a maybe rejected")
	}
	return m.Value
}

var _ ipld.Node = Parents{}
var _ typed.Node = Parents{}

func (Parents) Type() schema.Type {
	return nil /*TODO:typelit*/
}
func (Parents) ReprKind() ipld.ReprKind {
	return ipld.ReprKind_List
}
func (Parents) LookupString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents", MethodName: "LookupString", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_List}
}
func (x Parents) Lookup(key ipld.Node) (ipld.Node, error) {
	ki, err := key.AsInt()
	if err != nil {
		return nil, ipld.ErrInvalidKey{"got " + key.ReprKind().String() + ", need Int"}
	}
	return x.LookupIndex(ki)
}
func (x Parents) LookupIndex(index int) (ipld.Node, error) {
	if index >= len(x.x) {
		return nil, ipld.ErrNotExists{ipld.PathSegmentOfInt(index)}
	}
	return x.x[index], nil
}
func (n Parents) LookupSegment(seg ipld.PathSegment) (ipld.Node, error) {
	idx, err := seg.Index()
	if err != nil {
		return nil, err
	}
	return n.LookupIndex(idx)
}
func (Parents) MapIterator() ipld.MapIterator {
	return mapIteratorReject{ipld.ErrWrongKind{TypeName: "Parents", MethodName: "MapIterator", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_List}}
}
func (x Parents) ListIterator() ipld.ListIterator {
	return &_Parents__Itr{&x, 0}
}

type _Parents__Itr struct {
	node *Parents
	idx  int
}

func (itr *_Parents__Itr) Next() (idx int, value ipld.Node, _ error) {
	if itr.idx >= len(itr.node.x) {
		return 0, nil, ipld.ErrIteratorOverread{}
	}
	idx = itr.idx
	value = itr.node.x[idx]
	itr.idx++
	return
}

func (itr *_Parents__Itr) Done() bool {
	return itr.idx >= len(itr.node.x)
}

func (x Parents) Length() int {
	return len(x.x)
}
func (Parents) IsUndefined() bool {
	return false
}
func (Parents) IsNull() bool {
	return false
}
func (Parents) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "Parents", MethodName: "AsBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_List}
}
func (Parents) AsInt() (int, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Parents", MethodName: "AsInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_List}
}
func (Parents) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Parents", MethodName: "AsFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_List}
}
func (Parents) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "Parents", MethodName: "AsString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_List}
}
func (Parents) AsBytes() ([]byte, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents", MethodName: "AsBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_List}
}
func (Parents) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents", MethodName: "AsLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_List}
}
func (Parents) NodeBuilder() ipld.NodeBuilder {
	return _Parents__NodeBuilder{}
}

type _Parents__NodeBuilder struct{}

func Parents__NodeBuilder() ipld.NodeBuilder {
	return _Parents__NodeBuilder{}
}
func (_Parents__NodeBuilder) CreateMap() (ipld.MapBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents.Builder", MethodName: "CreateMap", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_List}
}
func (_Parents__NodeBuilder) AmendMap() (ipld.MapBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents.Builder", MethodName: "AmendMap", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_List}
}
func (nb _Parents__NodeBuilder) CreateList() (ipld.ListBuilder, error) {
	return &_Parents__ListBuilder{v: &Parents{}}, nil
}

type _Parents__ListBuilder struct {
	v *Parents
}

func (lb *_Parents__ListBuilder) growList(k int) {
	oldLen := len(lb.v.x)
	minLen := k + 1
	if minLen > oldLen {
		// Grow.
		oldCap := cap(lb.v.x)
		if minLen > oldCap {
			// Out of cap; do whole new backing array allocation.
			//  Growth maths are per stdlib's reflect.grow.
			// First figure out how much growth to do.
			newCap := oldCap
			if newCap == 0 {
				newCap = minLen
			} else {
				for minLen > newCap {
					if minLen < 1024 {
						newCap += newCap
					} else {
						newCap += newCap / 4
					}
				}
			}
			// Now alloc and copy over old.
			newArr := make([]Link, minLen, newCap)
			copy(newArr, lb.v.x)
			lb.v.x = newArr
		} else {
			// Still have cap, just extend the slice.
			lb.v.x = lb.v.x[0:minLen]
		}
	}
}

func (lb *_Parents__ListBuilder) validate(v ipld.Node) error {
	if v.IsNull() {
		panic("type mismatch on struct field assignment: cannot assign null to non-nullable field") // FIXME need an error type for this
	}
	tv, ok := v.(typed.Node)
	if !ok {
		panic("need typed.Node for insertion into struct") // FIXME need an error type for this
	}
	_, ok = v.(Link)
	if !ok {
		panic("value for type Parents is type Link; cannot assign " + tv.Type().Name()) // FIXME need an error type for this
	}
	return nil
}

func (lb *_Parents__ListBuilder) unsafeSet(idx int, v ipld.Node) {
	x := v.(Link)
	lb.v.x[idx] = x
}

func (lb *_Parents__ListBuilder) AppendAll(vs []ipld.Node) error {
	for _, v := range vs {
		err := lb.validate(v)
		if err != nil {
			return err
		}
	}
	off := len(lb.v.x)
	new := off + len(vs)
	lb.growList(new - 1)
	for _, v := range vs {
		lb.unsafeSet(off, v)
		off++
	}
	return nil
}

func (lb *_Parents__ListBuilder) Append(v ipld.Node) error {
	err := lb.validate(v)
	if err != nil {
		return err
	}
	off := len(lb.v.x)
	lb.growList(off)
	lb.unsafeSet(off, v)
	return nil
}
func (lb *_Parents__ListBuilder) Set(idx int, v ipld.Node) error {
	err := lb.validate(v)
	if err != nil {
		return err
	}
	lb.growList(idx)
	lb.unsafeSet(idx, v)
	return nil
}

func (lb *_Parents__ListBuilder) Build() (ipld.Node, error) {
	v := *lb.v
	lb = nil
	return v, nil
}

func (lb *_Parents__ListBuilder) BuilderForValue(_ int) ipld.NodeBuilder {
	return Link__NodeBuilder()
}

func (nb _Parents__NodeBuilder) AmendList() (ipld.ListBuilder, error) {
	panic("TODO later")
}
func (_Parents__NodeBuilder) CreateNull() (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents.Builder", MethodName: "CreateNull", AppropriateKind: ipld.ReprKindSet_JustNull, ActualKind: ipld.ReprKind_List}
}
func (_Parents__NodeBuilder) CreateBool(bool) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents.Builder", MethodName: "CreateBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_List}
}
func (_Parents__NodeBuilder) CreateInt(int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents.Builder", MethodName: "CreateInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_List}
}
func (_Parents__NodeBuilder) CreateFloat(float64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents.Builder", MethodName: "CreateFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_List}
}
func (_Parents__NodeBuilder) CreateString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents.Builder", MethodName: "CreateString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_List}
}
func (_Parents__NodeBuilder) CreateBytes([]byte) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents.Builder", MethodName: "CreateBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_List}
}
func (_Parents__NodeBuilder) CreateLink(ipld.Link) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Parents.Builder", MethodName: "CreateLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_List}
}
func (n Parents) Representation() ipld.Node {
	panic("TODO representation")
}

type Messages struct {
	x []Bytes
}

// TODO generateKindList.EmitNativeAccessors
// TODO generateKindList.EmitNativeBuilder
type MaybeMessages struct {
	Maybe typed.Maybe
	Value Messages
}

func (m MaybeMessages) Must() Messages {
	if m.Maybe != typed.Maybe_Value {
		panic("unbox of a maybe rejected")
	}
	return m.Value
}

var _ ipld.Node = Messages{}
var _ typed.Node = Messages{}

func (Messages) Type() schema.Type {
	return nil /*TODO:typelit*/
}
func (Messages) ReprKind() ipld.ReprKind {
	return ipld.ReprKind_List
}
func (Messages) LookupString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages", MethodName: "LookupString", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_List}
}
func (x Messages) Lookup(key ipld.Node) (ipld.Node, error) {
	ki, err := key.AsInt()
	if err != nil {
		return nil, ipld.ErrInvalidKey{"got " + key.ReprKind().String() + ", need Int"}
	}
	return x.LookupIndex(ki)
}
func (x Messages) LookupIndex(index int) (ipld.Node, error) {
	if index >= len(x.x) {
		return nil, ipld.ErrNotExists{ipld.PathSegmentOfInt(index)}
	}
	return x.x[index], nil
}
func (n Messages) LookupSegment(seg ipld.PathSegment) (ipld.Node, error) {
	idx, err := seg.Index()
	if err != nil {
		return nil, err
	}
	return n.LookupIndex(idx)
}
func (Messages) MapIterator() ipld.MapIterator {
	return mapIteratorReject{ipld.ErrWrongKind{TypeName: "Messages", MethodName: "MapIterator", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_List}}
}
func (x Messages) ListIterator() ipld.ListIterator {
	return &_Messages__Itr{&x, 0}
}

type _Messages__Itr struct {
	node *Messages
	idx  int
}

func (itr *_Messages__Itr) Next() (idx int, value ipld.Node, _ error) {
	if itr.idx >= len(itr.node.x) {
		return 0, nil, ipld.ErrIteratorOverread{}
	}
	idx = itr.idx
	value = itr.node.x[idx]
	itr.idx++
	return
}

func (itr *_Messages__Itr) Done() bool {
	return itr.idx >= len(itr.node.x)
}

func (x Messages) Length() int {
	return len(x.x)
}
func (Messages) IsUndefined() bool {
	return false
}
func (Messages) IsNull() bool {
	return false
}
func (Messages) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "Messages", MethodName: "AsBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_List}
}
func (Messages) AsInt() (int, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Messages", MethodName: "AsInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_List}
}
func (Messages) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Messages", MethodName: "AsFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_List}
}
func (Messages) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "Messages", MethodName: "AsString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_List}
}
func (Messages) AsBytes() ([]byte, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages", MethodName: "AsBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_List}
}
func (Messages) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages", MethodName: "AsLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_List}
}
func (Messages) NodeBuilder() ipld.NodeBuilder {
	return _Messages__NodeBuilder{}
}

type _Messages__NodeBuilder struct{}

func Messages__NodeBuilder() ipld.NodeBuilder {
	return _Messages__NodeBuilder{}
}
func (_Messages__NodeBuilder) CreateMap() (ipld.MapBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages.Builder", MethodName: "CreateMap", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_List}
}
func (_Messages__NodeBuilder) AmendMap() (ipld.MapBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages.Builder", MethodName: "AmendMap", AppropriateKind: ipld.ReprKindSet_JustMap, ActualKind: ipld.ReprKind_List}
}
func (nb _Messages__NodeBuilder) CreateList() (ipld.ListBuilder, error) {
	return &_Messages__ListBuilder{v: &Messages{}}, nil
}

type _Messages__ListBuilder struct {
	v *Messages
}

func (lb *_Messages__ListBuilder) growList(k int) {
	oldLen := len(lb.v.x)
	minLen := k + 1
	if minLen > oldLen {
		// Grow.
		oldCap := cap(lb.v.x)
		if minLen > oldCap {
			// Out of cap; do whole new backing array allocation.
			//  Growth maths are per stdlib's reflect.grow.
			// First figure out how much growth to do.
			newCap := oldCap
			if newCap == 0 {
				newCap = minLen
			} else {
				for minLen > newCap {
					if minLen < 1024 {
						newCap += newCap
					} else {
						newCap += newCap / 4
					}
				}
			}
			// Now alloc and copy over old.
			newArr := make([]Bytes, minLen, newCap)
			copy(newArr, lb.v.x)
			lb.v.x = newArr
		} else {
			// Still have cap, just extend the slice.
			lb.v.x = lb.v.x[0:minLen]
		}
	}
}

func (lb *_Messages__ListBuilder) validate(v ipld.Node) error {
	if v.IsNull() {
		panic("type mismatch on struct field assignment: cannot assign null to non-nullable field") // FIXME need an error type for this
	}
	tv, ok := v.(typed.Node)
	if !ok {
		panic("need typed.Node for insertion into struct") // FIXME need an error type for this
	}
	_, ok = v.(Bytes)
	if !ok {
		panic("value for type Messages is type Bytes; cannot assign " + tv.Type().Name()) // FIXME need an error type for this
	}
	return nil
}

func (lb *_Messages__ListBuilder) unsafeSet(idx int, v ipld.Node) {
	x := v.(Bytes)
	lb.v.x[idx] = x
}

func (lb *_Messages__ListBuilder) AppendAll(vs []ipld.Node) error {
	for _, v := range vs {
		err := lb.validate(v)
		if err != nil {
			return err
		}
	}
	off := len(lb.v.x)
	new := off + len(vs)
	lb.growList(new - 1)
	for _, v := range vs {
		lb.unsafeSet(off, v)
		off++
	}
	return nil
}

func (lb *_Messages__ListBuilder) Append(v ipld.Node) error {
	err := lb.validate(v)
	if err != nil {
		return err
	}
	off := len(lb.v.x)
	lb.growList(off)
	lb.unsafeSet(off, v)
	return nil
}
func (lb *_Messages__ListBuilder) Set(idx int, v ipld.Node) error {
	err := lb.validate(v)
	if err != nil {
		return err
	}
	lb.growList(idx)
	lb.unsafeSet(idx, v)
	return nil
}

func (lb *_Messages__ListBuilder) Build() (ipld.Node, error) {
	v := *lb.v
	lb = nil
	return v, nil
}

func (lb *_Messages__ListBuilder) BuilderForValue(_ int) ipld.NodeBuilder {
	return Bytes__NodeBuilder()
}

func (nb _Messages__NodeBuilder) AmendList() (ipld.ListBuilder, error) {
	panic("TODO later")
}
func (_Messages__NodeBuilder) CreateNull() (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages.Builder", MethodName: "CreateNull", AppropriateKind: ipld.ReprKindSet_JustNull, ActualKind: ipld.ReprKind_List}
}
func (_Messages__NodeBuilder) CreateBool(bool) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages.Builder", MethodName: "CreateBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_List}
}
func (_Messages__NodeBuilder) CreateInt(int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages.Builder", MethodName: "CreateInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_List}
}
func (_Messages__NodeBuilder) CreateFloat(float64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages.Builder", MethodName: "CreateFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_List}
}
func (_Messages__NodeBuilder) CreateString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages.Builder", MethodName: "CreateString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_List}
}
func (_Messages__NodeBuilder) CreateBytes([]byte) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages.Builder", MethodName: "CreateBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_List}
}
func (_Messages__NodeBuilder) CreateLink(ipld.Link) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Messages.Builder", MethodName: "CreateLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_List}
}
func (n Messages) Representation() ipld.Node {
	panic("TODO representation")
}

type Block struct {
	Parents  Parents
	Messages Messages
}

func (x Parents) FieldParents() Parents {
	// TODO going to tear through here with changes to Maybe system in a moment anyway
	return Parents{}
}
func (x Messages) FieldMessages() Messages {
	// TODO going to tear through here with changes to Maybe system in a moment anyway
	return Messages{}
}

type Block__Content struct {
	// TODO
	// TODO
}

func (b Block__Content) Build() (Block, error) {
	x := Block{
		// TODO
	}
	// FUTURE : want to support customizable validation.
	//   but 'if v, ok := x.(schema.Validatable); ok {' doesn't fly: need a way to work on concrete types.
	return x, nil
}
func (b Block__Content) MustBuild() Block {
	if x, err := b.Build(); err != nil {
		panic(err)
	} else {
		return x
	}
}

type MaybeBlock struct {
	Maybe typed.Maybe
	Value Block
}

func (m MaybeBlock) Must() Block {
	if m.Maybe != typed.Maybe_Value {
		panic("unbox of a maybe rejected")
	}
	return m.Value
}

var _ ipld.Node = Block{}
var _ typed.Node = Block{}

func (Block) Type() schema.Type {
	return nil /*TODO:typelit*/
}
func (Block) ReprKind() ipld.ReprKind {
	return ipld.ReprKind_Map
}
func (x Block) LookupString(key string) (ipld.Node, error) {
	switch key {
	case "Parents":
		return x.Parents, nil
	case "Messages":
		return x.Messages, nil
	default:
		return nil, typed.ErrNoSuchField{Type: nil /*TODO*/, FieldName: key}
	}
}
func (x Block) Lookup(key ipld.Node) (ipld.Node, error) {
	ks, err := key.AsString()
	if err != nil {
		return nil, ipld.ErrInvalidKey{"got " + key.ReprKind().String() + ", need string"}
	}
	return x.LookupString(ks)
}
func (Block) LookupIndex(idx int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block", MethodName: "LookupIndex", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Map}
}
func (n Block) LookupSegment(seg ipld.PathSegment) (ipld.Node, error) {
	return n.LookupString(seg.String())
}
func (x Block) MapIterator() ipld.MapIterator {
	return &_Block__Itr{&x, 0}
}

type _Block__Itr struct {
	node *Block
	idx  int
}

func (itr *_Block__Itr) Next() (k ipld.Node, v ipld.Node, _ error) {
	if itr.idx >= 2 {
		return nil, nil, ipld.ErrIteratorOverread{}
	}
	switch itr.idx {
	case 0:
		k = String{"Parents"}
		v = itr.node.Parents
	case 1:
		k = String{"Messages"}
		v = itr.node.Messages
	default:
		panic("unreachable")
	}
	itr.idx++
	return
}
func (itr *_Block__Itr) Done() bool {
	return itr.idx >= 2
}

func (Block) ListIterator() ipld.ListIterator {
	return listIteratorReject{ipld.ErrWrongKind{TypeName: "Block", MethodName: "ListIterator", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Map}}
}
func (Block) Length() int {
	return 2
}
func (Block) IsUndefined() bool {
	return false
}
func (Block) IsNull() bool {
	return false
}
func (Block) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "Block", MethodName: "AsBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_Map}
}
func (Block) AsInt() (int, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Block", MethodName: "AsInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_Map}
}
func (Block) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Block", MethodName: "AsFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_Map}
}
func (Block) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "Block", MethodName: "AsString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_Map}
}
func (Block) AsBytes() ([]byte, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block", MethodName: "AsBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_Map}
}
func (Block) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block", MethodName: "AsLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_Map}
}
func (Block) NodeBuilder() ipld.NodeBuilder {
	return _Block__NodeBuilder{}
}

type _Block__NodeBuilder struct{}

func Block__NodeBuilder() ipld.NodeBuilder {
	return _Block__NodeBuilder{}
}
func (nb _Block__NodeBuilder) CreateMap() (ipld.MapBuilder, error) {
	return &_Block__MapBuilder{v: &Block{}}, nil
}

type _Block__MapBuilder struct {
	v               *Block
	Parents__isset  bool
	Messages__isset bool
}

func (mb *_Block__MapBuilder) Insert(k, v ipld.Node) error {
	ks, err := k.AsString()
	if err != nil {
		return ipld.ErrInvalidKey{"not a string: " + err.Error()}
	}
	switch ks {
	case "Parents":
		if v.IsNull() {
			panic("type mismatch on struct field assignment: cannot assign null to non-nullable field") // FIXME need an error type for this
		}
		tv, ok := v.(typed.Node)
		if !ok {
			panic("need typed.Node for insertion into struct") // FIXME need an error type for this
		}
		x, ok := v.(Parents)
		if !ok {
			panic("field 'Parents' in type Block is type Parents; cannot assign " + tv.Type().Name()) // FIXME need an error type for this
		}
		mb.v.Parents = x
		mb.Parents__isset = true
	case "Messages":
		if v.IsNull() {
			panic("type mismatch on struct field assignment: cannot assign null to non-nullable field") // FIXME need an error type for this
		}
		tv, ok := v.(typed.Node)
		if !ok {
			panic("need typed.Node for insertion into struct") // FIXME need an error type for this
		}
		x, ok := v.(Messages)
		if !ok {
			panic("field 'Messages' in type Block is type Messages; cannot assign " + tv.Type().Name()) // FIXME need an error type for this
		}
		mb.v.Messages = x
		mb.Messages__isset = true
	default:
		return typed.ErrNoSuchField{Type: nil /*TODO:typelit*/, FieldName: ks}
	}
	return nil
}
func (mb *_Block__MapBuilder) Delete(k ipld.Node) error {
	panic("TODO later")
}
func (mb *_Block__MapBuilder) Build() (ipld.Node, error) {
	if !mb.Parents__isset {
		panic("missing required field 'Parents' in building struct Block") // FIXME need an error type for this
	}
	if !mb.Messages__isset {
		panic("missing required field 'Messages' in building struct Block") // FIXME need an error type for this
	}
	v := *mb.v
	mb = nil
	return v, nil
}
func (mb *_Block__MapBuilder) BuilderForKeys() ipld.NodeBuilder {
	return _String__NodeBuilder{}
}
func (mb *_Block__MapBuilder) BuilderForValue(ks string) ipld.NodeBuilder {
	switch ks {
	case "Parents":
		return Parents__NodeBuilder()
	case "Messages":
		return Messages__NodeBuilder()
	default:
		panic(typed.ErrNoSuchField{Type: nil /*TODO:typelit*/, FieldName: ks})
	}
	return nil
}

func (nb _Block__NodeBuilder) AmendMap() (ipld.MapBuilder, error) {
	panic("TODO later")
}
func (_Block__NodeBuilder) CreateList() (ipld.ListBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Builder", MethodName: "CreateList", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Map}
}
func (_Block__NodeBuilder) AmendList() (ipld.ListBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Builder", MethodName: "AmendList", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Map}
}
func (_Block__NodeBuilder) CreateNull() (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Builder", MethodName: "CreateNull", AppropriateKind: ipld.ReprKindSet_JustNull, ActualKind: ipld.ReprKind_Map}
}
func (_Block__NodeBuilder) CreateBool(bool) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Builder", MethodName: "CreateBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_Map}
}
func (_Block__NodeBuilder) CreateInt(int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Builder", MethodName: "CreateInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_Map}
}
func (_Block__NodeBuilder) CreateFloat(float64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Builder", MethodName: "CreateFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_Map}
}
func (_Block__NodeBuilder) CreateString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Builder", MethodName: "CreateString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_Map}
}
func (_Block__NodeBuilder) CreateBytes([]byte) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Builder", MethodName: "CreateBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_Map}
}
func (_Block__NodeBuilder) CreateLink(ipld.Link) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Builder", MethodName: "CreateLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_Map}
}
func (n Block) Representation() ipld.Node {
	return _Block__Repr{&n}
}

var _ ipld.Node = _Block__Repr{}

type _Block__Repr struct {
	n *Block
}

func (_Block__Repr) ReprKind() ipld.ReprKind {
	return ipld.ReprKind_Map
}
func (rn _Block__Repr) LookupString(key string) (ipld.Node, error) {
	switch key {
	case "Parents":
		return rn.n.Parents, nil
	case "Messages":
		return rn.n.Messages, nil
	default:
		return nil, typed.ErrNoSuchField{Type: nil /*TODO*/, FieldName: key}
	}
}
func (rn _Block__Repr) Lookup(key ipld.Node) (ipld.Node, error) {
	ks, err := key.AsString()
	if err != nil {
		return nil, ipld.ErrInvalidKey{"got " + key.ReprKind().String() + ", need string"}
	}
	return rn.LookupString(ks)
}
func (_Block__Repr) LookupIndex(idx int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation", MethodName: "LookupIndex", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Map}
}
func (n _Block__Repr) LookupSegment(seg ipld.PathSegment) (ipld.Node, error) {
	return n.LookupString(seg.String())
}
func (rn _Block__Repr) MapIterator() ipld.MapIterator {
	return &_Block__ReprItr{rn.n, 0}
}

type _Block__ReprItr struct {
	node *Block
	idx  int
}

func (itr *_Block__ReprItr) Next() (k ipld.Node, v ipld.Node, _ error) {
	if itr.idx >= 2 {
		return nil, nil, ipld.ErrIteratorOverread{}
	}
	for {
		switch itr.idx {
		case 0:
			k = String{"Parents"}
			v = itr.node.Parents
		case 1:
			k = String{"Messages"}
			v = itr.node.Messages
		default:
			panic("unreachable")
		}
	}
	itr.idx++
	return
}
func (itr *_Block__ReprItr) Done() bool {
	return itr.idx >= 2
}

func (_Block__Repr) ListIterator() ipld.ListIterator {
	return listIteratorReject{ipld.ErrWrongKind{TypeName: "Block.Representation", MethodName: "ListIterator", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Map}}
}
func (rn _Block__Repr) Length() int {
	l := 2
	return l
}
func (_Block__Repr) IsUndefined() bool {
	return false
}
func (_Block__Repr) IsNull() bool {
	return false
}
func (_Block__Repr) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "Block.Representation", MethodName: "AsBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_Map}
}
func (_Block__Repr) AsInt() (int, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Block.Representation", MethodName: "AsInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_Map}
}
func (_Block__Repr) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "Block.Representation", MethodName: "AsFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_Map}
}
func (_Block__Repr) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "Block.Representation", MethodName: "AsString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_Map}
}
func (_Block__Repr) AsBytes() ([]byte, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation", MethodName: "AsBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_Map}
}
func (_Block__Repr) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation", MethodName: "AsLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_Map}
}
func (_Block__Repr) NodeBuilder() ipld.NodeBuilder {
	return _Block__ReprBuilder{}
}

type _Block__ReprBuilder struct{}

func Block__ReprBuilder() ipld.NodeBuilder {
	return _Block__ReprBuilder{}
}
func (nb _Block__ReprBuilder) CreateMap() (ipld.MapBuilder, error) {
	return &_Block__ReprMapBuilder{v: &Block{}}, nil
}

type _Block__ReprMapBuilder struct {
	v               *Block
	Parents__isset  bool
	Messages__isset bool
}

func (mb *_Block__ReprMapBuilder) Insert(k, v ipld.Node) error {
	ks, err := k.AsString()
	if err != nil {
		return ipld.ErrInvalidKey{"not a string: " + err.Error()}
	}
	switch ks {
	case "Parents":
		if mb.Parents__isset {
			panic("repeated assignment to field") // FIXME need an error type for this
		}
		if v.IsNull() {
			panic("type mismatch on struct field assignment: cannot assign null to non-nullable field") // FIXME need an error type for this
		}
		tv, ok := v.(typed.Node)
		if !ok {
			panic("need typed.Node for insertion into struct") // FIXME need an error type for this
		}
		x, ok := v.(Parents)
		if !ok {
			panic("field 'Parents' (key: 'Parents') in type Block is type Parents; cannot assign " + tv.Type().Name()) // FIXME need an error type for this
		}
		mb.v.Parents = x
		mb.Parents__isset = true
	case "Messages":
		if mb.Messages__isset {
			panic("repeated assignment to field") // FIXME need an error type for this
		}
		if v.IsNull() {
			panic("type mismatch on struct field assignment: cannot assign null to non-nullable field") // FIXME need an error type for this
		}
		tv, ok := v.(typed.Node)
		if !ok {
			panic("need typed.Node for insertion into struct") // FIXME need an error type for this
		}
		x, ok := v.(Messages)
		if !ok {
			panic("field 'Messages' (key: 'Messages') in type Block is type Messages; cannot assign " + tv.Type().Name()) // FIXME need an error type for this
		}
		mb.v.Messages = x
		mb.Messages__isset = true
	default:
		return typed.ErrNoSuchField{Type: nil /*TODO:typelit*/, FieldName: ks}
	}
	return nil
}
func (mb *_Block__ReprMapBuilder) Delete(k ipld.Node) error {
	panic("TODO later")
}
func (mb *_Block__ReprMapBuilder) Build() (ipld.Node, error) {
	if !mb.Parents__isset {
		panic("missing required field 'Parents' (key: 'Parents') in building struct Block") // FIXME need an error type for this
	}
	if !mb.Messages__isset {
		panic("missing required field 'Messages' (key: 'Messages') in building struct Block") // FIXME need an error type for this
	}
	v := mb.v
	mb = nil
	return v, nil
}
func (mb *_Block__ReprMapBuilder) BuilderForKeys() ipld.NodeBuilder {
	return _String__NodeBuilder{}
}
func (mb *_Block__ReprMapBuilder) BuilderForValue(ks string) ipld.NodeBuilder {
	switch ks {
	case "Parents":
		return Parents__NodeBuilder()
	case "Messages":
		return Messages__NodeBuilder()
	default:
		panic(typed.ErrNoSuchField{Type: nil /*TODO:typelit*/, FieldName: ks})
	}
	return nil
}

func (nb _Block__ReprBuilder) AmendMap() (ipld.MapBuilder, error) {
	panic("TODO later")
}
func (_Block__ReprBuilder) CreateList() (ipld.ListBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation.Builder", MethodName: "CreateList", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Map}
}
func (_Block__ReprBuilder) AmendList() (ipld.ListBuilder, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation.Builder", MethodName: "AmendList", AppropriateKind: ipld.ReprKindSet_JustList, ActualKind: ipld.ReprKind_Map}
}
func (_Block__ReprBuilder) CreateNull() (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation.Builder", MethodName: "CreateNull", AppropriateKind: ipld.ReprKindSet_JustNull, ActualKind: ipld.ReprKind_Map}
}
func (_Block__ReprBuilder) CreateBool(bool) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation.Builder", MethodName: "CreateBool", AppropriateKind: ipld.ReprKindSet_JustBool, ActualKind: ipld.ReprKind_Map}
}
func (_Block__ReprBuilder) CreateInt(int) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation.Builder", MethodName: "CreateInt", AppropriateKind: ipld.ReprKindSet_JustInt, ActualKind: ipld.ReprKind_Map}
}
func (_Block__ReprBuilder) CreateFloat(float64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation.Builder", MethodName: "CreateFloat", AppropriateKind: ipld.ReprKindSet_JustFloat, ActualKind: ipld.ReprKind_Map}
}
func (_Block__ReprBuilder) CreateString(string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation.Builder", MethodName: "CreateString", AppropriateKind: ipld.ReprKindSet_JustString, ActualKind: ipld.ReprKind_Map}
}
func (_Block__ReprBuilder) CreateBytes([]byte) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation.Builder", MethodName: "CreateBytes", AppropriateKind: ipld.ReprKindSet_JustBytes, ActualKind: ipld.ReprKind_Map}
}
func (_Block__ReprBuilder) CreateLink(ipld.Link) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{TypeName: "Block.Representation.Builder", MethodName: "CreateLink", AppropriateKind: ipld.ReprKindSet_JustLink, ActualKind: ipld.ReprKind_Map}
}
