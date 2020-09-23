package chaintypes

// Type is a struct embeding a NodePrototype/Type for every Node implementation in this package.
// One of its major uses is to start the construction of a value.
// You can use it like this:
//
// 		chaintypes.Type.YourTypeName.NewBuilder().BeginMap() //...
//
// and:
//
// 		chaintypes.Type.OtherTypeName.NewBuilder().AssignString("x") // ...
//
var Type typeSlab

type typeSlab struct {
	Block          _Block__Prototype
	Block__Repr    _Block__ReprPrototype
	Bytes          _Bytes__Prototype
	Bytes__Repr    _Bytes__ReprPrototype
	Link           _Link__Prototype
	Link__Repr     _Link__ReprPrototype
	Messages       _Messages__Prototype
	Messages__Repr _Messages__ReprPrototype
	Parents        _Parents__Prototype
	Parents__Repr  _Parents__ReprPrototype
	String         _String__Prototype
	String__Repr   _String__ReprPrototype
}
