package ipldutil

import (
	"bytes"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

func EncodeNode(node datamodel.Node) ([]byte, error) {
	return ipld.Encode(node, dagcbor.Encode)
}

func EncodeNodeInto(node datamodel.Node, buffer *bytes.Buffer) error {
	return ipld.EncodeStreaming(buffer, node, dagcbor.Encode)
}

func DecodeNode(encoded []byte) (datamodel.Node, error) {
	return DecodeNodeInto(encoded, basicnode.Prototype.Any)
}

func DecodeNodeInto(encoded []byte, proto datamodel.NodePrototype) (datamodel.Node, error) {
	return ipld.DecodeUsingPrototype(encoded, dagcbor.Decode, proto)
}
