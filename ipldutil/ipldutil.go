package ipldutil

import (
	"bytes"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

func EncodeNode(node datamodel.Node) ([]byte, error) {
	var buffer bytes.Buffer
	err := EncodeNodeInto(node, &buffer)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func EncodeNodeInto(node datamodel.Node, buffer *bytes.Buffer) error {
	return dagcbor.Encode(node, buffer)
}

func DecodeNode(encoded []byte) (datamodel.Node, error) {
	return DecodeNodeInto(encoded, basicnode.Prototype.Any.NewBuilder())
}

func DecodeNodeInto(encoded []byte, nb datamodel.NodeBuilder) (datamodel.Node, error) {
	if err := dagcbor.Decode(nb, bytes.NewReader(encoded)); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}
