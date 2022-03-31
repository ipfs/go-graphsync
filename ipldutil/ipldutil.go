package ipldutil

import (
	"bytes"

	"github.com/ipfs/go-graphsync/panics"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

func EncodeNode(node datamodel.Node, panicHandler panics.PanicHandler) ([]byte, error) {
	var buffer bytes.Buffer
	err := EncodeNodeInto(node, &buffer, panicHandler)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func EncodeNodeInto(node datamodel.Node, buffer *bytes.Buffer, panicHandler panics.PanicHandler) (err error) {
	defer func() {
		if rerr := panicHandler(recover()); rerr != nil {
			err = rerr
		}
	}()

	err = dagcbor.Encode(node, buffer)
	return err
}

func DecodeNode(encoded []byte, panicHandler panics.PanicHandler) (datamodel.Node, error) {
	return DecodeNodeInto(encoded, basicnode.Prototype.Any.NewBuilder(), panicHandler)
}

func DecodeNodeInto(encoded []byte, nb datamodel.NodeBuilder, panicHandler panics.PanicHandler) (_ datamodel.Node, err error) {
	defer func() {
		if rerr := panicHandler(recover()); rerr != nil {
			err = rerr
		}
	}()

	if err := dagcbor.Decode(nb, bytes.NewReader(encoded)); err != nil {
		return nil, err
	}

	return nb.Build(), err
}

func SafeUnwrap(node datamodel.Node, panicHandler panics.PanicHandler) (_ interface{}, err error) {
	defer func() {
		if rerr := panicHandler(recover()); rerr != nil {
			err = rerr
		}
	}()

	ptr := bindnode.Unwrap(node)
	return ptr, err
}

func SafeWrap(ptr interface{}, typ schema.Type, panicHandler panics.PanicHandler) (_ schema.TypedNode, err error) {
	defer func() {
		if rerr := panicHandler(recover()); rerr != nil {
			err = rerr
		}
	}()

	node := bindnode.Wrap(ptr, typ)
	return node, err
}
