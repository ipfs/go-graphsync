package metadata

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
)

// Item is a single link traversed in a repsonse
type Item struct {
	Link         cid.Cid
	BlockPresent bool
}

// Metadata is information about metadata contained in a response, which can be
// serialized back and forth to bytes
type Metadata []Item

// DecodeMetadata assembles metadata from a raw byte array, first deserializing
// as a node and then assembling into a metadata struct.
func DecodeMetadata(data datamodel.Node) (Metadata, error) {
	builder := Prototype.Metadata.Representation().NewBuilder()
	err := builder.AssignNode(data)
	if err != nil {
		return nil, err
	}
	metadata := bindnode.Unwrap(builder.Build()).(*Metadata)
	return *metadata, nil
}

// EncodeMetadata encodes metadata to an IPLD node then serializes to raw bytes
func EncodeMetadata(entries Metadata) datamodel.Node {
	return bindnode.Wrap(&entries, Prototype.Metadata.Type())
}
