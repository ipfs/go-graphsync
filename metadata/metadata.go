package metadata

import (
	"github.com/ipfs/go-graphsync/ipldbridge"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
)

// Item is a single link traversed in a repsonse
type Item struct {
	Link         ipld.Link
	BlockPresent bool
}

// Metadata is information about metadata contained in a response, which can be
// serialized back and forth to bytes
type Metadata []Item

// DecodeMetadata assembles metadata from a raw byte array, first deserializing
// as a node and then assembling into a metadata struct.
func DecodeMetadata(data []byte) (Metadata, error) {
	node, err := ipldbridge.DecodeNode(data)
	if err != nil {
		return nil, err
	}
	var decodedData interface{}
	err = fluent.Recover(func() {
		simpleNode := fluent.WrapNode(node)
		iterator := simpleNode.ListIterator()
		var metadata Metadata
		if simpleNode.Length() != -1 {
			metadata = make(Metadata, 0, simpleNode.Length())
		}

		for !iterator.Done() {
			_, item := iterator.Next()
			link := item.LookupString("link").AsLink()
			blockPresent := item.LookupString("blockPresent").AsBool()
			metadata = append(metadata, Item{link, blockPresent})
		}
		decodedData = metadata
	})
	if err != nil {
		return nil, err
	}
	return decodedData.(Metadata), err
}

// EncodeMetadata encodes metadata to an IPLD node then serializes to raw bytes
func EncodeMetadata(entries Metadata) ([]byte, error) {
	var node ipld.Node
	err := fluent.Recover(func() {
		nb := fluent.WrapNodeBuilder(ipldfree.NodeBuilder())
		node = nb.CreateList(func(lb fluent.ListBuilder, nb fluent.NodeBuilder) {
			for _, item := range entries {
				lb.Append(
					nb.CreateMap(func(mb fluent.MapBuilder, knb fluent.NodeBuilder, vnb fluent.NodeBuilder) {
						mb.Insert(knb.CreateString("link"), vnb.CreateLink(item.Link))
						mb.Insert(knb.CreateString("blockPresent"), vnb.CreateBool(item.BlockPresent))
					}),
				)
			}
		})
	})
	if err != nil {
		return nil, err
	}
	return ipldbridge.EncodeNode(node)
}
