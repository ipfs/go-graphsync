package metadata

import (
	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
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
	node, err := ipldutil.DecodeNode(data)
	if err != nil {
		return nil, err
	}
	iterator := node.ListIterator()
	var metadata Metadata
	if node.Length() != -1 {
		metadata = make(Metadata, 0, node.Length())
	}

	for !iterator.Done() {
		_, item, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		linkNode, err := item.LookupString("link")
		if err != nil {
			return nil, err
		}
		link, err := linkNode.AsLink()
		if err != nil {
			return nil, err
		}
		blockPresentNode, err := item.LookupString("blockPresent")
		if err != nil {
			return nil, err
		}
		blockPresent, err := blockPresentNode.AsBool()
		if err != nil {
			return nil, err
		}
		metadata = append(metadata, Item{link, blockPresent})
	}
	return metadata, err
}

// EncodeMetadata encodes metadata to an IPLD node then serializes to raw bytes
func EncodeMetadata(entries Metadata) ([]byte, error) {
	node, err := fluent.Build(basicnode.Style.List, func(na fluent.NodeAssembler) {
		na.CreateList(len(entries), func(na fluent.ListAssembler) {
			for _, item := range entries {
				na.AssembleValue().CreateMap(2, func(na fluent.MapAssembler) {
					na.AssembleEntry("link").AssignLink(item.Link)
					na.AssembleEntry("blockPresent").AssignBool(item.BlockPresent)
				})
			}
		})
	})
	if err != nil {
		return nil, err
	}
	return ipldutil.EncodeNode(node)
}
