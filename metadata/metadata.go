package metadata

import (
	"github.com/ipfs/go-graphsync/ipldbridge"
	"github.com/ipld/go-ipld-prime"
)

// Metadata is information about metadata contained in a response, which can be
// serialized back and forth to bytes
type Metadata map[ipld.Link]bool

// DecodeMetadata assembles metadata from a raw byte array, first deserializing
// as a node and then assembling into a metadata struct.
func DecodeMetadata(data []byte, ipldBridge ipldbridge.IPLDBridge) (Metadata, error) {
	node, err := ipldBridge.DecodeNode(data)
	if err != nil {
		return nil, err
	}
	decodedData, err := ipldBridge.ExtractData(node, func(simpleNode ipldbridge.SimpleNode) interface{} {
		iterator := simpleNode.ListIterator()
		metadata := make(Metadata)
		for !iterator.Done() {
			_, item := iterator.Next()
			link := item.TraverseField("link").AsLink()
			blockPresent := item.TraverseField("blockPresent").AsBool()
			metadata[link] = blockPresent
		}
		return metadata
	})
	if err != nil {
		return nil, err
	}
	return decodedData.(Metadata), err
}

// EncodeMetadata encodes metadata to an IPLD node then serializes to raw bytes
func EncodeMetadata(entries Metadata, ipldBridge ipldbridge.IPLDBridge) ([]byte, error) {
	node, err := ipldBridge.BuildNode(func(nb ipldbridge.NodeBuilder) ipld.Node {
		return nb.CreateList(func(lb ipldbridge.ListBuilder, nb ipldbridge.NodeBuilder) {
			for link, blockPresent := range entries {
				lb.Append(
					nb.CreateMap(func(mb ipldbridge.MapBuilder, knb ipldbridge.NodeBuilder, vnb ipldbridge.NodeBuilder) {
						mb.Insert(knb.CreateString("link"), vnb.CreateLink(link))
						mb.Insert(knb.CreateString("blockPresent"), vnb.CreateBool(blockPresent))
					}),
				)
			}
		})
	})
	if err != nil {
		return nil, err
	}
	return ipldBridge.EncodeNode(node)
}
