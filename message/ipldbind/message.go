package ipldbind

import (
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/message"
	"github.com/ipld/go-ipld-prime/datamodel"

	"github.com/ipfs/go-graphsync"
)

type MessagePartWithExtensions interface {
	ExtensionNames() []graphsync.ExtensionName
	Extension(name graphsync.ExtensionName) (datamodel.Node, bool)
}

type GraphSyncExtensions struct {
	Keys   []string
	Values map[string]datamodel.Node
}

func NewGraphSyncExtensions(part MessagePartWithExtensions) GraphSyncExtensions {
	names := part.ExtensionNames()
	keys := make([]string, 0, len(names))
	values := make(map[string]datamodel.Node, len(names))
	for _, name := range names {
		keys = append(keys, string(name))
		data, _ := part.Extension(graphsync.ExtensionName(name))
		values[string(name)] = data
	}
	return GraphSyncExtensions{keys, values}
}

func (gse GraphSyncExtensions) ToExtensionsList() []graphsync.ExtensionData {
	exts := make([]graphsync.ExtensionData, 0, len(gse.Values))
	for name, data := range gse.Values {
		exts = append(exts, graphsync.ExtensionData{Name: graphsync.ExtensionName(name), Data: data})
	}
	return exts
}

// GraphSyncRequest is a struct to capture data on a request contained in a
// GraphSyncMessage.
type GraphSyncRequest struct {
	Id []byte

	Root       *cid.Cid
	Selector   *datamodel.Node
	Extensions GraphSyncExtensions
	Priority   graphsync.Priority
	Cancel     bool
	Update     bool
}

// GraphSyncResponse is an struct to capture data on a response sent back
// in a GraphSyncMessage.
type GraphSyncResponse struct {
	Id []byte

	Status     graphsync.ResponseStatusCode
	Metadata   []message.GraphSyncMetadatum
	Extensions GraphSyncExtensions
}

type GraphSyncBlock struct {
	Prefix []byte
	Data   []byte
}

type GraphSyncMessage struct {
	Requests  []GraphSyncRequest
	Responses []GraphSyncResponse
	Blocks    []GraphSyncBlock
}

// NamedExtension exists just for the purpose of the constructors.
type NamedExtension struct {
	Name graphsync.ExtensionName
	Data datamodel.Node
}
