package ipldbind

import (
	"io"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"

	"github.com/ipfs/go-graphsync"
	pb "github.com/ipfs/go-graphsync/message/pb"
)

// IsTerminalSuccessCode returns true if the response code indicates the
// request terminated successfully.
// DEPRECATED: use status.IsSuccess()
func IsTerminalSuccessCode(status graphsync.ResponseStatusCode) bool {
	return status.IsSuccess()
}

// IsTerminalFailureCode returns true if the response code indicates the
// request terminated in failure.
// DEPRECATED: use status.IsFailure()
func IsTerminalFailureCode(status graphsync.ResponseStatusCode) bool {
	return status.IsFailure()
}

// IsTerminalResponseCode returns true if the response code signals
// the end of the request
// DEPRECATED: use status.IsTerminal()
func IsTerminalResponseCode(status graphsync.ResponseStatusCode) bool {
	return status.IsTerminal()
}

// Exportable is an interface that can serialize to a protobuf
type Exportable interface {
	ToProto() (*pb.Message, error)
	ToNet(w io.Writer) error
}

type GraphSyncExtensions struct {
	Keys   []string
	Values map[string]datamodel.Node
}

// GraphSyncRequest is a struct to capture data on a request contained in a
// GraphSyncMessage.
type GraphSyncRequest struct {
	Id []byte

	Root       *cid.Cid
	Selector   *ipld.Node
	Extensions GraphSyncExtensions
	Priority   graphsync.Priority
	Cancel     bool
	Update     bool
}

type GraphSyncMetadatum struct {
	Link         datamodel.Link
	BlockPresent bool
}

// GraphSyncResponse is an struct to capture data on a response sent back
// in a GraphSyncMessage.
type GraphSyncResponse struct {
	Id []byte

	Status     graphsync.ResponseStatusCode
	Metadata   []GraphSyncMetadatum
	Extensions GraphSyncExtensions
}

type GraphSyncBlock struct {
	Prefix []byte
	Data   []byte
}

func FromBlockFormat(block blocks.Block) GraphSyncBlock {
	return GraphSyncBlock{
		Prefix: block.Cid().Prefix().Bytes(),
		Data:   block.RawData(),
	}
}

func (b GraphSyncBlock) BlockFormat() *blocks.BasicBlock {
	pref, err := cid.PrefixFromBytes(b.Prefix)
	if err != nil {
		panic(err) // should never happen
	}

	c, err := pref.Sum(b.Data)
	if err != nil {
		panic(err) // should never happen
	}

	block, err := blocks.NewBlockWithCid(b.Data, c)
	if err != nil {
		panic(err) // should never happen
	}
	return block
}

func BlockFormatSlice(bs []GraphSyncBlock) []blocks.Block {
	blks := make([]blocks.Block, len(bs))
	for i, b := range bs {
		blks[i] = b.BlockFormat()
	}
	return blks
}

type GraphSyncMessage struct {
	Requests  []GraphSyncRequest
	Responses []GraphSyncResponse
	Blocks    []GraphSyncBlock
}

// NamedExtension exists just for the purpose of the constructors.
type NamedExtension struct {
	Name graphsync.ExtensionName
	Data ipld.Node
}
