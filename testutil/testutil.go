package testutil

import (
	"bytes"
	"errors"

	"github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	gsselector "github.com/ipfs/go-graphsync/selector"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	ipld "github.com/ipfs/go-ipld-format"
	random "github.com/jbenet/go-random"
	peer "github.com/libp2p/go-libp2p-peer"
	mh "github.com/multiformats/go-multihash"
)

var blockGenerator = blocksutil.NewBlockGenerator()
var prioritySeq int
var seedSeq int64

func randomBytes(n int64, seed int64) []byte {
	data := new(bytes.Buffer)
	random.WritePseudoRandomBytes(n, data, seed)
	return data.Bytes()
}

// GenerateBlocksOfSize generates a series of blocks of the given byte size
func GenerateBlocksOfSize(n int, size int64) []blocks.Block {
	generatedBlocks := make([]blocks.Block, 0, n)
	for i := 0; i < n; i++ {
		seedSeq++
		b := blocks.NewBlock(randomBytes(size, seedSeq))
		generatedBlocks = append(generatedBlocks, b)

	}
	return generatedBlocks
}

// GenerateCids produces n content identifiers.
func GenerateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		cids = append(cids, c)
	}
	return cids
}

var peerSeq int

// GeneratePeers creates n peer ids.
func GeneratePeers(n int) []peer.ID {
	peerIds := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		peerSeq++
		p := peer.ID(peerSeq)
		peerIds = append(peerIds, p)
	}
	return peerIds
}

// ContainsPeer returns true if a peer is found n a list of peers.
func ContainsPeer(peers []peer.ID, p peer.ID) bool {
	for _, n := range peers {
		if p == n {
			return true
		}
	}
	return false
}

// IndexOf returns the index of a given cid in an array of blocks
func IndexOf(blks []blocks.Block, c cid.Cid) int {
	for i, n := range blks {
		if n.Cid() == c {
			return i
		}
	}
	return -1
}

// ContainsBlock returns true if a block is found n a list of blocks
func ContainsBlock(blks []blocks.Block, block blocks.Block) bool {
	return IndexOf(blks, block.Cid()) != -1
}

const (
	blockSize = 512
)

type byteNode struct {
	data    []byte
	builder cid.Builder
}

var v0CidPrefix = cid.Prefix{
	Codec:    cid.DagProtobuf,
	MhLength: -1,
	MhType:   mh.SHA2_256,
	Version:  0,
}

// ErrEmptyNode is just a simple error code for stubbing out IPLD Node interface
// methods
var ErrEmptyNode = errors.New("dummy node")

func (n *byteNode) Resolve([]string) (interface{}, []string, error) {
	return nil, nil, ErrEmptyNode
}

func (n *byteNode) Tree(string, int) []string {
	return nil
}

func (n *byteNode) ResolveLink([]string) (*ipld.Link, []string, error) {
	return nil, nil, ErrEmptyNode
}

func (n *byteNode) Copy() ipld.Node {
	return &byteNode{}
}

func (n *byteNode) Cid() cid.Cid {
	c, err := n.builder.Sum(n.RawData())
	if err != nil {
		return cid.Cid{}
	}
	return c
}

func (n *byteNode) Links() []*ipld.Link {
	return nil
}

func (n *byteNode) Loggable() map[string]interface{} {
	return nil
}

func (n *byteNode) String() string {
	return string(n.data)
}

func (n *byteNode) RawData() []byte {
	return n.data
}

func (n *byteNode) Size() (uint64, error) {
	return 0, nil
}

func (n *byteNode) Stat() (*ipld.NodeStat, error) {
	return &ipld.NodeStat{}, nil
}

func newNode(data []byte) *byteNode {
	return &byteNode{
		data:    data,
		builder: v0CidPrefix,
	}
}

// MockDecodeSelectorFunc decodes raw data to a type that satisfies
// the Selector interface
func MockDecodeSelectorFunc(data []byte) gsselector.Selector {
	return newNode(data)
}

// MockDecodeSelectionResponseFunc decodes raw data to a type that
// satisfies the SelectionResponse interface
func MockDecodeSelectionResponseFunc(data []byte) gsselector.SelectionResponse {
	return newNode(data)
}

// GenerateSelector returns a new mock Selector
func GenerateSelector() gsselector.Selector {
	node := newNode(randomBytes(blockSize, seedSeq))
	seedSeq++
	return node
}

// GenerateSelectionResponse returns a new mock SelectionResponse
func GenerateSelectionResponse() gsselector.SelectionResponse {
	node := newNode(randomBytes(blockSize, seedSeq))
	seedSeq++
	return node
}

// GenerateRootCid generates a new mock CID to serve as a root
func GenerateRootCid() cid.Cid {
	node := newNode(randomBytes(blockSize, seedSeq))
	seedSeq++
	return node.Cid()
}
