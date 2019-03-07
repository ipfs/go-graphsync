package testselector

import (
	"bytes"
	"errors"

	cid "github.com/ipfs/go-cid"
	gsselector "github.com/ipfs/go-graphsync/selector"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/jbenet/go-random"
	mh "github.com/multiformats/go-multihash"
)

var seedSeq int64

const (
	blockSize = 512
)

func randomBytes(n int64, seed int64) []byte {
	data := new(bytes.Buffer)
	random.WritePseudoRandomBytes(n, data, seed)
	return data.Bytes()
}

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
