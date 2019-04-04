package testbridge

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ipfs/go-block-format"

	ipldbridge "github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// NewMockLoader returns a new loader function that loads only the given blocks
func NewMockLoader(blks []blocks.Block) ipldbridge.Loader {
	return func(ipldLink ipld.Link, lnkCtx ipldbridge.LinkContext) (io.Reader, error) {
		lnk := ipldLink.(cidlink.Link).Cid
		for _, block := range blks {
			if block.Cid() == lnk {
				return bytes.NewReader(block.RawData()), nil
			}
		}
		return nil, fmt.Errorf("unable to load block")
	}
}
