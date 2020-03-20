package testbridge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	blocks "github.com/ipfs/go-block-format"

	"github.com/ipld/go-ipld-prime"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/ipldbridge"
	"github.com/ipfs/go-graphsync/testutil"
)

func TestSelectorTraversal(t *testing.T) {
	blks := testutil.GenerateBlocksOfSize(5, 20)
	cids := make([]cid.Cid, 0, 5)
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	var uniqueBlocksVisited []blocks.Block
	loader := func(ipldLink ipld.Link, lnkCtx ipldbridge.LinkContext) (io.Reader, error) {
		lnk := ipldLink.(cidlink.Link).Cid
		for _, block := range blks {
			if block.Cid() == lnk {
				if testutil.ContainsBlock(uniqueBlocksVisited, block) {
					return nil, fmt.Errorf("loaded block twice")
				}
				uniqueBlocksVisited = append(uniqueBlocksVisited, block)
				return bytes.NewReader(block.RawData()), nil
			}
		}
		return nil, fmt.Errorf("unable to load block")
	}
	bridge := NewMockIPLDBridge()
	root := cidlink.Link{Cid: cids[0]}
	mockSelectorSpec := NewMockSelectorSpec(cids)
	selector, err := bridge.ParseSelector(mockSelectorSpec)
	if err != nil {
		t.Fatal("unable to decode selector")
	}
	var traversalFn ipldbridge.AdvVisitFn
	traversalFn = func(tp ipldbridge.TraversalProgress, node ipld.Node, traversalReason ipldbridge.TraversalReason) error {
		return nil
	}
	ctx := context.Background()
	err = bridge.Traverse(ctx, loader, root, selector, traversalFn)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(uniqueBlocksVisited) != 5 {
		t.Fatal("did not visit all blocks")
	}
}

func TestFailParseSelectorSpec(t *testing.T) {
	cids := testutil.GenerateCids(5)
	spec := NewUnparsableSelectorSpec(cids)
	bridge := NewMockIPLDBridge()
	_, err := bridge.ParseSelector(spec)
	if err == nil {
		t.Fatal("Spec should not decompose to node and selector")
	}
}

func TestFailEncodingSelectorSpec(t *testing.T) {
	cids := testutil.GenerateCids(5)
	spec := NewUnencodableSelectorSpec(cids)
	_, err := ipldbridge.EncodeNode(spec)
	if err == nil {
		t.Fatal("Spec should not be encodable")
	}
}
