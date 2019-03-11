package testbridge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

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
	loader := func(ctx context.Context, lnk cid.Cid, lnkCtx ipldbridge.LinkContext) (io.Reader, error) {
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
	linkloader := bridge.ComposeLinkLoader(loader)
	mockSelectorSpec := NewMockSelectorSpec(cids)
	node, selector, err := bridge.DecodeSelectorSpec(mockSelectorSpec)
	if err != nil {
		t.Fatal("unable to decode selector")
	}
	var traversalFn ipldbridge.AdvVisitFn
	traversalFn = func(tp ipldbridge.TraversalProgress, node ipld.Node, traversalReason ipldbridge.TraversalReason) error {
		return nil
	}
	ctx := context.Background()
	err = bridge.Traverse(ctx, linkloader, node, selector, traversalFn)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(uniqueBlocksVisited) != 5 {
		t.Fatal("did not visit all blocks")
	}
}

func TestEncodeDecodeSelectorSpec(t *testing.T) {
	cids := testutil.GenerateCids(5)
	spec := NewMockSelectorSpec(cids)
	bridge := NewMockIPLDBridge()
	data, err := bridge.EncodeNode(spec)
	if err != nil {
		t.Fatal("error encoding selector spec")
	}
	node, err := bridge.DecodeNode(data)
	if err != nil {
		t.Fatal("error decoding data")
	}
	returnedSpec, ok := node.(*mockSelectorSpec)
	if !ok {
		t.Fatal("did not decode a selector")
	}
	if len(returnedSpec.cidsVisited) != 5 {
		t.Fatal("did not decode enough cids")
	}
	if !reflect.DeepEqual(cids, returnedSpec.cidsVisited) {
		t.Fatal("did not decode correct cids")
	}
}
