package testbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	cid "github.com/ipfs/go-cid"
	ipldbridge "github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
	multihash "github.com/multiformats/go-multihash"
)

type mockIPLDBridge struct {
}

// NewMockIPLDBridge returns an IPLD bridge that works with MockSelectors
func NewMockIPLDBridge() ipldbridge.IPLDBridge {
	return &mockIPLDBridge{}
}

func (mb *mockIPLDBridge) ComposeLinkLoader(actualLoader ipldbridge.RawLoader) ipldbridge.LinkLoader {
	return func(ctx context.Context, lnk cid.Cid, lnkCtx ipldbridge.LinkContext) (ipld.Node, error) {
		r, err := actualLoader(ctx, lnk, lnkCtx)
		if err != nil {
			return nil, err
		}
		var buffer bytes.Buffer
		io.Copy(&buffer, r)
		data := buffer.Bytes()
		hash, err := multihash.Sum(data, lnk.Prefix().MhType, lnk.Prefix().MhLength)
		if err != nil {
			return nil, err
		}
		if hash.B58String() != lnk.Hash().B58String() {
			return nil, fmt.Errorf("hash mismatch")
		}
		return NewMockBlockNode(data), nil
	}
}

func (mb *mockIPLDBridge) ValidateSelectorSpec(cidRootedSelector ipld.Node) []error {
	_, ok := cidRootedSelector.(*mockSelectorSpec)
	if !ok {
		return []error{fmt.Errorf("not a selector")}
	}
	return nil
}

func (mb *mockIPLDBridge) EncodeNode(node ipld.Node) ([]byte, error) {
	spec, ok := node.(*mockSelectorSpec)
	if ok {
		data, err := json.Marshal(spec.cidsVisited)
		if err != nil {
			return nil, err
		}
		return data, nil
	}
	return nil, fmt.Errorf("format not supported")
}

func (mb *mockIPLDBridge) DecodeNode(data []byte) (ipld.Node, error) {
	var cidsVisited []cid.Cid
	err := json.Unmarshal(data, &cidsVisited)
	if err == nil {
		return &mockSelectorSpec{cidsVisited}, nil
	}
	return nil, fmt.Errorf("format not supported")
}

func (mb *mockIPLDBridge) DecodeSelectorSpec(cidRootedSelector ipld.Node) (ipld.Node, ipldbridge.Selector, error) {
	spec, ok := cidRootedSelector.(*mockSelectorSpec)
	if !ok {
		return nil, nil, fmt.Errorf("not a selector")
	}
	return nil, newMockSelector(spec), nil
}

func (mb *mockIPLDBridge) Traverse(ctx context.Context, loader ipldbridge.LinkLoader, root ipld.Node, s ipldbridge.Selector, fn ipldbridge.AdvVisitFn) error {
	ms, ok := s.(*mockSelector)
	if !ok {
		return fmt.Errorf("not supported")
	}
	var lastErr error
	for _, lnk := range ms.cidsVisited {
		node, err := loader(ctx, lnk, ipldbridge.LinkContext{})
		if err != nil {
			lastErr = err
		} else {
			fn(ipldbridge.TraversalProgress{}, node, 0)
		}
	}
	return lastErr
}
