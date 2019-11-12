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
	"github.com/ipld/go-ipld-prime/encoding/dagjson"
	free "github.com/ipld/go-ipld-prime/impl/free"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	multihash "github.com/multiformats/go-multihash"
)

type mockIPLDBridge struct {
}

// NewMockIPLDBridge returns an ipld bridge with mocked behavior
func NewMockIPLDBridge() ipldbridge.IPLDBridge {
	return &mockIPLDBridge{}
}

func (mb *mockIPLDBridge) EncodeNode(node ipld.Node) ([]byte, error) {
	spec, ok := node.(*mockSelectorSpec)
	if ok {
		if !spec.FailEncode {
			data, err := json.Marshal(*spec)
			if err != nil {
				return nil, err
			}
			return data, nil
		}
		return nil, fmt.Errorf("format not supported")
	}
	var buffer bytes.Buffer
	err := dagjson.Encoder(node, &buffer)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (mb *mockIPLDBridge) DecodeNode(data []byte) (ipld.Node, error) {
	var spec mockSelectorSpec
	err := json.Unmarshal(data, &spec)
	if err == nil {
		return &spec, nil
	}
	reader := bytes.NewReader(data)
	return dagjson.Decoder(free.NodeBuilder(), reader)
}

func (mb *mockIPLDBridge) ParseSelector(selectorSpec ipld.Node) (ipldbridge.Selector, error) {
	spec, ok := selectorSpec.(*mockSelectorSpec)
	if !ok || spec.FalseParse {
		return nil, fmt.Errorf("not a selector")
	}
	return newMockSelector(spec), nil
}

func (mb *mockIPLDBridge) Traverse(ctx context.Context, loader ipldbridge.Loader, root ipld.Link, s ipldbridge.Selector, fn ipldbridge.AdvVisitFn) error {
	ms, ok := s.(*mockSelector)
	if !ok {
		return fmt.Errorf("not supported")
	}
	for _, lnk := range ms.cidsVisited {

		node, err := loadNode(lnk, loader)
		if err == nil {
			fn(ipldbridge.TraversalProgress{LastBlock: struct {
				Path ipld.Path
				Link ipld.Link
			}{ipld.Path{}, cidlink.Link{Cid: lnk}}}, node, 0)
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
	return nil
}

func (mb *mockIPLDBridge) WalkMatching(node ipld.Node, s ipldbridge.Selector, fn ipldbridge.VisitFn) error {
	spec, ok := node.(*mockSelectorSpec)
	if ok && spec.FailValidation {
		return fmt.Errorf("not a valid kind of selector")
	}
	return nil
}

func loadNode(lnk cid.Cid, loader ipldbridge.Loader) (ipld.Node, error) {
	r, err := loader(cidlink.Link{Cid: lnk}, ipldbridge.LinkContext{})
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
