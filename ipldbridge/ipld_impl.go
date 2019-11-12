package ipldbridge

import (
	"bytes"
	"context"

	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/encoding/dagcbor"
	free "github.com/ipld/go-ipld-prime/impl/free"
	ipldtraversal "github.com/ipld/go-ipld-prime/traversal"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
)

// TraversalConfig is an alias from ipld, in case it's renamed/moved.
type TraversalConfig = ipldtraversal.Config

type ipldBridge struct {
}

// NewIPLDBridge returns an IPLD Bridge.
func NewIPLDBridge() IPLDBridge {
	return &ipldBridge{}
}

func (rb *ipldBridge) Traverse(ctx context.Context, loader Loader, root ipld.Link, s Selector, fn AdvVisitFn) error {
	node, err := root.Load(ctx, LinkContext{}, free.NodeBuilder(), loader)
	if err != nil {
		return err
	}
	return TraversalProgress{
		Cfg: &TraversalConfig{
			Ctx:        ctx,
			LinkLoader: loader,
		},
	}.WalkAdv(node, s, fn)
}

func (rb *ipldBridge) WalkMatching(node ipld.Node, s Selector, fn VisitFn) error {
	return ipldtraversal.WalkMatching(node, s, fn)
}

func (rb *ipldBridge) EncodeNode(node ipld.Node) ([]byte, error) {
	var buffer bytes.Buffer
	err := dagcbor.Encoder(node, &buffer)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (rb *ipldBridge) DecodeNode(encoded []byte) (ipld.Node, error) {
	reader := bytes.NewReader(encoded)
	return dagcbor.Decoder(free.NodeBuilder(), reader)
}

func (rb *ipldBridge) ParseSelector(selector ipld.Node) (Selector, error) {
	return ipldselector.ParseSelector(selector)
}
