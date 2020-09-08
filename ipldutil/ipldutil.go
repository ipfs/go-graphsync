package ipldutil

import (
	"bytes"
	"context"

	ipld "github.com/ipld/go-ipld-prime"
	dagpb "github.com/ipld/go-ipld-prime-proto"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	ipldtraversal "github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
)

var (
	defaultChooser traversal.LinkTargetNodePrototypeChooser = dagpb.AddDagPBSupportToChooser(func(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
		return basicnode.Prototype.Any, nil
	})
)

func Traverse(ctx context.Context, loader ipld.Loader, chooser traversal.LinkTargetNodePrototypeChooser, root ipld.Link, s selector.Selector, fn traversal.AdvVisitFn) error {
	if chooser == nil {
		chooser = defaultChooser
	}
	ns, err := chooser(root, ipld.LinkContext{})
	if err != nil {
		return err
	}
	nb := ns.NewBuilder()
	err = root.Load(ctx, ipld.LinkContext{}, nb, loader)
	if err != nil {
		return err
	}
	node := nb.Build()
	return traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                        ctx,
			LinkLoader:                 loader,
			LinkTargetNodePrototypeChooser: chooser,
		},
	}.WalkAdv(node, s, fn)
}

func WalkMatching(node ipld.Node, s selector.Selector, fn traversal.VisitFn) error {
	return ipldtraversal.WalkMatching(node, s, fn)
}

func EncodeNode(node ipld.Node) ([]byte, error) {
	var buffer bytes.Buffer
	err := dagcbor.Encoder(node, &buffer)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func DecodeNode(encoded []byte) (ipld.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decoder(nb, bytes.NewReader(encoded)); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

func ParseSelector(selector ipld.Node) (selector.Selector, error) {
	return ipldselector.ParseSelector(selector)
}
