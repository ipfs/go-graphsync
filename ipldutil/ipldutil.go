package ipldutil

import (
	"bytes"
	"context"
	"errors"

	ipld "github.com/ipld/go-ipld-prime"
	dagpb "github.com/ipld/go-ipld-prime-proto"
	"github.com/ipld/go-ipld-prime/encoding/dagcbor"
	free "github.com/ipld/go-ipld-prime/impl/free"
	"github.com/ipld/go-ipld-prime/traversal"
	ipldtraversal "github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
)

var errDoNotFollow = errors.New("Dont Follow Me")

// ErrDoNotFollow is just a wrapper for whatever IPLD's ErrDoNotFollow ends up looking like
func ErrDoNotFollow() error {
	return errDoNotFollow
}

var (
	defaultChooser traversal.NodeBuilderChooser = dagpb.AddDagPBSupportToChooser(func(ipld.Link, ipld.LinkContext) ipld.NodeBuilder {
		return free.NodeBuilder()
	})
)

func Traverse(ctx context.Context, loader ipld.Loader, root ipld.Link, s selector.Selector, fn traversal.AdvVisitFn) error {
	builder := defaultChooser(root, ipld.LinkContext{})
	node, err := root.Load(ctx, ipld.LinkContext{}, builder, loader)
	if err != nil {
		return err
	}
	return traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                    ctx,
			LinkLoader:             loader,
			LinkNodeBuilderChooser: defaultChooser,
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
	reader := bytes.NewReader(encoded)
	return dagcbor.Decoder(free.NodeBuilder(), reader)
}

func ParseSelector(selector ipld.Node) (selector.Selector, error) {
	return ipldselector.ParseSelector(selector)
}
