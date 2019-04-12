package ipldbridge

import (
	"bytes"
	"context"
	"errors"

	"github.com/ipld/go-ipld-prime/fluent"

	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/encoding/dagcbor"
	free "github.com/ipld/go-ipld-prime/impl/free"
	ipldtraversal "github.com/ipld/go-ipld-prime/traversal"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
	ipldtypesystem "github.com/ipld/go-ipld-prime/typed/system"
)

// not sure how these will come into being or from where
var cidRootedSelectorType ipldtypesystem.Type
var universe ipldtypesystem.Universe

// TraversalConfig is an alias from ipld, in case it's renamed/moved.
type TraversalConfig = ipldtraversal.TraversalConfig

type ipldBridge struct {
}

// NewIPLDBridge returns an IPLD Bridge.
func NewIPLDBridge() IPLDBridge {
	return &ipldBridge{}
}

func (rb *ipldBridge) ExtractData(node ipld.Node, buildFn func(SimpleNode) interface{}) (interface{}, error) {
	var value interface{}
	err := fluent.Recover(func() {
		simpleNode := fluent.WrapNode(node)
		value = buildFn(simpleNode)
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (rb *ipldBridge) BuildNode(buildFn func(NodeBuilder) ipld.Node) (ipld.Node, error) {
	var node ipld.Node
	err := fluent.Recover(func() {
		nb := fluent.WrapNodeBuilder(free.NodeBuilder())
		node = buildFn(nb)
	})
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (rb *ipldBridge) Traverse(ctx context.Context, loader Loader, root ipld.Node, s Selector, fn AdvVisitFn) error {
	config := &TraversalConfig{Ctx: ctx, LinkLoader: loader}
	return TraversalProgress{TraversalConfig: config}.TraverseInformatively(root, s, fn)
}

func (rb *ipldBridge) ValidateSelectorSpec(cidRootedSelector ipld.Node) []error {
	return ipldtypesystem.Validate(universe, cidRootedSelectorType, cidRootedSelector)
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

func (rb *ipldBridge) DecodeSelectorSpec(rootedSelector ipld.Node) (ipld.Node, Selector, error) {

	errs := rb.ValidateSelectorSpec(rootedSelector)
	if len(errs) != 0 {
		return nil, nil, errors.New("Node does not validate as selector spec")
	}

	var node ipld.Node
	err := fluent.Recover(func() {
		link := fluent.WrapNode(rootedSelector).TraverseField("root").AsLink()
		node = fluent.WrapNodeBuilder(free.NodeBuilder()).CreateLink(link)
	})
	if err != nil {
		return nil, nil, err
	}

	selector, err := ipldselector.ReifySelector(rootedSelector)
	if err != nil {
		return nil, nil, err
	}

	return node, selector, nil
}
