package ipldbridge

import (
	"bytes"
	"context"
	"errors"

	ipld "github.com/ipld/go-ipld-prime"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	ipldrepose "github.com/ipld/go-ipld-prime/repose"
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
	nodeBuilderChooser NodeBuilderChooser
	multicodecTable    MulticodecDecodeTable
}

// NewIPLDBridge returns an IPLD Bridge.
func NewIPLDBridge(nodeBuilderChooser NodeBuilderChooser,
	multicodecTable MulticodecDecodeTable) IPLDBridge {
	return &ipldBridge{nodeBuilderChooser, multicodecTable}
}

func (rb *ipldBridge) ComposeLinkLoader(
	actualLoader RawLoader) LinkLoader {
	return ipldrepose.ComposeLinkLoader(actualLoader, rb.nodeBuilderChooser, rb.multicodecTable)
}

func (rb *ipldBridge) Traverse(ctx context.Context, loader LinkLoader, root ipld.Node, s Selector, fn AdvVisitFn) error {
	config := &TraversalConfig{Ctx: ctx, LinkLoader: loader}
	return TraversalProgress{TraversalConfig: config}.TraverseInformatively(root, s, fn)
}

func (rb *ipldBridge) ValidateSelectorSpec(cidRootedSelector ipld.Node) []error {
	return ipldtypesystem.Validate(universe, cidRootedSelectorType, cidRootedSelector)
}

func (rb *ipldBridge) EncodeNode(node ipld.Node) ([]byte, error) {
	var buffer bytes.Buffer
	err := ipldrepose.EncoderDagCbor(node, &buffer)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (rb *ipldBridge) DecodeNode(encoded []byte) (ipld.Node, error) {
	reader := bytes.NewReader(encoded)
	return ipldrepose.DecoderDagCbor(ipldfree.NodeBuilder(), reader)
}

func (rb *ipldBridge) DecodeSelectorSpec(cidRootedSelector ipld.Node) (ipld.Node, Selector, error) {

	errs := rb.ValidateSelectorSpec(cidRootedSelector)
	if len(errs) != 0 {
		return nil, nil, errors.New("Node does not validate as selector spec")
	}

	root, err := cidRootedSelector.TraverseField("root")
	if err != nil {
		return nil, nil, err
	}
	rootCid, err := root.AsLink()
	if err != nil {
		return nil, nil, err
	}

	node, err := ipldfree.NodeBuilder().CreateLink(rootCid)
	if err != nil {
		return nil, nil, err
	}

	selector, err := ipldselector.ReifySelector(cidRootedSelector)
	if err != nil {
		return nil, nil, err
	}

	return node, selector, nil
}
