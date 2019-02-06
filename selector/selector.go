package selector

import (
	ipld "github.com/ipfs/go-ipld-format"
)

type Selector interface {
	ipld.Node
}

type SelectionResponse interface {
	ipld.Node
}

type SelectionTraverser interface {
	Next() (SelectionResponse, error)
	Cancel()
}

type SelectorManager interface {
	Select(Selector, root ipld.Node) SelectionTraverser
	Validate(Selector, root ipld.Node, incomingResponses SelectionTraverser) SelectionTraverser
	DecodeSelector([]byte) Selector
	DecodeSelectorResponse([]byte) SelectionResponse
}
