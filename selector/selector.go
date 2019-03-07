package selector

import (
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// Selector is an interface for an IPLD Selector.
type Selector interface {
	ipld.Node
}

// SelectionResponse is an interface that represents part of the results
// of a selector query.
type SelectionResponse interface {
	ipld.Node
}

// SelectionTraverser is an interface for navigating a response to a selector
// query.
type SelectionTraverser interface {
	Next() (SelectionResponse, error)
	Cancel()
}

// SelectorQuerier can be used to make and validate selector queries.
type SelectorQuerier interface {
	Select(Selector, root cid.Cid) SelectionTraverser
	Validate(Selector, root cid.Cid, incomingResponses SelectionTraverser) SelectionTraverser
}
