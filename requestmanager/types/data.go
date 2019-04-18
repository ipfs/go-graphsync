package types

import ipld "github.com/ipld/go-ipld-prime"

// AsyncLoadResult is sent once over the channel returned by an async load.
type AsyncLoadResult struct {
	Data []byte
	Err  error
}

// ResponseProgress is the fundamental unit of responses making progress in
// the RequestManager.
type ResponseProgress struct {
	Node      ipld.Node // a node which matched the graphsync query
	Path      ipld.Path // the path of that node relative to the traversal start
	LastBlock struct {  // LastBlock stores the Path and Link of the last block edge we had to load.
		ipld.Path
		ipld.Link
	}
}
