package loader

import (
	"bytes"
	"context"
	"io"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal"
)

// ContextCancelError is a sentinel that indicates the passed in request context
// was cancelled
type ContextCancelError struct{}

func (ContextCancelError) Error() string {
	return "request context cancelled"
}

// AsyncLoadFn is a function which given a request id and an ipld.Link, returns
// a channel which will eventually return data for the link or an err
type AsyncLoadFn func(graphsync.RequestID, ipld.Link) <-chan types.AsyncLoadResult

// OnNewBlockFn is a function that is called whenever a new block is successfully loaded
// before the loader completes
type OnNewBlockFn func(graphsync.BlockData) error

// WrapAsyncLoader creates a regular ipld link laoder from an asynchronous load
// function, with the given cancellation context, for the given requests, and will
// transmit load errors on the given channel
func WrapAsyncLoader(
	ctx context.Context,
	asyncLoadFn AsyncLoadFn,
	requestID graphsync.RequestID,
	errorChan chan error,
	onNewBlockFn OnNewBlockFn) ipld.Loader {
	return func(link ipld.Link, linkContext ipld.LinkContext) (io.Reader, error) {
		resultChan := asyncLoadFn(requestID, link)
		select {
		case <-ctx.Done():
			return nil, ContextCancelError{}
		case result := <-resultChan:
			if result.Err != nil {
				select {
				case <-ctx.Done():
					return nil, ContextCancelError{}
				case errorChan <- result.Err:
					return nil, traversal.SkipMe{}
				}
			}
			err := onNewBlockFn(&blockData{link, result.Local, uint64(len(result.Data))})
			if err != nil {
				return nil, err
			}
			return bytes.NewReader(result.Data), nil
		}
	}
}

type blockData struct {
	link  ipld.Link
	local bool
	size  uint64
}

// Link is the link/cid for the block
func (bd *blockData) Link() ipld.Link {
	return bd.link
}

// BlockSize specifies the size of the block
func (bd *blockData) BlockSize() uint64 {
	return bd.size
}

// BlockSize specifies the amount of data actually transmitted over the network
func (bd *blockData) BlockSizeOnWire() uint64 {
	if bd.local {
		return 0
	}
	return bd.size
}
