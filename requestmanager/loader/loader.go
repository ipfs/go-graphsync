package loader

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldbridge"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	ipld "github.com/ipld/go-ipld-prime"
)

// AsyncLoadFn is a function which given a request id and an ipld.Link, returns
// a channel which will eventually return data for the link or an err
type AsyncLoadFn func(graphsync.RequestID, ipld.Link) <-chan types.AsyncLoadResult

// WrapAsyncLoader creates a regular ipld link laoder from an asynchronous load
// function, with the given cancellation context, for the given requests, and will
// transmit load errors on the given channel
func WrapAsyncLoader(
	ctx context.Context,
	asyncLoadFn AsyncLoadFn,
	requestID graphsync.RequestID,
	errorChan chan error) ipld.Loader {
	return func(link ipld.Link, linkContext ipldbridge.LinkContext) (io.Reader, error) {
		resultChan := asyncLoadFn(requestID, link)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("request finished")
		case result := <-resultChan:
			if result.Err != nil {
				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("request finished")
				case errorChan <- result.Err:
					return nil, ipldbridge.ErrDoNotFollow()
				}
			}
			return bytes.NewReader(result.Data), nil
		}
	}
}
