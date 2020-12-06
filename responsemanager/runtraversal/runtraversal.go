package runtraversal

import (
	"bytes"
	"io"

	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal"

	"github.com/ipfs/go-graphsync/ipldutil"
)

// ResponseSender sends responses over the network
type ResponseSender func(
	link ipld.Link,
	data []byte,
) error

// RunTraversal wraps a given loader with an interceptor that sends loaded
// blocks out to the network with the given response sender.
func RunTraversal(
	loader ipld.Loader,
	traverser ipldutil.Traverser,
	sendResponse ResponseSender) error {
	for {
		isComplete, err := traverser.IsComplete()
		if isComplete {
			return err
		}
		lnk, lnkCtx := traverser.CurrentRequest()
		result, err := loader(lnk, lnkCtx)
		var data []byte
		if err != nil {
			traverser.Error(traversal.SkipMe{})
		} else {
			blockBuffer, ok := result.(*bytes.Buffer)
			if !ok {
				blockBuffer = new(bytes.Buffer)
				_, err = io.Copy(blockBuffer, result)
			}
			if err != nil {
				traverser.Error(err)
			} else {
				data = blockBuffer.Bytes()
				err = traverser.Advance(blockBuffer)
				if err != nil {
					return err
				}
			}
		}
		err = sendResponse(lnk, data)
		if err != nil {
			return err
		}
	}
}
