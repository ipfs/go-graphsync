package runtraversal

import (
	"bytes"
	"io"

	logging "github.com/ipfs/go-log/v2"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal"

	"github.com/ipfs/go-graphsync/ipldutil"
)

var logger = logging.Logger("gs-traversal")

type errorString string

func (e errorString) Error() string {
	return string(e)
}

// ErrFirstBlockLoad indicates the traversal was unable to load the very first block in the traversal
const ErrFirstBlockLoad = errorString("Unable to load first block")

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
			if err != nil {
				logger.Errorf("traversal completion check failed, nBlocksRead=%d, err=%s", traverser.NBlocksTraversed(), err)
				if (traverser.NBlocksTraversed() == 0 && err == traversal.SkipMe{}) {
					return ErrFirstBlockLoad
				}
			} else {
				logger.Debugf("traversal completed successfully, nBlocksRead=%d", traverser.NBlocksTraversed())
			}
			return err
		}
		lnk, lnkCtx := traverser.CurrentRequest()
		logger.Debugf("will load link=%s", lnk)
		result, err := loader(lnk, lnkCtx)
		var data []byte
		if err != nil {
			logger.Errorf("failed to load link=%s, nBlocksRead=%d, err=%s", lnk, traverser.NBlocksTraversed(), err)
			traverser.Error(traversal.SkipMe{})
		} else {
			blockBuffer, ok := result.(*bytes.Buffer)
			if !ok {
				blockBuffer = new(bytes.Buffer)
				_, err = io.Copy(blockBuffer, result)
			}
			if err != nil {
				logger.Errorf("failed to write to buffer, link=%s, nBlocksRead=%d, err=%s", lnk, traverser.NBlocksTraversed(), err)
				traverser.Error(err)
			} else {
				data = blockBuffer.Bytes()
				err = traverser.Advance(blockBuffer)
				if err != nil {
					logger.Errorf("failed to advance traversal, link=%s, nBlocksRead=%d, err=%s", lnk, traverser.NBlocksTraversed(), err)
					return err
				}
			}
			logger.Debugf("successfully loaded link=%s, nBlocksRead=%d", lnk, traverser.NBlocksTraversed())
		}
		err = sendResponse(lnk, data)
		if err != nil {
			return err
		}
	}
}
