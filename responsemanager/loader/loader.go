package loader

import (
	"bytes"
	"io"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
)

// ResponseSender sends responses over the network
type ResponseSender interface {
	SendResponse(
		requestID graphsync.RequestID,
		link ipld.Link,
		data []byte,
	)
}

// WrapLoader wraps a given loader with an interceptor that sends loaded
// blocks out to the network with the given response sender.
func WrapLoader(loader ipldbridge.Loader,
	requestID graphsync.RequestID,
	responseSender ResponseSender) ipldbridge.Loader {
	return func(lnk ipld.Link, lnkCtx ipldbridge.LinkContext) (io.Reader, error) {
		result, err := loader(lnk, lnkCtx)
		var data []byte
		var blockBuffer bytes.Buffer
		if err == nil {
			_, err = io.Copy(&blockBuffer, result)
			if err == nil {
				result = &blockBuffer
				data = blockBuffer.Bytes()
			}
		}
		responseSender.SendResponse(requestID, lnk, data)
		if data == nil {
			err = ipldbridge.ErrDoNotFollow()
		}
		return result, err
	}
}
