package testbridge

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
)

// NewMockStore provides a loader and storer for the given in memory link -> byte data map
func NewMockStore(blocksWritten map[ipld.Link][]byte) (ipldbridge.Loader, ipldbridge.Storer) {
	var storeLk sync.RWMutex
	storer := func(lnkCtx ipldbridge.LinkContext) (io.Writer, ipldbridge.StoreCommitter, error) {
		var buffer bytes.Buffer
		committer := func(lnk ipld.Link) error {
			storeLk.Lock()
			blocksWritten[lnk] = buffer.Bytes()
			storeLk.Unlock()
			return nil
		}
		return &buffer, committer, nil
	}
	loader := func(lnk ipld.Link, lnkCtx ipldbridge.LinkContext) (io.Reader, error) {
		storeLk.RLock()
		data, ok := blocksWritten[lnk]
		storeLk.RUnlock()
		if ok {
			return bytes.NewReader(data), nil
		}
		return nil, fmt.Errorf("unable to load block")
	}

	return loader, storer
}
