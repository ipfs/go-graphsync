package testutil

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// NewTestStore provides a loader and storer for the given in memory link -> byte data map
func NewTestStore(blocksWritten map[ipld.Link][]byte) ipld.LinkSystem {
	var storeLk sync.RWMutex
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	lsys.StorageWriteOpener = func(lnkCtx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		var buffer bytes.Buffer
		committer := func(lnk ipld.Link) error {
			storeLk.Lock()
			blocksWritten[lnk] = buffer.Bytes()
			storeLk.Unlock()
			return nil
		}
		return &buffer, committer, nil
	}
	lsys.StorageReadOpener = func(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		storeLk.RLock()
		data, ok := blocksWritten[lnk]
		storeLk.RUnlock()
		if ok {
			return bytes.NewReader(data), nil
		}
		return nil, fmt.Errorf("unable to load block")
	}

	return lsys
}
