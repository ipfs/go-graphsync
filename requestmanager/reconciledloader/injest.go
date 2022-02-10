package reconciledloader

import (
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"go.opentelemetry.io/otel/trace"
)

func (rl *ReconciledLoader) IngestResponse(md graphsync.LinkMetadata, traceLink trace.Link, blocks map[cid.Cid][]byte) {
	if md.Length() == 0 {
		return
	}
	duplicates := make(map[cid.Cid]struct{}, md.Length())
	items := make([]*remotedLinkedItem, 0, md.Length())
	md.Iterate(func(link cid.Cid, action graphsync.LinkAction) {
		newItem := newRemote()
		newItem.link = link
		newItem.action = action
		_, isDuplicate := duplicates[link]
		if action == graphsync.LinkActionPresent && !isDuplicate {
			duplicates[link] = struct{}{}
			newItem.block = blocks[link]
		}
		newItem.traceLink = traceLink
		items = append(items, newItem)
	})
	rl.remoteQueue.queue(items)
}
