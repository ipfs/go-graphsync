# go-graphsync

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://ipfs.io/)
[![](https://img.shields.io/badge/freenode-%23ipfs-blue.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23ipfs)
[![Coverage Status](https://codecov.io/gh/ipfs/go-graphsync/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-graphsync)
[![Travis CI](https://travis-ci.org/ipfs/go-graphsync.svg?branch=master)](https://travis-ci.org/ipfs/go-graphsync)

> An implementation of the [graphsync protocol](https://github.com/ipld/specs/blob/master/block-layer/graphsync/graphsync.md) in go!

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Background

[GraphSync](https://github.com/ipld/specs/blob/master/block-layer/graphsync/graphsync.md) is a protocol for synchronizing IPLD graphs among peers. It allows a host to make a single request to a remote peer for all of the results of traversing an [IPLD selector](https://github.com/ipld/specs/blob/master/block-layer/selectors/selectors.md) on the remote peer's local IPLD graph. 

`go-graphsync` provides an implementation of the Graphsync protocol in go.

### Go-IPLD-Prime

`go-graphsync` relies on `go-ipld-prime` to traverse IPLD Selectors in an IPLD graph. `go-ipld-prime` implements the [IPLD specification](https://github.com/ipld/specs) in go and is an alternative to older implementations such as `go-ipld-format` and `go-ipld-cbor`. In order to use `go-graphsync`, some understanding and use of `go-ipld-prime` concepts is necessary. 

If your existing library (i.e. `go-ipfs` or `go-filecoin`) uses these other older libraries, `go-graphsync` provide translation layers so you can largely use it without switching to `go-ipld-prime` across your codebase.

## Install

`go-graphsync` requires Go >= 1.11 and can be installed using Go modules

## Usage

### Initializing a GraphSync Exchange

```golang
import (
  graphsync "github.com/ipfs/go-graphsync"
  gsnet "github.com/ipfs/go-graphsync/network"
  gsbridge "github.com/ipfs/go-graphsync/ipldbridge"
  ipld "github.com/ipfs/go-ipld-prime"
)

var ctx context.Context
var host libp2p.Host
var loader ipld.Loader

network := gsnet.NewFromLibp2pHost(host)
ipldBridge := gsbridge.NewIPLDBridge()
exchange := graphsync.New(ctx, network, ipldBridge, loader)
```

Parameter Notes:

1. `context` is just the parent context for all of GraphSync
2. `network` is a network abstraction provided to Graphsync on top
of libp2p. This allows graphsync to be tested without the actual network
3. `ipldBridge` is an IPLD abstraction provided to Graphsync on top of  go-ipld-prime. This makes the graphsync library testable in isolation
4. `loader` is used to load blocks from content ids from the local block store. It's used when RESPONDING to requests from other clients. It should conform to the IPLD loader interface: https://github.com/ipld/go-ipld-prime/blob/master/linking.go

### Write A Loader From The Stuff You Know

Coming from a pre-`go-ipld-prime` world, you probably expect a link loading function signature to look like this:

```golang
type Cid2BlockFn func (lnk cid.Cid) (blocks.Block, error)
```

in `go-ipld-prime`, the signature for a link loader is as follows:

```golang
type Loader func(lnk Link, lnkCtx LinkContext) (io.Reader, error)
```

`go-ipld-prime` intentionally keeps its interfaces as abstract as possible to limit dependencies on other ipfs/filecoin specific packages. An IPLD Link is an abstraction for a CID, and IPLD expects io.Reader's rather than an actual block. IPLD provides a `cidLink` package for working with Links that use CIDs as the underlying data, and it's safe to assume that's the type in use if your code deals only with CIDs. Anyway, a conversion would look something like this:

```golang
import (
   ipld "github.com/ipld/go-ipld-prime"
   cidLink "github.com/ipld/go-ipld-prime/linking/cid"
)

func LoaderFromCid2BlockFn(cid2BlockFn Cid2BlockFn) ipld.Loader {
	return func(lnk ipld.Link, lnkCtx ipld.LinkContext) (io.Reader, error) {
		asCidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("Unsupported Link Type")
		}
		block, err := cid2BlockFn(asCidLink.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(block.RawData()), nil
	}
}
```

Alternatively, you can just call:

```golang
loader := graphsync.LoaderFromCid2BlockFn(cid2BlockFn)
```

### Calling Graphsync

```golang
var exchange graphsync.GraphSync
var ctx context.Context
var p peer.ID
var cidRootedSelector ipld.Node

var responseProgress <-chan graphsync.ResponseProgress
var errors <-chan error

responseProgress, errors = exchange.Request(ctx context.Context, p peer.ID, rootedSelector Node)
```

Paramater Notes:
1. `ctx` is the context for this request. To cancel an in progress request, cancel the context.
2. `p` is the peer you will send this request to
3. `rootedSelector` is the a go-ipld-prime node the specifies a rooted selector

### Building a path selector

A rooted selector is a `go-ipld-prime` node that follows the spec outlined here: https://github.com/ipld/specs/blob/master/block-layer/selectors/selectors.md

`go-ipld-prime` provides a series of builder interfaces for building this kind of structured data into a node. If your library simply wants to make a selector from CID and a path, represented by an array of strings, you could construct the node as follows:

```golang
import (
	ipld "github.com/ipld/go-ipld-prime"
	free "github.com/ipld/go-ipld-prime/impl/free"
	fluent "github.com/ipld/go-ipld-prime/fluent"
	cidLink "github.com/ipld/go-ipld-prime/linking/cid"
)

func SelectorSpecFromCidAndPath(lnk cid.Cid, pathSegments []string) (ipld.Node, error) {
	var node ipld.Node
	err := fluent.Recover(func() {
		builder := fluent.WrapNodeBuilder(free.NodeBuilder())
		node = builder.CreateMap(func (mb fluent.MapBuilder, knb fluent.NodeBuilder, vnb fluent.NodeBuilder) {
			mb.Insert(knb.CreateString("root"), vnb.CreateLink(cidLink.Link{lnk}))
			mb.Insert(knb.CreateString("selectors"), 
			vnb.CreateList(func (lb fluent.ListBuilder, vnb fluent.NodeBuilder) {
				for _, pathSegment := range pathSegments {
					lb.Append(CreateMap(
						func (mb fluent.MapBuilder, knb fluent.NodeBuilder, vnb fluent.NodeBuilder) {
							mb.Insert(knb.CreateString("selectPath"), vnb.CreateString(pathSegment))
						},
					))
				}
			}))
		});
	})
	if err != nil {
		return nil, err
	}
	return node, nil
}
```

Alternatively, just call:

```golang
rootedSelector := graphsync.SelectorSpecFromCidAndPath(lnk, pathSegments)
```

### Response Type

```golang

type ResponseProgress struct {
  Node      ipld.Node // a node which matched the graphsync query
  Path      ipld.Path // the path of that node relative to the traversal start
	LastBlock struct {  // LastBlock stores the Path and Link of the last block edge we had to load. 
		ipld.Path
		ipld.Link
	}
}

```

The above provides both immediate and relevant metadata for matching nodes in a traversal, and is very similar to the information provided by a local IPLD selector traversal in `go-ipld-prime`

## Compatibility: Block Requests

While the above is extremely useful if your library already uses `go-ipld-prime`, if your library uses an older version of go ipld libraries, working with these types of `go-ipld-prime` data may prove challenging.

To support these clients, Graphsync provides a compatibility version of the above function that returns just blocks that were traversed:

```golang
var blocksChan <-chan blocks.Block
var errors <-chan error

blocksChan, errors = exchange.GetBlocks(ctx context.Context, p peer.ID, rootedSelector Node)
```

This is provided as a transitional layer and `go-graphsync` may drop support for this format in the future.

## Contribute

PRs are welcome!

Small note: If editing the Readme, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

This library is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2019. Protocol Labs, Inc.
