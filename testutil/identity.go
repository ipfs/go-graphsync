package testutil

import (
	"context"
	"fmt"
	"io"
	"testing"

	_ "github.com/ipld/go-ipld-prime/codec/raw"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// roughly duplicated in github.com/ipld/go-trustless-utils/testutil

type TestIdentityDAG struct {
	t      testing.TB
	loader ipld.BlockReadOpener

	RootLink        ipld.Link
	AllLinks        []ipld.Link
	AllLinksNoIdent []ipld.Link
}

/* ugly, but it makes a DAG with paths that look like this but doesn't involved dag-pb or unixfs */
var identityDagLinkPaths = []string{
	"",
	"a/!foo",
	"a/b/!bar",
	"a/b/c/!baz", // identity
	"a/b/c/!baz/identity jump",
	"a/b/c/!baz/identity jump/these are my children/blip",
	"a/b/c/!baz/identity jump/these are my children/bloop",
	"a/b/c/!baz/identity jump/these are my children/bloop/  leaf  ",
	"a/b/c/!baz/identity jump/these are my children/blop",
	"a/b/c/!baz/identity jump/these are my children/leaf",
	"a/b/c/d/!leaf",
}

func SetupIdentityDAG(
	ctx context.Context,
	t testing.TB,
	lsys ipld.LinkSystem,
) *TestIdentityDAG {
	allLinks := make([]ipld.Link, 0)
	allLinksNoIdent := make([]ipld.Link, 0)
	store := func(lp datamodel.LinkPrototype, n datamodel.Node) datamodel.Link {
		l, err := lsys.Store(linking.LinkContext{}, lp, n)
		require.NoError(t, err)
		allLinks = append(allLinks, l)
		allLinksNoIdent = append(allLinksNoIdent, l)
		return l
	}

	rootLeaf := store(rawlp, basicnode.NewBytes([]byte("leaf node in the root")))
	bazLeaf := store(rawlp, basicnode.NewBytes([]byte("leaf node in baz")))
	blop := store(djlp, must(qp.BuildList(basicnode.Prototype.Any, -1, func(la datamodel.ListAssembler) {
		qp.ListEntry(la, qp.Int(100))
		qp.ListEntry(la, qp.Int(200))
		qp.ListEntry(la, qp.Int(300))
	}))(t))
	bloopLeaf := store(rawlp, basicnode.NewBytes([]byte("leaf node in bloop")))
	bloop := store(djlp, must(qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "desc", qp.List(-1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.String("this"))
			qp.ListEntry(la, qp.String("is"))
			qp.ListEntry(la, qp.String("bloop"))
		}))
		qp.MapEntry(ma, "  leaf  ", qp.Link(bloopLeaf))
	}))(t))
	blip := store(djlp, basicnode.NewString("blip!"))
	baz := store(djlp, must(qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "desc", qp.List(-1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.String("this"))
			qp.ListEntry(la, qp.String("is"))
			qp.ListEntry(la, qp.String("baz"))
		}))
		qp.MapEntry(ma, "these are my children", qp.Map(-1, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "blip", qp.Link(blip))
			qp.MapEntry(ma, "bloop", qp.Link(bloop))
			qp.MapEntry(ma, "blop", qp.Link(blop))
			qp.MapEntry(ma, "leaf", qp.Link(bazLeaf))
		}))
	}))(t))
	var bazIdent datamodel.Link
	{
		// not stored, shouldn't count as a block
		ident := must(qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "identity jump", qp.Link(baz))
		}))(t)
		identBytes := must(ipld.Encode(ident, dagjson.Encode))(t)
		mh := must(multihash.Sum(identBytes, multihash.IDENTITY, len(identBytes)))(t)
		bazIdent = cidlink.Link{Cid: cid.NewCidV1(cid.DagJSON, mh)}
		w, wc, err := lsys.StorageWriteOpener(linking.LinkContext{})
		require.NoError(t, err)
		w.Write(identBytes)
		require.NoError(t, wc(bazIdent))
		allLinks = append(allLinks, bazIdent)
	}
	bar := store(djlp, basicnode.NewInt(2020202020202020))
	foo := store(djlp, basicnode.NewInt(1010101010101010))
	root := store(djlp, must(qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "a", qp.Map(-1, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "b", qp.Map(-1, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "c", qp.Map(-1, func(ma datamodel.MapAssembler) {
					qp.MapEntry(ma, "d", qp.Map(-1, func(ma datamodel.MapAssembler) {
						qp.MapEntry(ma, "!leaf", qp.Link(rootLeaf))
					}))
					qp.MapEntry(ma, "!baz", qp.Link(bazIdent))
				}))
				qp.MapEntry(ma, "!bar", qp.Link(bar))
			}))
			qp.MapEntry(ma, "!foo", qp.Link(foo))
		}))
	}))(t))

	return &TestIdentityDAG{
		t:               t,
		loader:          lsys.StorageReadOpener,
		RootLink:        root,
		AllLinks:        reverse(allLinks), // TODO: slices.Reverse post 1.21
		AllLinksNoIdent: reverse(allLinksNoIdent),
	}
}

func (tid *TestIdentityDAG) AllBlocks() []blocks.Block {
	blks := make([]blocks.Block, len(tid.AllLinks))
	for ii, link := range tid.AllLinks {
		reader, err := tid.loader(ipld.LinkContext{}, link)
		require.NoError(tid.t, err)
		data, err := io.ReadAll(reader)
		require.NoError(tid.t, err)
		blk, err := blocks.NewBlockWithCid(data, link.(cidlink.Link).Cid)
		require.NoError(tid.t, err)
		blks[ii] = blk
	}
	return blks
}

// VerifyWholeDAGWithTypes verifies the given response channel returns the expected responses for the whole DAG
// and that the types in the response are the expected types for the DAG
func (tid *TestIdentityDAG) VerifyWholeDAG(ctx context.Context, responseChan <-chan graphsync.ResponseProgress) {
	responses := CollectResponses(ctx, tid.t, responseChan)
	tid.checkResponses(responses)
}

func (tid *TestIdentityDAG) checkResponses(responses []graphsync.ResponseProgress) {
	var pathIndex int
	for ii, response := range responses {
		// only check the paths that have links, assume the rest are just describing
		// the non-link nodes of the DAG
		if response.Path.String() == identityDagLinkPaths[pathIndex] {
			if response.LastBlock.Link != nil {
				expectedLink := tid.AllLinks[pathIndex]
				require.Equal(tid.t, expectedLink.String(), response.LastBlock.Link.String(), fmt.Sprintf("response %d has correct link (%d)", ii, pathIndex))
			}
			pathIndex++
		}
	}
	require.Equal(tid.t, len(identityDagLinkPaths), pathIndex, "traverses all nodes")
}

func must[T any](v T, err error) func(t testing.TB) T {
	return func(t testing.TB) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

var djlp = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.DagJSON,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}

var rawlp = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}

func reverse[S ~[]E, E any](s S) S {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}
