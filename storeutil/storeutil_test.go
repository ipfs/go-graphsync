package storeutil

import (
	"io/ioutil"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipfs/go-graphsync/testutil"
)

func TestLoader(t *testing.T) {
	store := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))
	blk := testutil.GenerateBlocksOfSize(1, 1000)[0]
	err := store.Put(blk)
	if err != nil {
		t.Fatal("Unable to put block to store")
	}
	loader := LoaderForBlockstore(store)
	data, err := loader(cidlink.Link{Cid: blk.Cid()}, ipld.LinkContext{})
	if err != nil {
		t.Fatal("Unable to load block with loader")
	}
	bytes, err := ioutil.ReadAll(data)
	if err != nil {
		t.Fatal("Unable to read bytes from reader returned by loader")
	}
	returnedBlock := blocks.NewBlock(bytes)
	if returnedBlock.Cid() != blk.Cid() {
		t.Fatal("Did not return correct block with loader")
	}
}

func TestStorer(t *testing.T) {
	store := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))
	blk := testutil.GenerateBlocksOfSize(1, 1000)[0]
	storer := StorerForBlockstore(store)
	buffer, commit, err := storer(ipld.LinkContext{})
	if err != nil {
		t.Fatal("Unable to setup buffer")
	}
	_, err = buffer.Write(blk.RawData())
	if err != nil {
		t.Fatal("Unable to write data to buffer")
	}
	err = commit(cidlink.Link{Cid: blk.Cid()})
	if err != nil {
		t.Fatal("Unable to commit with storer function")
	}
	_, err = store.Get(blk.Cid())
	if err != nil {
		t.Fatal("Block not written to store")
	}
}
