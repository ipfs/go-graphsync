package unverifiedblockstore

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/ipld/go-ipld-prime"

	"github.com/ipfs/go-graphsync/testutil"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func TestVerifyBlockPresent(t *testing.T) {
	blocksWritten := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blocksWritten)
	unverifiedBlockStore := New(storer)
	block := testutil.GenerateBlocksOfSize(1, 100)[0]
	reader, err := loader(cidlink.Link{Cid: block.Cid()}, ipld.LinkContext{})
	if reader != nil || err == nil {
		t.Fatal("block should not be loadable till it's verified and stored")
	}
	data, err := unverifiedBlockStore.VerifyBlock(cidlink.Link{Cid: block.Cid()})
	if data != nil || err == nil {
		t.Fatal("block should not be verifiable till it's added as an unverifiable block")
	}
	unverifiedBlockStore.AddUnverifiedBlock(cidlink.Link{Cid: block.Cid()}, block.RawData())
	reader, err = loader(cidlink.Link{Cid: block.Cid()}, ipld.LinkContext{})
	if reader != nil || err == nil {
		t.Fatal("block should not be loadable till it's verified and stored")
	}
	data, err = unverifiedBlockStore.VerifyBlock(cidlink.Link{Cid: block.Cid()})
	if !reflect.DeepEqual(data, block.RawData()) || err != nil {
		t.Fatal("block should be returned on verification if added")
	}
	reader, err = loader(cidlink.Link{Cid: block.Cid()}, ipld.LinkContext{})
	var buffer bytes.Buffer
	io.Copy(&buffer, reader)
	if !reflect.DeepEqual(buffer.Bytes(), block.RawData()) || err != nil {
		t.Fatal("block should be stored after verification and therefore loadable")
	}
	data, err = unverifiedBlockStore.VerifyBlock(cidlink.Link{Cid: block.Cid()})
	if data != nil || err == nil {
		t.Fatal("block cannot be verified twice")
	}
}
