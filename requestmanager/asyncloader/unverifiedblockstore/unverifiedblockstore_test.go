package unverifiedblockstore

import (
	"bytes"
	"io"
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/testutil"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func TestVerifyBlockPresent(t *testing.T) {
	blocksWritten := make(map[ipld.Link][]byte)
	loader, storer := testutil.NewTestStore(blocksWritten)
	unverifiedBlockStore := New(storer)
	block := testutil.GenerateBlocksOfSize(1, 100)[0]
	reader, err := loader(cidlink.Link{Cid: block.Cid()}, ipld.LinkContext{})
	require.Nil(t, reader)
	require.Error(t, err, "block should not be loadable till it's verified and stored")

	data, err := unverifiedBlockStore.VerifyBlock(cidlink.Link{Cid: block.Cid()})
	require.Nil(t, data)
	require.Error(t, err, "block should not be verifiable till it's added as an unverifiable block")

	unverifiedBlockStore.AddUnverifiedBlock(cidlink.Link{Cid: block.Cid()}, block.RawData())
	reader, err = loader(cidlink.Link{Cid: block.Cid()}, ipld.LinkContext{})
	require.Nil(t, reader)
	require.Error(t, err, "block should not be loadable till it's verified")

	data, err = unverifiedBlockStore.VerifyBlock(cidlink.Link{Cid: block.Cid()})
	require.NoError(t, err)
	require.Equal(t, data, block.RawData(), "block should be returned on verification if added")

	reader, err = loader(cidlink.Link{Cid: block.Cid()}, ipld.LinkContext{})
	require.NoError(t, err)
	var buffer bytes.Buffer
	_, err = io.Copy(&buffer, reader)
	require.NoError(t, err)
	require.Equal(t, buffer.Bytes(), block.RawData(), "block should be stored and loadable after verification")
	data, err = unverifiedBlockStore.VerifyBlock(cidlink.Link{Cid: block.Cid()})
	require.Nil(t, data)
	require.Error(t, err, "block cannot be verified twice")
}
