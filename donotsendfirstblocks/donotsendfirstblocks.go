package donotsendfirstblocks

import (
	basicnode "github.com/ipld/go-ipld-prime/node/basic"

	"github.com/ipfs/go-graphsync/ipldutil"
)

// EncodeDoNotSendFirstBlocks returns encoded cbor data for the given number
// of blocks to skip
func EncodeDoNotSendFirstBlocks(skipBlockCount int64) ([]byte, error) {
	nb := basicnode.Prototype.Int.NewBuilder()
	err := nb.AssignInt(skipBlockCount)
	if err != nil {
		return nil, err
	}
	nd := nb.Build()
	return ipldutil.EncodeNode(nd)
}

// DecodeDoNotSendFirstBlocks returns the number of blocks to skip
func DecodeDoNotSendFirstBlocks(data []byte) (int64, error) {
	nd, err := ipldutil.DecodeNode(data)
	if err != nil {
		return 0, err
	}
	return nd.AsInt()
}
