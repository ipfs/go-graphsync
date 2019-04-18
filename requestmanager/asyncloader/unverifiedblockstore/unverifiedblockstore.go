package unverifiedblockstore

import (
	"fmt"

	"github.com/ipfs/go-graphsync/ipldbridge"
	ipld "github.com/ipld/go-ipld-prime"
)

// UnverifiedBlockStore holds an in memory cache of receied blocks from the network
// that have not been verified to be part of a traversal
type UnverifiedBlockStore struct {
	inMemoryBlocks map[ipld.Link][]byte
	storer         ipldbridge.Storer
}

// New initializes a new unverified store with the given storer function for writing
// to permaneant storage if the block is verified
func New(storer ipldbridge.Storer) *UnverifiedBlockStore {
	return &UnverifiedBlockStore{
		inMemoryBlocks: make(map[ipld.Link][]byte),
		storer:         storer,
	}
}

// AddUnverifiedBlock adds a new unverified block to the in memory cache as it
// comes in as part of a traversal.
func (ubs *UnverifiedBlockStore) AddUnverifiedBlock(lnk ipld.Link, data []byte) {
	ubs.inMemoryBlocks[lnk] = data
}

// PruneBlocks removes blocks from the unverified store without committing them,
// if the passed in function returns true for the given link
func (ubs *UnverifiedBlockStore) PruneBlocks(shouldPrune func(ipld.Link) bool) {
	for link := range ubs.inMemoryBlocks {
		if shouldPrune(link) {
			delete(ubs.inMemoryBlocks, link)
		}
	}
}

// VerifyBlock verifies the data for the given link as being part of a traversal,
// removes it from the unverified store, and writes it to permaneant storage.
func (ubs *UnverifiedBlockStore) VerifyBlock(lnk ipld.Link) ([]byte, error) {
	data, ok := ubs.inMemoryBlocks[lnk]
	if !ok {
		return nil, fmt.Errorf("Block not found")
	}
	delete(ubs.inMemoryBlocks, lnk)
	buffer, committer, err := ubs.storer(ipldbridge.LinkContext{})
	if err != nil {
		return nil, err
	}
	_, err = buffer.Write(data)
	if err != nil {
		return nil, err
	}
	err = committer(lnk)
	if err != nil {
		return nil, err
	}
	return data, nil
}
