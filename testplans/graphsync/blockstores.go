package main

import (
	"errors"
	"fmt"
	"io"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-filestore"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	mh "github.com/multiformats/go-multihash"
)

type ClosableBlockstore interface {
	bstore.Blockstore
	io.Closer
}

var ErrNotFound = errors.New("not found")

func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// ReadWriteBlockstores tracks open ReadWrite CAR blockstores.
type ReadWriteBlockstores struct {
	mu     sync.RWMutex
	stores map[string]*blockstore.ReadWrite
}

func NewReadWriteBlockstores() *ReadWriteBlockstores {
	return &ReadWriteBlockstores{
		stores: make(map[string]*blockstore.ReadWrite),
	}
}

func (r *ReadWriteBlockstores) Get(key string) (*blockstore.ReadWrite, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if bs, ok := r.stores[key]; ok {
		return bs, nil
	}
	return nil, fmt.Errorf("could not get blockstore for key %s: %w", key, ErrNotFound)
}

func (r *ReadWriteBlockstores) GetOrOpen(key string, path string, rootCid cid.Cid) (*blockstore.ReadWrite, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if bs, ok := r.stores[key]; ok {
		return bs, nil
	}

	bs, err := blockstore.OpenReadWrite(path, []cid.Cid{rootCid}, blockstore.UseWholeCIDs(true))
	if err != nil {
		return nil, fmt.Errorf("failed to create read-write blockstore: %w", err)
	}
	r.stores[key] = bs
	return bs, nil
}

func (r *ReadWriteBlockstores) Untrack(key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if bs, ok := r.stores[key]; ok {
		// If the blockstore has already been finalized, calling Finalize again
		// will return an error. For our purposes it's simplest if Finalize is
		// idempotent so we just ignore any error.
		_ = bs.Finalize()
	}

	delete(r.stores, key)
	return nil
}

// ReadWriteFilestore opens the CAR in the specified path as as a read-write
// blockstore, and fronts it with a Filestore whose positional mappings are
// stored inside the CAR itself. It must be closed after done. Closing will
// finalize the CAR blockstore.
func ReadWriteFilestore(path string, roots ...cid.Cid) (ClosableBlockstore, error) {
	rw, err := blockstore.OpenReadWrite(path, roots,
		carv2.ZeroLengthSectionAsEOF(true),
		blockstore.UseWholeCIDs(true),
	)
	if err != nil {
		return nil, err
	}

	bs, err := FilestoreOf(rw)
	if err != nil {
		return nil, err
	}

	return &closableBlockstore{Blockstore: bs, closeFn: rw.Finalize}, nil
}

// FilestoreOf returns a FileManager/Filestore backed entirely by a
// blockstore without requiring a datastore. It achieves this by coercing the
// blockstore into a datastore. The resulting blockstore is suitable for usage
// with DagBuilderHelper with DagBuilderParams#NoCopy=true.
func FilestoreOf(bs bstore.Blockstore) (bstore.Blockstore, error) {
	coercer := &dsCoercer{bs}

	// the FileManager stores positional infos (positional mappings) in a
	// datastore, which in our case is the blockstore coerced into a datastore.
	//
	// Passing the root dir as a base path makes me uneasy, but these filestores
	// are only used locally.
	fm := filestore.NewFileManager(coercer, "/")
	fm.AllowFiles = true

	// the Filestore sifts leaves (PosInfos) from intermediate nodes. It writes
	// PosInfo leaves to the datastore (which in our case is the coerced
	// blockstore), and the intermediate nodes to the blockstore proper (since
	// they cannot be mapped to the file.
	fstore := filestore.NewFilestore(bs, fm)
	bs = bstore.NewIdStore(fstore)

	return bs, nil
}

var cidBuilder = cid.V1Builder{Codec: cid.Raw, MhType: mh.IDENTITY}

// dsCoercer coerces a Blockstore to present a datastore interface, apt for
// usage with the Filestore/FileManager. Only PosInfos will be written through
// this path.
type dsCoercer struct {
	bstore.Blockstore
}

var _ datastore.Batching = (*dsCoercer)(nil)

func (crcr *dsCoercer) Get(key datastore.Key) (value []byte, err error) {
	c, err := cidBuilder.Sum(key.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to create cid: %w", err)
	}

	blk, err := crcr.Blockstore.Get(c)
	if err != nil {
		return nil, fmt.Errorf("failed to get cid %s: %w", c, err)
	}
	return blk.RawData(), nil
}

func (crcr *dsCoercer) Put(key datastore.Key, value []byte) error {
	c, err := cidBuilder.Sum(key.Bytes())
	if err != nil {
		return fmt.Errorf("failed to create cid: %w", err)
	}
	blk, err := blocks.NewBlockWithCid(value, c)
	if err != nil {
		return fmt.Errorf("failed to create block: %w", err)
	}
	if err := crcr.Blockstore.Put(blk); err != nil {
		return fmt.Errorf("failed to put block: %w", err)
	}
	return nil
}

func (crcr *dsCoercer) Has(key datastore.Key) (exists bool, err error) {
	c, err := cidBuilder.Sum(key.Bytes())
	if err != nil {
		return false, fmt.Errorf("failed to create cid: %w", err)
	}
	return crcr.Blockstore.Has(c)
}

func (crcr *dsCoercer) Batch() (datastore.Batch, error) {
	return datastore.NewBasicBatch(crcr), nil
}

func (crcr *dsCoercer) GetSize(_ datastore.Key) (size int, err error) {
	return 0, errors.New("operation NOT supported: GetSize")
}

func (crcr *dsCoercer) Query(_ query.Query) (query.Results, error) {
	return nil, errors.New("operation NOT supported: Query")
}

func (crcr *dsCoercer) Delete(_ datastore.Key) error {
	return errors.New("operation NOT supported: Delete")
}

func (crcr *dsCoercer) Sync(_ datastore.Key) error {
	return errors.New("operation NOT supported: Sync")
}

func (crcr *dsCoercer) Close() error {
	return nil
}

type closableBlockstore struct {
	bstore.Blockstore
	closeFn func() error
}

func (c *closableBlockstore) Close() error {
	return c.closeFn()
}
