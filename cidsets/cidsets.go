package cidsets

import (
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
)

// SetID is a unique ID for a CID set
type SetID string

// CIDSetManager keeps track of several CID sets, by SetID
type CIDSetManager struct {
	ds   datastore.Datastore
	lk   sync.Mutex
	sets map[SetID]*cidSet
}

func NewCIDSetManager(ds datastore.Datastore) *CIDSetManager {
	return &CIDSetManager{ds: ds, sets: make(map[SetID]*cidSet)}
}

// InsertSetCID inserts a CID into a CID set.
// Returns true if the set already contained the CID.
func (mgr *CIDSetManager) InsertSetCID(sid SetID, c cid.Cid) (exists bool, err error) {
	return mgr.getSet(sid).Insert(c)
}

// SetToArray gets the set as an array of CIDs
func (mgr *CIDSetManager) SetToArray(sid SetID) ([]cid.Cid, error) {
	return mgr.getSet(sid).ToArray()
}

// SetLen gets the number of CIDs in the set
func (mgr *CIDSetManager) SetLen(sid SetID) (int, error) {
	return mgr.getSet(sid).Len()
}

// DeleteSet deletes a CID set
func (mgr *CIDSetManager) DeleteSet(sid SetID) error {
	return mgr.getSet(sid).Truncate()
}

// getSet gets the cidSet for the given SetID
func (mgr *CIDSetManager) getSet(sid SetID) *cidSet {
	mgr.lk.Lock()
	defer mgr.lk.Unlock()

	s, ok := mgr.sets[sid]
	if !ok {
		s = NewCIDSet(mgr.getSetDS(sid))
		mgr.sets[sid] = s
	}
	return s
}

// getSetDS gets the wrapped datastore for the given SetID
func (mgr *CIDSetManager) getSetDS(sid SetID) datastore.Batching {
	setDSKey := datastore.NewKey(string(sid) + "/cids")
	return namespace.Wrap(mgr.ds, setDSKey)
}

// cidSet persists a set of CIDs
type cidSet struct {
	lk  sync.Mutex
	ds  datastore.Batching
	len int // cached length of set, starts at -1
}

func NewCIDSet(ds datastore.Batching) *cidSet {
	return &cidSet{ds: ds, len: -1}
}

// Insert a CID into the set.
// Returns true if the the CID was already in the set.
func (s *cidSet) Insert(c cid.Cid) (exists bool, err error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	// Check if the key is in the set already
	k := datastore.NewKey(c.String())
	has, err := s.ds.Has(k)
	if err != nil {
		return false, err
	}
	if has {
		// Already in the set, just return true
		return true, nil
	}

	// Get the length of the set
	len, err := s.unlockedLen()
	if err != nil {
		return false, err
	}

	// Add the new CID to the set
	err = s.ds.Put(k, nil)
	if err != nil {
		return false, err
	}

	// Increment the cached length of the set
	s.len = len + 1

	return false, nil
}

// Returns the number of CIDs in the set
func (s *cidSet) Len() (int, error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	return s.unlockedLen()
}

func (s *cidSet) unlockedLen() (int, error) {
	// If the length is already cached, return it
	if s.len >= 0 {
		return s.len, nil
	}

	// Query the datastore for all keys
	res, err := s.ds.Query(query.Query{KeysOnly: true})
	if err != nil {
		return 0, err
	}

	entries, err := res.Rest()
	if err != nil {
		return 0, err
	}

	// Cache the length of the set
	s.len = len(entries)

	return s.len, nil
}

// Get all cids in the set as an array
func (s *cidSet) ToArray() ([]cid.Cid, error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	res, err := s.ds.Query(query.Query{KeysOnly: true})
	if err != nil {
		return nil, err
	}

	entries, err := res.Rest()
	if err != nil {
		return nil, err
	}

	cids := make([]cid.Cid, 0, len(entries))
	for _, entry := range entries {
		// When we create a datastore Key, a "/" is automatically pre-pended,
		// so here we need to remove the preceding "/" before parsing as a CID
		k := entry.Key
		if string(k[0]) == "/" {
			k = k[1:]
		}

		c, err := cid.Parse(k)
		if err != nil {
			return nil, err
		}
		cids = append(cids, c)
	}
	return cids, nil
}

// Truncate removes all CIDs in the set
func (s *cidSet) Truncate() error {
	s.lk.Lock()
	defer s.lk.Unlock()

	// Get all keys in the datastore
	res, err := s.ds.Query(query.Query{KeysOnly: true})
	if err != nil {
		return err
	}

	entries, err := res.Rest()
	if err != nil {
		return err
	}

	// Create a batch to perform all deletes as one operation
	batched, err := s.ds.Batch()
	if err != nil {
		return err
	}

	// Add delete operations for each key to the batch
	for _, entry := range entries {
		err := batched.Delete(datastore.NewKey(entry.Key))
		if err != nil {
			return err
		}
	}

	// Commit the batch
	err = batched.Commit()
	if err != nil {
		return err
	}

	// Set the cached length of the set to zero
	s.len = 0

	return nil
}
