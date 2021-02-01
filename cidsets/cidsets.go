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
	lk sync.Mutex
	ds datastore.Batching
}

func NewCIDSet(ds datastore.Batching) *cidSet {
	return &cidSet{ds: ds}
}

// Insert a CID into the set.
// Returns true if the the CID was already in the set.
func (s *cidSet) Insert(c cid.Cid) (exists bool, err error) {
	s.lk.Lock()
	defer s.lk.Unlock()

	k := datastore.NewKey(c.String())
	has, err := s.ds.Has(k)
	if err != nil {
		return false, err
	}
	if has {
		return true, nil
	}
	return false, s.ds.Put(k, nil)
}

// Truncate removes all CIDs in the set
func (s *cidSet) Truncate() error {
	s.lk.Lock()
	defer s.lk.Unlock()

	res, err := s.ds.Query(query.Query{KeysOnly: true})
	if err != nil {
		return err
	}

	entries, err := res.Rest()
	if err != nil {
		return err
	}

	batched, err := s.ds.Batch()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		err := batched.Delete(datastore.NewKey(entry.Key))
		if err != nil {
			return err
		}
	}

	return batched.Commit()
}
