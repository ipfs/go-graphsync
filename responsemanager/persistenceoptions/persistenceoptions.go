package persistenceoptions

import (
	"errors"
	"sync"

	"github.com/ipld/go-ipld-prime"
)

// PersistenceOptions is a registry of loaders for persistence options
type PersistenceOptions struct {
	persistenceOptionsLk sync.RWMutex
	persistenceOptions   map[string]ipld.Loader
}

// New returns a new registry of persistence options
func New() *PersistenceOptions {
	return &PersistenceOptions{
		persistenceOptions: make(map[string]ipld.Loader),
	}
}

// Register registers a new loader for the response manager
func (po *PersistenceOptions) Register(name string, loader ipld.Loader) error {
	po.persistenceOptionsLk.Lock()
	defer po.persistenceOptionsLk.Unlock()
	_, ok := po.persistenceOptions[name]
	if ok {
		return errors.New("persistence option alreayd registered")
	}
	po.persistenceOptions[name] = loader
	return nil
}

// Unregister unregisters a loader for the response manager
func (po *PersistenceOptions) Unregister(name string) error {
	po.persistenceOptionsLk.Lock()
	defer po.persistenceOptionsLk.Unlock()
	_, ok := po.persistenceOptions[name]
	if !ok {
		return errors.New("persistence option is not registered")
	}
	delete(po.persistenceOptions, name)
	return nil
}

// GetLoader returns the loader for the named persistence option
func (po *PersistenceOptions) GetLoader(name string) (ipld.Loader, bool) {
	po.persistenceOptionsLk.RLock()
	defer po.persistenceOptionsLk.RUnlock()
	loader, ok := po.persistenceOptions[name]
	return loader, ok
}
