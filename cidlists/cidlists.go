package cidlists

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

// CIDLists maintains files that contain a list of CIDs received for different data transfers
type CIDLists interface {
	CreateList(chid datatransfer.ChannelID, initalCids []cid.Cid) error
	AppendList(chid datatransfer.ChannelID, c cid.Cid) error
	ReadList(chid datatransfer.ChannelID) ([]cid.Cid, error)
	DeleteList(chid datatransfer.ChannelID) error
}

type cidLists struct {
	baseDir string
}

// NewCIDLists initializes a new set of cid lists in a given directory
func NewCIDLists(baseDir string) (CIDLists, error) {
	base := filepath.Clean(string(baseDir))
	info, err := os.Stat(string(base))
	if err != nil {
		return nil, fmt.Errorf("error getting %s info: %s", base, err.Error())
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", base)
	}
	return &cidLists{
		baseDir: base,
	}, nil
}

// CreateList initializes a new CID list with the given initial cids (or can be empty) for a data transfer channel
func (cl *cidLists) CreateList(chid datatransfer.ChannelID, initialCids []cid.Cid) (err error) {
	f, err := os.Create(transferFilename(cl.baseDir, chid))
	if err != nil {
		return err
	}
	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()
	for _, c := range initialCids {
		err := cbg.WriteCid(f, c)
		if err != nil {
			return err
		}
	}
	return nil
}

// AppendList appends a single CID to the list for a given data transfer channel
func (cl *cidLists) AppendList(chid datatransfer.ChannelID, c cid.Cid) (err error) {
	f, err := os.OpenFile(transferFilename(cl.baseDir, chid), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()
	return cbg.WriteCid(f, c)
}

// ReadList reads an on disk list of cids for the given data transfer channel
func (cl *cidLists) ReadList(chid datatransfer.ChannelID) (cids []cid.Cid, err error) {
	f, err := os.Open(transferFilename(cl.baseDir, chid))
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()
	var receivedCids []cid.Cid
	for {
		c, err := cbg.ReadCid(f)
		if err != nil {
			if err == io.EOF {
				return receivedCids, nil
			}
			return nil, err
		}
		receivedCids = append(receivedCids, c)
	}
}

// DeleteList deletes the list for the given data transfer channel
func (cl *cidLists) DeleteList(chid datatransfer.ChannelID) error {
	return os.Remove(transferFilename(cl.baseDir, chid))
}

func transferFilename(baseDir string, chid datatransfer.ChannelID) string {
	filename := fmt.Sprintf("%d-%s-%s", chid.ID, chid.Initiator, chid.Responder)
	return filepath.Join(baseDir, filename)
}
