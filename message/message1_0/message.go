package message1_0

import (
	"io"

	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

// NewTransferRequest creates a transfer request for the 1_0 Data Transfer Protocol.
func NewTransferRequest(bcid *cid.Cid, typ uint64, paus, part, pull bool, stor, vouch *cbg.Deferred,
	vtyp datatransfer.TypeIdentifier, xferId uint64) datatransfer.Request {
	return &transferRequest{
		BCid:   bcid,
		Type:   typ,
		Paus:   paus,
		Part:   part,
		Pull:   pull,
		Stor:   stor,
		Vouch:  vouch,
		VTyp:   vtyp,
		XferID: xferId,
	}
}

// NewTransferRequest creates a transfer response for the 1_0 Data Transfer Protocol.
func NewTransferResponse(typ uint64, acpt bool, paus bool, xferId uint64, vRes *cbg.Deferred, vtyp datatransfer.TypeIdentifier) datatransfer.Response {
	return &transferResponse{
		Type:   typ,
		Acpt:   acpt,
		Paus:   paus,
		XferID: xferId,
		VRes:   vRes,
		VTyp:   vtyp,
	}
}

// FromNet can read a network stream to deserialize a GraphSyncMessage
func FromNet(r io.Reader) (datatransfer.Message, error) {
	tresp := transferMessage{}
	err := tresp.UnmarshalCBOR(r)
	if err != nil {
		return nil, err
	}

	if (tresp.IsRequest() && tresp.Request == nil) || (!tresp.IsRequest() && tresp.Response == nil) {
		return nil, xerrors.Errorf("invalid/malformed message")
	}

	if tresp.IsRequest() {
		return tresp.Request, nil
	}
	return tresp.Response, nil
}
