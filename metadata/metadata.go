package metadata

import (
	"bytes"
	"fmt"

	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

// ItemBinarySizeWithoutCID is the fixed binary size of each link item
// added to the metadata, minus the CID itself which varies in length
const ItemBinarySizeWithoutCID uint64 = (1 + //  w.Write([]byte{162})
	1 + // cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("link")))
	4 + // io.WriteString(w, "link")
	2 + //  WriteMajorTypeHeaderBuf(buf, w, MajTag, 42)
	2 + // WriteMajorTypeHeaderBuf(buf, w, MajByteString, uint64(c.ByteLen()+1))
	1 + // w.Write(byteArrZero)
	// left out: c.WriteBytes(w)
	1 + // cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len("blockPresent")))
	13 + //io.WriteString(w, "blockPresent")
	1) // cbg.WriteBool(w, t.BlockPresent)
// == 26

// 32768 * ~66bytes per link = ~2MB -- seems like a reasonable upper bound to
// allocate
const maxLinks = 1 << 15

// Item is a single link traversed in a repsonse
type Item struct {
	Link         cid.Cid
	BlockPresent bool
}

// Metadata is information about metadata contained in a response, which can be
// serialized back and forth to bytes
type Metadata []Item

// DecodeMetadata assembles metadata from a raw byte array, first deserializing
// as a node and then assembling into a metadata struct.
func DecodeMetadata(data []byte) (Metadata, error) {
	var metadata Metadata
	r := bytes.NewReader(data)

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return nil, err
	}

	if extra > maxLinks {
		return nil, fmt.Errorf("t.Metadata: array too large (%d)", extra)
	}

	if maj != cbg.MajArray {
		return nil, fmt.Errorf("expected cbor array")
	}

	if extra > 0 {
		metadata = make(Metadata, extra)
	}

	for i := 0; i < int(extra); i++ {

		var v Item
		if err := v.UnmarshalCBOR(br); err != nil {
			return nil, err
		}

		metadata[i] = v
	}

	return metadata, nil
}

// EncodeMetadata encodes metadata to an IPLD node then serializes to raw bytes
func EncodeMetadata(entries Metadata) ([]byte, error) {
	w := new(bytes.Buffer)
	scratch := make([]byte, 9)
	if len(entries) > maxLinks {
		return nil, xerrors.Errorf("Slice value was too long")
	}
	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajArray, uint64(len(entries))); err != nil {
		return nil, err
	}
	for _, v := range entries {
		if err := v.MarshalCBOR(w); err != nil {
			return nil, err
		}
	}
	return w.Bytes(), nil
}
