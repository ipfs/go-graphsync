package graphsyncimpl

import (
	"bytes"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-graphsync"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	// ExtensionDataTransfer is the identifier for the data transfer extension to graphsync
	ExtensionDataTransfer = graphsync.ExtensionName("fil/data-transfer")
)

//go:generate cbor-gen-for ExtensionDataTransferData

// ExtensionDataTransferData is the extension data for
// the graphsync extension.
type ExtensionDataTransferData struct {
	TransferID uint64
	Initiator  peer.ID
	IsPull     bool
}

// GetChannelID gets the channelID for this extension, given the peers on either side
func (e ExtensionDataTransferData) GetChannelID() datatransfer.ChannelID {
	return datatransfer.ChannelID{Initiator: e.Initiator, ID: datatransfer.TransferID(e.TransferID)}
}

func newTransferData(transferID datatransfer.TransferID, initiator peer.ID, isPull bool) ExtensionDataTransferData {
	return ExtensionDataTransferData{
		TransferID: uint64(transferID),
		Initiator:  initiator,
		IsPull:     isPull,
	}
}

// gsExtended is a small interface used by getExtensionData
type gsExtended interface {
	Extension(name graphsync.ExtensionName) ([]byte, bool)
}

// getExtensionData unmarshals extension data.
// Returns:
//    * nil + nil if the extension is not found
//    * nil + error if the extendedData fails to unmarshal
//    * unmarshaled ExtensionDataTransferData + nil if all goes well
func getExtensionData(extendedData gsExtended) (*ExtensionDataTransferData, error) {
	data, ok := extendedData.Extension(ExtensionDataTransfer)
	if !ok {
		return nil, nil
	}
	var extStruct ExtensionDataTransferData

	reader := bytes.NewReader(data)
	if err := extStruct.UnmarshalCBOR(reader); err != nil {
		return nil, err
	}
	return &extStruct, nil
}
