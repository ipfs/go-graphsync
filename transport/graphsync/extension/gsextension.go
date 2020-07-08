package extension

import (
	"bytes"

	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/ipfs/go-graphsync"
)

const (
	// ExtensionDataTransfer is the identifier for the data transfer extension to graphsync
	ExtensionDataTransfer = graphsync.ExtensionName("fil/data-transfer")
)

// ToExtensionData converts a message to a graphsync extension
func ToExtensionData(msg message.DataTransferMessage) (graphsync.ExtensionData, error) {
	buf := new(bytes.Buffer)
	err := msg.ToNet(buf)
	if err != nil {
		return graphsync.ExtensionData{}, err
	}
	return graphsync.ExtensionData{
		Name: ExtensionDataTransfer,
		Data: buf.Bytes(),
	}, nil
}

// GsExtended is a small interface used by getExtensionData
type GsExtended interface {
	Extension(name graphsync.ExtensionName) ([]byte, bool)
}

// GetTransferData unmarshals extension data.
// Returns:
//    * nil + nil if the extension is not found
//    * nil + error if the extendedData fails to unmarshal
//    * unmarshaled ExtensionDataTransferData + nil if all goes well
func GetTransferData(extendedData GsExtended) (message.DataTransferMessage, error) {
	data, ok := extendedData.Extension(ExtensionDataTransfer)
	if !ok {
		return nil, nil
	}

	reader := bytes.NewReader(data)
	return message.FromNet(reader)
}
