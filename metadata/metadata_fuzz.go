//+build gofuzz

package metadata

import (
	"bytes"
	"fmt"
	fleece "github.com/leastauthority/fleece/fuzzing"
	"reflect"
)

// FuzzDecodeMetadata fuzzes the decode metadata function
func FuzzDecodeEncodeMetadata(data []byte) int {
	metadata1, err := DecodeMetadata(data)
	if err != nil {
		return fleece.FuzzNormal
	}

	data1, err := EncodeMetadata(metadata1)
	if err != nil {
		panic(fmt.Errorf("unable to encode metadata: %w", err))
	}

	metadata2, err := DecodeMetadata(data1)
	if err != nil {
		panic(fmt.Errorf("unable to decode metadata: %w", err))
	}

	data2, err := EncodeMetadata(metadata2)
	if err != nil {
		panic(fmt.Errorf("unable to re-encode metadata: %w", err))
	}

	if !reflect.DeepEqual(metadata1, metadata2) {
		panic("deserialized metadata are not deeply equal!")
	}

	if bytes.Equal(data1, data2) {
		panic("serialized metadata are not deeply equal!")
	}
	return fleece.FuzzInteresting
}

