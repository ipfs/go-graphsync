//+build gofuzz

package message

import (
	"bytes"
	"fmt"
	"reflect"

	gofuzz "github.com/google/gofuzz"
	fleece "github.com/leastauthority/fleece/fuzzing"
)

func FuzzFromNetToNet(data []byte) int {
	buf1 := bytes.NewBuffer(data)
	msg1, err := FromNet(buf1)
	if err != nil {
		return fleece.FuzzNormal
	}

	buf2 := new(bytes.Buffer)
	if err := msg1.ToNet(buf2); err != nil {
		//return fleece.FuzzNormal
		panic(fmt.Errorf("unable to serialize message: %w", err))
	}

	msg2, err := FromNet(buf2)
	if err != nil {
		panic(fmt.Errorf("unable to deserialize message: %w", err))
	}

	if !reflect.DeepEqual(msg1, msg2) {
		panic("deserialized messages are not deeply equal!")
	}
	return fleece.FuzzInteresting
}

func FuzzFromNetToNetStructural(data []byte) int {
	msg1 := New()
	f := gofuzz.NewFromGoFuzz(data).NilChance(0).NumElements(3, 20)
	f.Fuzz(&msg1)

	buf1 := bytes.NewBuffer(data)
	if err := msg1.ToNet(buf1); err != nil {
		return fleece.FuzzNormal
	}

	msg2, err := FromNet(buf1)
	if err != nil {
		panic(fmt.Errorf("unable to deserialize message: %w", err))
	}

	buf2 := new(bytes.Buffer)
	if err := msg2.ToNet(buf2); err != nil {
		panic(fmt.Errorf("unable to serialize message: %w", err))
	}

	if !reflect.DeepEqual(msg1, msg2) {
		panic("deserialized messages are not deeply equal!")
	}

	if bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		panic("serialized messages are not equal!")
	}
	return fleece.FuzzInteresting
}
