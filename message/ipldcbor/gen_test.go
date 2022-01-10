//go:build bindnodegen
// +build bindnodegen

package ipldcbor

import (
	"fmt"
	"os"
	"testing"

	"github.com/ipld/go-ipld-prime/node/bindnode"
)

func TestGenerate(t *testing.T) {
	f, err := os.Create("types.go")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(f, "package ipldcbor\n\n")
	fmt.Fprintf(f, "import \"github.com/ipld/go-ipld-prime/datamodel\"\n\n")
	if err := bindnode.ProduceGoTypes(f, schemaTypeSystem); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
