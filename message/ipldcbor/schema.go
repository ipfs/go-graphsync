package ipldcbor

//go:generate go test -run=Generate -vet=off -tags=bindnodegen

import (
	_ "embed"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/schema"
)

//go:embed schema.ipldsch
var embedSchema []byte

var schemaTypeSystem *schema.TypeSystem

func init() {
	ts, err := ipld.LoadSchemaBytes(embedSchema)
	if err != nil {
		panic(err)
	}
	schemaTypeSystem = ts
}
