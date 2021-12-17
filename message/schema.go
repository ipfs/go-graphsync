package message

import (
	_ "embed"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

//go:embed schema.ipldsch
var embedSchema []byte

var schemaTypeSystem *schema.TypeSystem

var Prototype struct {
	Message schema.TypedPrototype
}

func init() {
	ts, err := ipld.LoadSchemaBytes(embedSchema)
	if err != nil {
		panic(err)
	}
	schemaTypeSystem = ts

	Prototype.Message = bindnode.Prototype((*GraphSyncMessage)(nil), ts.TypeByName("GraphSyncMessage"))
}
