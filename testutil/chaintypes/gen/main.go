package main

import (
	"os"

	"github.com/ipld/go-ipld-prime/schema"
	gengo "github.com/ipld/go-ipld-prime/schema/gen/go"
)

func main() {
	openOrPanic := func(filename string) *os.File {
		y, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		return y
	}

	tLink := schema.SpawnLink("Link")
	tBytes := schema.SpawnBytes("Bytes")
	tString := schema.SpawnString("String")
	tParents := schema.SpawnList("Parents", tLink, false)
	tMessages := schema.SpawnList("Messages", tBytes, false)
	tBlock := schema.SpawnStruct("Block",
		[]schema.StructField{
			schema.SpawnStructField("Parents", tParents, false, false),
			schema.SpawnStructField("Messages", tMessages, false, false),
		},
		schema.StructRepresentation_Map{},
	)

	f := openOrPanic("testchain_minima.go")
	gengo.EmitMinima("chaintypes", f)

	f = openOrPanic("testchain_gen.go")
	gengo.EmitFileHeader("chaintypes", f)
	gengo.EmitEntireType(gengo.NewGeneratorForKindBytes(tBytes), f)
	gengo.EmitEntireType(gengo.NewGeneratorForKindLink(tLink), f)
	gengo.EmitEntireType(gengo.NewGeneratorForKindString(tString), f)
	gengo.EmitEntireType(gengo.NewGeneratorForKindList(tParents), f)
	gengo.EmitEntireType(gengo.NewGeneratorForKindList(tMessages), f)
	gengo.EmitEntireType(gengo.NewGeneratorForKindStruct(tBlock), f)

}
