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

	adjCfg := &gengo.AdjunctCfg{}
	pkgName := "chaintypes"

	f := openOrPanic("testchain_minima.go")
	gengo.EmitInternalEnums(pkgName, f)

	f = openOrPanic("testchain_gen.go")
	gengo.EmitFileHeader(pkgName, f)
	gengo.EmitEntireType(gengo.NewBytesReprBytesGenerator(pkgName, tBytes, adjCfg), f)
	gengo.EmitEntireType(gengo.NewLinkReprLinkGenerator(pkgName, tLink, adjCfg), f)
	gengo.EmitEntireType(gengo.NewStringReprStringGenerator(pkgName, tString, adjCfg), f)
	gengo.EmitEntireType(gengo.NewListReprListGenerator(pkgName, tParents, adjCfg), f)
	gengo.EmitEntireType(gengo.NewListReprListGenerator(pkgName, tMessages, adjCfg), f)
	gengo.EmitEntireType(gengo.NewStructReprMapGenerator(pkgName, tBlock, adjCfg), f)

}
