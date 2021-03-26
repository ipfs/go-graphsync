package main

import (
	"os/exec"

	"github.com/ipld/go-ipld-prime/schema"
	gengo "github.com/ipld/go-ipld-prime/schema/gen/go"
)

func main() {

	ts := schema.TypeSystem{}
	ts.Init()
	adjCfg := &gengo.AdjunctCfg{}

	pkgName := "chaintypes"

	ts.Accumulate(schema.SpawnLink("Link"))
	ts.Accumulate(schema.SpawnBytes("Bytes"))
	ts.Accumulate(schema.SpawnString("String"))
	ts.Accumulate(schema.SpawnList("Parents", "Link", false))
	ts.Accumulate(schema.SpawnList("Messages", "Bytes", false))
	ts.Accumulate(schema.SpawnStruct("Block",
		[]schema.StructField{
			schema.SpawnStructField("Parents", "Parents", false, false),
			schema.SpawnStructField("Messages", "Messages", false, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))
	gengo.Generate(".", pkgName, ts, adjCfg)
	_ = exec.Command("go", "fmt").Run()
}
