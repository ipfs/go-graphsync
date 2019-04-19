package testbridge

import (
	"fmt"

	ipld "github.com/ipld/go-ipld-prime"
)

type mockBuilder struct{}

func (m *mockBuilder) CreateMap() (ipld.MapBuilder, error)     { return nil, fmt.Errorf("Invalid") }
func (m *mockBuilder) AmendMap() (ipld.MapBuilder, error)      { return nil, fmt.Errorf("Invalid") }
func (m *mockBuilder) CreateList() (ipld.ListBuilder, error)   { return nil, fmt.Errorf("Invalid") }
func (m *mockBuilder) AmendList() (ipld.ListBuilder, error)    { return nil, fmt.Errorf("Invalid") }
func (m *mockBuilder) CreateNull() (ipld.Node, error)          { return nil, fmt.Errorf("Invalid") }
func (m *mockBuilder) CreateBool(bool) (ipld.Node, error)      { return nil, fmt.Errorf("Invalid") }
func (m *mockBuilder) CreateInt(int) (ipld.Node, error)        { return nil, fmt.Errorf("Invalid") }
func (m *mockBuilder) CreateFloat(float64) (ipld.Node, error)  { return nil, fmt.Errorf("Invalid") }
func (m *mockBuilder) CreateString(string) (ipld.Node, error)  { return nil, fmt.Errorf("Invalid") }
func (m *mockBuilder) CreateBytes([]byte) (ipld.Node, error)   { return nil, fmt.Errorf("Invalid") }
func (m *mockBuilder) CreateLink(ipld.Link) (ipld.Node, error) { return nil, fmt.Errorf("Invalid") }
