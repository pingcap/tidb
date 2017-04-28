package ddl

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
)

type testFieldCaseSuite struct {
	colDefs []*ast.ColumnDef
}

func (s *testFieldCaseSuite) SetUpSuite(c *C) {
	var fields = []string{"field", "Field"}

	for _, name := range fields {
		colDef := &ast.ColumnDef{
			Name: &ast.ColumnName{
				Schema: model.NewCIStr("TestSchema"),
				Table:  model.NewCIStr("TestTable"),
				Name:   model.NewCIStr(name),
			},
		}
		s.colDefs = append(s.colDefs, colDef)
	}
}

func (s *testFieldCaseSuite) TestFieldCase(c *C) {
	c.Assert(checkDuplicateColumn(s.colDefs) != nil, Matches, true)
}
