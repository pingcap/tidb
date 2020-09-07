package aggfuncs_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
)

func (s *testSuite) TestMemRowNumber(c *C) {
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncRowNumber, mysql.TypeLonglong, 0, 0, 4,
			aggfuncs.DefPartialResult4RowNumberSize, defaultUpdateMemDeltaGens, false),
	}
	for _, test := range tests {
		s.testWindowMemFunc(c, test)
	}
}
