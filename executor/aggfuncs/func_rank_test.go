package aggfuncs_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"

	"github.com/pingcap/tidb/executor/aggfuncs"
)

func (s *testSuite) TestMemRank(c *C) {
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncRank, mysql.TypeLonglong, 0, 1, 1,
			aggfuncs.DefPartialResult4RankSize, rowMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncRank, mysql.TypeLonglong, 0, 3, 0,
			aggfuncs.DefPartialResult4RankSize, rowMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncRank, mysql.TypeLonglong, 0, 4, 1,
			aggfuncs.DefPartialResult4RankSize, rowMemDeltaGens),
	}
	for _, test := range tests {
		s.testWindowAggMemFunc(c, test)
	}
}
