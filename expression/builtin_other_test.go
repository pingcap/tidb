package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var bitCountCases = []struct {
	origin int64
	count  int64
}{
	{8, 1},
	{29, 4},
	{0, 0},
	{-1, 64},
}

func (s *testEvaluatorSuite) TestBitCount(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.BitCount]
	for _, test := range bitCountCases {
		in := types.NewDatum(test.origin)
		f, _ := fc.getFunction(datumsToConstants([]types.Datum{in}), s.ctx)
		count, err := f.eval(nil)
		c.Assert(err, IsNil)
		sc := new(variable.StatementContext)
		sc.IgnoreTruncate = true
		res, err := count.ToInt64(sc)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.count)
	}
}
