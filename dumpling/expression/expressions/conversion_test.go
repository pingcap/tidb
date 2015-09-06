package expressions

import (
	"errors"

	. "github.com/pingcap/check"
	mysql "github.com/pingcap/tidb/mysqldef"
)

var _ = Suite(&testConversionSuite{})

type testConversionSuite struct {
}

func (s *testConversionSuite) TestConversion(c *C) {
	expr := Conversion{
		Tp:  mysql.TypeLonglong,
		Val: Value{int64(1)},
	}

	v, err := expr.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(1))
	c.Assert(expr.IsStatic(), IsTrue)
	c.Assert(len(expr.String()), Greater, 0)

	_, err = expr.Clone()
	c.Assert(err, IsNil)

	// test error
	expr.Val = mockExpr{err: errors.New("must error")}
	_, err = expr.Clone()
	c.Assert(err, NotNil)

	_, err = expr.Eval(nil, nil)
	c.Assert(err, NotNil)
}
