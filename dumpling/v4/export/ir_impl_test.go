package export

import (
	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testIRImplSuite{})

type testIRImplSuite struct{}

type simpleRowReceiver struct {
	data string
}

func (s *simpleRowReceiver) BindAddress(arg []interface{}) {
	arg[0] = &s.data
}

func (s *simpleRowReceiver) ReportSize() uint64 {
	panic("not implement")
}

func (s *testIRImplSuite) TestRowIter(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	expectedRows := mock.NewRows([]string{"id"}).
		AddRow("1").
		AddRow("2").
		AddRow("3")
	mock.ExpectQuery("SELECT id from t").WillReturnRows(expectedRows)
	rows, err := db.Query("SELECT id from t")
	c.Assert(err, IsNil)

	iter := newRowIter(rows, 1)
	for i := 0; i < 100; i += 1 {
		c.Assert(iter.HasNext(), IsTrue)
	}
	res := &simpleRowReceiver{}
	c.Assert(iter.Next(res), IsNil)
	c.Assert(res.data, Equals, "1")
	c.Assert(iter.HasNext(), IsTrue)
	c.Assert(iter.HasNext(), IsTrue)
	c.Assert(iter.Next(res), IsNil)
	c.Assert(res.data, Equals, "2")
	c.Assert(iter.HasNext(), IsTrue)
	c.Assert(iter.Next(res), IsNil)
	c.Assert(res.data, Equals, "3")
	c.Assert(iter.HasNext(), IsFalse)
}
