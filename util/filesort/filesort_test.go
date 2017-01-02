package filesort

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testFileSortSuite{})

type testFileSortSuite struct {
}

func (s *testFileSortSuite) TestFileSort(c *C) {
	defer testleak.AfterTest(c)()
	sc := new(variable.StatementContext)
	keySize := 1
	valSize := 1
	bufSize := 1
	byDesc := []bool{false}

	var (
		err    error
		fs     *FileSorter
		handle int64
	)

	fs, err = NewFileSorter(sc, keySize, valSize, bufSize, byDesc)
	c.Assert(err, IsNil)

	err = fs.Input([]types.Datum{types.NewDatum(4)}, []types.Datum{types.NewDatum(5)}, 6)
	c.Assert(err, IsNil)

	err = fs.Input([]types.Datum{types.NewDatum(1)}, []types.Datum{types.NewDatum(2)}, 3)
	c.Assert(err, IsNil)

	_, handle, err = fs.Output()
	c.Assert(err, IsNil)
	c.Assert(handle, Equals, int64(3))

	_, handle, err = fs.Output()
	c.Assert(err, IsNil)
	c.Assert(handle, Equals, int64(6))
}
