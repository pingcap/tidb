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
	bufSize := 3
	byDesc := []bool{false}
	fs := NewFileSorter(sc, keySize, valSize, bufSize, byDesc)

	fs.Input([]types.Datum{types.NewDatum(1)}, []types.Datum{types.NewDatum(2)}, 3)

	_, handle := fs.Output()
	c.Assert(handle, Equals, int64(3))
}
