package filesort

import (
	"encoding/gob"
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testFileSortSuite{})

type testFileSortSuite struct {
}

type ComparableInt int

func (i ComparableInt) LessThan(other ComparableItem) bool {
	return i < other.(ComparableInt)
}

type ComparableIntGobHelper int

func (ComparableIntGobHelper) EncodeComparable(g *gob.Encoder, item ComparableItem) error {
	return g.Encode(item)
}
func (ComparableIntGobHelper) DecodeComparable(g *gob.Decoder) (ComparableItem, error) {
	var tmp ComparableInt
	err := g.Decode(&tmp)
	return ComparableInt(tmp), err
}

func (s *testFileSortSuite) TestFileSort(c *C) {
	defer testleak.AfterTest(c)()
	gob.Register(ComparableInt(0))

	r := rand.New(rand.NewSource(0))

	for memorySize := 1; memorySize < 5; memorySize++ {
		unsortedChan := make(chan ComparableItem)
		sortedChan := make(chan ComparableItem)
		go FileSort(memorySize, ComparableIntGobHelper(0), unsortedChan, sortedChan)

		for i := 0; i < 1049; i++ {
			in := ComparableItem(ComparableInt(r.Int()))
			unsortedChan <- in
		}
		close(unsortedChan)

		last := ComparableInt(-1)
		for iw := range sortedChan {
			i := iw.(ComparableInt)
			c.Assert(int(last), LessEqual, int(i))
			last = i
		}
	}
}
