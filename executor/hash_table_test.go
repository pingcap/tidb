package executor

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/chunk"
)

func (s *pkgTestSuite) TestRowHashMap(c *C) {
	m := newRowHashMap()
	m.Put(1, chunk.RowPtr{ChkIdx: 1, RowIdx: 1})
	c.Check(m.Get(1), DeepEquals, []chunk.RowPtr{{1, 1}})

	rawData := map[uint64][]chunk.RowPtr{}
	for i := uint64(0); i < 10; i++ {
		for j := uint64(0); j < initialEntrySliceLen*i; j++ {
			rawData[i] = append(rawData[i], chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)})
		}
	}
	m = newRowHashMap()
	// put all rawData into m vertically
	for j := uint64(0); j < initialEntrySliceLen*9; j++ {
		for i := 9; i >= 0; i-- {
			i := uint64(i)
			if !(j < initialEntrySliceLen*i) {
				break
			}
			m.Put(i, rawData[i][j])
		}
	}
	// check
	totalCount := 0
	for i := uint64(0); i < 10; i++ {
		totalCount += len(rawData[i])
		c.Check(m.Get(i), DeepEquals, rawData[i])
	}
	c.Check(m.Len(), Equals, totalCount)
}
