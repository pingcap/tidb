// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"sync/atomic"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/memory"
)

var _ = check.Suite(&rowContainerTestSuite{})

type rowContainerTestSuite struct{}

func (r *rowContainerTestSuite) TestNewRowContainer(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rc := NewRowContainer(fields, 1024)
	c.Assert(rc, check.NotNil)
	c.Assert(rc.AlreadySpilled(), check.Equals, false)
}

func (r *rowContainerTestSuite) TestSel(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	sz := 4
	rc := NewRowContainer(fields, sz)
	c.Assert(rc, check.NotNil)
	c.Assert(rc.AlreadySpilled(), check.Equals, false)
	n := 64
	chk := NewChunkWithCapacity(fields, sz)
	numRows := 0
	for i := 0; i < n-sz; i++ {
		chk.AppendInt64(0, int64(i))
		if chk.NumRows() == sz {
			chk.SetSel([]int{0, 2})
			numRows += 2
			err := rc.Add(chk)
			c.Assert(err, check.IsNil)
			chk = NewChunkWithCapacity(fields, sz)
		}
	}
	c.Assert(rc.NumChunks(), check.Equals, numRows/2)
	c.Assert(rc.NumRow(), check.Equals, numRows)
	for i := n - sz; i < n; i++ {
		chk.AppendInt64(0, int64(i))
	}
	chk.SetSel([]int{0, 1, 2})

	checkByIter := func(it Iterator) {
		i := 0
		for row := it.Begin(); row != it.End(); row = it.Next() {
			c.Assert(row.GetInt64(0), check.Equals, int64(i))
			if i < n-sz {
				i += 2
			} else {
				i++
			}
		}
		c.Assert(i, check.Equals, n-1)
	}
	checkByIter(NewMultiIterator(NewIterator4RowContainer(rc), NewIterator4Chunk(chk)))
	err := rc.spillToDisk()
	c.Assert(err, check.IsNil)
	c.Assert(rc.AlreadySpilled(), check.Equals, true)
	checkByIter(NewMultiIterator(NewIterator4RowContainer(rc), NewIterator4Chunk(chk)))
	err = rc.Close()
	c.Assert(err, check.IsNil)
	c.Assert(rc.memTracker.BytesConsumed(), check.Equals, int64(0))
	c.Assert(rc.memTracker.MaxConsumed(), check.Greater, int64(0))
}

func (r *rowContainerTestSuite) TestSpillAction(c *check.C) {
	sz := 4
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rc := NewRowContainer(fields, sz)
	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(chk.MemoryUsage() + 1)
	tracker.FallbackOldAndSetNewAction(rc.ActionSpill())

	c.Assert(atomic.LoadUint32(&rc.spilled), check.Equals, uint32(0))
	c.Assert(atomic.LoadUint32(&rc.exceeded), check.Equals, uint32(0))
	err = rc.Add(chk)
	c.Assert(err, check.IsNil)
	c.Assert(atomic.LoadUint32(&rc.spilled), check.Equals, uint32(0))
	c.Assert(atomic.LoadUint32(&rc.exceeded), check.Equals, uint32(0))
	c.Assert(rc.GetMemTracker().BytesConsumed(), check.Equals, chk.MemoryUsage())
	// The following line is erroneous, since chk is already handled by rc, Add it again causes duplicated memory usage account.
	// It is only for test of spill, do not double-add a chunk elsewhere.
	err = rc.Add(chk)
	c.Assert(err, check.IsNil)
	c.Assert(atomic.LoadUint32(&rc.exceeded), check.Equals, uint32(1))
	c.Assert(atomic.LoadUint32(&rc.spilled), check.Equals, uint32(1))
	err = rc.Reset()
	c.Assert(err, check.IsNil)
}
