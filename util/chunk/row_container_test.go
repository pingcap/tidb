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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
)

var _ = check.Suite(&rowContainerTestSuite{})

type rowContainerTestSuite struct{}

func (r *rowContainerTestSuite) TestNewRowContainer(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	rc := NewRowContainer(fields, 1024)
	c.Assert(rc, check.NotNil)
	c.Assert(rc.AlreadySpilled(), check.Equals, false)
}

func (r *rowContainerTestSuite) TestAppendRow(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	sz := 4
	rc := NewRowContainer(fields, sz)
	c.Assert(rc, check.NotNil)
	c.Assert(rc.AlreadySpilled(), check.Equals, false)
	n := 64
	chk := NewChunkWithCapacity(fields, n)
	for i := 0; i < n; i++ {
		chk.AppendInt64(0, int64(i))
	}
	for i := 0; i < n; i++ {
		_, err := rc.AppendRow(chk.GetRow(i))
		c.Assert(err, check.IsNil)
	}
	c.Assert(rc.NumChunks(), check.Equals, n/sz)
	err := rc.spillToDisk()
	c.Assert(err, check.IsNil)
	c.Assert(rc.AlreadySpilled(), check.Equals, true)
	it := NewIterator4RowContainer(rc)
	row := it.Begin()
	for i := 0; i < n; i++ {
		c.Assert(row.GetInt64(0), check.Equals, int64(i))
		row = it.Next()
	}
	err = rc.Close()
	c.Assert(err, check.IsNil)
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
	err = rc.Add(chk)
	c.Assert(err, check.IsNil)
	c.Assert(atomic.LoadUint32(&rc.exceeded), check.Equals, uint32(1))
	c.Assert(atomic.LoadUint32(&rc.spilled), check.Equals, uint32(1))
	err = rc.Reset()
	c.Assert(err, check.IsNil)

	c.Assert(atomic.LoadUint32(&rc.spilled), check.Equals, uint32(0))
	c.Assert(atomic.LoadUint32(&rc.exceeded), check.Equals, uint32(0))
	for i := 0; i < sz; i++ {
		_, err = rc.AppendRow(chk.GetRow(i))
		c.Assert(err, check.IsNil)
	}
	c.Assert(err, check.IsNil)
	c.Assert(rc.GetMemTracker().BytesConsumed(), check.Equals, chk.MemoryUsage())
	for i := 0; i < sz; i++ {
		_, err = rc.AppendRow(chk.GetRow(i))
		c.Assert(err, check.IsNil)
	}
	c.Assert(err, check.IsNil)
	c.Assert(atomic.LoadUint32(&rc.exceeded), check.Equals, uint32(1))
	c.Assert(atomic.LoadUint32(&rc.spilled), check.Equals, uint32(1))
}
