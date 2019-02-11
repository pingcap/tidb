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
	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

func (s *testChunkSuite) TestRecordBatch(c *check.C) {
	maxChunkSize := 10
	chk := New([]*types.FieldType{types.NewFieldType(mysql.TypeLong)}, maxChunkSize, maxChunkSize)
	batch := NewRecordBatch(chk)
	c.Assert(batch.RequiredRows(), check.Equals, UnspecifiedNumRows)
	for i := 1; i < 10; i++ {
		batch.SetRequiredRows(i)
		c.Assert(batch.RequiredRows(), check.Equals, i)
	}
	batch.SetRequiredRows(0)
	c.Assert(batch.RequiredRows(), check.Equals, UnspecifiedNumRows)
	batch.SetRequiredRows(-1)
	c.Assert(batch.RequiredRows(), check.Equals, UnspecifiedNumRows)
	batch.SetRequiredRows(1).SetRequiredRows(2).SetRequiredRows(3)
	c.Assert(batch.RequiredRows(), check.Equals, 3)

	batch.SetRequiredRows(5)
	batch.AppendInt64(0, 1)
	batch.AppendInt64(0, 1)
	batch.AppendInt64(0, 1)
	batch.AppendInt64(0, 1)
	c.Assert(batch.NumRows(), check.Equals, 4)
	c.Assert(batch.IsFull(maxChunkSize), check.IsFalse)

	batch.AppendInt64(0, 1)
	c.Assert(batch.NumRows(), check.Equals, 5)
	c.Assert(batch.IsFull(maxChunkSize), check.IsTrue)

	batch.AppendInt64(0, 1)
	batch.AppendInt64(0, 1)
	batch.AppendInt64(0, 1)
	c.Assert(batch.NumRows(), check.Equals, 8)
	c.Assert(batch.IsFull(maxChunkSize), check.IsTrue)
}
