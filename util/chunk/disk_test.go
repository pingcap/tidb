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
	"fmt"
	"os"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (s *testChunkSuite) TestListInDisk(c *check.C) {
	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeJSON),
	}
	l := NewListInDisk(fields)
	defer func() {
		err := l.Close()
		c.Check(err, check.IsNil)
		_, err = os.Stat(l.disk.Name())
		c.Check(os.IsNotExist(err), check.IsTrue)
	}()

	numChk, numRow := 2, 2
	chks := make([]*Chunk, 0, numChk)
	for chkIdx := 0; chkIdx < numChk; chkIdx++ {
		chk := NewChunkWithCapacity(fields, 2)
		for rowIdx := 0; rowIdx < numRow; rowIdx++ {
			data := int64(chkIdx*numRow + rowIdx)
			chk.AppendString(0, fmt.Sprint(data))
			chk.AppendNull(1)
			chk.AppendNull(2)
			chk.AppendInt64(3, data)
			if chkIdx == 0 {
				chk.AppendJSON(4, json.CreateBinary(fmt.Sprint(data)))
			} else {
				chk.AppendNull(4)
			}
		}
		chks = append(chks, chk)
		err := l.Add(chk)
		c.Check(err, check.IsNil)
	}

	c.Check(l.NumChunks(), check.Equals, 2)
	c.Check(l.GetDiskTracker().BytesConsumed() > 0, check.IsTrue)

	for chkIdx := 0; chkIdx < numChk; chkIdx++ {
		for rowIdx := 0; rowIdx < numRow; rowIdx++ {
			row, err := l.GetRow(RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
			c.Check(err, check.IsNil)
			c.Check(row.GetDatumRow(fields), check.DeepEquals, chks[chkIdx].GetRow(rowIdx).GetDatumRow(fields))
		}
	}
}
