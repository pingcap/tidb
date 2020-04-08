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
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/cznic/mathutil"
	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func initChunks(numChk, numRow int) ([]*Chunk, []*types.FieldType) {
	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeJSON),
	}

	chks := make([]*Chunk, 0, numChk)
	for chkIdx := 0; chkIdx < numChk; chkIdx++ {
		chk := NewChunkWithCapacity(fields, numRow)
		for rowIdx := 0; rowIdx < numRow; rowIdx++ {
			data := int64(chkIdx*numRow + rowIdx)
			chk.AppendString(0, fmt.Sprint(data))
			chk.AppendNull(1)
			chk.AppendNull(2)
			chk.AppendInt64(3, data)
			if chkIdx%2 == 0 {
				chk.AppendJSON(4, json.CreateBinary(fmt.Sprint(data)))
			} else {
				chk.AppendNull(4)
			}
		}
		chks = append(chks, chk)
	}
	return chks, fields
}

func (s *testChunkSuite) TestListInDisk(c *check.C) {

	numChk, numRow := 2, 2
	chks, fields := initChunks(numChk, numRow)
	l := NewListInDisk(fields)
	defer func() {
		err := l.Close()
		c.Check(err, check.IsNil)
		c.Check(l.disk, check.NotNil)
		_, err = os.Stat(l.disk.Name())
		c.Check(os.IsNotExist(err), check.IsTrue)
	}()
	for _, chk := range chks {
		err := l.Add(chk)
		c.Check(err, check.IsNil)
	}
	c.Assert(strings.HasPrefix(l.disk.Name(), "/tmp/tidb/test-temp-storage"), check.Equals, true)
	c.Check(l.NumChunks(), check.Equals, numChk)
	c.Check(l.GetDiskTracker().BytesConsumed() > 0, check.IsTrue)

	for chkIdx := 0; chkIdx < numChk; chkIdx++ {
		for rowIdx := 0; rowIdx < numRow; rowIdx++ {
			row, err := l.GetRow(RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
			c.Check(err, check.IsNil)
			c.Check(row.GetDatumRow(fields), check.DeepEquals, chks[chkIdx].GetRow(rowIdx).GetDatumRow(fields))
		}
	}
}

func BenchmarkListInDiskAdd(b *testing.B) {
	numChk, numRow := 1, 2
	chks, fields := initChunks(numChk, numRow)
	chk := chks[0]
	l := NewListInDisk(fields)
	defer l.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := l.Add(chk)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListInDiskGetRow(b *testing.B) {
	numChk, numRow := 10000, 2
	chks, fields := initChunks(numChk, numRow)
	l := NewListInDisk(fields)
	defer l.Close()
	for _, chk := range chks {
		err := l.Add(chk)
		if err != nil {
			b.Fatal(err)
		}
	}
	rand.Seed(0)
	ptrs := make([]RowPtr, 0, b.N)
	for i := 0; i < mathutil.Min(b.N, 10000); i++ {
		ptrs = append(ptrs, RowPtr{
			ChkIdx: rand.Uint32() % uint32(numChk),
			RowIdx: rand.Uint32() % uint32(numRow),
		})
	}
	for i := 10000; i < cap(ptrs); i++ {
		ptrs = append(ptrs, ptrs[i%10000])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := l.GetRow(ptrs[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}
