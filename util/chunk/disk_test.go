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
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cznic/mathutil"
	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
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
	c.Assert(strings.HasPrefix(l.disk.Name(), filepath.Join(os.TempDir(), "oom-use-tmp-storage")), check.Equals, true)
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

type listInDiskWriteDisk struct {
	ListInDisk
}

func newListInDiskWriteDisk(fieldTypes []*types.FieldType) (*listInDiskWriteDisk, error) {
	l := listInDiskWriteDisk{*NewListInDisk(fieldTypes)}
	disk, err := ioutil.TempFile(config.GetGlobalConfig().TempStoragePath, strconv.Itoa(l.diskTracker.Label()))
	if err != nil {
		return nil, err
	}
	l.disk = disk
	l.w = disk
	return &l, nil
}

func (l *listInDiskWriteDisk) GetRow(ptr RowPtr) (row Row, err error) {
	err = l.flush()
	if err != nil {
		return
	}
	off := l.offsets[ptr.ChkIdx][ptr.RowIdx]

	r := io.NewSectionReader(l.disk, off, l.offWrite-off)
	format := rowInDisk{numCol: len(l.fieldTypes)}
	_, err = format.ReadFrom(r)
	if err != nil {
		return row, err
	}
	row = format.toMutRow(l.fieldTypes).ToRow()
	return row, err
}

func checkRow(c *check.C, row1, row2 Row) {
	c.Assert(row1.GetString(0), check.Equals, row2.GetString(0))
	c.Assert(row1.GetInt64(1), check.Equals, row2.GetInt64(1))
	c.Assert(row1.GetString(2), check.Equals, row2.GetString(2))
	c.Assert(row1.GetInt64(3), check.Equals, row2.GetInt64(3))
	if !row1.IsNull(4) {
		c.Assert(row1.GetJSON(4).String(), check.Equals, row2.GetJSON(4).String())
	}
}

func testListInDisk(c *check.C) {
	numChk, numRow := 10, 1000
	chks, fields := initChunks(numChk, numRow)
	lChecksum := NewListInDisk(fields)
	defer lChecksum.Close()
	lDisk, err := newListInDiskWriteDisk(fields)
	c.Assert(err, check.IsNil)
	defer lDisk.Close()
	for _, chk := range chks {
		err := lChecksum.Add(chk)
		c.Assert(err, check.IsNil)
		err = lDisk.Add(chk)
		c.Assert(err, check.IsNil)
	}

	var ptrs []RowPtr
	for i := 0; i < numChk; i++ {
		for j := 0; j < numRow; j++ {
			ptrs = append(ptrs, RowPtr{
				ChkIdx: uint32(i),
				RowIdx: uint32(j),
			})
		}
	}

	for _, rowPtr := range ptrs {
		row1, err := lChecksum.GetRow(rowPtr)
		c.Assert(err, check.IsNil)
		row2, err := lDisk.GetRow(rowPtr)
		c.Assert(err, check.IsNil)
		checkRow(c, row1, row2)
	}
}

func (s *testChunkSuite) TestListInDiskWithChecksum(c *check.C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Security.SpilledFileEncryptionMethod = config.SpilledFileEncryptionMethodPlaintext
	})
	testListInDisk(c)

}

func (s *testChunkSuite) TestListInDiskWithChecksumAndEncrypt(c *check.C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Security.SpilledFileEncryptionMethod = config.SpilledFileEncryptionMethodAES128CTR
	})
	testListInDisk(c)
}
