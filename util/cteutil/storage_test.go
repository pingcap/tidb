// Copyright 2021 PingCAP, Inc.
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

package cteutil

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&StorageRCTestSuite{})

type StorageRCTestSuite struct{}

func (test *StorageRCTestSuite) TestStorageBasic(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 1
	storage := NewStorageRowContainer(fields, chkSize)
	c.Assert(storage, check.NotNil)

	// Close before open.
	err := storage.DerefAndClose()
	c.Assert(err, check.NotNil)

	err = storage.OpenAndRef()
	c.Assert(err, check.IsNil)

	err = storage.DerefAndClose()
	c.Assert(err, check.IsNil)

	err = storage.DerefAndClose()
	c.Assert(err, check.NotNil)

	// Open twice.
	err = storage.OpenAndRef()
	c.Assert(err, check.IsNil)
	err = storage.OpenAndRef()
	c.Assert(err, check.IsNil)
	err = storage.DerefAndClose()
	c.Assert(err, check.IsNil)
	err = storage.DerefAndClose()
	c.Assert(err, check.IsNil)
	err = storage.DerefAndClose()
	c.Assert(err, check.NotNil)
}

func (test *StorageRCTestSuite) TestOpenAndClose(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 1
	storage := NewStorageRowContainer(fields, chkSize)

	for i := 0; i < 10; i++ {
		err := storage.OpenAndRef()
		c.Assert(err, check.IsNil)
	}

	for i := 0; i < 9; i++ {
		err := storage.DerefAndClose()
		c.Assert(err, check.IsNil)
	}
	err := storage.DerefAndClose()
	c.Assert(err, check.IsNil)

	err = storage.DerefAndClose()
	c.Assert(err, check.NotNil)
}

func (test *StorageRCTestSuite) TestAddAndGetChunk(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10

	storage := NewStorageRowContainer(fields, chkSize)

	inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk.AppendInt64(0, int64(i))
	}

	err := storage.Add(inChk)
	c.Assert(err, check.NotNil)

	err = storage.OpenAndRef()
	c.Assert(err, check.IsNil)

	err = storage.Add(inChk)
	c.Assert(err, check.IsNil)

	outChk, err1 := storage.GetChunk(0)
	c.Assert(err1, check.IsNil)

	in64s := inChk.Column(0).Int64s()
	out64s := outChk.Column(0).Int64s()

	c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)
}

func (test *StorageRCTestSuite) TestSpillToDisk(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10
	storage := NewStorageRowContainer(fields, chkSize)
	var tmp interface{} = storage

	inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk.AppendInt64(0, int64(i))
	}

	err := storage.OpenAndRef()
	c.Assert(err, check.IsNil)

	memTracker := storage.GetMemTracker()
	memTracker.SetBytesLimit(inChk.MemoryUsage() + 1)
	action := tmp.(*StorageRC).ActionSpillForTest()
	memTracker.FallbackOldAndSetNewAction(action)
	diskTracker := storage.GetDiskTracker()

	// All data is in memory.
	err = storage.Add(inChk)
	c.Assert(err, check.IsNil)
	outChk, err1 := storage.GetChunk(0)
	c.Assert(err1, check.IsNil)
	in64s := inChk.Column(0).Int64s()
	out64s := outChk.Column(0).Int64s()
	c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)

	c.Assert(memTracker.BytesConsumed(), check.Greater, int64(0))
	c.Assert(memTracker.MaxConsumed(), check.Greater, int64(0))
	c.Assert(diskTracker.BytesConsumed(), check.Equals, int64(0))
	c.Assert(diskTracker.MaxConsumed(), check.Equals, int64(0))

	// Add again, and will trigger spill to disk.
	err = storage.Add(inChk)
	c.Assert(err, check.IsNil)
	action.WaitForTest()
	c.Assert(memTracker.BytesConsumed(), check.Equals, int64(0))
	c.Assert(memTracker.MaxConsumed(), check.Greater, int64(0))
	c.Assert(diskTracker.BytesConsumed(), check.Greater, int64(0))
	c.Assert(diskTracker.MaxConsumed(), check.Greater, int64(0))

	outChk, err = storage.GetChunk(0)
	c.Assert(err, check.IsNil)
	out64s = outChk.Column(0).Int64s()
	c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)

	outChk, err = storage.GetChunk(1)
	c.Assert(err, check.IsNil)
	out64s = outChk.Column(0).Int64s()
	c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)
}

func (test *StorageRCTestSuite) TestReopen(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10
	storage := NewStorageRowContainer(fields, chkSize)
	err := storage.OpenAndRef()
	c.Assert(err, check.IsNil)

	inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk.AppendInt64(0, int64(i))
	}
	err = storage.Add(inChk)
	c.Assert(err, check.IsNil)
	c.Assert(storage.NumChunks(), check.Equals, 1)

	err = storage.Reopen()
	c.Assert(err, check.IsNil)
	c.Assert(storage.NumChunks(), check.Equals, 0)

	err = storage.Add(inChk)
	c.Assert(err, check.IsNil)
	c.Assert(storage.NumChunks(), check.Equals, 1)

	outChk, err := storage.GetChunk(0)
	c.Assert(err, check.IsNil)
	in64s := inChk.Column(0).Int64s()
	out64s := outChk.Column(0).Int64s()
	c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)

	// Reopen multiple times.
	for i := 0; i < 100; i++ {
		err = storage.Reopen()
		c.Assert(err, check.IsNil)
	}
	err = storage.Add(inChk)
	c.Assert(err, check.IsNil)
	c.Assert(storage.NumChunks(), check.Equals, 1)

	outChk, err = storage.GetChunk(0)
	c.Assert(err, check.IsNil)
	in64s = inChk.Column(0).Int64s()
	out64s = outChk.Column(0).Int64s()
	c.Assert(reflect.DeepEqual(in64s, out64s), check.IsTrue)
}

func (test *StorageRCTestSuite) TestSwapData(c *check.C) {
	tp1 := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10
	storage1 := NewStorageRowContainer(tp1, chkSize)
	err := storage1.OpenAndRef()
	c.Assert(err, check.IsNil)
	inChk1 := chunk.NewChunkWithCapacity(tp1, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk1.AppendInt64(0, int64(i))
	}
	in1 := inChk1.Column(0).Int64s()
	err = storage1.Add(inChk1)
	c.Assert(err, check.IsNil)

	tp2 := []*types.FieldType{types.NewFieldType(mysql.TypeVarString)}
	storage2 := NewStorageRowContainer(tp2, chkSize)
	err = storage2.OpenAndRef()
	c.Assert(err, check.IsNil)

	inChk2 := chunk.NewChunkWithCapacity(tp2, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk2.AppendString(0, strconv.FormatInt(int64(i), 10))
	}
	var in2 []string
	for i := 0; i < inChk2.NumRows(); i++ {
		in2 = append(in2, inChk2.Column(0).GetString(i))
	}
	err = storage2.Add(inChk2)
	c.Assert(err, check.IsNil)

	err = storage1.SwapData(storage2)
	c.Assert(err, check.IsNil)

	outChk1, err := storage1.GetChunk(0)
	c.Assert(err, check.IsNil)
	outChk2, err := storage2.GetChunk(0)
	c.Assert(err, check.IsNil)

	var out1 []string
	for i := 0; i < outChk1.NumRows(); i++ {
		out1 = append(out1, outChk1.Column(0).GetString(i))
	}
	out2 := outChk2.Column(0).Int64s()

	c.Assert(reflect.DeepEqual(in1, out2), check.IsTrue)
	c.Assert(reflect.DeepEqual(in2, out1), check.IsTrue)
}
