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
	"strconv"
	"testing"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestStorageBasic(t *testing.T) {
	t.Parallel()

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 1
	storage := NewStorageRowContainer(fields, chkSize)
	require.NotNil(t, storage)

	// Close before open.
	err := storage.DerefAndClose()
	require.Error(t, err)

	err = storage.OpenAndRef()
	require.NoError(t, err)

	err = storage.DerefAndClose()
	require.NoError(t, err)

	err = storage.DerefAndClose()
	require.Error(t, err)

	// Open twice.
	err = storage.OpenAndRef()
	require.NoError(t, err)
	err = storage.OpenAndRef()
	require.NoError(t, err)
	err = storage.DerefAndClose()
	require.NoError(t, err)
	err = storage.DerefAndClose()
	require.NoError(t, err)
	err = storage.DerefAndClose()
	require.Error(t, err)
}

func TestOpenAndClose(t *testing.T) {
	t.Parallel()

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 1
	storage := NewStorageRowContainer(fields, chkSize)

	for i := 0; i < 10; i++ {
		err := storage.OpenAndRef()
		require.NoError(t, err)
	}

	for i := 0; i < 9; i++ {
		err := storage.DerefAndClose()
		require.NoError(t, err)
	}
	err := storage.DerefAndClose()
	require.NoError(t, err)

	err = storage.DerefAndClose()
	require.Error(t, err)
}

func TestAddAndGetChunk(t *testing.T) {
	t.Parallel()

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10

	storage := NewStorageRowContainer(fields, chkSize)

	inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk.AppendInt64(0, int64(i))
	}

	err := storage.Add(inChk)
	require.Error(t, err)

	err = storage.OpenAndRef()
	require.NoError(t, err)

	err = storage.Add(inChk)
	require.NoError(t, err)

	outChk, err1 := storage.GetChunk(0)
	require.NoError(t, err1)

	in64s := inChk.Column(0).Int64s()
	out64s := outChk.Column(0).Int64s()
	require.Equal(t, in64s, out64s)
}

func TestSpillToDisk(t *testing.T) {
	t.Parallel()

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10
	storage := NewStorageRowContainer(fields, chkSize)
	var tmp interface{} = storage

	inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk.AppendInt64(0, int64(i))
	}

	err := storage.OpenAndRef()
	require.NoError(t, err)

	memTracker := storage.GetMemTracker()
	memTracker.SetBytesLimit(inChk.MemoryUsage() + 1)
	action := tmp.(*StorageRC).ActionSpillForTest()
	memTracker.FallbackOldAndSetNewAction(action)
	diskTracker := storage.GetDiskTracker()

	// All data is in memory.
	err = storage.Add(inChk)
	require.NoError(t, err)
	outChk, err1 := storage.GetChunk(0)
	require.NoError(t, err1)

	in64s := inChk.Column(0).Int64s()
	out64s := outChk.Column(0).Int64s()
	require.Equal(t, in64s, out64s)

	require.Greater(t, memTracker.BytesConsumed(), int64(0))
	require.Greater(t, memTracker.MaxConsumed(), int64(0))
	require.Equal(t, int64(0), diskTracker.BytesConsumed())
	require.Equal(t, int64(0), diskTracker.MaxConsumed())

	// Add again, and will trigger spill to disk.
	err = storage.Add(inChk)
	require.NoError(t, err)
	action.WaitForTest()
	require.Equal(t, int64(0), memTracker.BytesConsumed())
	require.Greater(t, memTracker.MaxConsumed(), int64(0))
	require.Greater(t, diskTracker.BytesConsumed(), int64(0))
	require.Greater(t, diskTracker.MaxConsumed(), int64(0))

	outChk, err = storage.GetChunk(0)
	require.NoError(t, err)
	out64s = outChk.Column(0).Int64s()
	require.Equal(t, in64s, out64s)

	outChk, err = storage.GetChunk(1)
	require.NoError(t, err)
	out64s = outChk.Column(0).Int64s()
	require.Equal(t, in64s, out64s)
}

func TestReopen(t *testing.T) {
	t.Parallel()

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10
	storage := NewStorageRowContainer(fields, chkSize)
	err := storage.OpenAndRef()
	require.NoError(t, err)

	inChk := chunk.NewChunkWithCapacity(fields, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk.AppendInt64(0, int64(i))
	}
	err = storage.Add(inChk)
	require.NoError(t, err)
	require.Equal(t, 1, storage.NumChunks())
	err = storage.Reopen()
	require.NoError(t, err)
	require.Equal(t, 0, storage.NumChunks())

	err = storage.Add(inChk)
	require.NoError(t, err)
	require.Equal(t, 1, storage.NumChunks())

	outChk, err := storage.GetChunk(0)
	require.NoError(t, err)
	in64s := inChk.Column(0).Int64s()
	out64s := outChk.Column(0).Int64s()
	require.Equal(t, in64s, out64s)
	// Reopen multiple times.
	for i := 0; i < 100; i++ {
		err = storage.Reopen()
		require.NoError(t, err)
	}
	err = storage.Add(inChk)
	require.NoError(t, err)
	require.Equal(t, 1, storage.NumChunks())

	outChk, err = storage.GetChunk(0)
	require.NoError(t, err)
	in64s = inChk.Column(0).Int64s()
	out64s = outChk.Column(0).Int64s()
	require.Equal(t, in64s, out64s)
}

func TestSwapData(t *testing.T) {
	t.Parallel()

	tp1 := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	chkSize := 10
	storage1 := NewStorageRowContainer(tp1, chkSize)
	err := storage1.OpenAndRef()
	require.NoError(t, err)
	inChk1 := chunk.NewChunkWithCapacity(tp1, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk1.AppendInt64(0, int64(i))
	}
	in1 := inChk1.Column(0).Int64s()
	err = storage1.Add(inChk1)
	require.NoError(t, err)

	tp2 := []*types.FieldType{types.NewFieldType(mysql.TypeVarString)}
	storage2 := NewStorageRowContainer(tp2, chkSize)
	err = storage2.OpenAndRef()
	require.NoError(t, err)

	inChk2 := chunk.NewChunkWithCapacity(tp2, chkSize)
	for i := 0; i < chkSize; i++ {
		inChk2.AppendString(0, strconv.FormatInt(int64(i), 10))
	}
	var in2 []string
	for i := 0; i < inChk2.NumRows(); i++ {
		in2 = append(in2, inChk2.Column(0).GetString(i))
	}
	err = storage2.Add(inChk2)
	require.NoError(t, err)

	err = storage1.SwapData(storage2)
	require.NoError(t, err)

	outChk1, err := storage1.GetChunk(0)
	require.NoError(t, err)
	outChk2, err := storage2.GetChunk(0)
	require.NoError(t, err)

	var out1 []string
	for i := 0; i < outChk1.NumRows(); i++ {
		out1 = append(out1, outChk1.Column(0).GetString(i))
	}
	out2 := outChk2.Column(0).Int64s()
	require.Equal(t, in1, out2)
	require.Equal(t, in2, out1)
}
