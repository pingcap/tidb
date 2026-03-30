// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/require"
)

// testKV is a key-value pair for the test mock.
type testKV struct {
	key   kv.Key
	value []byte
}

// testMemBuffer is a minimal mock of kv.MemBuffer that supports Set and Iter.
type testMemBuffer struct {
	kvs []testKV
}

func newTestMemBuf() *testMemBuffer {
	return &testMemBuffer{}
}

func (m *testMemBuffer) Set(k kv.Key, v []byte) error {
	keyCopy := make([]byte, len(k))
	copy(keyCopy, k)
	valCopy := make([]byte, len(v))
	copy(valCopy, v)
	m.kvs = append(m.kvs, testKV{key: keyCopy, value: valCopy})
	sort.Slice(m.kvs, func(i, j int) bool {
		return bytes.Compare(m.kvs[i].key, m.kvs[j].key) < 0
	})
	return nil
}

func (m *testMemBuffer) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	var filtered []testKV
	for _, pair := range m.kvs {
		if bytes.Compare(pair.key, k) >= 0 && (len(upperBound) == 0 || bytes.Compare(pair.key, upperBound) < 0) {
			filtered = append(filtered, pair)
		}
	}
	return &testMemBufIter{kvs: filtered, idx: 0}, nil
}

// Unused interface methods — only Set and Iter are needed by BuildCachedDatumData.
func (m *testMemBuffer) Delete(kv.Key) error                              { panic("unused") }
func (m *testMemBuffer) Get(_ context.Context, _ kv.Key) ([]byte, error)  { panic("unused") }
func (m *testMemBuffer) IterReverse(kv.Key, kv.Key) (kv.Iterator, error)  { panic("unused") }
func (m *testMemBuffer) RLock()                                            {}
func (m *testMemBuffer) RUnlock()                                          {}
func (m *testMemBuffer) GetFlags(kv.Key) (kv.KeyFlags, error)             { panic("unused") }
func (m *testMemBuffer) SetWithFlags(kv.Key, []byte, ...kv.FlagsOp) error { panic("unused") }
func (m *testMemBuffer) UpdateFlags(kv.Key, ...kv.FlagsOp)                { panic("unused") }
func (m *testMemBuffer) DeleteWithFlags(kv.Key, ...kv.FlagsOp) error      { panic("unused") }
func (m *testMemBuffer) Staging() kv.StagingHandle                        { panic("unused") }
func (m *testMemBuffer) Release(kv.StagingHandle)                         { panic("unused") }
func (m *testMemBuffer) Cleanup(kv.StagingHandle)                         { panic("unused") }
func (m *testMemBuffer) InspectStage(kv.StagingHandle, func(kv.Key, kv.KeyFlags, []byte)) {
	panic("unused")
}
func (m *testMemBuffer) SnapshotGetter() kv.Getter                          { panic("unused") }
func (m *testMemBuffer) SnapshotIter(kv.Key, kv.Key) kv.Iterator            { panic("unused") }
func (m *testMemBuffer) SnapshotIterReverse(kv.Key, kv.Key) kv.Iterator     { panic("unused") }
func (m *testMemBuffer) Len() int                                           { return len(m.kvs) }
func (m *testMemBuffer) Size() int                                          { panic("unused") }
func (m *testMemBuffer) RemoveFromBuffer(kv.Key)                            { panic("unused") }
func (m *testMemBuffer) GetLocal(context.Context, []byte) ([]byte, error)   { panic("unused") }
func (m *testMemBuffer) BatchGet(context.Context, [][]byte) (map[string][]byte, error) {
	panic("unused")
}

type testMemBufIter struct {
	kvs []testKV
	idx int
}

func (it *testMemBufIter) Valid() bool   { return it.idx < len(it.kvs) }
func (it *testMemBufIter) Key() kv.Key   { return it.kvs[it.idx].key }
func (it *testMemBufIter) Value() []byte { return it.kvs[it.idx].value }
func (it *testMemBufIter) Next() error   { it.idx++; return nil }
func (it *testMemBufIter) Close()        {}

// --- test helpers ---

const testTableID = int64(42)

type testDatumCacheSetup struct {
	colIDs     []int64
	cols       []rowcodec.ColInfo
	fieldTypes []*types.FieldType
	pkColIDs   []int64
}

func newTestSetup() *testDatumCacheSetup {
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftStr := types.NewFieldType(mysql.TypeVarchar)
	ftTs := types.NewFieldType(mysql.TypeTimestamp)
	ftTs.SetDecimal(0)

	return &testDatumCacheSetup{
		colIDs: []int64{1, 2, 3},
		cols: []rowcodec.ColInfo{
			{ID: 1, IsPKHandle: true, Ft: ftInt},
			{ID: 2, Ft: ftStr},
			{ID: 3, Ft: ftTs},
		},
		fieldTypes: []*types.FieldType{ftInt, ftStr, ftTs},
		pkColIDs:   []int64{-1},
	}
}

func encodeAndSetRow(t *testing.T, mb kv.MemBuffer, tableID int64, handle kv.Handle, colIDs []int64, values []types.Datum) {
	var encoder rowcodec.Encoder
	rowBytes, err := encoder.Encode(time.UTC, colIDs, values, nil, nil)
	require.NoError(t, err)
	key := tablecodec.EncodeRowKeyWithHandle(tableID, handle)
	require.NoError(t, mb.Set(key, rowBytes))
}

func nilDefDatum(i int, chk *chunk.Chunk) error {
	chk.AppendNull(i)
	return nil
}

// --- tests ---

func TestBuildCachedDatumDataBasic(t *testing.T) {
	setup := newTestSetup()
	mb := newTestMemBuf()

	ts := types.NewTime(types.FromGoTime(time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)), mysql.TypeTimestamp, 0)

	encodeAndSetRow(t, mb, testTableID, kv.IntHandle(1), setup.colIDs, []types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("hello"),
		types.NewTimeDatum(ts),
	})
	encodeAndSetRow(t, mb, testTableID, kv.IntHandle(2), setup.colIDs, []types.Datum{
		types.NewIntDatum(2),
		types.NewStringDatum("world"),
		types.NewTimeDatum(ts),
	})

	cd, err := BuildCachedDatumData(mb, testTableID, setup.cols, setup.pkColIDs, nilDefDatum, setup.fieldTypes)
	require.NoError(t, err)
	require.Equal(t, 2, cd.TotalRows)
	require.Len(t, cd.Chunks, 1)
	require.Equal(t, 2, cd.Chunks[0].NumRows())

	// Verify values.
	row0 := cd.Chunks[0].GetRow(0)
	require.Equal(t, int64(1), row0.GetInt64(0))
	require.Equal(t, "hello", row0.GetString(1))

	row1 := cd.Chunks[0].GetRow(1)
	require.Equal(t, int64(2), row1.GetInt64(0))
	require.Equal(t, "world", row1.GetString(1))

	// TIMESTAMP should be stored as-is (UTC, no timezone conversion).
	tsVal := row0.GetTime(2)
	require.Equal(t, ts.String(), tsVal.String())

	// TsColIndices should identify column 2 (0-indexed).
	require.Equal(t, []int{2}, cd.TsColIndices)

	// FieldTypes should match.
	require.Len(t, cd.FieldTypes, 3)
}

func TestBuildCachedDatumDataEmpty(t *testing.T) {
	setup := newTestSetup()
	mb := newTestMemBuf()

	cd, err := BuildCachedDatumData(mb, testTableID, setup.cols, setup.pkColIDs, nilDefDatum, setup.fieldTypes)
	require.NoError(t, err)
	require.Equal(t, 0, cd.TotalRows)
	// One initial (empty) chunk is always allocated.
	require.Len(t, cd.Chunks, 1)
	require.Equal(t, 0, cd.Chunks[0].NumRows())
}

func TestBuildCachedDatumDataChunkSplit(t *testing.T) {
	// Use int-only columns for simplicity.
	ft := types.NewFieldType(mysql.TypeLonglong)
	cols := []rowcodec.ColInfo{{ID: 1, IsPKHandle: true, Ft: ft}}
	fts := []*types.FieldType{ft}
	colIDs := []int64{1}
	pkColIDs := []int64{-1}

	mb := newTestMemBuf()

	totalRows := datumCacheChunkSize + 100
	for i := 1; i <= totalRows; i++ {
		encodeAndSetRow(t, mb, testTableID, kv.IntHandle(int64(i)), colIDs, []types.Datum{
			types.NewIntDatum(int64(i)),
		})
	}

	cd, err := BuildCachedDatumData(mb, testTableID, cols, pkColIDs, nilDefDatum, fts)
	require.NoError(t, err)
	require.Equal(t, totalRows, cd.TotalRows)
	require.Len(t, cd.Chunks, 2)
	require.Equal(t, datumCacheChunkSize, cd.Chunks[0].NumRows())
	require.Equal(t, 100, cd.Chunks[1].NumRows())
}

func TestBuildCachedDatumDataSkipDeleted(t *testing.T) {
	setup := newTestSetup()
	mb := newTestMemBuf()

	ts := types.NewTime(types.FromGoTime(time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)), mysql.TypeTimestamp, 0)

	// Insert a valid row.
	encodeAndSetRow(t, mb, testTableID, kv.IntHandle(1), setup.colIDs, []types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("keep"),
		types.NewTimeDatum(ts),
	})

	// Insert a "deleted" row (empty value).
	deletedKey := tablecodec.EncodeRowKeyWithHandle(testTableID, kv.IntHandle(2))
	require.NoError(t, mb.Set(deletedKey, []byte{}))

	// Insert another valid row.
	encodeAndSetRow(t, mb, testTableID, kv.IntHandle(3), setup.colIDs, []types.Datum{
		types.NewIntDatum(3),
		types.NewStringDatum("also keep"),
		types.NewTimeDatum(ts),
	})

	cd, err := BuildCachedDatumData(mb, testTableID, setup.cols, setup.pkColIDs, nilDefDatum, setup.fieldTypes)
	require.NoError(t, err)
	require.Equal(t, 2, cd.TotalRows)

	row0 := cd.Chunks[0].GetRow(0)
	require.Equal(t, int64(1), row0.GetInt64(0))
	row1 := cd.Chunks[0].GetRow(1)
	require.Equal(t, int64(3), row1.GetInt64(0))
}

func TestFindTimestampColumns(t *testing.T) {
	fts := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeTimestamp),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeTimestamp),
	}
	indices := findTimestampColumns(fts)
	require.Equal(t, []int{1, 3}, indices)

	// No timestamp columns.
	fts2 := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarchar),
	}
	indices2 := findTimestampColumns(fts2)
	require.Nil(t, indices2)
}
