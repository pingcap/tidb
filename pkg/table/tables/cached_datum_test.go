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
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
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
func (m *testMemBuffer) RLock()                                           {}
func (m *testMemBuffer) RUnlock()                                         {}
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
func (m *testMemBuffer) SnapshotGetter() kv.Getter                        { panic("unused") }
func (m *testMemBuffer) SnapshotIter(kv.Key, kv.Key) kv.Iterator          { panic("unused") }
func (m *testMemBuffer) SnapshotIterReverse(kv.Key, kv.Key) kv.Iterator   { panic("unused") }
func (m *testMemBuffer) Len() int                                         { return len(m.kvs) }
func (m *testMemBuffer) Size() int                                        { panic("unused") }
func (m *testMemBuffer) RemoveFromBuffer(kv.Key)                          { panic("unused") }
func (m *testMemBuffer) GetLocal(context.Context, []byte) ([]byte, error) { panic("unused") }
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

func TestBuildCachedDatumDataCommonHandleTimestamp(t *testing.T) {
	mb := newTestMemBuf()

	ftTs := types.NewFieldType(mysql.TypeTimestamp)
	ftTs.SetDecimal(0)
	ftInt := types.NewFieldType(mysql.TypeLonglong)

	ts := types.NewTime(types.FromGoTime(time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)), mysql.TypeTimestamp, 0)
	handleEncoded, err := codec.EncodeKey(time.UTC, nil, types.NewTimeDatum(ts))
	require.NoError(t, err)
	handle, err := kv.NewCommonHandle(handleEncoded)
	require.NoError(t, err)

	var encoder rowcodec.Encoder
	rowBytes, err := encoder.Encode(time.UTC, []int64{2}, []types.Datum{types.NewIntDatum(123)}, nil, nil)
	require.NoError(t, err)
	key := tablecodec.EncodeRowKeyWithHandle(testTableID, handle)
	require.NoError(t, mb.Set(key, rowBytes))

	colInfos := []rowcodec.ColInfo{
		{ID: 1, Ft: ftTs},
		{ID: 2, Ft: ftInt},
	}
	fieldTypes := []*types.FieldType{ftTs, ftInt}
	handleColIDs := []int64{1}

	cd, err := BuildCachedDatumData(mb, testTableID, colInfos, handleColIDs, nilDefDatum, fieldTypes)
	require.NoError(t, err)
	require.Equal(t, 1, cd.TotalRows)
	require.Equal(t, []int{0}, cd.TsColIndices)

	row := cd.Chunks[0].GetRow(0)
	require.Equal(t, ts.String(), row.GetTime(0).String())
	require.Equal(t, int64(123), row.GetInt64(1))
}

func TestBuildCachedIndexDatumDataRestoredDecoderDurability(t *testing.T) {
	mb := newTestMemBuf()

	ftPK := types.NewFieldType(mysql.TypeLonglong)
	ftPK.AddFlag(mysql.PriKeyFlag)
	ftJSON := types.NewFieldType(mysql.TypeJSON)

	tblInfo := &model.TableInfo{
		ID:         testTableID,
		PKIsHandle: true,
		Columns: []*model.ColumnInfo{
			{ID: 1, Offset: 0, FieldType: *ftPK},
			{ID: 2, Offset: 1, FieldType: *ftJSON},
		},
	}
	idxInfo := &model.IndexInfo{
		ID:     1,
		Unique: true,
		Columns: []*model.IndexColumn{
			{Offset: 1, Length: types.UnspecifiedLength},
		},
	}

	buildIndexKV := func(handle int64, j types.BinaryJSON) (kv.Key, []byte) {
		encodedCols, err := codec.EncodeKey(time.UTC, nil, types.NewJSONDatum(j))
		require.NoError(t, err)
		key := tablecodec.EncodeIndexSeekKey(testTableID, idxInfo.ID, encodedCols)

		rd := rowcodec.Encoder{Enable: true}
		restoredBytes, err := rd.Encode(time.UTC, []int64{tblInfo.Columns[1].ID}, []types.Datum{types.NewJSONDatum(j)}, nil, nil)
		require.NoError(t, err)

		val := make([]byte, 0, 1+len(restoredBytes)+8)
		val = append(val, 8) // tailLen = 8 (int handle)
		val = append(val, restoredBytes...)
		var hBuf [8]byte
		binary.BigEndian.PutUint64(hBuf[:], uint64(handle))
		val = append(val, hBuf[:]...)
		return key, val
	}

	j1, err := types.ParseBinaryJSONFromString(`{"a":1}`)
	require.NoError(t, err)
	j2, err := types.ParseBinaryJSONFromString(`{"a":2}`)
	require.NoError(t, err)

	key1, val1 := buildIndexKV(1, j1)
	key2, val2 := buildIndexKV(2, j2)
	require.NoError(t, mb.Set(key1, val1))
	require.NoError(t, mb.Set(key2, val2))

	data, err := BuildCachedIndexDatumData(mb, testTableID, idxInfo, tblInfo)
	require.NoError(t, err)
	require.Len(t, data.Entries, 2)

	row1, ok := data.Entries[string(key1)]
	require.True(t, ok)
	require.Len(t, row1, 2)
	require.Equal(t, j1, row1[0].GetMysqlJSON())
	require.Equal(t, int64(1), row1[1].GetInt64())

	row2, ok := data.Entries[string(key2)]
	require.True(t, ok)
	require.Len(t, row2, 2)
	require.Equal(t, j2, row2[0].GetMysqlJSON())
	require.Equal(t, int64(2), row2[1].GetInt64())
}

func TestCachedTablePutCachedResultNoDoubleAccount(t *testing.T) {
	ct := &cachedTable{}
	chk := makeTestChunk()
	ft := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ft}
	key := ResultCacheKey{PlanDigest: [16]byte{7}, ParamHash: 7}
	paramBytes := []byte("pb")
	expectedMem := estimateChunksMemory([]*chunk.Chunk{chk}) + int64(len(paramBytes))

	require.True(t, ct.PutCachedResult(key, paramBytes, []*chunk.Chunk{chk}, fts))
	require.Equal(t, expectedMem, ct.resultCacheMem.Load())

	// A duplicate fill for the same cache entry should be a no-op for memory accounting.
	require.True(t, ct.PutCachedResult(key, paramBytes, []*chunk.Chunk{chk}, fts))
	require.Equal(t, expectedMem, ct.resultCacheMem.Load())

	// A hash collision should still be rejected without changing the accounted memory.
	require.False(t, ct.PutCachedResult(key, []byte("other"), []*chunk.Chunk{chk}, fts))
	require.Equal(t, expectedMem, ct.resultCacheMem.Load())

	ct.invalidateResultCache()
	require.Zero(t, ct.resultCacheMem.Load())
}

func TestCachedTablePinnedDatumCacheAccessors(t *testing.T) {
	mb1 := newTestMemBuf()
	mb2 := newTestMemBuf()
	ft := types.NewFieldType(mysql.TypeLonglong)

	datumCache1 := &CachedDatumData{FieldTypes: []*types.FieldType{ft}}
	datumCache2 := &CachedDatumData{FieldTypes: []*types.FieldType{ft}}
	indexCache1 := &CachedIndexDatumData{Entries: map[string][]types.Datum{"k1": {types.NewIntDatum(1)}}}
	indexCache2 := &CachedIndexDatumData{Entries: map[string][]types.Datum{"k2": {types.NewIntDatum(2)}}}

	ct := &cachedTable{}
	ct.cacheData.Store(&cacheData{
		MemBuffer:        mb1,
		datumCache:       datumCache1,
		indexDatumCaches: map[int64]*CachedIndexDatumData{1: indexCache1},
	})

	require.Same(t, datumCache1, ct.GetCachedDatumDataForMemBuffer(mb1))
	require.Same(t, indexCache1, ct.GetCachedIndexDatumDataForMemBuffer(mb1, 1))
	require.Nil(t, ct.GetCachedDatumDataForMemBuffer(mb2))
	require.Nil(t, ct.GetCachedIndexDatumDataForMemBuffer(mb2, 1))

	ct.cacheData.Store(&cacheData{
		MemBuffer:        mb2,
		datumCache:       datumCache2,
		indexDatumCaches: map[int64]*CachedIndexDatumData{1: indexCache2},
	})

	// Unpinned accessors expose the latest generation, but the pinned variants must
	// reject the stale MemBuffer from the earlier generation.
	require.Same(t, datumCache2, ct.GetCachedDatumData())
	require.Same(t, indexCache2, ct.GetCachedIndexDatumData(1))
	require.Nil(t, ct.GetCachedDatumDataForMemBuffer(mb1))
	require.Nil(t, ct.GetCachedIndexDatumDataForMemBuffer(mb1, 1))
	require.Same(t, datumCache2, ct.GetCachedDatumDataForMemBuffer(mb2))
	require.Same(t, indexCache2, ct.GetCachedIndexDatumDataForMemBuffer(mb2, 1))
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
