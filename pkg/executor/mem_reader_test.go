package executor

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

var memReaderBenchSink int64

func TestMemRowsIterFastDecodeRowKey(t *testing.T) {
	const tableID int64 = 1

	intKey := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(-123))
	fastHandle, err := decodeHandleFromRowKey(intKey, true)
	require.NoError(t, err)
	slowHandle, err := tablecodec.DecodeRowKey(intKey)
	require.NoError(t, err)
	require.True(t, fastHandle.IsInt())
	require.Equal(t, int64(-123), fastHandle.IntValue())
	require.True(t, fastHandle.Equal(slowHandle))

	encoded, err := codec.EncodeKey(stmtctx.NewStmtCtx().TimeZone(), nil, types.MakeDatums(int64(100), "abc")...)
	require.NoError(t, err)
	commonHandle, err := kv.NewCommonHandle(encoded)
	require.NoError(t, err)
	commonKey := tablecodec.EncodeRowKeyWithHandle(tableID, commonHandle)
	fastHandle, err = decodeHandleFromRowKey(commonKey, false)
	require.NoError(t, err)
	slowHandle, err = tablecodec.DecodeRowKey(commonKey)
	require.NoError(t, err)
	require.False(t, fastHandle.IsInt())
	require.True(t, fastHandle.Equal(slowHandle))
}

func BenchmarkMemRowsIterForTable(b *testing.B) {
	b.ReportAllocs()

	store, err := mockstore.NewMockStore()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = store.Close()
	})

	sctx := mock.NewContext()
	sctx.Store = store
	sctx.GetSessionVars().EnableVectorizedExpression = true

	const (
		tableID int64 = 1
		numRows       = cachedTableBatchSize * 512
	)

	intTp1 := types.NewFieldType(mysql.TypeLonglong)
	intTp2 := types.NewFieldType(mysql.TypeLonglong)
	retFieldTypes := []*types.FieldType{intTp1, intTp2}

	col1Expr := &expression.Column{ID: 1, UniqueID: 1, Index: 0, RetType: intTp1}
	col2Expr := &expression.Column{ID: 2, UniqueID: 2, Index: 1, RetType: intTp2}
	schema := expression.NewSchema(col1Expr, col2Expr)

	col1Info := &model.ColumnInfo{ID: 1, Offset: 0, FieldType: *intTp1}
	col2Info := &model.ColumnInfo{ID: 2, Offset: 1, FieldType: *intTp2}
	tblInfo := &model.TableInfo{ID: tableID, Columns: []*model.ColumnInfo{col1Info, col2Info}}

	cd := NewRowDecoder(sctx, schema, tblInfo)

	buffTxn, err := store.Begin(tikv.WithStartTS(0))
	if err != nil {
		b.Fatal(err)
	}
	cacheTable := buffTxn.GetMemBuffer()

	var encoder rowcodec.Encoder
	colIDs := []int64{1, 2}
	datums := make([]types.Datum, 2)
	buf := make([]byte, 0, 64)
	loc := sctx.GetSessionVars().Location()
	for i := 0; i < numRows; i++ {
		key := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(i))
		datums[0] = types.NewIntDatum(int64(i))
		datums[1] = types.NewIntDatum(int64(i & 63))
		buf = buf[:0]
		value, err := encoder.Encode(loc, colIDs, datums, nil, buf)
		if err != nil {
			b.Fatal(err)
		}
		if err := cacheTable.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}

	recordPrefix := tablecodec.GenTableRecordPrefix(tableID)
	kvRanges := []kv.KeyRange{{StartKey: recordPrefix, EndKey: recordPrefix.PrefixNext()}}

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	constExpr := &expression.Constant{Value: types.NewIntDatum(32), RetType: intTp2}
	filter, err := expression.NewFunction(sctx.GetExprCtx(), ast.LT, tinyTp, col2Expr, constExpr)
	if err != nil {
		b.Fatal(err)
	}

	memTblReader := &memTableReader{
		ctx:           sctx,
		table:         tblInfo,
		columns:       []*model.ColumnInfo{col1Info, col2Info},
		kvRanges:      kvRanges,
		conditions:    []expression.Expression{filter},
		retFieldTypes: retFieldTypes,
		colIDs:        map[int64]int{1: 0, 2: 1},
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			cd:          cd,
		},
		cacheTable: cacheTable,
	}
	memTblReader.offsets = []int{0, 1}

	newRowIter := func(tb testing.TB) memRowsIter {
		kvIter, err := newTxnMemBufferIter(sctx, cacheTable, kvRanges, false)
		if err != nil {
			tb.Fatal(err)
		}
		return &memRowsIterForTable{
			kvIter:         kvIter,
			cd:             cd,
			chk:            chunk.New(retFieldTypes, 1, 1),
			datumRow:       make([]types.Datum, len(retFieldTypes)),
			intHandle:      true,
			memTableReader: memTblReader,
		}
	}

	newBatchIter := func(tb testing.TB) memRowsIter {
		kvIter, err := newTxnMemBufferIter(sctx, cacheTable, kvRanges, false)
		if err != nil {
			tb.Fatal(err)
		}
		batchChk := chunk.New(retFieldTypes, cachedTableBatchSize, cachedTableBatchSize)
		return &memRowsBatchIterForTable{
			kvIter:         kvIter,
			cd:             cd,
			batchChk:       batchChk,
			batchIt:        chunk.NewIterator4Chunk(batchChk),
			sel:            make([]int, 0, cachedTableBatchSize),
			datumRow:       make([]types.Datum, len(retFieldTypes)),
			retFieldTypes:  retFieldTypes,
			intHandle:      true,
			memTableReader: memTblReader,
		}
	}

	scanOnce := func(tb testing.TB, it memRowsIter) (matched int, sum int64) {
		defer it.Close()
		for {
			row, err := it.Next()
			if err != nil {
				tb.Fatal(err)
			}
			if row == nil {
				return matched, sum
			}
			matched++
			sum += row[0].GetInt64()
		}
	}

	expected := numRows / 2
	b.StopTimer()
	if got, _ := scanOnce(b, newRowIter(b)); got != expected {
		b.Fatalf("unexpected matched rows by memRowsIterForTable: got %d, want %d", got, expected)
	}
	if got, _ := scanOnce(b, newBatchIter(b)); got != expected {
		b.Fatalf("unexpected matched rows by memRowsBatchIterForTable: got %d, want %d", got, expected)
	}
	b.StartTimer()

	b.Run("row", func(b *testing.B) {
		var sum int64
		for i := 0; i < b.N; i++ {
			it := newRowIter(b)
			rows, s := scanOnce(b, it)
			if rows != expected {
				b.Fatalf("unexpected matched rows: got %d, want %d", rows, expected)
			}
			sum += s
		}
		memReaderBenchSink = sum
	})
	b.Run("batch", func(b *testing.B) {
		var sum int64
		for i := 0; i < b.N; i++ {
			it := newBatchIter(b)
			rows, s := scanOnce(b, it)
			if rows != expected {
				b.Fatalf("unexpected matched rows: got %d, want %d", rows, expected)
			}
			sum += s
		}
		memReaderBenchSink = sum
	})
}

// buildTestDatumCache builds a CachedDatumData with 3 columns: int64 (id), varchar (name), int64 (val).
// rows is a list of (id, name, val) tuples.
func buildTestDatumCache(rows [][]any) *tables.CachedDatumData {
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftStr := types.NewFieldType(mysql.TypeVarchar)
	ftStr.SetFlen(64)
	ftVal := types.NewFieldType(mysql.TypeLonglong)
	fieldTypes := []*types.FieldType{ftInt, ftStr, ftVal}

	chk := chunk.New(fieldTypes, 1024, 1024)
	for _, row := range rows {
		chk.AppendInt64(0, row[0].(int64))
		chk.AppendString(1, row[1].(string))
		chk.AppendInt64(2, row[2].(int64))
	}

	return &tables.CachedDatumData{
		Chunks:     []*chunk.Chunk{chk},
		FieldTypes: fieldTypes,
		TotalRows:  len(rows),
	}
}

func TestMemCachedDatumIterProjection(t *testing.T) {
	data := buildTestDatumCache([][]any{
		{int64(1), "alice", int64(100)},
		{int64(2), "bob", int64(200)},
		{int64(3), "charlie", int64(300)},
	})

	// SELECT id, val (skip name column) — project columns 0 and 2 from cache.
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftVal := types.NewFieldType(mysql.TypeLonglong)
	iter := &memCachedDatumIter{
		data:            data,
		colProjection:   []int{0, 2}, // query col 0 -> cache col 0 (id), query col 1 -> cache col 2 (val)
		cacheFieldTypes: data.FieldTypes,
		datumRow:        make([]types.Datum, 2),
		retFieldTypes:   []*types.FieldType{ftInt, ftVal},
	}

	row, err := iter.Next()
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Len(t, row, 2)
	require.Equal(t, int64(1), row[0].GetInt64())
	require.Equal(t, int64(100), row[1].GetInt64())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(2), row[0].GetInt64())
	require.Equal(t, int64(200), row[1].GetInt64())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(3), row[0].GetInt64())
	require.Equal(t, int64(300), row[1].GetInt64())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Nil(t, row)
}

func TestMemCachedDatumIterProjectionAllCols(t *testing.T) {
	data := buildTestDatumCache([][]any{
		{int64(1), "alice", int64(100)},
		{int64(2), "bob", int64(200)},
	})

	// SELECT * — all columns in same order.
	iter := &memCachedDatumIter{
		data:            data,
		colProjection:   []int{0, 1, 2},
		cacheFieldTypes: data.FieldTypes,
		datumRow:        make([]types.Datum, 3),
		retFieldTypes:   data.FieldTypes,
	}

	row, err := iter.Next()
	require.NoError(t, err)
	require.Len(t, row, 3)
	require.Equal(t, int64(1), row[0].GetInt64())
	require.Equal(t, "alice", row[1].GetString())
	require.Equal(t, int64(100), row[2].GetInt64())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(2), row[0].GetInt64())
	require.Equal(t, "bob", row[1].GetString())
	require.Equal(t, int64(200), row[2].GetInt64())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Nil(t, row)
}

func TestMemCachedDatumIterDesc(t *testing.T) {
	data := buildTestDatumCache([][]any{
		{int64(1), "alice", int64(100)},
		{int64(2), "bob", int64(200)},
		{int64(3), "charlie", int64(300)},
	})

	iter := &memCachedDatumIter{
		data:            data,
		desc:            true,
		chunkIdx:        len(data.Chunks) - 1,
		rowIdx:          data.Chunks[len(data.Chunks)-1].NumRows() - 1,
		colProjection:   []int{0, 1, 2},
		cacheFieldTypes: data.FieldTypes,
		datumRow:        make([]types.Datum, 3),
		retFieldTypes:   data.FieldTypes,
	}

	// Descending: should yield rows 3, 2, 1.
	row, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(3), row[0].GetInt64())
	require.Equal(t, "charlie", row[1].GetString())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(2), row[0].GetInt64())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(1), row[0].GetInt64())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Nil(t, row)
}

func TestMemCachedDatumIterDescWithFilter(t *testing.T) {
	sctx := mock.NewContext()

	data := buildTestDatumCache([][]any{
		{int64(1), "alice", int64(100)},
		{int64(2), "bob", int64(200)},
		{int64(3), "charlie", int64(300)},
		{int64(4), "dave", int64(400)},
	})

	// Filter: val < 300 (should match rows 1 and 2).
	ftVal := types.NewFieldType(mysql.TypeLonglong)
	// Projected columns: id (idx 0), val (idx 1).
	col := &expression.Column{UniqueID: 1, Index: 1, RetType: ftVal} // val is at projected index 1
	constVal := &expression.Constant{Value: types.NewIntDatum(300), RetType: ftVal}
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	filter, err := expression.NewFunction(sctx.GetExprCtx(), ast.LT, tinyTp, col, constVal)
	require.NoError(t, err)

	ftInt := types.NewFieldType(mysql.TypeLonglong)
	iter := &memCachedDatumIter{
		data:            data,
		desc:            true,
		chunkIdx:        len(data.Chunks) - 1,
		rowIdx:          data.Chunks[len(data.Chunks)-1].NumRows() - 1,
		colProjection:   []int{0, 2}, // query col 0 -> id, query col 1 -> val
		cacheFieldTypes: data.FieldTypes,
		datumRow:        make([]types.Datum, 2),
		retFieldTypes:   []*types.FieldType{ftInt, ftVal},
		conditions:      []expression.Expression{filter},
		evalCtx:         sctx.GetExprCtx().GetEvalCtx(),
	}

	// DESC + filter: row 4 (val=400) skipped, row 3 (val=300) skipped, row 2 (val=200) matched, row 1 (val=100) matched.
	row, err := iter.Next()
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, int64(2), row[0].GetInt64())   // id=2
	require.Equal(t, int64(200), row[1].GetInt64()) // val=200

	row, err = iter.Next()
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, int64(1), row[0].GetInt64())   // id=1
	require.Equal(t, int64(100), row[1].GetInt64()) // val=100

	row, err = iter.Next()
	require.NoError(t, err)
	require.Nil(t, row)
}

func TestMemCachedDatumIterProjectionWithTimestamp(t *testing.T) {
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftStr := types.NewFieldType(mysql.TypeVarchar)
	ftStr.SetFlen(64)
	ftTs := types.NewFieldType(mysql.TypeTimestamp)
	ftTs.SetDecimal(0)
	cacheFieldTypes := []*types.FieldType{ftInt, ftStr, ftTs}

	// Build chunk with TIMESTAMP stored in UTC.
	chk := chunk.New(cacheFieldTypes, 1024, 1024)
	utcTime1, err := types.ParseTimestamp(types.DefaultStmtNoWarningContext, "2025-01-15 10:00:00")
	require.NoError(t, err)
	utcTime2, err := types.ParseTimestamp(types.DefaultStmtNoWarningContext, "2025-06-20 18:30:00")
	require.NoError(t, err)

	chk.AppendInt64(0, 1)
	chk.AppendString(1, "alice")
	chk.AppendTime(2, utcTime1)
	chk.AppendInt64(0, 2)
	chk.AppendString(1, "bob")
	chk.AppendTime(2, utcTime2)

	data := &tables.CachedDatumData{
		Chunks:     []*chunk.Chunk{chk},
		FieldTypes: cacheFieldTypes,
		TotalRows:  2,
	}

	sessionLoc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)

	// SELECT id, ts (skip name) — project columns 0 and 2.
	// TIMESTAMP at projected index 1.
	iter := &memCachedDatumIter{
		data:            data,
		colProjection:   []int{0, 2},
		cacheFieldTypes: cacheFieldTypes,
		datumRow:        make([]types.Datum, 2),
		retFieldTypes:   []*types.FieldType{ftInt, ftTs},
		needTZConvert:   true,
		sessionLoc:      sessionLoc,
		tsColProjected:  []int{1}, // projected index 1 is the TIMESTAMP column
	}

	row, err := iter.Next()
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, int64(1), row[0].GetInt64())
	// UTC 10:00:00 → Asia/Shanghai +8 → 18:00:00
	ts := row[1].GetMysqlTime()
	require.Equal(t, "2025-01-15 18:00:00", ts.String())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(2), row[0].GetInt64())
	// UTC 18:30:00 → Asia/Shanghai +8 → next day 02:30:00
	ts = row[1].GetMysqlTime()
	require.Equal(t, "2025-06-21 02:30:00", ts.String())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Nil(t, row)
}

func TestMemCachedDatumIterDescMultiChunk(t *testing.T) {
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftStr := types.NewFieldType(mysql.TypeVarchar)
	ftStr.SetFlen(64)
	ftVal := types.NewFieldType(mysql.TypeLonglong)
	fieldTypes := []*types.FieldType{ftInt, ftStr, ftVal}

	// Create 2 chunks to test cross-chunk descending iteration.
	chk1 := chunk.New(fieldTypes, 1024, 1024)
	chk1.AppendInt64(0, 1)
	chk1.AppendString(1, "a")
	chk1.AppendInt64(2, 10)
	chk1.AppendInt64(0, 2)
	chk1.AppendString(1, "b")
	chk1.AppendInt64(2, 20)

	chk2 := chunk.New(fieldTypes, 1024, 1024)
	chk2.AppendInt64(0, 3)
	chk2.AppendString(1, "c")
	chk2.AppendInt64(2, 30)

	data := &tables.CachedDatumData{
		Chunks:     []*chunk.Chunk{chk1, chk2},
		FieldTypes: fieldTypes,
		TotalRows:  3,
	}

	iter := &memCachedDatumIter{
		data:            data,
		desc:            true,
		chunkIdx:        1,           // last chunk
		rowIdx:          0,           // last chunk has 1 row, index 0
		colProjection:   []int{0, 2}, // id and val only
		cacheFieldTypes: fieldTypes,
		datumRow:        make([]types.Datum, 2),
		retFieldTypes:   []*types.FieldType{ftInt, ftVal},
	}

	// DESC across chunks: 3, 2, 1.
	row, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(3), row[0].GetInt64())
	require.Equal(t, int64(30), row[1].GetInt64())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(2), row[0].GetInt64())
	require.Equal(t, int64(20), row[1].GetInt64())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, int64(1), row[0].GetInt64())
	require.Equal(t, int64(10), row[1].GetInt64())

	row, err = iter.Next()
	require.NoError(t, err)
	require.Nil(t, row)
}

func TestMemTableReaderDatumCacheFallbackOnTxnOverride(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Close()
	})

	sctx := mock.NewContext()
	sctx.Store = store

	const tableID int64 = 1

	ftID := types.NewFieldType(mysql.TypeLonglong)
	ftName := types.NewFieldType(mysql.TypeVarchar)
	ftName.SetFlen(64)
	ftVal := types.NewFieldType(mysql.TypeLonglong)
	retFieldTypes := []*types.FieldType{ftID, ftName, ftVal}

	colIDExpr := &expression.Column{ID: 1, UniqueID: 1, Index: 0, RetType: ftID}
	colNameExpr := &expression.Column{ID: 2, UniqueID: 2, Index: 1, RetType: ftName}
	colValExpr := &expression.Column{ID: 3, UniqueID: 3, Index: 2, RetType: ftVal}
	schema := expression.NewSchema(colIDExpr, colNameExpr, colValExpr)

	col1Info := &model.ColumnInfo{ID: 1, Offset: 0, State: model.StatePublic, FieldType: *ftID}
	col2Info := &model.ColumnInfo{ID: 2, Offset: 1, State: model.StatePublic, FieldType: *ftName}
	col3Info := &model.ColumnInfo{ID: 3, Offset: 2, State: model.StatePublic, FieldType: *ftVal}
	tblInfo := &model.TableInfo{ID: tableID, Columns: []*model.ColumnInfo{col1Info, col2Info, col3Info}}

	cd := NewRowDecoder(sctx, schema, tblInfo)

	buffTxn, err := store.Begin(tikv.WithStartTS(0))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = buffTxn.Rollback()
	})
	cacheTable := buffTxn.GetMemBuffer()

	var encoder rowcodec.Encoder
	colIDs := []int64{1, 2, 3}
	loc := sctx.GetSessionVars().Location()

	key := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(1))
	datums := []types.Datum{types.NewIntDatum(1), types.NewStringDatum("alice"), types.NewIntDatum(100)}
	valSnap, err := encoder.Encode(loc, colIDs, datums, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cacheTable.Set(key, append([]byte(nil), valSnap...)))

	txn, err := sctx.Txn(true)
	require.NoError(t, err)
	datums[2] = types.NewIntDatum(200)
	valTxn, err := encoder.Encode(loc, colIDs, datums, nil, nil)
	require.NoError(t, err)
	require.NoError(t, txn.GetMemBuffer().Set(key, append([]byte(nil), valTxn...)))

	recordPrefix := tablecodec.GenTableRecordPrefix(tableID)
	kvRanges := []kv.KeyRange{{StartKey: recordPrefix, EndKey: recordPrefix.PrefixNext()}}

	memTblReader := &memTableReader{
		ctx:           sctx,
		table:         tblInfo,
		columns:       []*model.ColumnInfo{col1Info, col2Info, col3Info},
		kvRanges:      kvRanges,
		retFieldTypes: retFieldTypes,
		colIDs:        map[int64]int{1: 0, 2: 1, 3: 2},
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			cd:          cd,
		},
		cacheTable: cacheTable,
		datumCache: buildTestDatumCache([][]any{{int64(1), "alice", int64(100)}}),
	}

	it, err := memTblReader.getMemRowsIter(context.Background())
	require.NoError(t, err)
	defer it.Close()

	_, isDatumIter := it.(*memCachedDatumIter)
	require.False(t, isDatumIter)

	row, err := it.Next()
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, int64(1), row[0].GetInt64())
	require.Equal(t, "alice", row[1].GetString())
	require.Equal(t, int64(200), row[2].GetInt64())

	row, err = it.Next()
	require.NoError(t, err)
	require.Nil(t, row)
}
