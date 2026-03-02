package executor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/store/mockstore"
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
