package rowcodec

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestChunkDecoderColMapping(t *testing.T) {
	ftNullable := types.NewFieldType(mysql.TypeLonglong)
	ftNotNull1 := types.NewFieldType(mysql.TypeLonglong)
	ftNotNull1.SetFlag(ftNotNull1.GetFlag() | mysql.NotNullFlag)
	ftNotNull2 := types.NewFieldType(mysql.TypeLonglong)
	ftNotNull2.SetFlag(ftNotNull2.GetFlag() | mysql.NotNullFlag)

	cols := []ColInfo{
		{ID: 1, Ft: ftNullable}, // nullable
		{ID: 2, Ft: ftNotNull1}, // not-null
		{ID: 3, Ft: ftNotNull2}, // not-null
	}
	fts := []*types.FieldType{ftNullable, ftNotNull1, ftNotNull2}

	var encoder Encoder
	colIDs := []int64{1, 2, 3}

	// Row1: all columns are not null, build the mapping.
	row1, err := encoder.Encode(time.UTC, colIDs, []types.Datum{
		types.NewIntDatum(10),
		types.NewIntDatum(20),
		types.NewIntDatum(30),
	}, nil, nil)
	require.NoError(t, err)

	// Row2: column 1 becomes NULL, shifting the not-null segment indices.
	var nullDatum types.Datum
	nullDatum.SetNull()
	row2, err := encoder.Encode(time.UTC, colIDs, []types.Datum{
		nullDatum,
		types.NewIntDatum(22),
		types.NewIntDatum(33),
	}, nil, nil)
	require.NoError(t, err)

	// Row3: column 1 becomes not null again.
	row3, err := encoder.Encode(time.UTC, colIDs, []types.Datum{
		types.NewIntDatum(11),
		types.NewIntDatum(23),
		types.NewIntDatum(34),
	}, nil, nil)
	require.NoError(t, err)

	decoder := NewChunkDecoder(cols, []int64{-1}, nil, time.UTC)
	chk := chunk.New(fts, 0, 3)

	require.NoError(t, decoder.DecodeToChunk(row1, kv.IntHandle(-1), chk))
	require.True(t, decoder.mappingInited)
	require.Equal(t, 3, decoder.mappingRowCols)
	require.Len(t, decoder.colMapping, 3)
	require.Equal(t, -1, decoder.colMapping[0]) // nullable column should not be cached
	require.GreaterOrEqual(t, decoder.colMapping[1], 0)
	require.GreaterOrEqual(t, decoder.colMapping[2], 0)

	mappingAfterRow1 := append([]int(nil), decoder.colMapping...)

	require.NoError(t, decoder.DecodeToChunk(row2, kv.IntHandle(-1), chk))
	require.Equal(t, mappingAfterRow1, decoder.colMapping) // no rebuild; rowCols unchanged

	require.NoError(t, decoder.DecodeToChunk(row3, kv.IntHandle(-1), chk))

	require.Equal(t, 3, chk.NumRows())

	r1 := chk.GetRow(0)
	require.False(t, r1.IsNull(0))
	require.Equal(t, int64(10), r1.GetInt64(0))
	require.Equal(t, int64(20), r1.GetInt64(1))
	require.Equal(t, int64(30), r1.GetInt64(2))

	r2 := chk.GetRow(1)
	require.True(t, r2.IsNull(0))
	require.Equal(t, int64(22), r2.GetInt64(1))
	require.Equal(t, int64(33), r2.GetInt64(2))

	r3 := chk.GetRow(2)
	require.False(t, r3.IsNull(0))
	require.Equal(t, int64(11), r3.GetInt64(0))
	require.Equal(t, int64(23), r3.GetInt64(1))
	require.Equal(t, int64(34), r3.GetInt64(2))
}

func TestChunkDecoderColMappingSchemaChange(t *testing.T) {
	ft1 := types.NewFieldType(mysql.TypeLonglong)
	ft1.SetFlag(ft1.GetFlag() | mysql.NotNullFlag)
	ft2 := types.NewFieldType(mysql.TypeLonglong)
	ft2.SetFlag(ft2.GetFlag() | mysql.NotNullFlag)
	ft3 := types.NewFieldType(mysql.TypeLonglong)
	ft3.SetFlag(ft3.GetFlag() | mysql.NotNullFlag)

	cols := []ColInfo{
		{ID: 1, Ft: ft1},
		{ID: 2, Ft: ft2},
		{ID: 3, Ft: ft3},
	}
	fts := []*types.FieldType{ft1, ft2, ft3}

	defDatum := func(i int, chk *chunk.Chunk) error {
		// Default value for column 3 only.
		if i == 2 {
			chk.AppendInt64(i, 999)
			return nil
		}
		chk.AppendNull(i)
		return nil
	}

	var encoder Encoder
	// Row1: old schema, missing column 3.
	row1, err := encoder.Encode(time.UTC, []int64{1, 2}, []types.Datum{
		types.NewIntDatum(10),
		types.NewIntDatum(20),
	}, nil, nil)
	require.NoError(t, err)

	// Row2: new schema includes column 3.
	row2, err := encoder.Encode(time.UTC, []int64{1, 2, 3}, []types.Datum{
		types.NewIntDatum(11),
		types.NewIntDatum(22),
		types.NewIntDatum(33),
	}, nil, nil)
	require.NoError(t, err)

	decoder := NewChunkDecoder(cols, []int64{-1}, defDatum, time.UTC)
	chk := chunk.New(fts, 0, 2)

	require.NoError(t, decoder.DecodeToChunk(row1, kv.IntHandle(-1), chk))
	require.True(t, decoder.mappingInited)
	require.Equal(t, 2, decoder.mappingRowCols)
	require.Equal(t, -1, decoder.colMapping[2]) // col3 not found in old rows

	require.NoError(t, decoder.DecodeToChunk(row2, kv.IntHandle(-1), chk))
	require.Equal(t, 3, decoder.mappingRowCols)         // mapping rebuilt
	require.GreaterOrEqual(t, decoder.colMapping[2], 0) // col3 becomes cacheable

	require.Equal(t, 2, chk.NumRows())

	r1 := chk.GetRow(0)
	require.Equal(t, int64(10), r1.GetInt64(0))
	require.Equal(t, int64(20), r1.GetInt64(1))
	require.Equal(t, int64(999), r1.GetInt64(2)) // default

	r2 := chk.GetRow(1)
	require.Equal(t, int64(11), r2.GetInt64(0))
	require.Equal(t, int64(22), r2.GetInt64(1))
	require.Equal(t, int64(33), r2.GetInt64(2))
}
