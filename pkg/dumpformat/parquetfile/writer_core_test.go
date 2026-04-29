// Copyright 2026 PingCAP, Inc.
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

package parquetfile

import (
	"bytes"
	"database/sql"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestParquetWriterWritesArrowReadableRows(t *testing.T) {
	var buf bytes.Buffer
	columns := []*ColumnInfo{
		{Name: "id", DatabaseTypeName: "INT"},
		{Name: "name", DatabaseTypeName: "VARCHAR", Nullable: true},
		{Name: "price", DatabaseTypeName: "DECIMAL", Precision: 10, Scale: 2},
		{Name: "created_at", DatabaseTypeName: "DATETIME"},
		{Name: "flag", DatabaseTypeName: "BOOLEAN", Nullable: true},
	}

	pw, err := NewParquetWriter(
		&buf,
		columns,
		WithCompression(compress.Codecs.Zstd),
		WithDataPageSize(1024),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write([]sql.RawBytes{
		sql.RawBytes("1"),
		sql.RawBytes("Alice"),
		sql.RawBytes("123.45"),
		sql.RawBytes("2024-01-02 03:04:05"),
		sql.RawBytes("true"),
	}))
	require.NoError(t, pw.Write([]sql.RawBytes{
		sql.RawBytes("2"),
		nil,
		sql.RawBytes("-0.50"),
		sql.RawBytes("0000-00-00 00:00:00"),
		sql.RawBytes("false"),
	}))
	require.NoError(t, pw.Close())
	require.NotEmpty(t, buf.Bytes())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()

	require.EqualValues(t, 2, reader.NumRows())
	require.Equal(t, 1, reader.NumRowGroups())

	metaSchema := reader.MetaData().Schema
	require.Equal(t, parquet.Types.Int32, metaSchema.Column(0).PhysicalType())
	require.EqualValues(t, 0, metaSchema.Column(0).MaxDefinitionLevel())
	require.Equal(t, parquet.Types.ByteArray, metaSchema.Column(1).PhysicalType())
	require.Equal(t, schema.ConvertedTypes.UTF8, metaSchema.Column(1).ConvertedType())
	require.EqualValues(t, 1, metaSchema.Column(1).MaxDefinitionLevel())
	require.Equal(t, parquet.Types.Int64, metaSchema.Column(2).PhysicalType())
	require.Equal(t, schema.ConvertedTypes.Decimal, metaSchema.Column(2).ConvertedType())
	require.Equal(t, parquet.Types.Int64, metaSchema.Column(3).PhysicalType())
	require.EqualValues(t, 1, metaSchema.Column(3).MaxDefinitionLevel())
	require.Equal(t, schema.ConvertedTypes.None, metaSchema.Column(3).ConvertedType())
	require.Equal(t, parquet.Types.ByteArray, metaSchema.Column(4).PhysicalType())

	rowGroup := reader.RowGroup(0)
	readInt32Column(t, rowGroup, 0, 2, []int32{1, 2}, []int16{0, 0}, 2)
	readByteArrayColumn(t, rowGroup, 1, 2, []string{"Alice"}, []int16{1, 0}, 1)
	readInt64Column(t, rowGroup, 2, 2, []int64{12345, -50}, []int16{0, 0}, 2)
	readInt64Column(t, rowGroup, 3, 2, []int64{time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC).UnixMicro()}, []int16{1, 0}, 1)
	readByteArrayColumn(t, rowGroup, 4, 2, []string{"true", "false"}, []int16{1, 1}, 2)

	t.Run("validates writer constructor inputs", func(t *testing.T) {
		_, err := NewParquetWriter(nil, columns)
		require.ErrorContains(t, err, "parquet output buffer is nil")

		_, err = NewParquetWriter(&bytes.Buffer{}, []*ColumnInfo{nil})
		require.ErrorContains(t, err, "parquet column info is nil")

		_, err = NewParquetWriter(&bytes.Buffer{}, []*ColumnInfo{{Name: "", DatabaseTypeName: "INT"}})
		require.ErrorContains(t, err, "parquet column name is empty")

		require.NotPanics(t, func() {
			_, err = NewParquetWriter(&bytes.Buffer{}, []*ColumnInfo{
				{Name: "bad_scale_gt_precision", DatabaseTypeName: "DECIMAL", Precision: 10, Scale: 11},
			})
			require.ErrorContains(t, err, "parquet decimal column bad_scale_gt_precision has invalid scale 11 for precision 10")
		})

		require.NotPanics(t, func() {
			_, err = NewParquetWriter(&bytes.Buffer{}, []*ColumnInfo{
				{Name: "bad_negative_scale", DatabaseTypeName: "DECIMAL", Precision: 10, Scale: -1},
			})
			require.ErrorContains(t, err, "parquet decimal column bad_negative_scale has invalid scale -1 for precision 10")
		})
	})

	t.Run("Close closes writer and Write fails afterwards", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriter(&buf, []*ColumnInfo{
			{Name: "id", DatabaseTypeName: "INT"},
		})
		require.NoError(t, err)
		require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("7")}))
		require.NoError(t, pw.Close())
		require.ErrorContains(t, pw.Write([]sql.RawBytes{sql.RawBytes("8")}), "parquet writer is closed")
	})
}

func TestParquetWriterMapsUnsignedBigIntAsFixedDecimal(t *testing.T) {
	var buf bytes.Buffer
	pw, err := NewParquetWriter(&buf, []*ColumnInfo{
		{Name: "u", DatabaseTypeName: "UNSIGNED BIGINT"},
	}, WithCompression(compress.Codecs.Uncompressed))
	require.NoError(t, err)
	require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("18446744073709551615")}))
	require.NoError(t, pw.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()

	col := reader.MetaData().Schema.Column(0)
	require.Equal(t, parquet.Types.FixedLenByteArray, col.PhysicalType())
	require.Equal(t, schema.ConvertedTypes.Decimal, col.ConvertedType())
	require.Equal(t, 9, col.TypeLength())

	chunkReader, err := reader.RowGroup(0).Column(0)
	require.NoError(t, err)
	fixedReader := chunkReader.(*file.FixedLenByteArrayColumnChunkReader)
	values := make([]parquet.FixedLenByteArray, 1)
	total, valuesRead, err := fixedReader.ReadBatch(1, values, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Equal(t, 1, valuesRead)
	require.Equal(t, []byte{0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, []byte(values[0]))
}
