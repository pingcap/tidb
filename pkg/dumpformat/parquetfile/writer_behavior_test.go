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
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

var errInjectedSinkWriteFailure = errors.New("forced sink write failure")

type writeFailingCloser struct {
	failWrites bool
	closeCalls int
}

func (w *writeFailingCloser) Write(p []byte) (int, error) {
	if w.failWrites {
		return 0, errInjectedSinkWriteFailure
	}
	return len(p), nil
}

func (w *writeFailingCloser) Close() error {
	w.closeCalls++
	return nil
}

func TestParquetWriterCopiesByteRowsBeforeClose(t *testing.T) {
	var buf bytes.Buffer
	pw, err := NewParquetWriter(&buf, []*ColumnInfo{
		{Name: "name", DatabaseTypeName: "VARCHAR"},
	})
	require.NoError(t, err)

	value := sql.RawBytes("before")
	require.NoError(t, pw.Write([]sql.RawBytes{value}))
	copy(value, "after!")
	require.NoError(t, pw.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()

	readByteArrayColumn(t, reader.RowGroup(0), 0, 1, []string{"before"}, []int16{0}, 1)

	t.Run("Close without buffered rows is safe", func(t *testing.T) {
		var closeBuf bytes.Buffer
		closePW, err := NewParquetWriter(&closeBuf, []*ColumnInfo{
			{Name: "name", DatabaseTypeName: "VARCHAR"},
		})
		require.NoError(t, err)
		require.NoError(t, closePW.Close())
		require.NoError(t, closePW.Close())
		require.ErrorContains(t, closePW.Write([]sql.RawBytes{sql.RawBytes("x")}), "parquet writer is closed")
	})
}

func TestParquetWriterFlushesRowGroupByMemoryLimit(t *testing.T) {
	localOptions := newWriterOptions([]WriterOption{
		WithCompression(compress.Codecs.Uncompressed),
		WithDataPageSize(2048),
		WithRowGroupMemoryLimit(4),
	})
	props := parquet.NewWriterProperties(localOptions.writerProperties...)
	require.Equal(t, compress.Codecs.Uncompressed, props.Compression())
	require.EqualValues(t, 2048, props.DataPageSize())
	require.EqualValues(t, 4, localOptions.rowGroupMemoryLimitBytes)

	defaultOptions := defaultWriterOptions()
	defaultProps := parquet.NewWriterProperties(defaultOptions.writerProperties...)
	require.Equal(t, defaultCompressionType, defaultProps.Compression())
	require.EqualValues(t, defaultRowGroupMemoryLimitBytes, defaultOptions.rowGroupMemoryLimitBytes)

	t.Run("flushes row group by accounted memory bytes", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriter(&buf, []*ColumnInfo{
			{Name: "name", DatabaseTypeName: "VARCHAR"},
		}, WithRowGroupMemoryLimit(4))
		require.NoError(t, err)

		require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("abcd")}))
		require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("efgh")}))
		require.NoError(t, pw.Close())

		reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		defer reader.Close()

		require.EqualValues(t, 2, reader.NumRows())
		require.Equal(t, 2, reader.NumRowGroups())
	})

	t.Run("accounts byte-array slice header memory", func(t *testing.T) {
		col := column{columnType: columnType{Physical: parquet.Types.ByteArray}}
		require.EqualValues(
			t,
			byteArraySliceHeaderBytes+4,
			accountColumnValueMemoryBytes(col, parquet.ByteArray([]byte("abcd"))),
		)
	})

	t.Run("accounts fixed-len byte-array slice header memory", func(t *testing.T) {
		col := column{columnType: columnType{Physical: parquet.Types.FixedLenByteArray}}
		require.EqualValues(
			t,
			fixedLenByteArraySliceHeaderBytes+6,
			accountColumnValueMemoryBytes(col, parquet.FixedLenByteArray([]byte("abcdef"))),
		)
	})

	t.Run("accounts primitive and unknown physical types", func(t *testing.T) {
		require.EqualValues(t, 1, accountColumnValueMemoryBytes(column{
			columnType: columnType{Physical: parquet.Types.Boolean},
		}, true))
		require.EqualValues(t, 4, accountColumnValueMemoryBytes(column{
			columnType: columnType{Physical: parquet.Types.Int32},
		}, int32(1)))
		require.EqualValues(t, 8, accountColumnValueMemoryBytes(column{
			columnType: columnType{Physical: parquet.Types.Int64},
		}, int64(1)))
		require.EqualValues(t, 0, accountColumnValueMemoryBytes(column{
			columnType: columnType{Physical: parquet.Types.Int96},
		}, nil))
	})

	t.Run("estimates written bytes plus buffered bytes", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriter(&buf, []*ColumnInfo{
			{Name: "name", DatabaseTypeName: "VARCHAR"},
		}, WithRowGroupMemoryLimit(defaultRowGroupMemoryLimitBytes))
		require.NoError(t, err)

		require.Equal(t, uint64(pw.totalWrittenBytes()), pw.EstimateFileSize())

		require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("abcd")}))
		expected := pw.totalWrittenBytes() + pw.bufferedMemoryBytes
		require.Greater(t, pw.bufferedMemoryBytes, int64(0))
		require.Equal(t, uint64(expected), pw.EstimateFileSize())

		require.NoError(t, pw.flushRows())
		require.Equal(t, uint64(pw.totalWrittenBytes()), pw.EstimateFileSize())
		require.NoError(t, pw.Close())
	})

	t.Run("Close still closes sink when flush fails", func(t *testing.T) {
		sink := &writeFailingCloser{}
		pw, err := NewParquetWriter(sink, []*ColumnInfo{
			{Name: "name", DatabaseTypeName: "VARCHAR"},
		})
		require.NoError(t, err)
		require.NoError(t, pw.Write([]sql.RawBytes{sql.RawBytes("alice")}))

		sink.failWrites = true
		err = pw.Close()
		require.ErrorContains(t, err, errInjectedSinkWriteFailure.Error())
		require.Equal(t, 1, sink.closeCalls)

		require.NoError(t, pw.Close())
		require.Equal(t, 1, sink.closeCalls)
	})
}

func BenchmarkParquetWriterParseAndAppendRow(b *testing.B) {
	pw, err := NewParquetWriter(io.Discard, []*ColumnInfo{
		{Name: "id", DatabaseTypeName: "INT"},
	})
	if err != nil {
		b.Fatal(err)
	}

	row := []sql.RawBytes{sql.RawBytes("123")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pw.parseAndAppendRow(row); err != nil {
			b.Fatal(err)
		}
		// Reset row/accounting state to keep the benchmark focused on per-row
		// parse+append work.
		pw.buffers[0].reset()
		pw.bufferedRows = 0
		pw.bufferedMemoryBytes = 0
	}
}

func TestParquetWriterRecoversAfterRowConversionError(t *testing.T) {
	var buf bytes.Buffer
	pw, err := NewParquetWriter(&buf, []*ColumnInfo{
		{Name: "id", DatabaseTypeName: "INT"},
		{Name: "flag", DatabaseTypeName: "INT"},
	})
	require.NoError(t, err)

	require.NoError(t, pw.Write([]sql.RawBytes{
		sql.RawBytes("1"),
		sql.RawBytes("10"),
	}))
	err = pw.Write([]sql.RawBytes{
		sql.RawBytes("2"),
		sql.RawBytes("bad-int"),
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "convert parquet column flag")
	require.Error(t, pw.Close())

	t.Run("Write validates row length and required NULL", func(t *testing.T) {
		var localBuf bytes.Buffer
		localPW, err := NewParquetWriter(&localBuf, []*ColumnInfo{
			{Name: "id", DatabaseTypeName: "INT"},
			{Name: "name", DatabaseTypeName: "VARCHAR"},
		})
		require.NoError(t, err)

		err = localPW.Write([]sql.RawBytes{
			sql.RawBytes("1"),
		})
		require.ErrorContains(t, err, "parquet row has 1 values, expected 2")

		err = localPW.Write([]sql.RawBytes{
			nil,
			sql.RawBytes("alice"),
		})
		require.ErrorContains(t, err, "required column receives NULL")

		require.NoError(t, localPW.Close())
	})

	t.Run("newColumnBuffer and appendColumnValue validate unsupported cases", func(t *testing.T) {
		_, err := newColumnBuffer(column{
			ColumnInfo: ColumnInfo{Name: "f"},
			columnType: columnType{
				Physical:   parquet.Types.FixedLenByteArray,
				TypeLength: 0,
			},
		}, 1)
		require.ErrorContains(t, err, "invalid fixed-size byte width")

		_, err = newColumnBuffer(column{
			ColumnInfo: ColumnInfo{Name: "u"},
			columnType: columnType{
				Physical: parquet.Types.Int96,
			},
		}, 1)
		require.ErrorContains(t, err, "unsupported parquet physical type")

		_, err = newColumnBuffers([]column{
			{
				ColumnInfo: ColumnInfo{Name: "bad"},
				columnType: columnType{
					Physical:   parquet.Types.FixedLenByteArray,
					TypeLength: 0,
				},
			},
		}, 1)
		require.ErrorContains(t, err, "init parquet buffer for column bad")

		err = appendColumnValue(&columnBuffer{}, column{
			columnType: columnType{Physical: parquet.Types.Int96},
		}, nil)
		require.ErrorContains(t, err, "unsupported parquet physical type")
	})

	t.Run("newColumnBuffer initializes supported float and double columns", func(t *testing.T) {
		floatBuffer, err := newColumnBuffer(column{
			ColumnInfo: ColumnInfo{Name: "f"},
			columnType: columnType{
				Physical: parquet.Types.Float,
			},
		}, 2)
		require.NoError(t, err)
		require.NotNil(t, floatBuffer.float32Values)

		doubleBuffer, err := newColumnBuffer(column{
			ColumnInfo: ColumnInfo{Name: "d"},
			columnType: columnType{
				Physical: parquet.Types.Double,
			},
		}, 2)
		require.NoError(t, err)
		require.NotNil(t, doubleBuffer.float64Values)
	})

	t.Run("writeColumnBatch handles float32 and float64 writers", func(t *testing.T) {
		newFloatSchema := func(physical parquet.Type, name string) *schema.GroupNode {
			field, err := schema.NewPrimitiveNode(name, parquet.Repetitions.Required, physical, -1, -1)
			require.NoError(t, err)
			root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, []schema.Node{field}, -1)
			require.NoError(t, err)
			return root
		}

		t.Run("float32 writer", func(t *testing.T) {
			var out bytes.Buffer
			writer := file.NewParquetWriter(&out, newFloatSchema(parquet.Types.Float, "f32"))
			rowGroupWriter := writer.AppendRowGroup()
			columnWriter, err := rowGroupWriter.NextColumn()
			require.NoError(t, err)

			err = writeColumnBatch(columnWriter, column{
				columnType: columnType{Physical: parquet.Types.Float},
			}, columnBuffer{
				float32Values: []float32{1.5},
			})
			require.NoError(t, err)
			require.NoError(t, columnWriter.Close())
			require.NoError(t, rowGroupWriter.Close())
			require.NoError(t, writer.Close())
		})

		t.Run("float64 writer", func(t *testing.T) {
			var out bytes.Buffer
			writer := file.NewParquetWriter(&out, newFloatSchema(parquet.Types.Double, "f64"))
			rowGroupWriter := writer.AppendRowGroup()
			columnWriter, err := rowGroupWriter.NextColumn()
			require.NoError(t, err)

			err = writeColumnBatch(columnWriter, column{
				columnType: columnType{Physical: parquet.Types.Double},
			}, columnBuffer{
				float64Values: []float64{2.5},
			})
			require.NoError(t, err)
			require.NoError(t, columnWriter.Close())
			require.NoError(t, rowGroupWriter.Close())
			require.NoError(t, writer.Close())
		})
	})

	t.Run("writeColumnBatch returns error for unsupported concrete writer type", func(t *testing.T) {
		var out bytes.Buffer
		pw, err := NewParquetWriter(&out, []*ColumnInfo{
			{Name: "id", DatabaseTypeName: "INT"},
		})
		require.NoError(t, err)

		rowGroupWriter := pw.writer.AppendRowGroup()
		columnWriter, err := rowGroupWriter.NextColumn()
		require.NoError(t, err)

		err = writeColumnBatch(wrappedColumnChunkWriter{ColumnChunkWriter: columnWriter}, column{
			columnType: columnType{Physical: parquet.Types.Int32},
		}, columnBuffer{
			int32Values: []int32{1},
		})
		require.ErrorContains(t, err, "unsupported column chunk writer")

		require.NoError(t, columnWriter.Close())
		require.NoError(t, rowGroupWriter.Close())
		require.NoError(t, pw.writer.Close())
	})
}
