// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apache/arrow-go/v18/parquet/file"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/stretchr/testify/require"
)

func TestTableDataReaderBackpressureAndClose(t *testing.T) {
	tctx, cancel := tcontext.Background().WithCancel()
	defer cancel()
	meta := newMockTableIR("test", "bounded", nil, nil, []string{"TEXT"})
	iter := &readerBackpressureIter{full: make(chan struct{}), value: make([]byte, readBatchBytes+1), args: make([]any, 1)}
	data := &readerIteratorData{mockTableIR: meta, iter: iter}
	reader := newTableDataReader(tctx, cancel, meta, data)

	select {
	case <-iter.full:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not fill its bounded pool")
	}
	// Each oversized row occupies one batch. The source's next HasNext call
	// happens after all batches are queued, before waiting for a free batch.
	require.Empty(t, reader.pool)
	require.Len(t, reader.results, readBatchCount)
	closed := make(chan error, 1)
	go func() { closed <- reader.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel a producer waiting for a free batch")
	}
	require.Equal(t, readBatchCount, iter.index, "source must not advance without a free batch")
	require.False(t, data.closed, "reader must join before its caller closes the source")
	require.NoError(t, data.Close())
	require.True(t, data.closed)
}

func TestTableDataReaderTerminalErrorAfterBufferedRows(t *testing.T) {
	for _, count := range []int{readBatchRows - 1, readBatchRows, readBatchRows + 1} {
		for _, stage := range []string{"decode", "next"} {
			t.Run(stage+"/"+strconv.Itoa(count), func(t *testing.T) {
				tctx, cancel := tcontext.Background().WithCancel()
				defer cancel()
				sourceErr := errors.New("terminal source failure")
				values := make([][]byte, count+1)
				for i := range values {
					values[i] = []byte("row")
				}
				iter := &workerTestRowIter{ctx: tctx, values: values, args: make([]any, 1), errorIndex: count}
				if stage == "decode" {
					iter.decodeErr = sourceErr
				} else {
					iter.readErr = sourceErr
				}
				meta := newMockTableIR("test", "terminal", nil, nil, []string{"TEXT"})
				reader := newTableDataReader(tctx, cancel, meta, &readerIteratorData{mockTableIR: meta, iter: iter})
				t.Cleanup(func() { require.NoError(t, reader.Close()) })
				row := MakeRowReceiver(meta.ColumnTypes())
				for range count {
					require.True(t, reader.HasNext())
					require.NoError(t, reader.Error(), "file rotation must not expose a future source error")
					require.NoError(t, reader.Decode(row))
					require.Equal(t, "row", string(row.GetRawBytes()[0]))
					reader.Next()
				}
				if stage == "decode" {
					require.True(t, reader.HasNext(), "a decode failure belongs to the current source row")
					require.NoError(t, reader.Error())
					require.ErrorIs(t, reader.Decode(row), sourceErr)
					reader.Next()
				}
				require.False(t, reader.HasNext())
				require.ErrorIs(t, reader.Error(), sourceErr)
			})
		}
	}
}

func TestWriterTerminalErrorAcrossFileBoundary(t *testing.T) {
	conf := defaultConfigForTest(t)
	conf.OutputDirPath = t.TempDir()
	conf.FileType = FileFormatCSVString
	conf.NoHeader = true
	conf.CsvLineTerminator = "\n"
	conf.FileSize = readBatchRows * 4 // One batch of CSV rows per file.
	w := createTestWriter(conf, t)
	sourceErr := errors.New("source failed after the second file's first row")
	meta := newMockTableIR("test", "boundary", nil, nil, []string{"TEXT"})
	data := &workerTestTableData{mockTableIR: meta, newIter: func(int) *workerTestRowIter {
		values := make([][]byte, readBatchRows+2)
		for i := range values {
			values[i] = []byte("r")
		}
		return &workerTestRowIter{values: values, errorIndex: readBatchRows + 1, readErr: sourceErr}
	}}
	require.ErrorIs(t, w.WriteTableData(meta, data, 0), sourceErr)
	output := workerTestOutput(t, conf.OutputDirPath)
	require.Equal(t, []string{strings.Repeat("\"r\"\n", readBatchRows), "\"r\"\n"}, output)
}

func TestWriterEmptyValuesRemainNonNull(t *testing.T) {
	for _, format := range []string{FileFormatSQLTextString, FileFormatCSVString} {
		t.Run(format, func(t *testing.T) {
			conf := defaultConfigForTest(t)
			conf.OutputDirPath = t.TempDir()
			conf.FileType = format
			conf.NoHeader = true
			conf.CsvLineTerminator = "\n"
			conf.StatementSize = UnspecifiedSize
			w := createTestWriter(conf, t)
			meta := newMockTableIR("test", "empty", nil, nil, []string{"TEXT"})
			data := &workerTestTableData{mockTableIR: meta, newIter: func(int) *workerTestRowIter {
				values := make([][]byte, readBatchRows+1)
				for i := range values {
					values[i] = []byte{}
				}
				return &workerTestRowIter{values: values}
			}}
			require.NoError(t, w.WriteTableData(meta, data, 0))
			if format == FileFormatCSVString {
				require.Equal(t, []string{strings.Repeat("\"\"\n", readBatchRows+1)}, workerTestOutput(t, conf.OutputDirPath))
			} else {
				expected := "INSERT INTO `empty` VALUES\n" + strings.Repeat("(''),\n", readBatchRows) + "('');\n"
				require.Equal(t, []string{expected}, workerTestOutput(t, conf.OutputDirPath))
			}
		})
	}
}

func TestWriterAllGeneratedColumnsAcrossBatches(t *testing.T) {
	const count = 2*readBatchRows + 1
	for _, format := range []string{FileFormatSQLTextString, FileFormatCSVString, FileFormatParquetString} {
		t.Run(format, func(t *testing.T) {
			conf := defaultConfigForTest(t)
			conf.OutputDirPath = t.TempDir()
			conf.FileType = format
			conf.StatementSize = UnspecifiedSize
			conf.CsvLineTerminator = "\n"
			w := createTestWriter(conf, t)
			meta := newMockTableIR("test", "generated", nil, nil, []string{"INT"})
			meta.selectedField = ""
			meta.columnInfos = []*ColumnInfo{{Name: "generated", DatabaseTypeName: "INT"}}
			data := &workerTestTableData{mockTableIR: meta, newIter: func(int) *workerTestRowIter {
				return &workerTestRowIter{
					values: make([][]byte, count), decodeErr: errors.New("generated-only source must not be decoded"),
				}
			}}
			require.NoError(t, w.WriteTableData(meta, data, 0))
			require.Equal(t, count, data.iter.index)
			require.True(t, data.iter.closed)
			require.EqualValues(t, count, ReadGauge(w.metrics.finishedRowsGauge))
			output := workerTestOutput(t, conf.OutputDirPath)
			require.Len(t, output, 1)
			switch format {
			case FileFormatSQLTextString:
				require.Equal(t, "INSERT INTO `generated` VALUES\n"+strings.Repeat("(),\n", count-1)+"();\n", output[0])
			case FileFormatCSVString:
				require.Equal(t, strings.Repeat("\n", count), output[0])
			case FileFormatParquetString:
				parquet, err := file.NewParquetReader(bytes.NewReader([]byte(output[0])))
				require.NoError(t, err)
				defer parquet.Close()
				require.Zero(t, parquet.NumRows(), "preserve generated-only parquet behavior")
			}
		})
	}
}

func TestTableDataReaderMultiQueries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	mock.ExpectQuery("SELECT first").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("first")).RowsWillBeClosed()
	mock.ExpectQuery("SELECT empty").WillReturnRows(sqlmock.NewRows([]string{"v"})).RowsWillBeClosed()
	mock.ExpectQuery("SELECT last").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("last")).RowsWillBeClosed()
	tctx, cancel := tcontext.Background().WithCancel()
	defer cancel()
	source := newMultiQueriesChunk([]string{"SELECT first", "SELECT empty", "SELECT last"}, 1)
	require.NoError(t, source.Start(tctx, conn))
	meta := newMockTableIR("test", "queries", nil, nil, []string{"TEXT"})
	reader := newTableDataReader(tctx, cancel, meta, source)
	defer func() {
		require.NoError(t, reader.Close())
		require.NoError(t, source.Close())
	}()
	row := MakeRowReceiver(meta.ColumnTypes())
	var values []string
	for reader.HasNext() {
		require.NoError(t, reader.Decode(row))
		values = append(values, string(row.GetRawBytes()[0]))
		reader.Next()
	}
	require.NoError(t, reader.Error())
	require.Equal(t, []string{"first", "last"}, values)
	require.NoError(t, mock.ExpectationsWereMet())
}

type readerIteratorData struct {
	*mockTableIR
	iter   SQLRowIter
	closed bool
}

func (d *readerIteratorData) Rows() SQLRowIter { return d.iter }
func (d *readerIteratorData) Close() error {
	d.closed = true
	return d.iter.Close()
}

type readerBackpressureIter struct {
	index int
	value []byte
	full  chan struct{}
	args  []any
}

func (i *readerBackpressureIter) HasNext() bool {
	if i.index == readBatchCount {
		close(i.full)
	}
	return true
}

func (i *readerBackpressureIter) Decode(row RowReceiver) error {
	row.BindAddress(i.args)
	*i.args[0].(*sql.RawBytes) = i.value
	return nil
}

func (i *readerBackpressureIter) Next()      { i.index++ }
func (*readerBackpressureIter) Error() error { return nil }
func (*readerBackpressureIter) Close() error { return nil }
