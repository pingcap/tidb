// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/stretchr/testify/require"
)

type simpleRowReceiver struct {
	data []string
}

func newSimpleRowReceiver(length int) *simpleRowReceiver {
	return &simpleRowReceiver{data: make([]string, length)}
}

func (s *simpleRowReceiver) BindAddress(args []any) {
	for i := range args {
		args[i] = &s.data[i]
	}
}

func TestRowIter(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	expectedRows := mock.NewRows([]string{"id"}).
		AddRow("1").
		AddRow("2").
		AddRow("3")
	mock.ExpectQuery("SELECT id from t").WillReturnRows(expectedRows)
	rows, err := db.Query("SELECT id from t")
	require.NoError(t, err)

	iter := newRowIter(rows, 1)
	for range 100 {
		require.True(t, iter.HasNext())
	}

	res := newSimpleRowReceiver(1)
	require.NoError(t, iter.Decode(res))
	require.Equal(t, []string{"1"}, res.data)

	iter.Next()
	require.True(t, iter.HasNext())
	require.True(t, iter.HasNext())
	require.NoError(t, iter.Decode(res))
	require.Equal(t, []string{"2"}, res.data)

	iter.Next()
	require.True(t, iter.HasNext())
	require.NoError(t, iter.Decode(res))

	iter.Next()
	require.Equal(t, []string{"3"}, res.data)
	require.False(t, iter.HasNext())
}

func TestChunkRowIter(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	twentyBytes := strings.Repeat("x", 20)
	thirtyBytes := strings.Repeat("x", 30)
	expectedRows := mock.NewRows([]string{"a", "b"})
	for range 10 {
		expectedRows.AddRow(twentyBytes, thirtyBytes)
	}
	mock.ExpectQuery("SELECT a, b FROM t").WillReturnRows(expectedRows)
	rows, err := db.Query("SELECT a, b FROM t")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	var (
		testFileSize      uint64 = 200
		testStatementSize uint64 = 101

		expectedSize = [][]uint64{
			{50, 50},
			{100, 100},
			{150, 150},
			{200, 50},
		}
	)

	sqlRowIter := newRowIter(rows, 2)

	res := newSimpleRowReceiver(2)
	metrics := newMetrics(promutil.NewDefaultFactory(), nil)
	wp := newWriterPipe(nil, testFileSize, testStatementSize, metrics, nil)

	var resSize [][]uint64
	for sqlRowIter.HasNext() {
		wp.currentStatementSize = 0
		for sqlRowIter.HasNext() {
			require.NoError(t, sqlRowIter.Decode(res))
			sz := uint64(len(res.data[0]) + len(res.data[1]))
			wp.AddFileSize(sz)
			sqlRowIter.Next()
			resSize = append(resSize, []uint64{wp.currentFileSize, wp.currentStatementSize})
			if wp.ShouldSwitchStatement() {
				break
			}
		}
		if wp.ShouldSwitchFile() {
			break
		}
	}

	require.Equal(t, expectedSize, resSize)
	require.True(t, sqlRowIter.HasNext())
	require.True(t, wp.ShouldSwitchFile())
	require.True(t, wp.ShouldSwitchStatement())
	require.NoError(t, rows.Close())
	require.Error(t, sqlRowIter.Decode(res))
	sqlRowIter.Next()
}

func TestRowIterWithStringKeyProgress(t *testing.T) {
	// Test row iteration with progress tracking for string key chunking

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Simulate a chunk from streaming string key chunking
	expectedRows := mock.NewRows([]string{"id", "data"}).
		AddRow("key_001", "data1").
		AddRow("key_002", "data2").
		AddRow("key_003", "data3")

	mock.ExpectQuery("SELECT id, data FROM table WHERE.*").WillReturnRows(expectedRows)
	rows, err := db.Query("SELECT id, data FROM table WHERE id >= 'key_001' AND id < 'key_100'")
	require.NoError(t, err)

	iter := newRowIter(rows, 2)

	// Test that iteration works with string-based chunking results
	rowCount := 0
	res := newSimpleRowReceiver(2)

	for iter.HasNext() {
		require.NoError(t, iter.Decode(res))
		require.True(t, strings.HasPrefix(res.data[0], "key_"), "Should have key prefix")
		require.True(t, strings.HasPrefix(res.data[1], "data"), "Should have data prefix")
		iter.Next()
		rowCount++
	}

	require.Equal(t, 3, rowCount, "Should process all rows in the chunk")
	require.False(t, iter.HasNext(), "Should reach end of iteration")
	require.NoError(t, iter.Close(), "Should close iterator cleanly")
}
