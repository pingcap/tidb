// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

type simpleRowReceiver struct {
	data []string
}

func newSimpleRowReceiver(length int) *simpleRowReceiver {
	return &simpleRowReceiver{data: make([]string, length)}
}

func (s *simpleRowReceiver) BindAddress(args []interface{}) {
	for i := range args {
		args[i] = &s.data[i]
	}
}

func TestRowIter(t *testing.T) {
	t.Parallel()

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
	for i := 0; i < 100; i++ {
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
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	twentyBytes := strings.Repeat("x", 20)
	thirtyBytes := strings.Repeat("x", 30)
	expectedRows := mock.NewRows([]string{"a", "b"})
	for i := 0; i < 10; i++ {
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
	wp := newWriterPipe(nil, testFileSize, testStatementSize, nil)

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
