// Copyright 2023 PingCAP, Inc.
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

package stmtsummary

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

func TestTimeRangeOverlap(t *testing.T) {
	require.False(t, timeRangeOverlap(1, 2, 3, 4))
	require.False(t, timeRangeOverlap(3, 4, 1, 2))
	require.True(t, timeRangeOverlap(1, 2, 2, 3))
	require.True(t, timeRangeOverlap(1, 3, 2, 4))
	require.True(t, timeRangeOverlap(2, 4, 1, 3))
	require.True(t, timeRangeOverlap(1, 0, 3, 4))
	require.True(t, timeRangeOverlap(1, 0, 2, 0))
}

func TestStmtFile(t *testing.T) {
	filename := "tidb-statements-2022-12-27T16-21-20.245.log"

	file, err := os.Create(filename)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(filename))
	}()
	_, err = file.WriteString("{\"begin\":1,\"end\":2}\n")
	require.NoError(t, err)
	_, err = file.WriteString("{\"begin\":3,\"end\":4}\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	f, err := openStmtFile(filename)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.file.Close())
	}()
	require.Equal(t, int64(1), f.begin)
	require.Equal(t, time.Date(2022, 12, 27, 16, 21, 20, 245000000, time.Local).Unix(), f.end)

	// Check if seek 0.
	firstLine, err := util.ReadLine(bufio.NewReader(f.file), maxLineSize)
	require.NoError(t, err)
	require.Equal(t, `{"begin":1,"end":2}`, string(firstLine))
}

func TestStmtFileInvalidLine(t *testing.T) {
	filename := "tidb-statements-2022-12-27T16-21-20.245.log"

	file, err := os.Create(filename)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(filename))
	}()
	_, err = file.WriteString("invalid line\n")
	require.NoError(t, err)
	_, err = file.WriteString("{\"begin\":1,\"end\":2}\n")
	require.NoError(t, err)
	_, err = file.WriteString("{\"begin\":3,\"end\":4}\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	f, err := openStmtFile(filename)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.file.Close())
	}()
	require.Equal(t, int64(1), f.begin)
	require.Equal(t, time.Date(2022, 12, 27, 16, 21, 20, 245000000, time.Local).Unix(), f.end)
}

func TestStmtFiles(t *testing.T) {
	t1 := time.Date(2022, 12, 27, 16, 21, 20, 245000000, time.Local)
	filename1 := "tidb-statements-2022-12-27T16-21-20.245.log"
	filename2 := "tidb-statements.log"

	file, err := os.Create(filename1)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(filename1))
	}()
	_, err = file.WriteString(fmt.Sprintf("{\"begin\":%d,\"end\":%d}\n", t1.Unix()-760, t1.Unix()-750))
	require.NoError(t, err)
	_, err = file.WriteString(fmt.Sprintf("{\"begin\":%d,\"end\":%d}\n", t1.Unix()-10, t1.Unix()))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	file, err = os.Create(filename2)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(filename2))
	}()
	_, err = file.WriteString(fmt.Sprintf("{\"begin\":%d,\"end\":%d}\n", t1.Unix()-10, t1.Unix()))
	require.NoError(t, err)
	_, err = file.WriteString(fmt.Sprintf("{\"begin\":%d,\"end\":%d}\n", t1.Unix()+100, t1.Unix()+110))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	func() {
		files, err := newStmtFiles(context.Background(), nil)
		require.NoError(t, err)
		defer files.close()
		require.Len(t, files.files, 2)
		require.Equal(t, filename1, files.files[0].file.Name())
		require.Equal(t, filename2, files.files[1].file.Name())
	}()

	func() {
		files, err := newStmtFiles(context.Background(), []*StmtTimeRange{
			{Begin: t1.Unix() - 10, End: t1.Unix() - 9},
		})
		require.NoError(t, err)
		defer files.close()
		require.Len(t, files.files, 2)
		require.Equal(t, filename1, files.files[0].file.Name())
		require.Equal(t, filename2, files.files[1].file.Name())
	}()

	func() {
		files, err := newStmtFiles(context.Background(), []*StmtTimeRange{
			{Begin: 0, End: t1.Unix() - 10},
		})
		require.NoError(t, err)
		defer files.close()
		require.Len(t, files.files, 2)
		require.Equal(t, filename1, files.files[0].file.Name())
		require.Equal(t, filename2, files.files[1].file.Name())
	}()

	func() {
		files, err := newStmtFiles(context.Background(), []*StmtTimeRange{
			{Begin: 0, End: t1.Unix() - 11},
		})
		require.NoError(t, err)
		defer files.close()
		require.Len(t, files.files, 1)
		require.Equal(t, filename1, files.files[0].file.Name())
	}()

	func() {
		files, err := newStmtFiles(context.Background(), []*StmtTimeRange{
			{Begin: 0, End: 1},
		})
		require.NoError(t, err)
		defer files.close()
		require.Empty(t, files.files)
	}()

	func() {
		files, err := newStmtFiles(context.Background(), []*StmtTimeRange{
			{Begin: t1.Unix() + 1, End: 0},
		})
		require.NoError(t, err)
		defer files.close()
		require.Len(t, files.files, 1)
		require.Equal(t, filename2, files.files[0].file.Name())
	}()
}

func TestStmtChecker(t *testing.T) {
	checker := &stmtChecker{}
	require.True(t, checker.hasPrivilege(nil))

	checker = &stmtChecker{
		user: &auth.UserIdentity{Username: "user1"},
	}
	require.False(t, checker.hasPrivilege(nil))
	require.False(t, checker.hasPrivilege(map[string]struct{}{"user2": {}}))
	require.True(t, checker.hasPrivilege(map[string]struct{}{"user1": {}, "user2": {}}))

	checker = &stmtChecker{}
	require.True(t, checker.isDigestValid("digest1"))

	checker = &stmtChecker{
		digests: set.NewStringSet("digest2"),
	}
	require.False(t, checker.isDigestValid("digest1"))
	require.True(t, checker.isDigestValid("digest2"))

	checker = &stmtChecker{
		digests: set.NewStringSet("digest1", "digest2"),
	}
	require.True(t, checker.isDigestValid("digest1"))
	require.True(t, checker.isDigestValid("digest2"))

	checker = &stmtChecker{}
	require.True(t, checker.isTimeValid(1, 2))
	require.False(t, checker.needStop(2))
	require.False(t, checker.needStop(3))

	checker = &stmtChecker{
		timeRanges: []*StmtTimeRange{
			{Begin: 1, End: 2},
		},
	}
	require.True(t, checker.isTimeValid(1, 2))
	require.False(t, checker.isTimeValid(3, 4))
	require.False(t, checker.needStop(2))
	require.True(t, checker.needStop(3))
}

func TestMemReader(t *testing.T) {
	timeLocation, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	columns := []*model.ColumnInfo{
		{Name: model.NewCIStr(DigestStr)},
		{Name: model.NewCIStr(ExecCountStr)},
	}

	ss := NewStmtSummary4Test(3)
	defer ss.Close()

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))
	ss.Add(GenerateStmtExecInfo4Test("digest4"))
	ss.Add(GenerateStmtExecInfo4Test("digest4"))
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	reader := NewMemReader(ss, columns, "", timeLocation, nil, false, nil, nil)
	rows := reader.Rows()
	require.Len(t, rows, 4) // 3 rows + 1 other
	require.Equal(t, len(reader.columnFactories), len(rows[0]))
	evicted := ss.Evicted()
	require.Len(t, evicted, 3) // begin, end, count
}

func TestHistoryReader(t *testing.T) {
	filename1 := "tidb-statements-2022-12-27T16-21-20.245.log"
	filename2 := "tidb-statements.log"

	file, err := os.Create(filename1)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(filename1))
	}()
	_, err = file.WriteString("{\"begin\":1672128520,\"end\":1672128530,\"digest\":\"digest1\",\"exec_count\":10}\n")
	require.NoError(t, err)
	_, err = file.WriteString("{\"begin\":1672129270,\"end\":1672129280,\"digest\":\"digest2\",\"exec_count\":20}\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	file, err = os.Create(filename2)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(filename2))
	}()
	_, err = file.WriteString("{\"begin\":1672129270,\"end\":1672129280,\"digest\":\"digest2\",\"exec_count\":30}\n")
	require.NoError(t, err)
	_, err = file.WriteString("{\"begin\":1672129380,\"end\":1672129390,\"digest\":\"digest3\",\"exec_count\":40}\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	timeLocation, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	columns := []*model.ColumnInfo{
		{Name: model.NewCIStr(DigestStr)},
		{Name: model.NewCIStr(ExecCountStr)},
	}

	func() {
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, nil, nil, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 4)
		for _, row := range rows {
			require.Equal(t, len(columns), len(row))
		}
	}()

	func() {
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, set.NewStringSet("digest2"), nil, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 2)
		for _, row := range rows {
			require.Equal(t, len(columns), len(row))
		}
	}()

	func() {
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, nil, []*StmtTimeRange{
			{Begin: 0, End: 1672128520 - 1},
		}, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 0)
	}()

	func() {
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, nil, []*StmtTimeRange{
			{Begin: 0, End: 1672129270 - 1},
		}, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 1)
		for _, row := range rows {
			require.Equal(t, len(columns), len(row))
		}
	}()

	func() {
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, nil, []*StmtTimeRange{
			{Begin: 0, End: 1672129270},
		}, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 3)
		for _, row := range rows {
			require.Equal(t, len(columns), len(row))
		}
	}()

	func() {
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, nil, []*StmtTimeRange{
			{Begin: 0, End: 1672129380},
		}, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 4)
		for _, row := range rows {
			require.Equal(t, len(columns), len(row))
		}
	}()

	func() {
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, nil, []*StmtTimeRange{
			{Begin: 1672129270, End: 1672129380},
		}, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 3)
		for _, row := range rows {
			require.Equal(t, len(columns), len(row))
		}
	}()

	func() {
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, nil, []*StmtTimeRange{
			{Begin: 1672129390, End: 0},
		}, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 1)
		for _, row := range rows {
			require.Equal(t, len(columns), len(row))
		}
	}()

	func() {
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, nil, []*StmtTimeRange{
			{Begin: 1672129391, End: 0},
		}, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 0)
	}()

	func() {
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, nil, []*StmtTimeRange{
			{Begin: 0, End: 0},
		}, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 4)
		for _, row := range rows {
			require.Equal(t, len(columns), len(row))
		}
	}()
}

func TestHistoryReaderInvalidLine(t *testing.T) {
	filename := "tidb-statements.log"

	file, err := os.Create(filename)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(filename))
	}()
	_, err = file.WriteString("invalid header line\n")
	require.NoError(t, err)
	_, err = file.WriteString("{\"begin\":1672129270,\"end\":1672129280,\"digest\":\"digest2\",\"exec_count\":30}\n")
	require.NoError(t, err)
	_, err = file.WriteString("corrupted line\n")
	require.NoError(t, err)
	_, err = file.WriteString("{\"begin\":1672129380,\"end\":1672129390,\"digest\":\"digest3\",\"exec_count\":40}\n")
	require.NoError(t, err)
	_, err = file.WriteString("invalid footer line")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	timeLocation, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	columns := []*model.ColumnInfo{
		{Name: model.NewCIStr(DigestStr)},
		{Name: model.NewCIStr(ExecCountStr)},
	}

	reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, nil, nil, 2)
	require.NoError(t, err)
	defer reader.Close()
	rows := readAllRows(t, reader)
	require.Len(t, rows, 2)
	for _, row := range rows {
		require.Equal(t, len(columns), len(row))
	}
}

func readAllRows(t *testing.T, reader *HistoryReader) [][]types.Datum {
	var results [][]types.Datum
	for {
		rows, err := reader.Rows()
		require.NoError(t, err)
		if rows == nil {
			break
		}
		results = append(results, rows...)
	}
	return results
}
