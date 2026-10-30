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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
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
		{Name: ast.NewCIStr(DigestStr)},
		{Name: ast.NewCIStr(ExecCountStr)},
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
	_, err = file.WriteString("{\"begin\":1672129270,\"end\":1672129280,\"digest\":\"evicted_digest\",\"exec_count\":99,\"evicted\":true}\n")
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
		{Name: ast.NewCIStr(DigestStr)},
		{Name: ast.NewCIStr(ExecCountStr)},
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
		// Digest filter combined with a time range; the parse worker's digest
		// pre-filter must not change the result.
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, set.NewStringSet("digest2"), []*StmtTimeRange{
			{Begin: 0, End: 1672129270 - 1},
		}, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 0)
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

	func() {
		// Duplicate digest keys: the full decode keeps the last occurrence, and the
		// pre-filter must agree — either by picking the same last value or by
		// falling back to the full decode.
		dupName := time.Unix(1672129480, 0).In(timeLocation).Format(logFileTimeFormat)
		dupFile := "tidb-statements-" + dupName + ".log"
		file, err := os.Create(dupFile)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.Remove(dupFile))
		}()
		_, err = file.WriteString("{\"begin\":1672129470,\"end\":1672129480,\"digest\":\"digest2\",\"digest\":\"digest_dup_last\",\"exec_count\":50}\n")
		require.NoError(t, err)
		require.NoError(t, file.Close())

		func() {
			// The full decode resolves the duplicate-key record to digest_dup_last,
			// so filtering for digest2 must still return only the two original rows.
			reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, set.NewStringSet("digest2"), nil, 2)
			require.NoError(t, err)
			defer reader.Close()
			rows := readAllRows(t, reader)
			require.Len(t, rows, 2)
		}()
		func() {
			reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false, set.NewStringSet("digest_dup_last"), nil, 2)
			require.NoError(t, err)
			defer reader.Close()
			rows := readAllRows(t, reader)
			require.Len(t, rows, 1)
			require.Equal(t, "digest_dup_last", rows[0][0].GetString())
		}()
	}()

	func() {
		// The scanner must agree with encoding/json for case-folded duplicates
		// and invalid UTF-8 digest values, otherwise matching rows can be lost.
		defer config.RestoreFunc()()
		filename := filepath.Join(t.TempDir(), "tidb-statements.log")
		config.UpdateGlobal(func(conf *config.Config) { conf.Instance.StmtSummaryFilename = filename })
		lines := []string{
			`{"begin":1672128520,"end":1672128580,"digest":"miss","DIGEST":"target","exec_count":7}`,
			`{"begin":1672128520,"end":1672128580,"digest":"a` + string([]byte{0xff}) + `b","exec_count":8}`,
		}
		require.NoError(t, os.WriteFile(filename, []byte(strings.Join(lines, "\n")+"\n"), 0o600))
		reader, err := NewHistoryReader(context.Background(), columns, "", timeLocation, nil, false,
			set.NewStringSet("target", "a\ufffdb"), nil, 2)
		require.NoError(t, err)
		defer reader.Close()
		rows := readAllRows(t, reader)
		require.Len(t, rows, 2)
		digests := make([]string, 0, len(rows))
		for _, row := range rows {
			digests = append(digests, row[0].GetString())
		}
		require.ElementsMatch(t, []string{"target", "a\ufffdb"}, digests)
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
		{Name: ast.NewCIStr(DigestStr)},
		{Name: ast.NewCIStr(ExecCountStr)},
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

// benchmarkHistoryReaderRows compares the same persisted-file workload before
// and after prefiltering, including high match rates and all text columns.
func benchmarkHistoryReaderRows(b *testing.B, matchPercent int, withDigestFilter bool, textSize int, allText bool) {
	const recordCount = 200
	const target = "digest_target"
	defer config.RestoreFunc()()
	filename := filepath.Join(b.TempDir(), "tidb-statements.log")
	config.UpdateGlobal(func(conf *config.Config) { conf.Instance.StmtSummaryFilename = filename })
	file, err := os.Create(filename)
	require.NoError(b, err)
	for i := 0; i < recordCount; i++ {
		digest := fmt.Sprintf("digest_%06d", i)
		if i%100 < matchPercent {
			digest = target
		}
		record := stmtPersistedRecord{StmtRecord: StmtRecord{
			Begin: 1672128520, End: 1672128580, Digest: digest, ExecCount: 10, StmtType: "Select",
			NormalizedSQL: "select ? from t", SampleSQL: strings.Repeat("s", textSize), SampleBinaryPlan: strings.Repeat("p", textSize),
		}}
		line, err := json.Marshal(record)
		require.NoError(b, err)
		_, err = file.Write(append(line, '\n'))
		require.NoError(b, err)
	}
	require.NoError(b, file.Close())
	names := []string{DigestStr, ExecCountStr}
	if allText {
		names = append(names, DigestTextStr, BindingDigestTextStr, QuerySampleTextStr, PrevSampleTextStr,
			PlanStr, BinaryPlan, PlanHint, PlanCacheUnqualifiedLastReasonStr)
	}
	columns := make([]*model.ColumnInfo, len(names))
	for i, name := range names {
		columns[i] = &model.ColumnInfo{Name: ast.NewCIStr(name)}
	}
	var digests set.StringSet
	expected := recordCount
	if withDigestFilter {
		digests = set.NewStringSet(target)
		expected = recordCount * matchPercent / 100
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, err := NewHistoryReader(context.Background(), columns, "", time.UTC, nil, false, digests, nil, 2)
		require.NoError(b, err)
		count := 0
		for {
			rows, err := reader.Rows()
			require.NoError(b, err)
			if rows == nil {
				break
			}
			count += len(rows)
		}
		require.NoError(b, reader.Close())
		require.Equal(b, expected, count)
	}
}

func BenchmarkHistoryReaderDigestFilter(b *testing.B) {
	for _, size := range []int{1024, 32768} {
		for _, allText := range []bool{false, true} {
			columns := "narrow"
			if allText {
				columns = "all-text"
			}
			for _, percent := range []int{0, 1, 50, 100} {
				b.Run(fmt.Sprintf("%dB/%s/match-%d", size, columns, percent), func(b *testing.B) {
					benchmarkHistoryReaderRows(b, percent, true, size, allText)
				})
			}
			b.Run(fmt.Sprintf("%dB/%s/no-filter", size, columns), func(b *testing.B) {
				benchmarkHistoryReaderRows(b, 0, false, size, allText)
			})
		}
	}
}

func TestExtractDigest(t *testing.T) {
	// Regular shape: digest is the fourth field and the large fields follow it, so
	// the key walk never decodes any other value.
	line := `{"begin":1672128520,"end":1672128521,"schema_name":"test","digest":"digest1","plan_digest":"p","normalized_sql":"select ?","sample_sql":"` +
		strings.Repeat("s", 4096) + `"}`
	digest, ok := extractDigest([]byte(line))
	require.True(t, ok)
	require.Equal(t, "digest1", digest)

	// Whitespace-tolerant and digest-first shapes still work.
	digest, ok = extractDigest([]byte(`{ "begin" : 1 , "digest" : "d1" , "end" : 2 }`))
	require.True(t, ok)
	require.Equal(t, "d1", digest)
	digest, ok = extractDigest([]byte(`{"digest":"d2","begin":1}`))
	require.True(t, ok)
	require.Equal(t, "d2", digest)

	// Escapes in earlier string fields don't confuse the scan.
	digest, ok = extractDigest([]byte(`{"schema_name":"a\"b\u4e2d","digest":"d3"}`))
	require.True(t, ok)
	require.Equal(t, "d3", digest)

	// Container values are skipped without decoding.
	digest, ok = extractDigest([]byte(`{"arr":[1,2,{"x":"y"}],"obj":{"k":"v"},"digest":"d4"}`))
	require.True(t, ok)
	require.Equal(t, "d4", digest)

	// Duplicate keys: the full decode keeps the last one, and so does the scan.
	digest, ok = extractDigest([]byte(`{"begin":1,"end":2,"digest":"miss","digest":"target"}`))
	require.True(t, ok)
	require.Equal(t, "target", digest)

	t.Run("case insensitive duplicate digest", func(t *testing.T) {
		for _, key := range []string{"DIGEST", "digeſt"} {
			raw := []byte(`{"digest":"miss","` + key + `":"target"}`)
			var full StmtRecord
			require.NoError(t, json.Unmarshal(raw, &full))
			digest, ok := extractDigest(raw)
			require.True(t, ok)
			require.Equal(t, full.Digest, digest)
			require.Equal(t, "target", digest)
		}
	})
	t.Run("invalid UTF-8 requires full decoding", func(t *testing.T) {
		_, ok := extractDigest([]byte("{\"digest\":\"a" + string([]byte{0xff}) + "b\"}"))
		require.False(t, ok)
	})

	// Unknown shapes must report ok=false so the caller falls back to the full decode.
	for _, bad := range []string{
		`{"begin":1}`, // no digest key
		`{"digest":"miss","di\u0067est":"target"}`, // escaped key: defer to the full decode
		`{"digest":"a\"b"}`,                        // escaped digest value
		`{"digest":null}`,                          // non-string digest
		`{"digest" "d"}`,                           // missing colon
		`"digest"`,                                 // not an object
		`{"begin":1672128520,"end":1672128`,        // truncated
		`{"arr":[1,2`,                              // truncated container
	} {
		_, ok = extractDigest([]byte(bad))
		require.False(t, ok, bad)
	}
}
