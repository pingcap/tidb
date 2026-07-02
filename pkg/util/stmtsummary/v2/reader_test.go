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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/set"
	stmtsummarybase "github.com/pingcap/tidb/pkg/util/stmtsummary"
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

func TestReadBillingDemoMemReader(t *testing.T) {
	timeLocation, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)

	ss := NewStmtSummary4Test(3)
	defer ss.Close()

	info := GenerateStmtExecInfo4Test("digest_read_billing")
	info.ReadBillingDemoStats = stmtsummarybase.ReadBillingDemoStatementStats{
		ModelVersion:  "v1",
		WeightVersion: "v1",
		Statuses: []stmtsummarybase.ReadBillingDemoStatusSample{{
			ModelVersion:  "v1",
			WeightVersion: "v1",
			Site:          "statement",
			OpClass:       "statement",
			OperatorKind:  "statement",
			Status:        "success",
			Reason:        "none",
		}},
		BaseUnits: []stmtsummarybase.ReadBillingDemoBaseUnitSample{{
			ModelVersion:   "v1",
			WeightVersion:  "v1",
			Site:           "tidb",
			OpClass:        "projection_eval",
			OperatorKind:   "projection",
			Unit:           "fixed_events",
			InputSource:    "runtime_act_rows",
			InputSide:      "all",
			RowWidthSource: "operator_helper",
			Value:          2,
			RowWidth:       16,
		}},
		Totals: stmtsummarybase.ReadBillingDemoBaseUnitSummary{
			SumReadBillingDemoFixedEvents: 2,
		},
	}
	ss.Add(info)

	statusOnly := GenerateStmtExecInfo4Test("digest_read_billing_error")
	statusOnly.ReadBillingDemoStats = stmtsummarybase.ReadBillingDemoStatementStats{
		ModelVersion:  "v1",
		WeightVersion: "v1",
		Statuses: []stmtsummarybase.ReadBillingDemoStatusSample{{
			ModelVersion:  "v1",
			WeightVersion: "v1",
			Site:          "statement",
			OpClass:       "statement",
			OperatorKind:  "statement",
			Status:        "error",
			Reason:        "statement_error",
		}},
	}
	ss.AddReadBillingDemoStatusOnly(statusOnly)

	baseUnitCols := []*model.ColumnInfo{
		{Name: ast.NewCIStr(DigestStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoSiteStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoOpClassStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoUnitStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoValueStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoSampleCountStr)},
	}
	baseRows := NewReadBillingDemoMemReader(ss, baseUnitCols, "", timeLocation, nil, false, nil, nil, stmtsummarybase.ReadBillingDemoTableBaseUnits).Rows()
	require.Len(t, baseRows, 1)
	require.Equal(t, map[string]string{
		"digest_read_billing/tidb/projection_eval/fixed_events": "2 1",
	}, readBillingDemoRowsByKey(baseRows, 4))

	statusCols := []*model.ColumnInfo{
		{Name: ast.NewCIStr(DigestStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoStatusStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoReasonStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoCountStr)},
	}
	statusRows := NewReadBillingDemoMemReader(ss, statusCols, "", timeLocation, nil, false, nil, nil, stmtsummarybase.ReadBillingDemoTableStatus).Rows()
	require.Len(t, statusRows, 2)
	require.Equal(t, map[string]string{
		"digest_read_billing/success/none":                "1",
		"digest_read_billing_error/error/statement_error": "1",
	}, readBillingDemoRowsByKey(statusRows, 3))

	normalReader := NewMemReader(ss, []*model.ColumnInfo{
		{Name: ast.NewCIStr(DigestStr)},
		{Name: ast.NewCIStr(ExecCountStr)},
	}, "", timeLocation, nil, false, nil, nil)
	normalRows := normalReader.Rows()
	require.Len(t, normalRows, 1)
	require.Equal(t, "digest_read_billing", normalRows[0][0].GetString())
	require.Equal(t, int64(1), normalRows[0][1].GetInt64())
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

func TestReadBillingDemoHistoryReader(t *testing.T) {
	filename := "tidb-statements.log"
	file, err := os.Create(filename)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(filename))
	}()
	_, err = file.WriteString(`{"begin":1672129270,"end":1672129280,"digest":"digest_read_billing","normalized_sql":"select ?","stmt_type":"Select","auth_users":{"user":{}},"read_billing_demo_base_unit_aggs":[{"model_version":"v1","weight_version":"v1","site":"tidb","op_class":"projection_eval","operator_kind":"projection","unit":"fixed_events","input_source":"runtime_act_rows","input_side":"all","row_width_source":"operator_helper","value":2,"sample_count":1,"row_width_sum":16}],"read_billing_demo_status_aggs":[{"model_version":"v1","weight_version":"v1","site":"statement","op_class":"statement","operator_kind":"statement","status":"success","reason":"none","count":1}]}` + "\n")
	require.NoError(t, err)
	_, err = file.WriteString(`{"begin":1672129270,"end":1672129280,"digest":"digest_read_billing_error","normalized_sql":"select ?","stmt_type":"Select","auth_users":{"user":{}},"read_billing_demo_status_aggs":[{"model_version":"v1","weight_version":"v1","site":"statement","op_class":"statement","operator_kind":"statement","status":"error","reason":"statement_error","count":1}]}` + "\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	timeLocation, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	baseUnitCols := []*model.ColumnInfo{
		{Name: ast.NewCIStr(DigestStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoSiteStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoOpClassStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoUnitStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoValueStr)},
	}
	baseReader, err := NewReadBillingDemoHistoryReader(context.Background(), baseUnitCols, "", timeLocation, nil, false, nil, nil, 2, stmtsummarybase.ReadBillingDemoTableBaseUnits)
	require.NoError(t, err)
	baseRows := readAllRows(t, baseReader)
	require.NoError(t, baseReader.Close())
	require.Equal(t, map[string]string{
		"digest_read_billing/tidb/projection_eval/fixed_events": "2",
	}, readBillingDemoRowsByKey(baseRows, 4))

	statusCols := []*model.ColumnInfo{
		{Name: ast.NewCIStr(DigestStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoStatusStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoReasonStr)},
		{Name: ast.NewCIStr(stmtsummarybase.ReadBillingDemoCountStr)},
	}
	statusReader, err := NewReadBillingDemoHistoryReader(context.Background(), statusCols, "", timeLocation, nil, false, nil, nil, 2, stmtsummarybase.ReadBillingDemoTableStatus)
	require.NoError(t, err)
	statusRows := readAllRows(t, statusReader)
	require.NoError(t, statusReader.Close())
	require.Equal(t, map[string]string{
		"digest_read_billing/success/none":                "1",
		"digest_read_billing_error/error/statement_error": "1",
	}, readBillingDemoRowsByKey(statusRows, 3))

	normalReader, err := NewHistoryReader(context.Background(), []*model.ColumnInfo{
		{Name: ast.NewCIStr(DigestStr)},
		{Name: ast.NewCIStr(ExecCountStr)},
	}, "", timeLocation, nil, false, nil, nil, 2)
	require.NoError(t, err)
	normalRows := readAllRows(t, normalReader)
	require.NoError(t, normalReader.Close())
	require.Empty(t, normalRows)
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

func readBillingDemoRowsByKey(rows [][]types.Datum, keyColumns int) map[string]string {
	result := make(map[string]string, len(rows))
	for _, row := range rows {
		keyParts := make([]string, 0, keyColumns)
		for i := 0; i < keyColumns; i++ {
			keyParts = append(keyParts, fmt.Sprintf("%v", row[i].GetValue()))
		}
		valueParts := make([]string, 0, len(row)-keyColumns)
		for i := keyColumns; i < len(row); i++ {
			valueParts = append(valueParts, fmt.Sprintf("%v", row[i].GetValue()))
		}
		result[strings.Join(keyParts, "/")] = strings.Join(valueParts, " ")
	}
	return result
}
