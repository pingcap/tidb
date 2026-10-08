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

package stmtsummary

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

func projectionColumns(names ...string) []*model.ColumnInfo {
	columns := make([]*model.ColumnInfo, len(names))
	for i, name := range names {
		columns[i] = &model.ColumnInfo{Name: ast.NewCIStr(name)}
	}
	return columns
}

func TestHistoryProjection(t *testing.T) {
	info := GenerateStmtExecInfo4Test("digest")
	record := NewStmtRecord(info)
	record.Add(info)
	record.Begin, record.End = 1672128520, 1672128580
	record.NormalizedSQL = "select ? from t"
	record.BindingSQL = "select /*+ use_index(t, idx) */ ? from t"
	record.SampleSQL = "select 'sample' from t"
	record.PrevSQL = "select 'previous' from t"
	record.SamplePlan = plancodec.PlanDiscardedEncoded
	record.SampleBinaryPlan = "binary plan"
	record.PlanHint = "use_index(t, idx)"
	record.PlanCacheUnqualifiedLastReason = "plan cache reason"
	raw, err := json.Marshal(stmtPersistedRecord{StmtRecord: *record})
	require.NoError(t, err)
	fullWorker := &stmtParseWorker{timeLocation: time.UTC}
	full, skipped, err := fullWorker.parse(raw)
	require.NoError(t, err)
	require.False(t, skipped)

	// Every column factory is checked against the full record. This catches a
	// missing dependency if a factory starts using another projected text field.
	allColumns := make([]*model.ColumnInfo, 0, len(columnFactoryMap))
	for name, factory := range columnFactoryMap {
		t.Run(name, func(t *testing.T) {
			p := makeStmtRecordProjection(projectionColumns(name))
			require.NotNil(t, p)
			projected, skipped, err := p.parse(raw)
			require.NoError(t, err)
			require.False(t, skipped)
			require.Equal(t, factory(fullWorker, full), factory(fullWorker, projected))
			require.Equal(t, full.AuthUsers, projected.AuthUsers)
			require.Equal(t, full.Begin, projected.Begin)
			require.Equal(t, full.End, projected.End)
			require.Equal(t, full.Digest, projected.Digest)
		})
		allColumns = append(allColumns, projectionColumns(name)...)
	}
	require.Nil(t, makeStmtRecordProjection(allColumns), "full-column queries retain full decoding")

	// Exercise all bounded text-field combinations, including the full path.
	textColumns := []string{DigestTextStr, BindingDigestTextStr, QuerySampleTextStr, PrevSampleTextStr,
		PlanStr, BinaryPlan, PlanHint, PlanCacheUnqualifiedLastReasonStr}
	for mask := 0; mask < 1<<len(textColumns); mask++ {
		var names []string
		for i, name := range textColumns {
			if mask&(1<<i) != 0 {
				names = append(names, name)
			}
		}
		worker := &stmtParseWorker{timeLocation: time.UTC, projection: makeStmtRecordProjection(projectionColumns(names...))}
		projected, skipped, err := worker.parse(raw)
		require.NoError(t, err, "mask=%d", mask)
		require.False(t, skipped)
		for _, name := range names {
			factory := columnFactoryMap[name]
			require.Equal(t, factory(fullWorker, full), factory(worker, projected), "mask=%d, column=%s", mask, name)
		}
	}

	p := makeStmtRecordProjection(projectionColumns(DigestStr, StmtTypeStr, ExecCountStr))
	projected, _, err := p.parse(raw)
	require.NoError(t, err)
	require.Empty(t, projected.NormalizedSQL)
	require.Empty(t, projected.BindingSQL)
	require.Empty(t, projected.SampleSQL)
	require.Empty(t, projected.PrevSQL)
	require.Empty(t, projected.SamplePlan)
	require.Empty(t, projected.SampleBinaryPlan)
	require.Empty(t, projected.PlanHint)
	require.Empty(t, projected.PlanCacheUnqualifiedLastReason)
	require.Equal(t, full.ExecCount, projected.ExecCount)

	planRecord, _, err := makeStmtRecordProjection(projectionColumns(PlanStr)).parse(raw)
	require.NoError(t, err)
	require.Equal(t, full.SampleSQL, planRecord.SampleSQL, "PLAN error diagnostics need sample SQL")

	selected, _, err := makeStmtRecordProjection(projectionColumns(DigestTextStr)).parse(raw)
	require.NoError(t, err)
	for i := range raw {
		raw[i] = ' '
	}
	require.Equal(t, full.NormalizedSQL, selected.NormalizedSQL, "selected strings must own their bytes")
}

func TestHistoryProjectionJSONCompatibility(t *testing.T) {
	fullWorker := &stmtParseWorker{}
	for _, columns := range [][]*model.ColumnInfo{
		projectionColumns(DigestStr),
		projectionColumns(DigestStr, QuerySampleTextStr, DigestTextStr),
	} {
		p := makeStmtRecordProjection(columns)
		for _, raw := range []string{
			`{"begin":1,"end":2,"digest":"d","sample_sql":"text","normalized_sql":"normalized","auth_users":{"user":{}}}`,
			`{"digest":"old","digest":"new","sample_sql":"old","SAMPLE_SQL":"new","normalized_sql":null}`,
			`{"digest":"d","sample_sql":"old","sample_sql":null,"normali\u007aed_sql":"a\"b\u4e2d"}`,
			`{"digest":"d","sample_sql":"\ud800","normalized_sql":"a` + string([]byte{0xff}) + `b"}`,
			`{"digest":"d","evicted":true,"sample_sql":"text"}`,
			`{"digest":"d","unknown":{"nested":[true,null,1]},"sample_sql":null}`,
			`null`,
			`{"sample_sql":123}`,
			`{"sample_sql":{},"sample_sql":"valid"}`,
			`{"normalized_sql":false}`,
			`{"binding_sql":[]}`,
			`{"prev_sql":123}`,
			`{"sample_plan":false}`,
			`{"sample_binary_plan":{}}`,
			`{"plan_hint":[]}`,
			`{"plan_cache_unqualified_last_reason":1}`,
			`{"sample_sql":"valid","exec_count":"invalid"}`,
			`{"max_prewrite_region_num":2147483648}`,
			`{"auth_users":[]}`,
			`{"first_seen":"invalid"}`,
			`{"sample_sql":"bad\xescape"}`,
			`{"sample_sql":"truncated`,
			`{"sample_sql":"valid"} trailing`,
		} {
			t.Run(raw, func(t *testing.T) {
				full, fullSkipped, fullErr := fullWorker.parse([]byte(raw))
				projected, skipped, err := p.parse([]byte(raw))
				require.Equal(t, fullErr != nil, err != nil, "record acceptance must not depend on columns")
				require.Equal(t, fullSkipped, skipped)
				if err != nil || skipped {
					return
				}
				worker := &stmtParseWorker{timeLocation: time.UTC}
				for _, column := range columns {
					factory := columnFactoryMap[column.Name.O]
					require.Equal(t, factory(worker, full), factory(worker, projected))
				}
				require.Equal(t, full.AuthUsers, projected.AuthUsers)
			})
		}
	}
}

func TestHistoryProjectionReader(t *testing.T) {
	defer config.RestoreFunc()()
	filename := filepath.Join(t.TempDir(), "tidb-statements.log")
	config.UpdateGlobal(func(conf *config.Config) { conf.Instance.StmtSummaryFilename = filename })
	lines := []string{
		`{"begin":1672128520,"end":1672128580,"digest":"d","stmt_type":"Select","exec_count":2,"auth_users":{"alice":{}},"sample_sql":"` + strings.Repeat("s", 32768) + `","sample_plan":"` + strings.Repeat("p", 32768) + `"}`,
		`{"begin":1672128520,"end":1672128580,"digest":"d","exec_count":3,"auth_users":{"bob":{}}}`,
		`{"begin":1672128520,"end":1672128580,"digest":"d","exec_count":4,"auth_users":{"alice":{}},"evicted":true}`,
		`{"begin":1672128520,"end":1672128580,"digest":"d","exec_count":5,"auth_users":{"alice":{}},"sample_sql":123}`,
		`{"begin":1672128520,"end":1672128580,"digest":"other","exec_count":6,"auth_users":{"alice":{}}}`,
		`{"begin":1672128520,"end":1672128580,"digest":"miss","DIGEST":"d","stmt_type":"Select","exec_count":7,"auth_users":{"alice":{}}}`,
		`{"begin":1672128520,"end":1672128580,"digest":"a` + string([]byte{0xff}) + `b","stmt_type":"Select","exec_count":8,"auth_users":{"alice":{}}}`,
	}
	require.NoError(t, os.WriteFile(filename, []byte(strings.Join(lines, "\n")+"\n"), 0o600))
	for _, projected := range []bool{false, true} {
		reader, err := NewHistoryReader(context.Background(), projectionColumns(DigestStr, StmtTypeStr, ExecCountStr), "", time.UTC,
			&auth.UserIdentity{Username: "alice"}, false, set.NewStringSet("d", "a\ufffdb"), []*StmtTimeRange{{Begin: 1672128520, End: 1672128580}}, 2)
		require.NoError(t, err)
		if !projected {
			reader.projection = nil
		}
		rows := readAllRows(t, reader)
		require.NoError(t, reader.Close())
		require.Len(t, rows, 3, "projected=%v: privilege, digest, eviction and invalid-record checks", projected)
		counts := make(map[string][]int64)
		for _, row := range rows {
			require.Equal(t, "Select", row[1].GetString())
			counts[row[0].GetString()] = append(counts[row[0].GetString()], row[2].GetInt64())
		}
		require.ElementsMatch(t, []int64{2, 7}, counts["d"])
		require.Equal(t, []int64{8}, counts["a\ufffdb"])
	}
}

func BenchmarkHistoryProjection(b *testing.B) {
	for _, size := range []int{1024, 32768} {
		record := stmtPersistedRecord{StmtRecord: StmtRecord{
			Begin: 1672128520, End: 1672128580, Digest: "digest", StmtType: "Select", ExecCount: 10,
			NormalizedSQL: "select ? from t", SampleSQL: strings.Repeat("s", size), SamplePlan: strings.Repeat("p", size),
		}}
		raw, err := json.Marshal(record)
		require.NoError(b, err)
		for _, columns := range []struct {
			name    string
			columns []*model.ColumnInfo
		}{
			{"types", projectionColumns(StmtTypeStr)},
			{"digest-text", projectionColumns(DigestStr, DigestTextStr)},
			{"aggregate", projectionColumns(DigestStr, ExecCountStr, SumLatencyStr)},
			{"sample", projectionColumns(QuerySampleTextStr)},
			{"plan", projectionColumns(PlanStr)},
			{"all-text", projectionColumns(DigestTextStr, BindingDigestTextStr, QuerySampleTextStr, PrevSampleTextStr, PlanStr, BinaryPlan, PlanHint, PlanCacheUnqualifiedLastReasonStr)},
		} {
			for _, projected := range []bool{false, true} {
				name := columns.name + "/full"
				worker := &stmtParseWorker{}
				if projected {
					name = columns.name + "/projected"
					worker.projection = makeStmtRecordProjection(columns.columns)
				}
				b.Run(name+"/"+fmtSize(size), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(raw)))
					for b.Loop() {
						record, skipped, err := worker.parse(raw)
						if err != nil || skipped || record.Digest != "digest" {
							b.Fatal("unexpected record", err)
						}
					}
				})
			}
		}
	}
}

func fmtSize(size int) string {
	if size == 1024 {
		return "1KiB"
	}
	return "32KiB"
}
