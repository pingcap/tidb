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

package infoschema

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestTablesCreateTimeLightweightPath(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table lightweight_time (a int)")
	for _, testCase := range []struct {
		query       string
		lightweight bool
	}{
		{"select table_name, create_time from information_schema.tables where table_schema='test'", false},
		{"select table_name from information_schema.tables where table_schema='test' and create_time >= '2000-01-01'", true},
		{"select table_catalog, table_schema, table_name, create_time from information_schema.tables where table_schema='test' and create_time >= '2000-01-01'", true},
		{"select table_name from information_schema.tables where table_schema='test' and table_name='lightweight_time' and create_time >= '2000-01-01'", false},
		{"select table_name, tidb_table_id from information_schema.tables where table_schema='test' and create_time >= '2000-01-01'", false},
		{"select table_name from information_schema.tables where table_schema='test' and tidb_table_id=1 and create_time >= '2000-01-01'", false},
	} {
		var covered bool
		ctx := context.WithValue(context.Background(), "cover-check", &covered)
		tk.MustQueryWithContext(ctx, testCase.query)
		if covered != testCase.lightweight {
			t.Errorf("lightweight path for %q = %v, want %v", testCase.query, covered, testCase.lightweight)
		}
	}
}

func TestTablesCreateTimeRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("create table create_time_test (a int)")
	dom := domain.GetDomain(tk.Session())

	checkCreateTime := func() time.Time {
		t.Helper()
		tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("create_time_test"))
		require.NoError(t, err)
		boundary := tbl.Meta().GetUpdateTime().In(tk.Session().GetSessionVars().TimeZone)
		literal := boundary.Format("2006-01-02 15:04:05.000000")
		for _, tableCondition := range []string{"", " and table_name='create_time_test'"} {
			query := fmt.Sprintf("select table_name from information_schema.tables where table_schema='test'%s and create_time = '%s'", tableCondition, literal)
			// CAST keeps the time predicate out of candidate filtering.
			selectionQuery := fmt.Sprintf("select table_name from information_schema.tables where table_schema='test'%s and cast(create_time as datetime) = '%s'", tableCondition, literal)
			selection := tk.MustQuery(selectionQuery)
			selection.Check(testkit.Rows("create_time_test"))
			tk.MustQuery(query).Check(selection.Rows())
		}
		return boundary
	}
	checkCreateTime()

	tk.MustExec("alter table create_time_test add column b int")
	checkCreateTime()

	// Rebuild lightweight table items from persisted metadata.
	isV2, _ := infoschema.IsV2(dom.InfoSchema())
	require.True(t, isV2, "the full-reload regression test must run with InfoSchema v2")
	dom.InfoCache().Reset(16)
	require.NoError(t, dom.Reload())
	boundary := checkCreateTime()

	// Candidate filtering must preserve sub-millisecond matches and leave
	// strict comparisons at the boundary to the original predicate.
	for _, testCase := range []struct {
		op      string
		delta   time.Duration
		matches bool
	}{
		{">", -500 * time.Microsecond, true},
		{"<", 500 * time.Microsecond, true},
		{"=", 0, true},
		{">", 0, false},
		{">=", 0, true},
		{"<", 0, false},
		{"<=", 0, true},
	} {
		literal := boundary.Add(testCase.delta).Format("2006-01-02 15:04:05.000000")
		query := fmt.Sprintf("select table_name from information_schema.tables where table_schema='test' and create_time %s '%s'", testCase.op, literal)
		expected := testkit.Rows()
		if testCase.matches {
			expected = testkit.Rows("create_time_test")
		}
		tk.MustQuery(query).Check(expected)
	}
}

func TestTablesCreateTimeRangeInfoSchemaV1(t *testing.T) {
	originalSchemaCacheSize := variable.SchemaCacheSize.Load()
	t.Cleanup(func() {
		variable.SchemaCacheSize.Store(originalSchemaCacheSize)
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_schema_cache_size = 0")
	require.NoError(t, dom.Reload())
	tk.MustExec("use test")

	isV2, _ := infoschema.IsV2(dom.InfoSchema())
	require.False(t, isV2, "the range regression test must run with InfoSchema v1")

	tk.MustExec("create table create_time_v1 (a int)")
	selection := tk.MustQuery(`
		select table_name
		from information_schema.tables
		where table_schema = 'test'
		  and cast(create_time as datetime) >= '2000-01-01'`)
	selection.Check(testkit.Rows("create_time_v1"))
	tk.MustQuery(`
		select table_name
		from information_schema.tables
		where table_schema = 'test'
		  and create_time >= '2000-01-01'`).Check(selection.Rows())
}
