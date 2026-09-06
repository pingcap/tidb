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

package executor_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestReadBillingDemoLogicalLookupKeys(t *testing.T) {
	core, recorded := observer.New(zap.InfoLevel)
	oldLogger := logutil.GeneralLogger
	logutil.GeneralLogger = zap.New(core)
	oldGeneralLog := vardef.ProcessGeneralLog.Swap(false)
	t.Cleanup(func() {
		logutil.GeneralLogger = oldLogger
		vardef.ProcessGeneralLog.Store(oldGeneralLog)
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("create table lookup_keys (id int primary key clustered, k int, v int, pad varchar(32), key idx_k(k), unique key uk(v))")
	tk.MustExec("insert into lookup_keys values (1,1,10,'a'), (2,1,20,'b'), (3,2,30,'c')")
	tk.MustExec("create table lookup_composite (a int, b int, v int, primary key(a,b) clustered)")
	tk.MustExec("insert into lookup_composite values (1,1,10), (2,2,20)")
	tk.MustExec("create table lookup_customers (id int primary key clustered, v int)")
	tk.MustExec("insert into lookup_customers values (101,10), (205,20)")
	tk.MustExec("create table lookup_orders (id int primary key clustered, customer_id int, key idx_customer(customer_id))")
	tk.MustExec("insert into lookup_orders values (1,101), (2,101), (3,205), (4,999), (5,null)")
	tk.MustExec("set tidb_enable_read_billing_demo = on")
	tk.MustExec("set tidb_index_lookup_pushdown_policy = 'hint-only'")
	tk.MustExec("set tidb_enable_index_merge = on")
	vardef.ProcessGeneralLog.Store(true)

	lookupUnits := []string{"logical_lookup_keys", "logical_index_probe_keys", "logical_table_fetch_keys"}
	run := func(t *testing.T, sql, plan string, want map[string]float64) {
		t.Helper()
		require.Contains(t, fmt.Sprint(tk.MustQuery("explain "+sql).Rows()), plan)
		recorded.TakeAll()
		before := make(map[string]float64)
		for _, unit := range lookupUnits {
			before[unit], _ = readExecutorCounterVecValueByLabels(t, metrics.ReadBillingDemoBaseUnitsCounter, map[string]string{"unit": unit})
		}
		tk.MustQuery(sql).Rows()
		entries := recorded.FilterMessage("GENERAL_LOG_RU_UNITS").TakeAll()
		require.Len(t, entries, 1, sql)
		require.NotEmpty(t, entries[0].ContextMap()["statuses"])
		got := make(map[string]map[string]float64)
		expected := make(map[string]map[string]float64)
		for _, unit := range lookupUnits {
			got[unit] = make(map[string]float64)
			expected[unit] = make(map[string]float64)
		}
		for kind, keys := range want {
			expected["logical_lookup_keys"][kind] = keys
			switch kind {
			case "indexjoin", "indexhashjoin", "indexmergejoin":
				expected["logical_index_probe_keys"][kind] = keys
			case "indexlookup", "indexmerge":
				expected["logical_table_fetch_keys"][kind] = keys
			}
		}
		for _, value := range entries[0].ContextMap()["units"].([]any) {
			unit := value.(map[string]any)
			if byKind, ok := got[unit["unit"].(string)]; ok {
				require.Equal(t, "executor_lookup_inputs", unit["input_source"])
				byKind[unit["operator_kind"].(string)] += unit["value"].(float64)
			}
		}
		require.Equal(t, expected, got, sql)
		for _, unit := range lookupUnits {
			after, _ := readExecutorCounterVecValueByLabels(t, metrics.ReadBillingDemoBaseUnitsCounter, map[string]string{"unit": unit})
			total := 0.0
			for _, n := range expected[unit] {
				total += n
			}
			require.Equal(t, total, after-before[unit], sql+" "+unit)
		}
	}

	for _, tc := range []struct {
		name, sql, plan, kind string
		keys                  float64
	}{
		{"primary hit", "select pad from lookup_keys where id=1", "Point_Get", "point_get", 1},
		{"primary miss", "select pad from lookup_keys where id=999", "Point_Get", "point_get", 1},
		{"unique hit", "select pad from lookup_keys where v=10", "Point_Get", "point_get", 2},
		{"unique miss", "select pad from lookup_keys where v=999", "Point_Get", "point_get", 1},
		{"batch dedup and miss", "select pad from lookup_keys where id in (1,1,2,999)", "Batch_Point_Get", "batch_point_get", 3},
		{"unique batch two stages", "select pad from lookup_keys where v in (10,10,20,999)", "Batch_Point_Get", "batch_point_get", 5},
		{"composite primary", "select v from lookup_composite where a=1 and b=1", "Point_Get", "point_get", 1},
		{"composite batch", "select v from lookup_composite where (a,b) in ((1,1),(1,1),(2,2),(9,9))", "Batch_Point_Get", "batch_point_get", 3},
		{"index lookup table handles", "select pad from lookup_keys force index(idx_k) where k<=2", "IndexLookUp", "indexlookup", 3},
		{"index lookup empty", "select pad from lookup_keys force index(idx_k) where k=999", "IndexLookUp", "indexlookup", 0},
		{"index merge union", "select /*+ use_index_merge(lookup_keys,idx_k,uk) */ pad from lookup_keys where k=1 or v>=20", "IndexMerge", "indexmerge", 3},
		{"index merge intersection", "select /*+ use_index_merge(lookup_keys,idx_k,uk) */ pad from lookup_keys where k=1 and v>=20", "IndexMerge", "indexmerge", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run(t, tc.sql, tc.plan, map[string]float64{tc.kind: tc.keys})
		})
	}
	for _, tc := range []struct{ hint, plan, kind string }{
		{"inl_join", "IndexJoin", "indexjoin"},
		{"inl_hash_join", "IndexHashJoin", "indexhashjoin"},
	} {
		t.Run(tc.plan, func(t *testing.T) {
			for _, batch := range []int{1, 32} {
				tk.MustExec(fmt.Sprintf("set tidb_index_join_batch_size=%d", batch))
				tk.MustExec(fmt.Sprintf("set tidb_index_lookup_join_concurrency=%d", batch%3+1))
				sql := fmt.Sprintf("select /*+ %s(c) */ o.id,c.v from lookup_orders o join lookup_customers c on o.customer_id=c.id", tc.hint)
				run(t, sql, tc.plan, map[string]float64{tc.kind: 4})
			}
		})
	}
	run(t, "select /*+ inl_join(c) */ o.id,c.v from lookup_orders o join lookup_customers c on o.customer_id=c.id where o.id>100", "IndexJoin", map[string]float64{"indexjoin": 0})
	// A non-covering inner lookup owns a table-fetch stage in addition to the
	// join's outer-key binding. Batch size is fixed to deduplicate shared handles.
	tk.MustExec("set tidb_index_join_batch_size=32")
	for _, tc := range []struct{ hint, plan, kind string }{
		{"inl_join", "IndexJoin", "indexjoin"},
		{"inl_hash_join", "IndexHashJoin", "indexhashjoin"},
	} {
		sql := fmt.Sprintf("select /*+ %s(k) */ o.id,k.pad from lookup_orders o join lookup_keys k force index(idx_k) on o.customer_id=k.k", tc.hint)
		// These outer keys do not match idx_k. The probe was attempted, while
		// the inner IndexLookup's observed table-fetch count is genuinely zero.
		run(t, sql, tc.plan, map[string]float64{tc.kind: 4, "indexlookup": 0})
		sql = fmt.Sprintf("select /*+ %s(k) */ o.id,k.pad from lookup_orders o join lookup_keys k force index(idx_k) on o.id=k.k", tc.hint)
		run(t, sql, tc.plan, map[string]float64{tc.kind: 5, "indexlookup": 3})
	}
	run(t, "select /*+ hash_join(c) */ o.id,c.v from lookup_orders o join lookup_customers c on o.customer_id=c.id", "HashJoin", map[string]float64{})
	run(t, "select /*+ hash_agg() */ k,count(*) from lookup_keys group by k", "HashAgg", map[string]float64{})

	// The detailed summary retains a real zero, rather than dropping its sample.
	_, digest := parser.NormalizeDigest("select pad from lookup_keys force index(idx_k) where k=999")
	tk.MustQuery(`select value, sample_count from information_schema.statements_summary_read_billing_demo_base_units
		where digest=? and unit='logical_lookup_keys'`, digest.String()).Check(testkit.Rows("0 1"))
	tk.MustQuery(`select value, sample_count from information_schema.statements_summary_read_billing_demo_base_units
		where digest=? and unit='logical_table_fetch_keys'`, digest.String()).Check(testkit.Rows("0 1"))
	_, digest = parser.NormalizeDigest("select pad from lookup_keys force index(idx_k) where k<=2")
	tk.MustQuery(`select value, sample_count from information_schema.statements_summary_read_billing_demo_base_units
		where digest=? and unit='logical_table_fetch_keys'`, digest.String()).Check(testkit.Rows("3 1"))
	_, digest = parser.NormalizeDigest("select /*+ inl_join(c) */ o.id,c.v from lookup_orders o join lookup_customers c on o.customer_id=c.id")
	tk.MustQuery(`select operator_kind, value, sample_count from information_schema.statements_summary_read_billing_demo_base_units
		where digest=? and unit='logical_index_probe_keys' order by operator_kind`, digest.String()).Check(testkit.Rows("indexhashjoin 8 2", "indexjoin 8 2"))

	t.Run("DML read stage remains observable", func(t *testing.T) {
		recorded.TakeAll()
		tk.MustExec("update lookup_keys set pad='updated' where id=1")
		entries := recorded.FilterMessage("GENERAL_LOG_RU_UNITS").TakeAll()
		require.Len(t, entries, 1)
		found := false
		for _, value := range entries[0].ContextMap()["units"].([]any) {
			unit := value.(map[string]any)
			if unit["unit"] == "logical_lookup_keys" {
				require.Equal(t, "update", unit["dml_kind"])
				require.Equal(t, 1.0, unit["value"])
				found = true
			}
		}
		require.True(t, found)
	})

	t.Run("prepared execution does not accumulate", func(t *testing.T) {
		tk.MustExec("prepare lookup_stmt from 'select pad from lookup_keys where id=?'")
		for _, key := range []int{1, 2, 999, 1} {
			tk.MustExec(fmt.Sprintf("set @lookup_id=%d", key))
			recorded.TakeAll()
			tk.MustQuery("execute lookup_stmt using @lookup_id").Rows()
			entries := recorded.FilterMessage("GENERAL_LOG_RU_UNITS").TakeAll()
			require.Len(t, entries, 1)
			var keys float64
			for _, v := range entries[0].ContextMap()["units"].([]any) {
				u := v.(map[string]any)
				if u["unit"] == "logical_lookup_keys" {
					keys += u["value"].(float64)
				}
			}
			require.Equal(t, 1.0, keys)
		}
	})
}
