// Copyright 2024 PingCAP, Inc.
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

package usage_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCleanupPredicateColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Create table and select data with predicate.
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustQuery("select * from t where a > 1").Check(testkit.Rows("2 2", "3 3"))
	tk.MustQuery("select * from t where b > 1").Check(testkit.Rows("2 2", "3 3"))

	// Dump the statistics usage.
	h := dom.StatsHandle()
	err := h.DumpColStatsUsageToKV()
	require.NoError(t, err)

	// Check the statistics usage.
	rows := tk.MustQuery("select * from mysql.column_stats_usage").Rows()
	require.Len(t, rows, 2)

	// Drop column b.
	tk.MustExec("alter table t drop column b")
	// Get table ID.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	columns, err := h.GetPredicateColumns(tbl.Meta().ID)
	require.NoError(t, err)
	require.Len(t, columns, 1)
}

func TestAnalyzeTableWithPredicateColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Create table and select data with predicate.
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustQuery("select * from t where a > 1").Check(testkit.Rows("2 2 2", "3 3 3"))

	// Dump the statistics usage.
	h := dom.StatsHandle()
	err := h.DumpColStatsUsageToKV()
	require.NoError(t, err)

	// Set tidb_analyze_column_options to PREDICATE.
	tk.MustExec("set global tidb_analyze_column_options='PREDICATE'")

	// Analyze table and check analyze jobs.
	tk.MustExec("analyze table t")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table column a with 256 buckets, 100 topn, 1 samplerate"),
	)

	// More columns.
	tk.MustQuery("select * from t where b > 1").Check(testkit.Rows("2 2 2", "3 3 3"))

	// Dump the statistics usage.
	err = h.DumpColStatsUsageToKV()
	require.NoError(t, err)

	// Analyze again.
	tk.MustExec("analyze table t")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table columns a, b with 256 buckets, 100 topn, 1 samplerate"),
	)

	// Set tidb_analyze_column_options to ALL.
	tk.MustExec("set global tidb_analyze_column_options='ALL'")
	// Analyze again.
	tk.MustExec("analyze table t")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table all columns with 256 buckets, 100 topn, 1 samplerate"),
	)
}

func TestAnalyzeTableWithTiDBPersistAnalyzeOptionsEnabled(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Check tidb_persist_analyze_options first.
	tk.MustQuery("select @@tidb_persist_analyze_options").Check(testkit.Rows("1"))
	// Set tidb_analyze_column_options to PREDICATE.
	tk.MustExec("set global tidb_analyze_column_options='PREDICATE'")
	// Create table and select data with predicate.
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustQuery("select * from t where a > 1").Check(testkit.Rows("2 2 2", "3 3 3"))

	// Dump the statistics usage.
	h := dom.StatsHandle()
	err := h.DumpColStatsUsageToKV()
	require.NoError(t, err)

	// Analyze table with specified columns.
	tk.MustExec("analyze table t columns a, b")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table columns a, b with 256 buckets, 100 topn, 1 samplerate"),
	)

	// Analyze again, it should use the same options.
	tk.MustExec("analyze table t")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table columns a, b with 256 buckets, 100 topn, 1 samplerate"),
	)

	// Analyze table with ALL syntax.
	tk.MustExec("analyze table t all columns")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table all columns with 256 buckets, 100 topn, 1 samplerate"),
	)

	// Analyze again, it should use the same options.
	tk.MustExec("analyze table t")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table all columns with 256 buckets, 100 topn, 1 samplerate"),
	)
}

func TestAnalyzeTableWithTiDBPersistAnalyzeOptionsDisabled(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Check tidb_persist_analyze_options first.
	tk.MustQuery("select @@tidb_persist_analyze_options").Check(testkit.Rows("1"))
	// Disable tidb_persist_analyze_options.
	tk.MustExec("set global tidb_persist_analyze_options = 0")
	tk.MustQuery("select @@tidb_persist_analyze_options").Check(testkit.Rows("0"))
	// Set tidb_analyze_column_options to PREDICATE.
	tk.MustExec("set global tidb_analyze_column_options='PREDICATE'")

	// Create table and select data with predicate.
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustQuery("select * from t where a > 1").Check(testkit.Rows("2 2 2", "3 3 3"))

	// Dump the statistics usage.
	h := dom.StatsHandle()
	err := h.DumpColStatsUsageToKV()
	require.NoError(t, err)

	// Analyze table with specified columns.
	tk.MustExec("analyze table t columns a, b")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table columns a, b with 256 buckets, 100 topn, 1 samplerate"),
	)

	// Analyze again, it should use the predicate columns.
	tk.MustExec("analyze table t")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table column a with 256 buckets, 100 topn, 1 samplerate"),
	)
}

func TestAnalyzeTableWhenV1StatsExists(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Set tidb_analyze_column_options to PREDICATE.
	tk.MustExec("set global tidb_analyze_column_options='PREDICATE'")

	// Create table and select data with predicate.
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustQuery("select * from t where a > 1").Check(testkit.Rows("2 2 2", "3 3 3"))
	// Dump the statistics usage.
	h := dom.StatsHandle()
	err := h.DumpColStatsUsageToKV()
	require.NoError(t, err)

	// Set tidb_analyze_version to 1.
	tk.MustExec("set tidb_analyze_version = 1")
	tk.MustQuery("select @@tidb_analyze_version").Check(testkit.Rows("1"))

	// Analyze table.
	tk.MustExec("analyze table t")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze columns"),
	)

	// Set tidb_analyze_version to 2.
	tk.MustExec("set tidb_analyze_version = 2")
	tk.MustQuery("select @@tidb_analyze_version").Check(testkit.Rows("2"))

	// Analyze table. It should analyze all columns because the v1 stats exists.
	tk.MustExec("analyze table t")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table all columns with 256 buckets, 100 topn, 1 samplerate"),
	)
}

func TestAnalyzeNoPredicateColumnsWithIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Set tidb_analyze_column_options to PREDICATE.
	tk.MustExec("set global tidb_analyze_column_options='PREDICATE'")

	// Create table and select data without predicate.
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int, index ia (a, b))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	// Dump the statistics usage.
	h := dom.StatsHandle()
	err := h.DumpColStatsUsageToKV()
	require.NoError(t, err)

	// Analyze table.
	tk.MustExec("analyze table t")
	// Show Warnings.
	tk.MustQuery("show warnings").Check(
		testkit.Rows(
			"Warning 1105 No predicate column has been collected yet for table test.t, so only indexes and the columns composing the indexes will be analyzed",
			"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
		),
	)
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table all indexes, columns a, b with 256 buckets, 100 topn, 1 samplerate"),
	)
}

func TestAnalyzeWithNoPredicateColumnsAndNoIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Set tidb_analyze_column_options to PREDICATE.
	tk.MustExec("set global tidb_analyze_column_options='PREDICATE'")

	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	// Dump the statistics usage.
	h := dom.StatsHandle()
	err := h.DumpColStatsUsageToKV()
	require.NoError(t, err)

	// Check stats_meta first.
	rows := tk.MustQuery("select * from mysql.stats_meta where version != 0").Rows()
	require.Len(t, rows, 0, "haven't been analyzed yet")
	// Analyze table.
	tk.MustExec("analyze table t")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table with 256 buckets, 100 topn, 1 samplerate"),
	)

	// Check stats_meta again.
	rows = tk.MustQuery("select * from mysql.stats_meta where version != 0 and modify_count = 0").Rows()
	require.Len(t, rows, 1, "modify_count should be flushed")
}

func TestAnalyzeNoPredicateColumnsWithPrimaryKey(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Set tidb_analyze_column_options to PREDICATE.
	tk.MustExec("set global tidb_analyze_column_options='PREDICATE'")

	// Create table and select data without predicate.
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int, primary key (a, b))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	// Dump the statistics usage.
	h := dom.StatsHandle()
	err := h.DumpColStatsUsageToKV()
	require.NoError(t, err)

	// Analyze table.
	tk.MustExec("analyze table t")
	tk.MustQuery("select table_name, job_info from mysql.analyze_jobs order by id desc limit 1").Check(
		testkit.Rows("t analyze table all indexes, columns a, b with 256 buckets, 100 topn, 1 samplerate"),
	)
}
