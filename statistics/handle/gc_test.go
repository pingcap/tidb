// Copyright 2018 PingCAP, Inc.
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

package handle_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestGCStats(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set @@tidb_analyze_version = 1")
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, index idx(a, b), index idx_a(a))")
	testKit.MustExec("insert into t values (1,1),(2,2),(3,3)")
	testKit.MustExec("analyze table t")

	testKit.MustExec("alter table t drop index idx")
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("4"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("12"))
	h := dom.StatsHandle()
	ddlLease := time.Duration(0)
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("3"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("9"))

	testKit.MustExec("alter table t drop index idx_a")
	testKit.MustExec("alter table t drop column a")
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("1"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("3"))

	testKit.MustExec("drop table t")
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("1"))
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("0"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("0"))
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("0"))
}

func TestGCPartition(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set @@tidb_analyze_version = 1")
	testkit.WithPruneMode(testKit, variable.Static, func() {
		testKit.MustExec("use test")
		testKit.MustExec("set @@session.tidb_enable_table_partition=1")
		testKit.MustExec(`create table t (a bigint(64), b bigint(64), index idx(a, b))
			    partition by range (a) (
			    partition p0 values less than (3),
			    partition p1 values less than (6))`)
		testKit.MustExec("insert into t values (1,2),(2,3),(3,4),(4,5),(5,6)")
		testKit.MustExec("analyze table t")

		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("6"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("15"))
		h := dom.StatsHandle()
		ddlLease := time.Duration(0)
		testKit.MustExec("alter table t drop index idx")
		require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("4"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("10"))

		testKit.MustExec("alter table t drop column b")
		require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("2"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("5"))

		testKit.MustExec("drop table t")
		require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
		testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("2"))
		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("0"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("0"))
		require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
		testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("0"))
	})
}

func TestGCExtendedStats(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set session tidb_enable_extended_stats = on")
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, c int)")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")
	testKit.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	testKit.MustExec("alter table t add stats_extended s2 correlation(b,c)")
	testKit.MustExec("analyze table t")

	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s1 2 [1,2] 1.000000 1",
		"s2 2 [2,3] 1.000000 1",
	))
	testKit.MustExec("alter table t drop column a")
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s1 2 [1,2] 1.000000 1",
		"s2 2 [2,3] 1.000000 1",
	))
	h := dom.StatsHandle()
	ddlLease := time.Duration(0)
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s1 2 [1,2] 1.000000 2",
		"s2 2 [2,3] 1.000000 1",
	))
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s2 2 [2,3] 1.000000 1",
	))

	testKit.MustExec("drop table t")
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s2 2 [2,3] 1.000000 1",
	))
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"s2 2 [2,3] 1.000000 2",
	))
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select name, type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows())
}

func TestGCColumnStatsUsage(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, c int)")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")
	testKit.MustExec("analyze table t")
	testKit.MustQuery("select count(*) from mysql.column_stats_usage").Check(testkit.Rows("3"))
	testKit.MustExec("alter table t drop column a")
	testKit.MustQuery("select count(*) from mysql.column_stats_usage").Check(testkit.Rows("3"))
	h := dom.StatsHandle()
	ddlLease := time.Duration(0)
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.column_stats_usage").Check(testkit.Rows("2"))
	testKit.MustExec("drop table t")
	testKit.MustQuery("select count(*) from mysql.column_stats_usage").Check(testkit.Rows("2"))
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.column_stats_usage").Check(testkit.Rows("0"))
}

func TestDeleteAnalyzeJobs(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int)")
	testKit.MustExec("insert into t values (1,2),(3,4)")
	testKit.MustExec("analyze table t")
	rows := testKit.MustQuery("show analyze status").Rows()
	require.Equal(t, 1, len(rows))
	require.NoError(t, dom.StatsHandle().DeleteAnalyzeJobs(time.Now().Add(time.Second)))
	rows = testKit.MustQuery("show analyze status").Rows()
	require.Equal(t, 0, len(rows))
}
