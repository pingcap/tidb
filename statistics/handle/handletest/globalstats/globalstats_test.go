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

package globalstats_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestGlobalStatsData2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	testGlobalStats2(t, tk, dom)
}

func TestGlobalStatsData2WithConcurrency(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_merge_partition_stats_concurrency=2")
	defer func() {
		tk.MustExec("set global tidb_merge_partition_stats_concurrency=1")
	}()
	testGlobalStats2(t, tk, dom)
}

func TestIssues24349(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	testKit.MustExec("set @@tidb_analyze_version=2")
	defer testKit.MustExec("set @@tidb_analyze_version=1")
	defer testKit.MustExec("set @@tidb_partition_prune_mode='static'")
	testIssues24349(testKit)
}

func TestIssues24349WithConcurrency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	testKit.MustExec("set @@tidb_analyze_version=2")
	testKit.MustExec("set global tidb_merge_partition_stats_concurrency=2")
	defer testKit.MustExec("set @@tidb_analyze_version=1")
	defer testKit.MustExec("set @@tidb_partition_prune_mode='static'")
	defer testKit.MustExec("set global tidb_merge_partition_stats_concurrency=1")
	testIssues24349(testKit)
}

func TestGlobalStatsAndSQLBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_merge_partition_stats_concurrency=1")
	testGlobalStatsAndSQLBinding(tk)
}

func TestGlobalStatsAndSQLBindingWithConcurrency(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_merge_partition_stats_concurrency=2")
	testGlobalStatsAndSQLBinding(tk)
}
