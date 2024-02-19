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

package exec_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/testkit"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIndexUsageReporter(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tableID := int64(1)
	indexID := int64(2)

	sc := tk.Session().GetSessionVars().StmtCtx
	statsMap := sc.GetUsedStatsInfo(true)
	statsMap.RecordUsedInfo(tableID, &stmtctx.UsedStatsInfoForTable{
		Version:       123,
		RealtimeCount: 100,
	})
	reporter := exec.NewIndexUsageReporter(sc.IndexUsageCollector, sc.RuntimeStatsColl, statsMap)
	runtimeStatsColl := sc.RuntimeStatsColl

	// For PointGet and BatchPointGet
	planID := 3
	runtimeStatsColl.GetBasicRuntimeStats(planID).Record(time.Second, 2024)
	reporter.ReportPointGetIndexUsage(tableID, tableID, indexID, planID, 1)

	require.Eventually(t, func() bool {
		tk.Session().ReportUsageStats()
		usage := dom.StatsHandle().GetIndexUsage(tableID, indexID)
		return usage.QueryTotal == 1 && usage.RowAccessTotal == 2024 && usage.KvReqTotal == 1
	}, time.Second*5, time.Millisecond)

	// For Index Scan
	planID = 4
	rows := uint64(2024)
	zero := uint64(0)
	executorID := "test-executor"
	runtimeStatsColl.GetOrCreateCopStats(planID, "test-store").RecordOneCopTask("1", &tipb.ExecutorExecutionSummary{
		TimeProcessedNs: &zero,
		NumProducedRows: &rows,
		NumIterations:   &zero,
		ExecutorId:      &executorID,
		Concurrency:     &zero,
	})
	reporter.ReportCopIndexUsage(tableID, tableID, indexID, planID)

	require.Eventually(t, func() bool {
		tk.Session().ReportUsageStats()
		usage := dom.StatsHandle().GetIndexUsage(tableID, indexID)
		return usage.QueryTotal == 1 && usage.RowAccessTotal == 2024+2024 && usage.KvReqTotal == 2
	}, time.Second*5, time.Millisecond)

	// If the version is pseudo, skip it
	statsMap.RecordUsedInfo(tableID, &stmtctx.UsedStatsInfoForTable{
		Version:       statistics.PseudoVersion,
		RealtimeCount: 100,
	})
	planID = 4
	runtimeStatsColl.GetBasicRuntimeStats(planID).Record(time.Second, 2024)
	reporter.ReportPointGetIndexUsage(tableID, tableID, indexID, planID, 1)

	require.Eventually(t, func() bool {
		tk.Session().ReportUsageStats()
		usage := dom.StatsHandle().GetIndexUsage(tableID, indexID)
		return usage.QueryTotal == 1 && usage.RowAccessTotal == 2024+2024 && usage.KvReqTotal == 2
	}, time.Second*5, time.Millisecond)
}

type indexStatsExpect struct {
	tableID int64
	idxID   int64
	samples []indexusage.Sample
}

type testCase struct {
	sql      string
	havePlan string
	expects  []indexStatsExpect
}

func mergeTwoSample(s1 *indexusage.Sample, s2 indexusage.Sample) {
	s1.QueryTotal += s2.QueryTotal
	s1.RowAccessTotal += s2.RowAccessTotal
	s1.KvReqTotal += s2.KvReqTotal
	for i, val := range s2.PercentageAccess {
		s1.PercentageAccess[i] += val
	}

	if s1.LastUsedAt.Before(s2.LastUsedAt) {
		s1.LastUsedAt = s2.LastUsedAt
	}
}

func runIndexUsageTestCases(t *testing.T, dom *domain.Domain, tk *testkit.TestKit, cases []testCase) {
	existUsageMap := make(map[indexusage.GlobalIndexID]*indexusage.Sample)
	for _, c := range cases {
		if len(c.havePlan) > 0 {
			tk.MustHavePlan(c.sql, c.havePlan)
		}
		// a special case to support preparing statement
		if strings.HasPrefix(c.sql, "prepare") || strings.HasPrefix(c.sql, "PREPARE") {
			tk.MustExec(c.sql)
			continue
		}
		tk.MustQuery(c.sql)

		for _, idx := range c.expects {
			globalIdxID := indexusage.GlobalIndexID{TableID: idx.tableID, IndexID: idx.idxID}

			existUsage := existUsageMap[globalIdxID]
			if existUsage == nil {
				existUsage = &indexusage.Sample{}
				existUsageMap[globalIdxID] = existUsage
			}
			for _, s := range idx.samples {
				mergeTwoSample(existUsage, s)
			}

			existUsageMap[globalIdxID] = existUsage
		}

		require.Eventuallyf(t, func() bool {
			tk.Session().ReportUsageStats()

			result := true
			for _, idx := range c.expects {
				usage := dom.StatsHandle().GetIndexUsage(idx.tableID, idx.idxID)
				require.NotNil(t, usage)

				globalIdxID := indexusage.GlobalIndexID{TableID: idx.tableID, IndexID: idx.idxID}
				existUsage := existUsageMap[globalIdxID]

				condition := existUsage.QueryTotal == usage.QueryTotal &&
					existUsage.KvReqTotal <= usage.KvReqTotal &&
					existUsage.RowAccessTotal == usage.RowAccessTotal &&
					existUsage.PercentageAccess == usage.PercentageAccess
				result = result && condition
				logutil.BgLogger().Info("assert index usage",
					zap.Uint64("QueryTotal", usage.QueryTotal),
					zap.Uint64("RowAccessTotal", usage.RowAccessTotal),
					zap.Uint64("KvReqTotal", usage.KvReqTotal),
					zap.Uint64s("PercentageAccess", usage.PercentageAccess[:]),
					zap.Uint64("Expect QueryTotal", existUsage.QueryTotal),
					zap.Uint64("Expect RowAccessTotal", existUsage.RowAccessTotal),
					zap.Uint64("Expect KvReqTotal", existUsage.KvReqTotal),
					zap.Uint64s("Expect PercentageAccess", existUsage.PercentageAccess[:]),
					zap.Bool("result", condition),
					zap.String("sql", c.sql))
			}
			return result
		}, time.Second*5, time.Millisecond*100, "test for sql %s failed", c.sql)
	}
}

func wrapTestCaseWithPrepare(cases []testCase) []testCase {
	result := make([]testCase, 0, len(cases)*3)
	for _, c := range cases {
		if !strings.HasPrefix(c.sql, "select") &&
			!strings.HasPrefix(c.sql, "SELECT") {
			result = append(result, c)
			continue
		}
		// For a statement beginning with select, try to prepare/execute it.
		result = append(result, []testCase{
			{
				fmt.Sprintf("prepare test_stmt from %s", driver.WrapInSingleQuotes(c.sql)),
				"",
				[]indexStatsExpect{},
			},
			// First "Execute" statement is to initialize the plan cache.
			{
				"execute test_stmt",
				"",
				c.expects,
			},
			// The second "Execute" statement will test whether index collector will successfully get a total rows for
			// the table
			{
				"execute test_stmt",
				"",
				c.expects,
			},
		}...)
	}
	return result
}

func TestIndexUsageReporterWithRealData(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id_1 int, id_2 int, unique key idx_1(id_1), unique key idx_2(id_2))")

	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableID := table.Meta().ID
	idx1ID := int64(0)
	idx2ID := int64(0)
	for _, idx := range table.Indices() {
		if idx.Meta().Name.L == "idx_1" {
			idx1ID = idx.Meta().ID
		}
		if idx.Meta().Name.L == "idx_2" {
			idx2ID = idx.Meta().ID
		}
	}

	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tk.MustExec("analyze table t")
	tk.RefreshSession()
	tk.MustExec("use test")

	cases := []testCase{
		{
			"select id_1 from t where id_1 >= 30",
			"IndexReader",
			[]indexStatsExpect{{tableID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 70, 100)}}},
		},
		{
			"select id_1 from t where id_1 - 95 >= 0 and id_1 >= 90",
			"IndexReader",
			[]indexStatsExpect{{tableID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 10, 100)}}},
		},
		{
			"select id_2 from t use index(idx_1) where id_1 >= 30 and id_1 - 50 >= 0",
			"IndexLookUp",
			[]indexStatsExpect{{tableID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 70, 100)}}},
		},
		{
			"select /*+ USE_INDEX_MERGE(t, idx_1, idx_2) */ id_2 from t where id_1 >= 30 and id_2 >= 80 and id_2 - 95 >= 0",
			"IndexMerge",
			[]indexStatsExpect{
				{tableID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 70, 100)}},
				{tableID, idx2ID, []indexusage.Sample{indexusage.NewSample(1, 1, 20, 100)}},
			},
		},
		{
			"select * from t where id_1 = 1",
			"Point_Get",
			[]indexStatsExpect{
				{tableID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 1, 100)}},
			},
		},
		{
			"select id_1 from t where id_1 = 1 or id_1 = 50 or id_1 = 25",
			"Batch_Point_Get",
			[]indexStatsExpect{
				{tableID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 3, 100)}},
			},
		},
	}

	runIndexUsageTestCases(t, dom, tk, append(cases, wrapTestCaseWithPrepare(cases)...))
}

func TestIndexUsageReporterWithPartitionTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (id_1 int, unique key idx_1(id_1))
partition by range (id_1) (
partition p0 values less than (10),
partition p1 values less than (20),
partition p2 values less than (50),
partition p3 values less than MAXVALUE)`)

	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	idx1ID := int64(0)
	for _, idx := range table.Indices() {
		if idx.Meta().Name.L == "idx_1" {
			idx1ID = idx.Meta().ID
		}
	}

	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?)", i)
	}
	tk.MustExec("analyze table t")
	tk.RefreshSession()
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")

	cases := []testCase{
		// IndexReader on two partitions
		{
			"select id_1 from t where id_1 >= 30",
			"PartitionUnion",
			[]indexStatsExpect{
				{table.Meta().ID, idx1ID, []indexusage.Sample{
					indexusage.NewSample(1, 1, 20, 30),
				}},
				{table.Meta().ID, idx1ID, []indexusage.Sample{
					indexusage.NewSample(0, 1, 50, 50),
				}},
			},
		},
		// IndexReader with selection on a single partition
		{
			"select id_1 from t where id_1 - 95 >= 0 and id_1 >= 90",
			"IndexReader",
			[]indexStatsExpect{
				{table.Meta().ID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 10, 50)}},
			},
		},
		// PointGet in a partition
		{
			"select * from t where id_1 = 1",
			"Point_Get",
			[]indexStatsExpect{
				{table.Meta().ID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 1, 10)}},
			},
		},
		// BatchPointGet in a partition
		{
			"select * from t where id_1 in (1,3,5,9)",
			"Batch_Point_Get",
			[]indexStatsExpect{
				{table.Meta().ID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 4, 100)}},
			},
		},
	}

	runIndexUsageTestCases(t, dom, tk, append(cases, wrapTestCaseWithPrepare(cases)...))
}

func TestIndexUsageReporterWithGlobalIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index='on'")
	tk.MustExec(`create table t (pk int primary key, id_1 int, unique key idx_1(id_1))
partition by range (pk) (
partition p0 values less than (10),
partition p1 values less than (20),
partition p2 values less than (50),
partition p3 values less than MAXVALUE)`)

	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	idx1ID := int64(0)
	for _, idx := range table.Indices() {
		if idx.Meta().Name.L == "idx_1" {
			idx1ID = idx.Meta().ID
		}
	}

	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tk.MustExec("analyze table t")
	tk.RefreshSession()
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index='on'")
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")

	cases := []testCase{
		// PointGet on global index
		{
			"select * from t use index(idx_1) where id_1 = 1",
			"Point_Get",
			[]indexStatsExpect{
				{table.Meta().ID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 1, 100)}},
			},
		},
		// BatchPointGet on global index
		{
			"select * from t ignore index(primary) where id_1 in (1,3,5,9)",
			"Batch_Point_Get",
			[]indexStatsExpect{
				{table.Meta().ID, idx1ID, []indexusage.Sample{indexusage.NewSample(1, 1, 4, 100)}},
			},
		},
	}

	runIndexUsageTestCases(t, dom, tk, append(cases, wrapTestCaseWithPrepare(cases)...))
}

func TestDisableIndexUsageReporter(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id_1 int, id_2 int, unique key idx_1(id_1), unique key idx_2(id_2))")

	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableID := table.Meta().ID
	idx1ID := int64(0)
	for _, idx := range table.Indices() {
		if idx.Meta().Name.L == "idx_1" {
			idx1ID = idx.Meta().ID
		}
	}

	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tk.MustExec("analyze table t")
	tk.RefreshSession()
	tk.MustExec("use test")
	tk.MustQuery("select id_1 from t where id_1 >= 30")
	tk.RefreshSession()
	require.Eventually(t, func() bool {
		return dom.StatsHandle().GetIndexUsage(tableID, idx1ID).QueryTotal == 1
	}, time.Second*5, time.Millisecond*100)
	tk.MustExec("use test")
	// turn off the index usage collection
	tk.MustExec("set tidb_enable_collect_execution_info='OFF'")
	tk.MustQuery("select id_1 from t where id_1 >= 30")
	tk.RefreshSession()
	for i := 0; i < 10; i++ {
		require.Equal(t, uint64(1), dom.StatsHandle().GetIndexUsage(tableID, idx1ID).QueryTotal)
		time.Sleep(time.Millisecond * 100)
	}
}
