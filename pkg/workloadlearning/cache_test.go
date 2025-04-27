// Copyright 2025 PingCAP, Inc.
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

package workloadlearning_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/workloadlearning"
	"github.com/stretchr/testify/require"
)

func TestUpdateTableCostCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// Create test table and insert test metrics
	tk.MustExec(`use test`)
	tk.MustExec("create table test (a int, b int, index idx(a))")

	// Create a workload learning handle to save metrics
	handle := workloadlearning.NewWorkloadLearningHandle(dom.SysSessionPool())

	// Create test metrics
	readTableCostMetrics := &workloadlearning.TableReadCostMetrics{
		DbName:        ast.CIStr{O: "test", L: "test"},
		TableName:     ast.CIStr{O: "test", L: "test"},
		TableScanTime: 10.0,
		TableMemUsage: 10.0,
		ReadFrequency: 10,
		TableReadCost: 1.0,
	}
	tableCostMetrics := map[ast.CIStr]*workloadlearning.TableReadCostMetrics{
		{O: "test", L: "test"}: readTableCostMetrics,
	}

	// Save metrics to storage
	handle.SaveTableReadCostMetrics(tableCostMetrics, time.Now(), time.Now(), dom.InfoSchema())

	// Create cache worker and test UpdateTableReadCostCache
	worker := workloadlearning.NewWLCacheWorker(dom.SysSessionPool())
	worker.UpdateTableReadCostCache()

	// Get table ID for verification
	rs := tk.MustQuery("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 'test'")
	tableIDi, _ := strconv.Atoi(rs.Rows()[0][0].(string))
	tableID := int64(tableIDi)

	// Verify cached metrics
	metrics := worker.GetTableReadCostMetrics(tableID)
	require.NotNil(t, metrics)
	require.Equal(t, 10.0, metrics.TableScanTime)
	require.Equal(t, 10.0, metrics.TableMemUsage)
	require.Equal(t, int64(10), metrics.ReadFrequency)
	require.Equal(t, 1.0, metrics.TableReadCost)
}

func TestGetTableReadCacheMetricsWithNoData(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)
	// Create cache worker without saving metrics
	worker := workloadlearning.NewWLCacheWorker(dom.SysSessionPool())
	result := worker.GetTableReadCostMetrics(1)
	require.Nil(t, result)
}
