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
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/workloadlearning"
	"github.com/stretchr/testify/require"
)

func TestSaveReadTableCostMetrics(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table test (a int, b int, index idx(a))")
	// mock a table cost metrics
	readTableCostMetrics := &workloadlearning.TableReadCostMetrics{
		DbName:        ast.CIStr{O: "test", L: "test"},
		TableName:     ast.CIStr{O: "test", L: "test"},
		TableScanTime: time.Duration(10),
		TableMemUsage: 10,
		ReadFrequency: 10,
		TableReadCost: 1.0,
	}
	tableCostMetrics := map[int64]*workloadlearning.TableReadCostMetrics{
		1: readTableCostMetrics,
	}
	handle := workloadlearning.NewWorkloadLearningHandle(dom.SysSessionPool())
	handle.SaveTableReadCostMetrics(tableCostMetrics, time.Now(), time.Now())

	// check the result
	result := tk.MustQuery("select * from mysql.tidb_workload_values").Rows()
	require.Equal(t, 1, len(result))
}

func TestAccumulateMetricsGroupByTableID(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table test1 (a int, b int, index idx(a))")
	tk.MustExec("create table test2 (a int, b int, index idx(b))")
	// mock a current record cost metrics
	currentRecordTest1Metric := &workloadlearning.TableReadCostMetrics{
		DbName:        ast.CIStr{O: "test", L: "test"},
		TableName:     ast.CIStr{O: "test1", L: "test1"},
		TableScanTime: time.Duration(10),
		TableMemUsage: 10,
		ReadFrequency: 0,
		TableReadCost: 0.0,
	}
	currentRecordTest2Metric := &workloadlearning.TableReadCostMetrics{
		DbName:        ast.CIStr{O: "test", L: "test"},
		TableName:     ast.CIStr{O: "test2", L: "test2"},
		TableScanTime: time.Duration(10),
		TableMemUsage: 10,
		ReadFrequency: 0,
		TableReadCost: 0.0,
	}
	currentRecordMetrics := [](*workloadlearning.TableReadCostMetrics){
		currentRecordTest1Metric,
		currentRecordTest2Metric,
	}
	// mock previous metrics
	previousRecordTest1Metric := &workloadlearning.TableReadCostMetrics{
		DbName:        ast.CIStr{O: "test", L: "test"},
		TableName:     ast.CIStr{O: "test1", L: "test1"},
		TableScanTime: time.Duration(10),
		TableMemUsage: 10,
		ReadFrequency: 1,
		TableReadCost: 0.0,
	}
	tblInfo1, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("test1"))
	require.NoError(t, err)
	previousMetrics := map[int64]*workloadlearning.TableReadCostMetrics{
		tblInfo1.Meta().ID: previousRecordTest1Metric,
	}

	// accumulate metrics
	workloadlearning.AccumulateMetricsGroupByTableID(context.Background(), currentRecordMetrics, 2, previousMetrics, dom.InfoSchema())

	// check the result
	require.Equal(t, 2, len(previousMetrics))
	require.Equal(t, int64(3), previousMetrics[tblInfo1.Meta().ID].ReadFrequency)
	require.Equal(t, time.Duration(30), previousMetrics[tblInfo1.Meta().ID].TableScanTime)
	require.Equal(t, int64(30), previousMetrics[tblInfo1.Meta().ID].TableMemUsage)
	require.Equal(t, 0.0, previousMetrics[tblInfo1.Meta().ID].TableReadCost)
	tblInfo2, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("test2"))
	require.NoError(t, err)
	require.Equal(t, int64(2), previousMetrics[tblInfo2.Meta().ID].ReadFrequency)
	require.Equal(t, time.Duration(20), previousMetrics[tblInfo2.Meta().ID].TableScanTime)
	require.Equal(t, int64(20), previousMetrics[tblInfo2.Meta().ID].TableMemUsage)
	require.Equal(t, 0.0, previousMetrics[tblInfo2.Meta().ID].TableReadCost)
}
