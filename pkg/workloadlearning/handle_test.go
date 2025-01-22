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
	readTableCostMetrics := &workloadlearning.ReadTableCostMetrics{
		DbName:        ast.CIStr{O: "test", L: "test"},
		TableName:     ast.CIStr{O: "test", L: "test"},
		TableScanTime: 10.0,
		TableMemUsage: 10.0,
		ReadFrequency: 10,
		TableCost:     1.0,
	}
	tableCostMetrics := map[ast.CIStr]*workloadlearning.ReadTableCostMetrics{
		ast.CIStr{O: "test", L: "test"}: readTableCostMetrics,
	}
	handle := workloadlearning.NewWorkloadLearningHandle(dom.SysSessionPool())
	handle.SaveReadTableCostMetrics(tableCostMetrics, time.Now(), time.Now(), dom.InfoSchema())

	// check the result
	result := tk.MustQuery("select * from mysql.workload_values").Rows()
	require.Equal(t, 1, len(result))
}
