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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestPhysicalUnionScanAttach2Task(t *testing.T) {
	ctx := mock.NewContext()
	col, cst := &expression.Column{RetType: types.NewFieldType(mysql.TypeString)}, &expression.Constant{RetType: types.NewFieldType(mysql.TypeLonglong)}
	stats := &property.StatsInfo{RowCount: 1000}
	schema := expression.NewSchema(col)
	tblInfo := &model.TableInfo{}

	// table scan
	tableScan := &physicalop.PhysicalTableScan{
		AccessCondition: []expression.Expression{col, cst},
		Table:           tblInfo,
	}
	tableScan = tableScan.Init(ctx, 0)
	tableScan.SetSchema(schema)

	// table reader
	tableReader := &physicalop.PhysicalTableReader{
		TablePlan:  tableScan,
		TablePlans: []base.PhysicalPlan{tableScan},
		StoreType:  kv.TiKV,
	}
	tableReader = tableReader.Init(ctx, 0)
	tableReader.SetSchema(schema)

	// selection
	sel := &physicalop.PhysicalSelection{Conditions: []expression.Expression{col, cst}}
	sel = sel.Init(ctx, stats, 0)

	// projection
	proj := &physicalop.PhysicalProjection{Exprs: []expression.Expression{col}}
	proj = proj.Init(ctx, stats, 0)
	proj.SetSchema(schema)

	// wrap as pointer p
	sel.SetChildren(proj)
	proj.SetChildren(tableReader)

	task := &physicalop.RootTask{}
	task.SetPlan(sel)
	// mock a union-scan and attach to task.
	unionScan := &physicalop.PhysicalUnionScan{Conditions: []expression.Expression{col, cst}}
	unionScan.Attach2Task(task)

	// assert the child task's p is unchanged.
	require.Equal(t, task.Plan(), sel)
	require.Equal(t, task.Plan().Children()[0], proj)
	require.Equal(t, task.Plan().Children()[0].Children()[0], tableReader)
	require.Equal(t, task.Plan().Children()[0].Children()[0].(*physicalop.PhysicalTableReader).TablePlans[0], tableScan)

	task2 := &physicalop.RootTask{}
	task2.SetPlan(proj)
	// mock a union-scan and attach to task.
	unionScan2 := &physicalop.PhysicalUnionScan{Conditions: []expression.Expression{col, cst}}
	unionScan2.Self = unionScan2
	unionScan2.Attach2Task(task2)

	// assert the child task's p is unchanged.
	require.Equal(t, task2.Plan(), proj)
	require.Equal(t, task2.Plan().Children()[0], tableReader)
	require.Equal(t, task2.Plan().Children()[0].(*physicalop.PhysicalTableReader).TablePlans[0], tableScan)
}
