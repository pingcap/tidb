// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/types"
)

//go:generate go run ./generator/plan_cache/plan_clone_generator.go -- plan_clone_generated.go

var (
	_ base.PhysicalPlan = &physicalop.PhysicalSelection{}
	_ base.PhysicalPlan = &physicalop.PhysicalProjection{}
	_ base.PhysicalPlan = &physicalop.PhysicalTopN{}
	_ base.PhysicalPlan = &physicalop.PhysicalMaxOneRow{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableDual{}
	_ base.PhysicalPlan = &physicalop.PhysicalUnionAll{}
	_ base.PhysicalPlan = &physicalop.PhysicalSort{}
	_ base.PhysicalPlan = &physicalop.NominalSort{}
	_ base.PhysicalPlan = &physicalop.PhysicalLock{}
	_ base.PhysicalPlan = &physicalop.PhysicalLimit{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexScan{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableScan{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexLookUpReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexMergeReader{}
	_ base.PhysicalPlan = &physicalop.PhysicalHashAgg{}
	_ base.PhysicalPlan = &physicalop.PhysicalStreamAgg{}
	_ base.PhysicalPlan = &physicalop.PhysicalApply{}
	_ base.PhysicalPlan = &physicalop.PhysicalIndexJoin{}
	_ base.PhysicalPlan = &physicalop.PhysicalHashJoin{}
	_ base.PhysicalPlan = &physicalop.PhysicalMergeJoin{}
	_ base.PhysicalPlan = &physicalop.PhysicalUnionScan{}
	_ base.PhysicalPlan = &physicalop.PhysicalWindow{}
	_ base.PhysicalPlan = &physicalop.PhysicalShuffle{}
	_ base.PhysicalPlan = &physicalop.PhysicalShuffleReceiverStub{}
	_ base.PhysicalPlan = &physicalop.BatchPointGetPlan{}
	_ base.PhysicalPlan = &physicalop.PhysicalTableSample{}
	_ base.PhysicalPlan = &physicalop.PhysicalSequence{}
)

// AddExtraPhysTblIDColumn for partition table.
// For keepOrder with partition table,
// we need use partitionHandle to distinct two handles,
// the `_tidb_rowid` in different partitions can have the same value.
func AddExtraPhysTblIDColumn(sctx base.PlanContext, columns []*model.ColumnInfo, schema *expression.Schema) ([]*model.ColumnInfo, *expression.Schema, bool) {
	// Not adding the ExtraPhysTblID if already exists
	if model.FindColumnInfoByID(columns, model.ExtraPhysTblID) != nil {
		return columns, schema, false
	}
	columns = append(columns, model.NewExtraPhysTblIDColInfo())
	schema.Append(&expression.Column{
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraPhysTblID,
	})
	return columns, schema, true
}
