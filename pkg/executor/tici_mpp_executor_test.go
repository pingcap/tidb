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

package executor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func buildMockIndexScanForTiCITests(ctx sessionctx.Context, isTiCI bool, ranges []*ranger.Range, schemaCols ...*expression.Column) *physicalop.PhysicalIndexScan {
	planCtx := ctx.GetPlanCtx()
	index := &model.IndexInfo{ID: 100, Name: ast.NewCIStr("idx")}
	if isTiCI {
		index.FullTextInfo = &model.FullTextIndexInfo{}
	}
	scan := (&physicalop.PhysicalIndexScan{
		Table:  &model.TableInfo{ID: 42, Name: ast.NewCIStr("t")},
		Index:  index,
		Ranges: ranges,
	}).Init(planCtx, 0)
	if len(schemaCols) == 0 {
		scan.SetSchema(expression.NewSchema())
	} else {
		scan.SetSchema(expression.NewSchema(schemaCols...))
	}
	return scan
}

func TestCloneMPPIndexSenderForTiCIRewritesChildScan(t *testing.T) {
	ctx := defaultCtx()
	planCtx := ctx.GetPlanCtx()
	originalRange := generateIndexRange(1)
	col := &expression.Column{ID: 1, Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	indexScan := buildMockIndexScanForTiCITests(ctx, true, []*ranger.Range{originalRange}, col)

	sender := (physicalop.PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_PassThrough,
	}).Init(planCtx, &property.StatsInfo{RowCount: 1})
	sender.SetChildren(indexScan)

	rewrittenRange := generateIndexRange(9)
	physicalTableID := int64(101)
	cloned, err := cloneMPPIndexSenderForTiCI(planCtx, sender, &physicalTableID, []*ranger.Range{rewrittenRange})
	require.NoError(t, err)
	require.NotSame(t, sender, cloned)

	clonedIndexScan, ok := cloned.Children()[0].(*physicalop.PhysicalIndexScan)
	require.True(t, ok)
	require.True(t, clonedIndexScan.IsPartition)
	require.Equal(t, physicalTableID, clonedIndexScan.PhysicalTableID)
	require.Equal(t, int64(9), clonedIndexScan.Ranges[0].LowVal[0].GetInt64())
	require.Same(t, clonedIndexScan.Schema(), cloned.Schema())

	// Verify clone rewrite isolation.
	require.False(t, indexScan.IsPartition)
	require.Equal(t, int64(1), indexScan.Ranges[0].LowVal[0].GetInt64())
	rewrittenRange.LowVal[0].SetInt64(999)
	require.Equal(t, int64(9), clonedIndexScan.Ranges[0].LowVal[0].GetInt64())
}

func TestCloneMPPIndexSenderForTiCIFailsWithoutTiCIIndexScan(t *testing.T) {
	ctx := defaultCtx()
	planCtx := ctx.GetPlanCtx()
	indexScan := buildMockIndexScanForTiCITests(ctx, false, []*ranger.Range{generateIndexRange(1)})
	sender := (physicalop.PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_PassThrough,
	}).Init(planCtx, &property.StatsInfo{RowCount: 1})
	sender.SetChildren(indexScan)

	_, err := cloneMPPIndexSenderForTiCI(planCtx, sender, nil, []*ranger.Range{generateIndexRange(2)})
	require.ErrorContains(t, err, "failed to locate TiCI index scan in MPP index sender")
}

func TestGetHandleOffsetsForTiCIMPPUsesExtraHandleColumn(t *testing.T) {
	ctx := defaultCtx()
	table := buildMockPhysicalTableForTiCIMPPTests(t, 88)
	col1 := &expression.Column{ID: 11, Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	extra := &expression.Column{ID: model.ExtraHandleID, Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	indexScan := buildMockIndexScanForTiCITests(ctx, true, nil, col1, extra)

	w := &indexWorker{
		idxLookup: &IndexLookUpExecutor{
			BaseExecutorV2: exec.NewBaseExecutorV2(ctx.GetSessionVars(), expression.NewSchema(), 0),
			table:          table,
			idxPlans:       []base.PhysicalPlan{indexScan},
			handleCols:     []*expression.Column{{ID: 999, RetType: types.NewFieldType(mysql.TypeLonglong)}},
		},
	}
	offsets, err := w.getHandleOffsetsForTiCIMPP()
	require.NoError(t, err)
	require.Equal(t, []int{1}, offsets)
}

func TestGetHandleOffsetsForTiCIMPPFailFastWhenHandleMissing(t *testing.T) {
	ctx := defaultCtx()
	table := buildMockPhysicalTableForTiCIMPPTests(t, 99)
	col := &expression.Column{ID: 11, Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	indexScan := buildMockIndexScanForTiCITests(ctx, true, nil, col)

	w := &indexWorker{
		idxLookup: &IndexLookUpExecutor{
			BaseExecutorV2: exec.NewBaseExecutorV2(ctx.GetSessionVars(), expression.NewSchema(), 0),
			table:          table,
			idxPlans:       []base.PhysicalPlan{indexScan},
			handleCols:     []*expression.Column{{ID: 999, RetType: types.NewFieldType(mysql.TypeLonglong)}},
		},
	}
	_, err := w.getHandleOffsetsForTiCIMPP()
	require.ErrorContains(t, err, "int handle column not found in TiCI MPP schema")
}

func TestGetHandleOffsetsForTiCIMPPUsesPKHandleWhenHandleColsEmpty(t *testing.T) {
	ctx := defaultCtx()
	pkFieldType := types.NewFieldType(mysql.TypeLonglong)
	pkFieldType.SetFlag(mysql.PriKeyFlag)
	tbl := tables.MockTableFromMeta(&model.TableInfo{
		ID:         199,
		Name:       ast.NewCIStr("t"),
		State:      model.StatePublic,
		PKIsHandle: true,
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				Offset:    0,
				State:     model.StatePublic,
				FieldType: *pkFieldType,
			},
			{
				ID:        3,
				Name:      ast.NewCIStr("content"),
				Offset:    1,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeString),
			},
		},
	})
	require.NotNil(t, tbl)

	idCol := &expression.Column{ID: 1, Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	contentCol := &expression.Column{ID: 3, Index: 1, RetType: types.NewFieldType(mysql.TypeString)}
	versionCol := &expression.Column{ID: -1024, Index: 2, RetType: types.NewFieldType(mysql.TypeLonglong)}
	indexScan := buildMockIndexScanForTiCITests(ctx, true, nil, idCol, contentCol, versionCol)

	w := &indexWorker{
		idxLookup: &IndexLookUpExecutor{
			BaseExecutorV2: exec.NewBaseExecutorV2(ctx.GetSessionVars(), expression.NewSchema(), 0),
			table:          tbl,
			idxPlans:       []base.PhysicalPlan{indexScan},
			handleCols:     nil,
		},
	}
	offsets, err := w.getHandleOffsetsForTiCIMPP()
	require.NoError(t, err)
	require.Equal(t, []int{0}, offsets)
}

func TestUseMPPExecutionTreatsTiCIIndexAsTiCIPath(t *testing.T) {
	ctx := defaultCtx()
	planCtx := ctx.GetPlanCtx()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(vardef.TiDBAllowMPPExecution, vardef.Off))

	schemaCol := &expression.Column{ID: 1, Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	ticiScan := buildMockIndexScanForTiCITests(ctx, true, nil, schemaCol)
	ticiSender := (physicalop.PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_PassThrough}).Init(planCtx, &property.StatsInfo{RowCount: 1})
	ticiSender.SetChildren(ticiScan)
	ticiReader := &physicalop.PhysicalTableReader{
		TablePlan:  ticiSender,
		TablePlans: []base.PhysicalPlan{ticiScan},
	}
	require.True(t, useMPPExecution(ctx, ticiReader))

	normalScan := buildMockIndexScanForTiCITests(ctx, false, nil, schemaCol)
	normalSender := (physicalop.PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_PassThrough}).Init(planCtx, &property.StatsInfo{RowCount: 1})
	normalSender.SetChildren(normalScan)
	normalReader := &physicalop.PhysicalTableReader{
		TablePlan:  normalSender,
		TablePlans: []base.PhysicalPlan{normalScan},
	}
	require.False(t, useMPPExecution(ctx, normalReader))
}
