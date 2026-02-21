// Copyright 2022 PingCAP, Inc.
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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/set"
)

func (b *executorBuilder) buildTblID2FKCascadeExecs(tblID2Table map[int64]table.Table, tblID2FKCascades map[int64][]*physicalop.FKCascade) (map[int64][]*FKCascadeExec, error) {
	fkCascadesMap := make(map[int64][]*FKCascadeExec)
	for tid, tbl := range tblID2Table {
		fkCascades, err := b.buildFKCascadeExecs(tbl, tblID2FKCascades[tid])
		if err != nil {
			return nil, err
		}
		if len(fkCascades) > 0 {
			fkCascadesMap[tid] = fkCascades
		}
	}
	return fkCascadesMap, nil
}

func (b *executorBuilder) buildFKCascadeExecs(tbl table.Table, fkCascades []*physicalop.FKCascade) ([]*FKCascadeExec, error) {
	fkCascadeExecs := make([]*FKCascadeExec, 0, len(fkCascades))
	for _, fkCascade := range fkCascades {
		fkCascadeExec, err := b.buildFKCascadeExec(tbl, fkCascade)
		if err != nil {
			return nil, err
		}
		if fkCascadeExec != nil {
			fkCascadeExecs = append(fkCascadeExecs, fkCascadeExec)
		}
	}
	return fkCascadeExecs, nil
}

func (b *executorBuilder) buildFKCascadeExec(tbl table.Table, fkCascade *physicalop.FKCascade) (*FKCascadeExec, error) {
	colsOffsets, err := getFKColumnsOffsets(tbl.Meta(), fkCascade.ReferredFK.Cols)
	if err != nil {
		return nil, err
	}
	helper := &fkValueHelper{
		colsOffsets: colsOffsets,
		fkValuesSet: set.NewStringSet(),
	}
	return &FKCascadeExec{
		b:                  b,
		fkValueHelper:      helper,
		plan:               fkCascade,
		tp:                 fkCascade.Tp,
		referredFK:         fkCascade.ReferredFK,
		childTable:         fkCascade.ChildTable.Meta(),
		fk:                 fkCascade.FK,
		fkCols:             fkCascade.FKCols,
		fkIdx:              fkCascade.FKIdx,
		fkUpdatedValuesMap: make(map[string]*UpdatedValuesCouple),
	}, nil
}

func (fkc *FKCascadeExec) onDeleteRow(sc *stmtctx.StatementContext, row []types.Datum) error {
	vals, err := fkc.fetchFKValuesWithCheck(sc, row)
	if err != nil || len(vals) == 0 {
		return err
	}
	fkc.fkValues = append(fkc.fkValues, vals)
	return nil
}

func (fkc *FKCascadeExec) onUpdateRow(sc *stmtctx.StatementContext, oldRow, newRow []types.Datum) error {
	oldVals, err := fkc.fetchFKValuesWithCheck(sc, oldRow)
	if err != nil || len(oldVals) == 0 {
		return err
	}
	if ast.ReferOptionType(fkc.fk.OnUpdate) == ast.ReferOptionSetNull {
		fkc.fkValues = append(fkc.fkValues, oldVals)
		return nil
	}
	newVals, err := fkc.fetchFKValues(newRow)
	if err != nil {
		return err
	}
	newValsKey, err := codec.EncodeKey(sc.TimeZone(), nil, newVals...)
	err = sc.HandleError(err)
	if err != nil {
		return err
	}
	couple := fkc.fkUpdatedValuesMap[string(newValsKey)]
	if couple == nil {
		couple = &UpdatedValuesCouple{
			NewValues: newVals,
		}
	}
	couple.OldValuesList = append(couple.OldValuesList, oldVals)
	fkc.fkUpdatedValuesMap[string(newValsKey)] = couple
	return nil
}

func (fkc *FKCascadeExec) buildExecutor(ctx context.Context) (exec.Executor, error) {
	p, err := fkc.buildFKCascadePlan(ctx)
	if err != nil || p == nil {
		return nil, err
	}
	fkc.plan.CascadePlans = append(fkc.plan.CascadePlans, p)
	e := fkc.b.build(p)
	return e, fkc.b.err
}

// maxHandleFKValueInOneCascade uses to limit the max handle fk value in one cascade executor,
// this is to avoid performance issue, see: https://github.com/pingcap/tidb/issues/38631
var maxHandleFKValueInOneCascade = 1024

func (fkc *FKCascadeExec) buildFKCascadePlan(ctx context.Context) (base.Plan, error) {
	if len(fkc.fkValues) == 0 && len(fkc.fkUpdatedValuesMap) == 0 {
		return nil, nil
	}
	var indexName ast.CIStr
	if fkc.fkIdx != nil {
		indexName = fkc.fkIdx.Name
	}
	var stmtNode ast.StmtNode
	switch fkc.tp {
	case physicalop.FKCascadeOnDelete:
		fkValues := fkc.fetchOnDeleteOrUpdateFKValues()
		switch ast.ReferOptionType(fkc.fk.OnDelete) {
		case ast.ReferOptionCascade:
			stmtNode = GenCascadeDeleteAST(fkc.referredFK.ChildSchema, fkc.childTable.Name, indexName, fkc.fkCols, fkValues)
		case ast.ReferOptionSetNull:
			stmtNode = GenCascadeSetNullAST(fkc.referredFK.ChildSchema, fkc.childTable.Name, indexName, fkc.fkCols, fkValues)
		}
	case physicalop.FKCascadeOnUpdate:
		switch ast.ReferOptionType(fkc.fk.OnUpdate) {
		case ast.ReferOptionCascade:
			couple := fkc.fetchUpdatedValuesCouple()
			if couple != nil && len(couple.NewValues) != 0 {
				if fkc.stats != nil {
					fkc.stats.Keys += len(couple.OldValuesList)
				}
				stmtNode = GenCascadeUpdateAST(fkc.referredFK.ChildSchema, fkc.childTable.Name, indexName, fkc.fkCols, couple)
			}
		case ast.ReferOptionSetNull:
			fkValues := fkc.fetchOnDeleteOrUpdateFKValues()
			stmtNode = GenCascadeSetNullAST(fkc.referredFK.ChildSchema, fkc.childTable.Name, indexName, fkc.fkCols, fkValues)
		}
	}
	if stmtNode == nil {
		return nil, errors.Errorf("generate foreign key cascade ast failed, %v", fkc.tp)
	}
	sctx := fkc.b.ctx
	nodeW := resolve.NewNodeW(stmtNode)
	err := plannercore.Preprocess(ctx, sctx, nodeW)
	if err != nil {
		return nil, err
	}
	finalPlan, err := planner.OptimizeForForeignKeyCascade(ctx, sctx.GetPlanCtx(), nodeW, fkc.b.is)
	if err != nil {
		return nil, err
	}
	return finalPlan, err
}

func (fkc *FKCascadeExec) fetchOnDeleteOrUpdateFKValues() [][]types.Datum {
	var fkValues [][]types.Datum
	if len(fkc.fkValues) <= maxHandleFKValueInOneCascade {
		fkValues = fkc.fkValues
		fkc.fkValues = nil
	} else {
		fkValues = fkc.fkValues[:maxHandleFKValueInOneCascade]
		fkc.fkValues = fkc.fkValues[maxHandleFKValueInOneCascade:]
	}
	if fkc.stats != nil {
		fkc.stats.Keys += len(fkValues)
	}
	return fkValues
}

func (fkc *FKCascadeExec) fetchUpdatedValuesCouple() *UpdatedValuesCouple {
	for k, couple := range fkc.fkUpdatedValuesMap {
		if len(couple.OldValuesList) <= maxHandleFKValueInOneCascade {
			delete(fkc.fkUpdatedValuesMap, k)
			return couple
		}
		result := &UpdatedValuesCouple{
			NewValues:     couple.NewValues,
			OldValuesList: couple.OldValuesList[:maxHandleFKValueInOneCascade],
		}
		couple.OldValuesList = couple.OldValuesList[maxHandleFKValueInOneCascade:]
		return result
	}
	return nil
}

