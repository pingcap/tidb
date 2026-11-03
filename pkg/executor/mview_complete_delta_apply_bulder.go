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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
)

func (b *executorBuilder) buildMViewCompleteDeltaApply(v *plannercore.MViewCompleteDeltaApply) exec.Executor {
	if v.Source == nil {
		b.err = errors.New("MViewCompleteDeltaApply source plan is nil")
		return nil
	}
	origin := b.inMViewCompleteDeltaApplyStmt
	b.inMViewCompleteDeltaApplyStmt = true
	defer func() { b.inMViewCompleteDeltaApplyStmt = origin }()
	if b.err = b.updateForUpdateTS(); b.err != nil {
		return nil
	}
	sourceExec := b.build(v.Source)
	if b.err != nil || sourceExec == nil {
		if b.err == nil {
			b.err = errors.New("MViewCompleteDeltaApply source executor is nil")
		}
		return nil
	}
	sourceTypes := sourceExec.RetFieldTypes()
	if v.OpColID < 0 || v.OpColID >= len(sourceTypes) {
		b.err = errors.Errorf("MViewCompleteDeltaApply op column id %d out of source range [0,%d)", v.OpColID, len(sourceTypes))
		return nil
	}
	if sourceTypes[v.OpColID] == nil || sourceTypes[v.OpColID].EvalType() != types.ETInt {
		b.err = errors.Errorf("MViewCompleteDeltaApply op column id %d must be integer typed", v.OpColID)
		return nil
	}
	target, ok := b.is.TableByID(context.Background(), v.MVTableID)
	if !ok {
		b.err = errors.Errorf("MViewCompleteDeltaApply target table id %d not found in infoschema", v.MVTableID)
		return nil
	}
	if v.CurrentHandleCols == nil {
		b.err = errors.New("MViewCompleteDeltaApply target handle cols is nil")
		return nil
	}
	if len(target.Cols()) != v.MVColumnCount {
		b.err = errors.Errorf("MViewCompleteDeltaApply target public column count %d != plan MV column count %d", len(target.Cols()), v.MVColumnCount)
		return nil
	}
	for i := range v.CurrentHandleCols.NumCols() {
		idx := v.CurrentHandleCols.GetCol(i).Index
		if idx < 0 || idx >= len(sourceTypes) {
			b.err = errors.Errorf("MViewCompleteDeltaApply handle col index %d out of source range [0,%d)", idx, len(sourceTypes))
			return nil
		}
	}
	currentIDs, err := buildMViewCompleteDeltaWritableInputColIDs(target, v.CurrentRowInputColIDs)
	if err != nil {
		b.err = err
		return nil
	}
	recomputedIDs, err := buildMViewCompleteDeltaWritableInputColIDs(target, v.RecomputedRowInputColIDs)
	if err != nil {
		b.err = err
		return nil
	}
	if err := validateMViewCompleteDeltaWritableInputColTypes(target, sourceTypes, currentIDs); err != nil {
		b.err = err
		return nil
	}
	if err := validateMViewCompleteDeltaWritableInputColTypes(target, sourceTypes, recomputedIDs); err != nil {
		b.err = err
		return nil
	}
	compareIdx, currentCompare, recomputedCompare, err := buildMViewCompleteDeltaCompareMappings(target, v.GroupKeyMVOffsets, currentIDs, recomputedIDs)
	if err != nil {
		b.err = err
		return nil
	}
	return &MViewCompleteDeltaApplyExec{
		BaseExecutor: exec.NewBaseExecutor(b.sctx, v.Schema(), v.ID(), sourceExec),
		TargetTable:  target, TargetHandleCols: v.CurrentHandleCols, OpColID: v.OpColID,
		CurrentWritableInputColIDs: currentIDs, RecomputedWritableInputColIDs: recomputedIDs,
		CompareWritableIdxes: compareIdx, CurrentCompareInputColIDs: currentCompare,
		RecomputedCompareInputColIDs: recomputedCompare,
	}
}

func buildMViewCompleteDeltaWritableInputColIDs(target table.Table, rowInputColIDs []int) ([]int, error) {
	if target == nil {
		return nil, errors.New("MViewCompleteDeltaApply target table is nil")
	}
	writableCols := target.WritableCols()
	if len(writableCols) == 0 {
		return nil, errors.New("MViewCompleteDeltaApply target table has no writable columns")
	}
	publicCols := target.Cols()
	if len(publicCols) != len(writableCols) {
		return nil, errors.New("MViewCompleteDeltaApply does not support target table with non-public writable columns")
	}
	if len(rowInputColIDs) != len(publicCols) {
		return nil, errors.Errorf("MViewCompleteDeltaApply row input column mapping length %d != target public column count %d", len(rowInputColIDs), len(publicCols))
	}
	ids := make([]int, 0, len(writableCols))
	for i, col := range writableCols {
		if col == nil {
			return nil, errors.Errorf("MViewCompleteDeltaApply target writable column at offset %d is nil", i)
		}
		if col.Offset < 0 || col.Offset >= len(rowInputColIDs) {
			return nil, errors.Errorf(
				"MViewCompleteDeltaApply target writable column `%s` offset %d out of range [0,%d)",
				col.Name.O, col.Offset, len(rowInputColIDs),
			)
		}
		ids = append(ids, rowInputColIDs[col.Offset])
	}
	return ids, nil
}

func buildMViewCompleteDeltaCompareMappings(target table.Table, groupKeyOffsets, currentIDs, recomputedIDs []int) (compareWritableIdxes, currentCompareInputColIDs, recomputedCompareInputColIDs []int, err error) {
	if target == nil {
		return nil, nil, nil, errors.New("MViewCompleteDeltaApply target table is nil")
	}
	if len(currentIDs) != len(recomputedIDs) {
		return nil, nil, nil, errors.Errorf(
			"MViewCompleteDeltaApply writable input mapping length mismatch: current=%d recomputed=%d",
			len(currentIDs), len(recomputedIDs),
		)
	}
	publicCols, writableCols := target.Cols(), target.WritableCols()
	if len(publicCols) != len(writableCols) {
		return nil, nil, nil, errors.New("MViewCompleteDeltaApply does not support target table with non-public writable columns")
	}
	if len(groupKeyOffsets) == 0 {
		return nil, nil, nil, errors.New("MViewCompleteDeltaApply group key offsets are empty")
	}
	isKey := make([]bool, len(publicCols))
	for i, off := range groupKeyOffsets {
		if off < 0 || off >= len(publicCols) {
			return nil, nil, nil, errors.Errorf(
				"MViewCompleteDeltaApply group key offset %d at index %d out of range [0,%d)",
				off, i, len(publicCols),
			)
		}
		isKey[off] = true
	}
	compareIdx := make([]int, 0, len(writableCols))
	currentCompare := make([]int, 0, len(writableCols))
	recomputedCompare := make([]int, 0, len(writableCols))
	for writableIdx, col := range writableCols {
		if col == nil {
			return nil, nil, nil, errors.Errorf("MViewCompleteDeltaApply target writable column at offset %d is nil", writableIdx)
		}
		if col.Offset < 0 || col.Offset >= len(publicCols) {
			return nil, nil, nil, errors.Errorf(
				"MViewCompleteDeltaApply target writable column `%s` offset %d out of range [0,%d)",
				col.Name.O, col.Offset, len(publicCols),
			)
		}
		if !isKey[col.Offset] {
			compareIdx = append(compareIdx, writableIdx)
			currentCompare = append(currentCompare, currentIDs[writableIdx])
			recomputedCompare = append(recomputedCompare, recomputedIDs[writableIdx])
		}
	}
	return compareIdx, currentCompare, recomputedCompare, nil
}
