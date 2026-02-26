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

package mvdeltamergeagg

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func (e *Exec) buildCountMerger(
	mapping Mapping,
	colID2ComputedIdx map[int]int,
	childTypes []*types.FieldType,
) (aggMerger, error) {
	if len(mapping.ColID) != 1 {
		return nil, errors.Errorf("COUNT mapping expects exactly 1 output column, got %d", len(mapping.ColID))
	}
	if len(mapping.DependencyColID) < 1 {
		return nil, errors.New("COUNT mapping requires at least one dependency column")
	}
	outputColID := mapping.ColID[0]
	deltaRef, err := resolveDepRef(mapping.DependencyColID[0], colID2ComputedIdx, e.DeltaAggColCount)
	if err != nil {
		return nil, err
	}
	deltaColID := mapping.DependencyColID[0]
	deltaTp, err := resolveFieldTypeByColID(deltaColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "COUNT mapping dependency col %d", deltaColID)
	}
	retTp, err := resolveFieldTypeByColID(outputColID, childTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "COUNT mapping output col %d", outputColID)
	}
	if err := validateCountValueTypes(retTp, deltaTp); err != nil {
		return nil, err
	}
	return &countMerger{
		outputCols: []int{outputColID},
		deltaRef:   deltaRef,
		retTp:      retTp,
	}, nil
}

type countMerger struct {
	outputCols []int
	deltaRef   depRef
	retTp      *types.FieldType
}

func (m *countMerger) outputColIDs() []int {
	return m.outputCols
}

func (m *countMerger) mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column) error {
	if len(outputCols) != 1 {
		return errors.Errorf("count merger expects exactly 1 output column slot, got %d", len(outputCols))
	}
	outputColID := m.outputCols[0]
	if outputColID < 0 || outputColID >= input.NumCols() {
		return errors.Errorf("count output col %d out of input range", outputColID)
	}
	oldCol := input.Column(outputColID)
	deltaCol, err := getDepColumn(input, computedByOrder, m.deltaRef)
	if err != nil {
		return err
	}

	numRows := input.NumRows()
	resultCol := chunk.NewColumn(m.retTp, numRows)
	resultCol.ResizeInt64(numRows, false)
	resultVals := resultCol.Int64s()

	oldVals := oldCol.Int64s()
	deltaVals := deltaCol.Int64s()
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		var oldVal int64
		if !oldCol.IsNull(rowIdx) {
			oldVal = oldVals[rowIdx]
		}
		var deltaVal int64
		if !deltaCol.IsNull(rowIdx) {
			deltaVal = deltaVals[rowIdx]
		}
		newVal, err := types.AddInt64(oldVal, deltaVal)
		if err != nil {
			return err
		}
		if newVal < 0 {
			return errors.Errorf("count becomes negative (%d) at row %d", newVal, rowIdx)
		}
		resultVals[rowIdx] = newVal
	}
	outputCols[0] = resultCol
	return nil
}

func validateCountValueTypes(outputTp, deltaTp *types.FieldType) error {
	if outputTp == nil || deltaTp == nil {
		return errors.New("COUNT mapping type is unavailable")
	}
	if outputTp.EvalType() != types.ETInt {
		return errors.Errorf("COUNT mapping output eval type must be int, got %s", outputTp.EvalType())
	}
	if deltaTp.EvalType() != types.ETInt {
		return errors.Errorf("COUNT mapping dependency eval type must be int, got %s", deltaTp.EvalType())
	}
	// COUNT merge requires signed integers.
	if mysql.HasUnsignedFlag(outputTp.GetFlag()) {
		return errors.New("COUNT mapping output type must be signed integer")
	}
	if mysql.HasUnsignedFlag(deltaTp.GetFlag()) {
		return errors.New("COUNT mapping dependency type must be signed integer")
	}
	return nil
}
