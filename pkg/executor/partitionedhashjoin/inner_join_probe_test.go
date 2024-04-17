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

package partitionedhashjoin

import (
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/util/mock"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func createEmptyResultChunk(ctx sessionctx.Context, probeIsLeft bool, probeTypes []*types.FieldType, buildTypes []*types.FieldType, probeUsedColumns []int, buildUsedColumns []int) *chunk.Chunk {
	resultTypes := make([]*types.FieldType, len(probeUsedColumns)+len(buildUsedColumns))
	if probeIsLeft {
		// first is probe column
		for _, colIndex := range probeUsedColumns {
			resultTypes = append(resultTypes, probeTypes[colIndex])
		}
		for _, colIndex := range buildUsedColumns {
			resultTypes = append(resultTypes, buildTypes[colIndex])
		}
	} else {
		// first is build columns
		for _, colIndex := range buildUsedColumns {
			resultTypes = append(resultTypes, buildTypes[colIndex])
		}
		for _, colIndex := range probeUsedColumns {
			resultTypes = append(resultTypes, probeTypes[colIndex])
		}
	}
	return chunk.New(resultTypes, ctx.GetSessionVars().MaxChunkSize, ctx.GetSessionVars().MaxChunkSize)
}

func evalOtherCondition(sessCtx sessionctx.Context, probeIsLeft bool, probeRow chunk.Row, buildRow chunk.Row, shallowRow chunk.MutRow, otherCondition expression.CNFExprs) (bool, error) {
	if probeIsLeft {
		shallowRow.ShallowCopyPartialRow(0, probeRow)
		shallowRow.ShallowCopyPartialRow(probeRow.Len(), buildRow)
	} else {
		shallowRow.ShallowCopyPartialRow(0, buildRow)
		shallowRow.ShallowCopyPartialRow(buildRow.Len(), probeRow)
	}
	valid, _, err := expression.EvalBool(sessCtx.GetExprCtx().GetEvalCtx(), otherCondition, shallowRow.ToRow())
	return valid, err
}

func appendToResultChk(probeIsLeft bool, probeRow chunk.Row, buildRow chunk.Row, probeUsedColumns []int, buildUsedColumns []int, resultChunk *chunk.Chunk) {
	if probeIsLeft {
		lWide := resultChunk.AppendRowByColIdxs(probeRow, probeUsedColumns)
		resultChunk.AppendPartialRowByColIdxs(lWide, buildRow, buildUsedColumns)
	} else {
		lWide := resultChunk.AppendRowByColIdxs(buildRow, buildUsedColumns)
		resultChunk.AppendPartialRowByColIdxs(lWide, probeRow, probeUsedColumns)
	}
}

// generate inner join result using nested loop
func genInnerJoinResult(t *testing.T, sessCtx sessionctx.Context, probeIsLeft bool, probeChunk *chunk.Chunk, buildChunks []*chunk.Chunk, probeKeyIndex []int, buildKeyIndex []int,
	probeTypes []*types.FieldType, buildTypes []*types.FieldType, probeKeyTypes []*types.FieldType, buildKeyTypes []*types.FieldType, probeUsedColumns []int, buildUsedColumns []int, otherConditions expression.CNFExprs) []*chunk.Chunk {
	returnChks := make([]*chunk.Chunk, 0, 1)
	resultChk := createEmptyResultChunk(sessCtx, probeIsLeft, probeTypes, buildTypes, probeUsedColumns, buildUsedColumns)
	shallowRowTypes := make([]*types.FieldType, 0, len(probeTypes)+len(buildTypes))
	if probeIsLeft {
		shallowRowTypes = append(shallowRowTypes, probeTypes...)
		shallowRowTypes = append(shallowRowTypes, buildTypes...)
	} else {
		shallowRowTypes = append(shallowRowTypes, buildTypes...)
		shallowRowTypes = append(shallowRowTypes, probeTypes...)
	}
	shallowRow := chunk.MutRowFromTypes(shallowRowTypes)

	for probeIndex := 0; probeIndex < probeChunk.NumRows(); probeIndex++ {
		probeRow := probeChunk.GetRow(probeIndex)
		for buildChunkIndex := 0; buildChunkIndex < len(buildChunks); buildChunkIndex++ {
			buildChunk := buildChunks[buildChunkIndex]
			for buildIndex := 0; buildIndex < buildChunk.NumRows(); buildIndex++ {
				if resultChk.IsFull() {
					returnChks = append(returnChks, resultChk)
					resultChk = createEmptyResultChunk(sessCtx, probeIsLeft, probeTypes, buildTypes, probeUsedColumns, buildUsedColumns)
				}
				buildRow := buildChunk.GetRow(buildIndex)
				ok, err := codec.EqualChunkRow(sessCtx.GetSessionVars().StmtCtx.TypeCtx(), probeRow, probeKeyTypes, probeKeyIndex,
					buildRow, buildKeyTypes, buildKeyIndex)
				require.NoError(t, err)
				if ok && otherConditions != nil {
					// key is match, check other condition
					ok, err = evalOtherCondition(sessCtx, probeIsLeft, probeRow, buildRow, shallowRow, otherConditions)
					require.NoError(t, err)
				}
				if ok {
					// construct result chunk
					appendToResultChk(probeIsLeft, probeRow, buildRow, probeUsedColumns, buildUsedColumns, resultChk)
				}
			}
		}
	}
	if resultChk.NumRows() > 0 {
		returnChks = append(returnChks, resultChk)
	}
	return returnChks
}

func TestInnerJoinProbeBasic(t *testing.T) {
	// todo test nullable type after builder support nullable type
	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.NotNullFlag)
	uintTp.AddFlag(mysql.UnsignedFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)

	buildKeyIndex := []int{0}
	probeKeyIndex := buildKeyIndex
	buildTypes := []*types.FieldType{intTp, stringTp, uintTp, stringTp}
	probeTypes := buildTypes
	buildKeyTypes := []*types.FieldType{intTp}
	probeKeyTypes := buildKeyTypes
	buildUsed := []int{0, 1, 2, 3}
	probeUsed := []int{0, 1, 2, 3}
	partitionNumber := 3
	joinType := plannercore.InnerJoin
	rightAsBuildSide := true
	needUsedFlag := joinType == plannercore.LeftOuterJoin && !rightAsBuildSide

	joinedTypes := make([]*types.FieldType, len(probeTypes)+len(buildTypes))
	if rightAsBuildSide {
		joinedTypes = append(joinedTypes, probeTypes...)
		joinedTypes = append(joinedTypes, buildTypes...)
	} else {
		joinedTypes = append(joinedTypes, buildTypes...)
		joinedTypes = append(joinedTypes, probeTypes...)
	}
	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, buildUsed, needUsedFlag)
	hashJoinCtx := &PartitionedHashJoinCtx{
		SessCtx:         mock.NewContext(),
		JoinType:        joinType,
		hashTableMeta:   meta,
		Concurrency:     uint(partitionNumber),
		OtherCondition:  nil,
		PartitionNumber: partitionNumber,
		BuildKeyTypes:   buildKeyTypes,
		ProbeKeyTypes:   probeKeyTypes,
	}
	joinProbe := NewJoinProbe(hashJoinCtx, 0, joinType, probeKeyIndex, joinedTypes, probeTypes, rightAsBuildSide)
	buildSchema := &expression.Schema{}
	for _, tp := range buildTypes {
		buildSchema.Append(&expression.Column{
			RetType: tp,
		})
	}
	hasNullableKey := false
	for _, buildKeyType := range buildKeyTypes {
		if !mysql.HasNotNullFlag(buildKeyType.GetFlag()) {
			hasNullableKey = true
			break
		}
	}
	builder := createRowTableBuilder(buildKeyIndex, buildSchema, meta, partitionNumber, hasNullableKey, false, joinProbe.NeedScanRowTable())

}
