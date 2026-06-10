// Copyright 2019 PingCAP, Inc.
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

package join

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func defaultCtx() sessionctx.Context {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = vardef.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = vardef.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, ctx.GetSessionVars().MemQuotaQuery)
	ctx.GetSessionVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)
	ctx.GetSessionVars().SnapshotTS = uint64(1)
	ctx.BindDomainAndSchValidator(domain.NewMockDomain(), nil)
	return ctx
}

func TestJoinerOtherConditionChunkUsesInitChunkSize(t *testing.T) {
	lfields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	rfields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	fields := append(append([]*types.FieldType{}, lfields...), rfields...)
	conditions := []expression.Expression{expression.NewOne()}
	defaultInner := []types.Datum{types.NewIntDatum(0)}

	for _, joinType := range []base.JoinType{base.InnerJoin, base.LeftOuterJoin, base.RightOuterJoin} {
		ctx := defaultCtx()
		initChunkSize := 8
		ctx.GetSessionVars().InitChunkSize = initChunkSize
		maxChunkSize := ctx.GetSessionVars().MaxChunkSize
		joiner := NewJoiner(ctx, joinType, false, defaultInner, conditions, lfields, rfields, nil, false)
		base := joinerBaseForTest(t, joiner)
		require.NotNil(t, base.chk)
		require.Equal(t, initChunkSize, base.chk.Capacity())

		cloned := joiner.Clone()
		clonedBase := joinerBaseForTest(t, cloned)
		require.NotNil(t, clonedBase.chk)
		require.Equal(t, initChunkSize, clonedBase.chk.Capacity())

		outerRow := genTestChunk(maxChunkSize, 1, lfields).GetRow(0)
		innerChk := genTestChunk(maxChunkSize, initChunkSize+1, rfields)
		result := chunk.New(fields, maxChunkSize, maxChunkSize)
		iter := chunk.NewIterator4Chunk(innerChk)
		iter.Begin()
		_, _, err := joiner.TryToMatchInners(outerRow, iter, result)
		require.NoError(t, err)
		require.Equal(t, initChunkSize+1, result.NumRows())
	}
}

func joinerBaseForTest(t *testing.T, joiner Joiner) *baseJoiner {
	switch j := joiner.(type) {
	case *innerJoiner:
		return &j.baseJoiner
	case *leftOuterJoiner:
		return &j.baseJoiner
	case *rightOuterJoiner:
		return &j.baseJoiner
	default:
		t.Fatalf("unexpected joiner type %T", joiner)
	}
	return nil
}

func TestRequiredRows(t *testing.T) {
	joinTypes := []base.JoinType{base.InnerJoin, base.LeftOuterJoin, base.RightOuterJoin}
	lTypes := [][]byte{
		{mysql.TypeLong},
		{mysql.TypeFloat},
		{mysql.TypeLong, mysql.TypeFloat},
	}
	rTypes := lTypes

	convertTypes := func(mysqlTypes []byte) []*types.FieldType {
		fieldTypes := make([]*types.FieldType, 0, len(mysqlTypes))
		for _, t := range mysqlTypes {
			fieldTypes = append(fieldTypes, types.NewFieldType(t))
		}
		return fieldTypes
	}

	for _, joinType := range joinTypes {
		for _, ltype := range lTypes {
			for _, rtype := range rTypes {
				maxChunkSize := defaultCtx().GetSessionVars().MaxChunkSize
				lfields := convertTypes(ltype)
				rfields := convertTypes(rtype)
				outerIsRight := joinType == base.RightOuterJoin
				outerFields, innerFields := lfields, rfields
				if outerIsRight {
					outerFields, innerFields = rfields, lfields
				}
				outerRow := genTestChunk(maxChunkSize, 1, outerFields).GetRow(0)
				innerChk := genTestChunk(maxChunkSize, maxChunkSize, innerFields)
				var defaultInner []types.Datum
				for i, f := range innerFields {
					defaultInner = append(defaultInner, innerChk.GetRow(0).GetDatum(i, f))
				}
				conditionCases := []struct {
					name       string
					conditions []expression.Expression
				}{
					{name: "withoutConditions"},
					{name: "withConditions", conditions: []expression.Expression{expression.NewOne()}},
				}

				fields := make([]*types.FieldType, 0, len(lfields)+len(rfields))
				fields = append(fields, lfields...)
				fields = append(fields, rfields...)

				for _, conditionCase := range conditionCases {
					joiner := NewJoiner(defaultCtx(), joinType, outerIsRight, defaultInner, conditionCase.conditions, lfields, rfields, nil, false)
					result := chunk.New(fields, maxChunkSize, maxChunkSize)

					for _, required := range []int{1, vardef.DefInitChunkSize + 1, maxChunkSize} {
						result.SetRequiredRows(required, maxChunkSize)
						result.Reset()
						it := chunk.NewIterator4Chunk(innerChk)
						it.Begin()
						_, _, err := joiner.TryToMatchInners(outerRow, it, result)
						require.NoError(t, err, conditionCase.name)
						require.Equal(t, required, result.NumRows(), conditionCase.name)
					}
				}
			}
		}
	}
}

func genTestChunk(maxChunkSize int, numRows int, fields []*types.FieldType) *chunk.Chunk {
	chk := chunk.New(fields, maxChunkSize, maxChunkSize)
	for numRows > 0 {
		numRows--
		for col, field := range fields {
			switch field.GetType() {
			case mysql.TypeLong:
				chk.AppendInt64(col, 0)
			case mysql.TypeFloat:
				chk.AppendFloat32(col, 0)
			default:
				panic("not support")
			}
		}
	}
	return chk
}
