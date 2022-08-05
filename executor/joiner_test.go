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

package executor

import (
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestRequiredRows(t *testing.T) {
	joinTypes := []core.JoinType{core.InnerJoin, core.LeftOuterJoin, core.RightOuterJoin}
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
				outerRow := genTestChunk(maxChunkSize, 1, lfields).GetRow(0)
				innerChk := genTestChunk(maxChunkSize, maxChunkSize, rfields)
				var defaultInner []types.Datum
				for i, f := range rfields {
					defaultInner = append(defaultInner, innerChk.GetRow(0).GetDatum(i, f))
				}
				joiner := newJoiner(defaultCtx(), joinType, false, defaultInner, nil, lfields, rfields, nil)

				fields := make([]*types.FieldType, 0, len(lfields)+len(rfields))
				fields = append(fields, rfields...)
				fields = append(fields, lfields...)
				result := chunk.New(fields, maxChunkSize, maxChunkSize)

				for i := 0; i < 10; i++ {
					required := rand.Int()%maxChunkSize + 1
					result.SetRequiredRows(required, maxChunkSize)
					result.Reset()
					it := chunk.NewIterator4Chunk(innerChk)
					it.Begin()
					_, _, err := joiner.tryToMatchInners(outerRow, it, result)
					require.NoError(t, err)
					require.Equal(t, required, result.NumRows())
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
