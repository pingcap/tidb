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
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestKey(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)

	buildTypes := []*types.FieldType{intTp, uintTp, uintTp}
	buildKeyIndex := []int{0}
	buildKeyTypes := []*types.FieldType{intTp}
	probeKeyTypes := []*types.FieldType{intTp}

	// inlined keys
	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, []int{}, false)
	buildSchema := &expression.Schema{}
	for _, tp := range buildTypes {
		buildSchema.Append(&expression.Column{
			RetType: tp,
		})
	}

	builder := createRowTableBuilder(buildKeyIndex, buildSchema, meta, 1, true, false, false)
	chk := chunk.GenRandomChunks(buildTypes, 2049)
	hashJoinCtx := &PartitionedHashJoinCtx{
		SessCtx:         mock.NewContext(),
		PartitionNumber: 1,
		hashTableMeta:   meta,
	}
	rowTables := make([]*rowTable, hashJoinCtx.PartitionNumber)
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, rowTables)
	builder.appendRemainingRowLocations(rowTables)
	require.NoError(t, err, "processOneChunk returns error")
	// check valid key pos, filtered rows is not kept
	for _, seg := range rowTables[0].segments {
		for index, value := range seg.validJoinKeyPos {
			require.Equal(t, index, value, "wrong result in validJoinKeyPos")
		}
	}
	// check key value
	keyCol := chk.Column(0)
	for rowIndex, i := 0, 0; i < keyCol.Rows(); i++ {
		if keyCol.IsNull(i) {
			continue
		}
		rowStart := rowTables[0].getRowStart(rowIndex)
		require.Equal(t, builder.serializedKeyVectorBuffer[i], meta.getKeyBytes(rowStart), "key not matched, index = "+strconv.Itoa(i))
		rowIndex++
	}
}
