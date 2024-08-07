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

package join

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestLeftOuterJoinSpillBasic(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource := buildLeftAndRightDataSource(ctx, leftCols, rightCols)

	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	stringTp.AddFlag(mysql.NotNullFlag)

	leftTypes := []*types.FieldType{intTp, intTp, intTp, stringTp, intTp}
	rightTypes := []*types.FieldType{intTp, intTp, stringTp, intTp, intTp}

	leftKeys := []*expression.Column{
		{Index: 1, RetType: intTp},
		{Index: 3, RetType: stringTp},
	}
	rightKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
		{Index: 2, RetType: stringTp},
	}

	params := []spillTestParam{
		// Normal case
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{4000000, 1700000, 6400000, 1500000, 10000}},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{4000000, 1700000, 6400000, 1500000, 10000}},
		// rightUsed is empty
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, nil, nil, nil, []int64{2000000, 1700000, 3300000, 750000, 10000}},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, nil, nil, nil, []int64{4000000, 1700000, 6400000, 1500000, 10000}},
		// leftUsed is empty
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{4000000, 1700000, 6400000, 1500000, 10000}},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{2000000, 1700000, 3300000, 750000, 10000}},
	}

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers", `return(true)`)
	require.NoError(t, err)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers")

	maxRowTableSegmentSize = 100
	spillChunkSize = 100

	for _, param := range params {
		testSpill(t, ctx, plannercore.LeftOuterJoin, leftDataSource, rightDataSource, param)
	}
}
func TestLeftOuterJoinSpillWithOtherCondition(t *testing.T) {

}
