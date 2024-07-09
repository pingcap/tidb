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
	"context"
	"crypto/rand"
	"encoding/base64"
	"math/big"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// left table:
//   col0: int (other cond)
//   col1: int (join key)
//   col2: int (dropped)
//   col3: varchar (join key)
//   col4: int

// right table:
//   col0: int (join key)
//   col1: int (dropped)
//   col2: varchar (join key)
//   col3: int (right cond)
//   col4: int (other cond)

var leftCols = []*expression.Column{
	{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 2, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 3, RetType: types.NewFieldType(mysql.TypeVarString)},
	{Index: 4, RetType: types.NewFieldType(mysql.TypeLonglong)},
}

var rightCols = []*expression.Column{
	{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 2, RetType: types.NewFieldType(mysql.TypeVarString)},
	{Index: 3, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 4, RetType: types.NewFieldType(mysql.TypeLonglong)},
}

func buildJoinKeyIntDatums(num int) []any {
	datumSet := make(map[int64]bool, num)
	datums := make([]any, 0, num)
	for len(datums) < num {
		randVal, _ := rand.Int(rand.Reader, big.NewInt(100000))
		val := randVal.Int64()
		if datumSet[val] {
			continue
		}
		datumSet[val] = true
		datums = append(datums, val)
	}
	return datums
}

func buildJoinKeyStringDatums(num int) []any {
	datumSet := make(map[string]bool, num)
	datums := make([]any, 0, num)
	for len(datums) < num {
		buff := make([]byte, 10)
		_, err := rand.Read(buff)
		if err != nil {
			panic("rand.Read returns error")
		}

		val := base64.RawURLEncoding.EncodeToString(buff)
		if datumSet[val] {
			continue
		}
		datumSet[val] = true
		datums = append(datums, val)
	}
	return datums
}

func buildLeftAndRightDataSource(ctx sessionctx.Context) (*testutil.MockDataSource, *testutil.MockDataSource) {
	leftSchema := expression.NewSchema(leftCols...)
	rightSchema := expression.NewSchema(rightCols...)

	joinKeyIntDatums := buildJoinKeyIntDatums(3000)
	joinKeyStringDatums := buildJoinKeyStringDatums(2)
	leftMockSrcParm := testutil.MockDataSourceParameters{DataSchema: leftSchema, Ctx: ctx, Rows: 6000, Ndvs: []int{0, -1, 0, -1, 0}, Datums: [][]any{nil, joinKeyIntDatums, nil, joinKeyStringDatums, nil}}
	rightMockSrcParm := testutil.MockDataSourceParameters{DataSchema: rightSchema, Ctx: ctx, Rows: 6000, Ndvs: []int{-1, 0, -1, 0, 0}, Datums: [][]any{joinKeyIntDatums, nil, joinKeyStringDatums, nil, nil}}
	return testutil.BuildMockDataSource(leftMockSrcParm), testutil.BuildMockDataSource(rightMockSrcParm)
}

func buildSchema(schemaTypes []*types.FieldType) *expression.Schema {
	schema := &expression.Schema{}
	for _, tp := range schemaTypes {
		schema.Append(&expression.Column{
			RetType: tp,
		})
	}
	return schema
}

func TestInnerJoinSpillCorrectness(t *testing.T) {
	for i := 0; i < 30; i++ { // TODO remove this loop
		logutil.BgLogger().Info("xzxdebug start a test")
		spillChunkSize = 100

		// TODO trigger spill in different stages
		ctx := mock.NewContext()
		leftDataSource, rightDataSource := buildLeftAndRightDataSource(ctx)
		leftDataSource.PrepareChunks()
		rightDataSource.PrepareChunks()

		info := &hashJoinInfo{
			ctx: ctx,
			schema: buildSchema([]*types.FieldType{
				types.NewFieldType(mysql.TypeLonglong),
				types.NewFieldType(mysql.TypeLonglong),
				types.NewFieldType(mysql.TypeVarString),
				types.NewFieldType(mysql.TypeLonglong),
				types.NewFieldType(mysql.TypeLonglong),
				types.NewFieldType(mysql.TypeVarString),
				types.NewFieldType(mysql.TypeLonglong),
				types.NewFieldType(mysql.TypeLonglong),
			}),
			leftExec:         leftDataSource,
			rightExec:        rightDataSource,
			joinType:         plannercore.InnerJoin,
			rightAsBuildSide: true,
			buildKeys: []*expression.Column{
				{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
				{Index: 2, RetType: types.NewFieldType(mysql.TypeVarString)},
			},
			probeKeys: []*expression.Column{
				{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
				{Index: 3, RetType: types.NewFieldType(mysql.TypeVarString)},
			},
			lUsed:                 []int{0, 1, 3, 4},
			rUsed:                 []int{0, 2, 3, 4},
			otherCondition:        expression.CNFExprs{},
			lUsedInOtherCondition: []int{0},
			rUsedInOtherCondition: []int{4},
		}

		hashJoinExec := buildHashJoinV2Exec(info)
		tmpCtx := context.Background()
		err := hashJoinExec.Open(tmpCtx)
		require.NoError(t, err)
		chk := exec.NewFirstChunk(hashJoinExec)
		for {
			err = hashJoinExec.Next(tmpCtx, chk)
			require.NoError(t, err)
			if chk.NumRows() == 0 {
				break
			}
		}

		maxRowTableSegmentSize = 100 // TODO modify it after no-spill join has been executed
	}

}

func TestLeftOuterJoinSpillCorrectness(t *testing.T) {
	// TODO trigger spill in different stages
}
