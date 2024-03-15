// Copyright 2023 PingCAP, Inc.
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

package vecgroupchecker

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestVecGroupCheckerDATARACE(t *testing.T) {
	ctx := mock.NewContext()

	mTypes := []byte{mysql.TypeVarString, mysql.TypeNewDecimal, mysql.TypeJSON}
	for _, mType := range mTypes {
		exprs := make([]expression.Expression, 1)
		exprs[0] = &expression.Column{
			RetType: types.NewFieldTypeBuilder().SetType(mType).BuildP(),
			Index:   0,
		}
		vgc := NewVecGroupChecker(ctx, ctx.GetSessionVars().EnableVectorizedExpression, exprs)

		fts := []*types.FieldType{types.NewFieldType(mType)}
		chk := chunk.New(fts, 1, 1)
		vgc.allocateBuffer = func(evalType types.EvalType, capacity int) (*chunk.Column, error) {
			return chk.Column(0), nil
		}
		vgc.releaseBuffer = func(column *chunk.Column) {}

		switch mType {
		case mysql.TypeVarString:
			chk.Column(0).ReserveString(1)
			chk.Column(0).AppendString("abc")
		case mysql.TypeNewDecimal:
			chk.Column(0).ResizeDecimal(1, false)
			chk.Column(0).Decimals()[0] = *types.NewDecFromInt(123)
		case mysql.TypeJSON:
			chk.Column(0).ReserveJSON(1)
			j := new(types.BinaryJSON)
			require.NoError(t, j.UnmarshalJSON([]byte(fmt.Sprintf(`{"%v":%v}`, 123, 123))))
			chk.Column(0).AppendJSON(*j)
		}

		_, err := vgc.SplitIntoGroups(chk)
		require.NoError(t, err)

		switch mType {
		case mysql.TypeVarString:
			require.Equal(t, "abc", vgc.firstRowDatums[0].GetString())
			require.Equal(t, "abc", vgc.lastRowDatums[0].GetString())
			chk.Column(0).ReserveString(1)
			chk.Column(0).AppendString("edf")
			require.Equal(t, "abc", vgc.firstRowDatums[0].GetString())
			require.Equal(t, "abc", vgc.lastRowDatums[0].GetString())
		case mysql.TypeNewDecimal:
			require.Equal(t, "123", vgc.firstRowDatums[0].GetMysqlDecimal().String())
			require.Equal(t, "123", vgc.lastRowDatums[0].GetMysqlDecimal().String())
			chk.Column(0).ResizeDecimal(1, false)
			chk.Column(0).Decimals()[0] = *types.NewDecFromInt(456)
			require.Equal(t, "123", vgc.firstRowDatums[0].GetMysqlDecimal().String())
			require.Equal(t, "123", vgc.lastRowDatums[0].GetMysqlDecimal().String())
		case mysql.TypeJSON:
			require.Equal(t, `{"123": 123}`, vgc.firstRowDatums[0].GetMysqlJSON().String())
			require.Equal(t, `{"123": 123}`, vgc.lastRowDatums[0].GetMysqlJSON().String())
			chk.Column(0).ReserveJSON(1)
			j := new(types.BinaryJSON)
			require.NoError(t, j.UnmarshalJSON([]byte(fmt.Sprintf(`{"%v":%v}`, 456, 456))))
			chk.Column(0).AppendJSON(*j)
			require.Equal(t, `{"123": 123}`, vgc.firstRowDatums[0].GetMysqlJSON().String())
			require.Equal(t, `{"123": 123}`, vgc.lastRowDatums[0].GetMysqlJSON().String())
		}
	}
}

func genTestChunk4VecGroupChecker(chkRows []int, sameNum int) (expr []expression.Expression, inputs []*chunk.Chunk) {
	chkNum := len(chkRows)
	numRows := 0
	inputs = make([]*chunk.Chunk, chkNum)
	fts := make([]*types.FieldType, 1)
	fts[0] = types.NewFieldType(mysql.TypeLonglong)
	for i := 0; i < chkNum; i++ {
		inputs[i] = chunk.New(fts, chkRows[i], chkRows[i])
		numRows += chkRows[i]
	}
	var numGroups int
	if numRows%sameNum == 0 {
		numGroups = numRows / sameNum
	} else {
		numGroups = numRows/sameNum + 1
	}

	nullPos := rand.Intn(numGroups)
	cnt := 0
	val := rand.Int63()
	for i := 0; i < chkNum; i++ {
		col := inputs[i].Column(0)
		col.ResizeInt64(chkRows[i], false)
		i64s := col.Int64s()
		for j := 0; j < chkRows[i]; j++ {
			if cnt == sameNum {
				val = rand.Int63()
				cnt = 0
				nullPos--
			}
			if nullPos == 0 {
				col.SetNull(j, true)
			} else {
				i64s[j] = val
			}
			cnt++
		}
	}

	expr = make([]expression.Expression, 1)
	expr[0] = &expression.Column{
		RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlen(mysql.MaxIntWidth).BuildP(),
		Index:   0,
	}
	return
}

func TestVecGroupChecker4GroupCount(t *testing.T) {
	testCases := []struct {
		chunkRows      []int
		expectedGroups int
		expectedFlag   []bool
		sameNum        int
	}{
		{
			chunkRows:      []int{1024, 1},
			expectedGroups: 1025,
			expectedFlag:   []bool{false, false},
			sameNum:        1,
		},
		{
			chunkRows:      []int{1024, 1},
			expectedGroups: 1,
			expectedFlag:   []bool{false, true},
			sameNum:        1025,
		},
		{
			chunkRows:      []int{1, 1},
			expectedGroups: 1,
			expectedFlag:   []bool{false, true},
			sameNum:        2,
		},
		{
			chunkRows:      []int{1, 1},
			expectedGroups: 2,
			expectedFlag:   []bool{false, false},
			sameNum:        1,
		},
		{
			chunkRows:      []int{2, 2},
			expectedGroups: 2,
			expectedFlag:   []bool{false, false},
			sameNum:        2,
		},
		{
			chunkRows:      []int{2, 2},
			expectedGroups: 1,
			expectedFlag:   []bool{false, true},
			sameNum:        4,
		},
	}

	ctx := mock.NewContext()
	for _, testCase := range testCases {
		expr, inputChks := genTestChunk4VecGroupChecker(testCase.chunkRows, testCase.sameNum)
		groupChecker := NewVecGroupChecker(ctx, ctx.GetSessionVars().EnableVectorizedExpression, expr)
		groupNum := 0
		for i, inputChk := range inputChks {
			flag, err := groupChecker.SplitIntoGroups(inputChk)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedFlag[i], flag)
			if flag {
				groupNum += groupChecker.GroupCount() - 1
			} else {
				groupNum += groupChecker.GroupCount()
			}
		}
		require.Equal(t, testCase.expectedGroups, groupNum)
	}
}

func TestVecGroupChecker(t *testing.T) {
	tp := types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).BuildP()
	col0 := &expression.Column{
		RetType: tp,
		Index:   0,
	}
	ctx := mock.NewContext()
	groupChecker := NewVecGroupChecker(ctx, ctx.GetSessionVars().EnableVectorizedExpression, []expression.Expression{col0})

	chk := chunk.New([]*types.FieldType{tp}, 6, 6)
	chk.Reset()
	chk.Column(0).AppendString("aaa")
	chk.Column(0).AppendString("AAA")
	chk.Column(0).AppendString("😜")
	chk.Column(0).AppendString("😃")
	chk.Column(0).AppendString("À")
	chk.Column(0).AppendString("A")

	tp.SetCollate("bin")
	groupChecker.Reset()
	_, err := groupChecker.SplitIntoGroups(chk)
	require.NoError(t, err)
	for i := 0; i < 6; i++ {
		b, e := groupChecker.GetNextGroup()
		require.Equal(t, b, i)
		require.Equal(t, e, i+1)
	}
	require.True(t, groupChecker.IsExhausted())

	tp.SetCollate("utf8_general_ci")
	groupChecker.Reset()
	_, err = groupChecker.SplitIntoGroups(chk)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		b, e := groupChecker.GetNextGroup()
		require.Equal(t, b, i*2)
		require.Equal(t, e, i*2+2)
	}
	require.True(t, groupChecker.IsExhausted())

	tp.SetCollate("utf8_unicode_ci")
	groupChecker.Reset()
	_, err = groupChecker.SplitIntoGroups(chk)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		b, e := groupChecker.GetNextGroup()
		require.Equal(t, b, i*2)
		require.Equal(t, e, i*2+2)
	}
	require.True(t, groupChecker.IsExhausted())

	// test padding
	tp.SetCollate("utf8_bin")
	tp.SetFlen(6)
	chk.Reset()
	chk.Column(0).AppendString("a")
	chk.Column(0).AppendString("a  ")
	chk.Column(0).AppendString("a    ")
	groupChecker.Reset()
	_, err = groupChecker.SplitIntoGroups(chk)
	require.NoError(t, err)
	b, e := groupChecker.GetNextGroup()
	require.Equal(t, b, 0)
	require.Equal(t, e, 3)
	require.True(t, groupChecker.IsExhausted())
}
