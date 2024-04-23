// Copyright 2018 PingCAP, Inc.
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

package pattern

import (
	"testing"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/stretchr/testify/require"
)

func TestGetOperand(t *testing.T) {
	require.Equal(t, OperandJoin, GetOperand(&plannercore.LogicalJoin{}))
	require.Equal(t, OperandAggregation, GetOperand(&plannercore.LogicalAggregation{}))
	require.Equal(t, OperandProjection, GetOperand(&plannercore.LogicalProjection{}))
	require.Equal(t, OperandSelection, GetOperand(&plannercore.LogicalSelection{}))
	require.Equal(t, OperandApply, GetOperand(&plannercore.LogicalApply{}))
	require.Equal(t, OperandMaxOneRow, GetOperand(&plannercore.LogicalMaxOneRow{}))
	require.Equal(t, OperandTableDual, GetOperand(&plannercore.LogicalTableDual{}))
	require.Equal(t, OperandDataSource, GetOperand(&plannercore.DataSource{}))
	require.Equal(t, OperandUnionScan, GetOperand(&plannercore.LogicalUnionScan{}))
	require.Equal(t, OperandUnionAll, GetOperand(&plannercore.LogicalUnionAll{}))
	require.Equal(t, OperandSort, GetOperand(&plannercore.LogicalSort{}))
	require.Equal(t, OperandTopN, GetOperand(&plannercore.LogicalTopN{}))
	require.Equal(t, OperandLock, GetOperand(&plannercore.LogicalLock{}))
	require.Equal(t, OperandLimit, GetOperand(&plannercore.LogicalLimit{}))
}

func TestOperandMatch(t *testing.T) {
	require.True(t, OperandAny.Match(OperandLimit))
	require.True(t, OperandAny.Match(OperandSelection))
	require.True(t, OperandAny.Match(OperandJoin))
	require.True(t, OperandAny.Match(OperandMaxOneRow))
	require.True(t, OperandAny.Match(OperandAny))

	require.True(t, OperandLimit.Match(OperandAny))
	require.True(t, OperandSelection.Match(OperandAny))
	require.True(t, OperandJoin.Match(OperandAny))
	require.True(t, OperandMaxOneRow.Match(OperandAny))
	require.True(t, OperandAny.Match(OperandAny))

	require.True(t, OperandLimit.Match(OperandLimit))
	require.True(t, OperandSelection.Match(OperandSelection))
	require.True(t, OperandJoin.Match(OperandJoin))
	require.True(t, OperandMaxOneRow.Match(OperandMaxOneRow))
	require.True(t, OperandAny.Match(OperandAny))

	require.False(t, OperandLimit.Match(OperandSelection))
	require.False(t, OperandLimit.Match(OperandJoin))
	require.False(t, OperandLimit.Match(OperandMaxOneRow))
}

func TestNewPattern(t *testing.T) {
	p := NewPattern(OperandAny, EngineAll)
	require.Equal(t, OperandAny, p.Operand)
	require.Nil(t, p.Children)

	p = NewPattern(OperandJoin, EngineAll)
	require.Equal(t, OperandJoin, p.Operand)
	require.Nil(t, p.Children)
}

func TestPatternSetChildren(t *testing.T) {
	p := NewPattern(OperandAny, EngineAll)
	p.SetChildren(NewPattern(OperandLimit, EngineAll))
	require.Len(t, p.Children, 1)
	require.Equal(t, OperandLimit, p.Children[0].Operand)
	require.Nil(t, p.Children[0].Children)

	p = NewPattern(OperandJoin, EngineAll)
	p.SetChildren(NewPattern(OperandProjection, EngineAll), NewPattern(OperandSelection, EngineAll))
	require.Len(t, p.Children, 2)
	require.Equal(t, OperandProjection, p.Children[0].Operand)
	require.Nil(t, p.Children[0].Children)
	require.Equal(t, OperandSelection, p.Children[1].Operand)
	require.Nil(t, p.Children[1].Children)
}
