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

package mview

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestExtractMLogCommitTSFilterBound(t *testing.T) {
	evalCtx := exprstatic.NewEvalContext()
	commitTSCol := &expression.Column{
		ID:       model.ExtraCommitTSID,
		UniqueID: 1,
		RetType:  model.NewExtraCommitTSColInfo().FieldType.Clone(),
	}
	otherCommitTSCol := &expression.Column{
		ID:       model.ExtraCommitTSID,
		UniqueID: 2,
		RetType:  model.NewExtraCommitTSColInfo().FieldType.Clone(),
	}
	constant := &expression.Constant{
		Value:   types.NewUintDatum(42),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	testCases := []struct {
		name     string
		op       string
		args     []expression.Expression
		expected string
		ok       bool
	}{
		{
			name:     "column on left",
			op:       ast.GT,
			args:     []expression.Expression{commitTSCol, constant},
			expected: ast.GT,
			ok:       true,
		},
		{
			name:     "column on right",
			op:       ast.GT,
			args:     []expression.Expression{constant, commitTSCol},
			expected: ast.LT,
			ok:       true,
		},
		{
			name:     "column on right greater or equal",
			op:       ast.GE,
			args:     []expression.Expression{constant, commitTSCol},
			expected: ast.LE,
			ok:       true,
		},
		{
			name:     "column on right less than",
			op:       ast.LT,
			args:     []expression.Expression{constant, commitTSCol},
			expected: ast.GT,
			ok:       true,
		},
		{
			name:     "column on right inclusive",
			op:       ast.LE,
			args:     []expression.Expression{constant, commitTSCol},
			expected: ast.GE,
			ok:       true,
		},
		{
			name: "different commit ts column",
			op:   ast.GT,
			args: []expression.Expression{otherCommitTSCol, constant},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op, value, ok := extractMLogCommitTSFilterBound(evalCtx, commitTSCol, tc.op, tc.args)
			require.Equal(t, tc.ok, ok)
			if !tc.ok {
				return
			}
			require.Equal(t, tc.expected, op)
			require.Equal(t, uint64(42), value)
		})
	}
}

func TestNormalizeMLogCommitTSInclusiveLower(t *testing.T) {
	testCases := []struct {
		name     string
		value    uint64
		expected uint64
	}{
		{
			name:     "decrement physical millisecond",
			value:    oracle.ComposeTS(1000, 123),
			expected: oracle.ComposeTS(999, 0),
		},
		{
			name:     "keep zero physical timestamp",
			value:    oracle.ComposeTS(0, 123),
			expected: oracle.ComposeTS(0, 123),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, normalizeMLogCommitTSInclusiveLower(tc.value))
		})
	}
}

func TestMLogCommitTSFilterWindowNormalizesInclusiveLower(t *testing.T) {
	ctx := mock.NewContext()
	commitTSCol := &expression.Column{
		ID:       model.ExtraCommitTSID,
		UniqueID: 1,
		RetType:  model.NewExtraCommitTSColInfo().FieldType.Clone(),
	}
	constant := &expression.Constant{
		Value:   types.NewUintDatum(oracle.ComposeTS(1000, 123)),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	cond := expression.NewFunctionInternal(
		ctx.GetExprCtx(), ast.GE, types.NewFieldType(mysql.TypeTiny), commitTSCol, constant,
	)

	var filter mlogCommitTSFilterWindow
	require.True(t, filter.addCond(ctx.GetExprCtx().GetEvalCtx(), commitTSCol, cond))
	require.Equal(t, oracle.ComposeTS(999, 0), filter.lowerTSO)
	require.True(t, filter.hasLower)

	filter.upperTSO = oracle.ComposeTS(998, 0)
	filter.hasUpper = true
	selectivity, ok := estimateMLogCommitTSSelectivity(
		&planctx.MLogCommitTSEstimation{RetainedUpperTSO: oracle.ComposeTS(2000, 0)},
		&model.TableInfo{UpdateTS: oracle.ComposeTS(999, 0)},
		filter,
	)
	require.Equal(t, 0.0, selectivity)
	require.False(t, ok)
}

func TestEstimateMLogCommitTSSelectivityDisjointLower(t *testing.T) {
	testCases := []struct {
		name        string
		estimation  planctx.MLogCommitTSEstimation
		tableUpdate uint64
		filter      mlogCommitTSFilterWindow
		expected    float64
		expectedOK  bool
	}{
		{
			name:        "fallback lower is not proven empty",
			estimation:  planctx.MLogCommitTSEstimation{RetainedUpperTSO: oracle.ComposeTS(20, 0)},
			tableUpdate: oracle.ComposeTS(15, 0),
			filter:      mlogCommitTSFilterWindow{upperTSO: oracle.ComposeTS(14, 0), hasUpper: true},
			expectedOK:  false,
		},
		{
			name:       "retained lower proves empty",
			estimation: planctx.MLogCommitTSEstimation{RetainedLowerTSO: oracle.ComposeTS(15, 0), RetainedUpperTSO: oracle.ComposeTS(20, 0)},
			filter:     mlogCommitTSFilterWindow{upperTSO: oracle.ComposeTS(14, 0), hasUpper: true},
			expectedOK: true,
		},
		{
			name:        "sql lower proves empty despite fallback retained lower",
			estimation:  planctx.MLogCommitTSEstimation{RetainedUpperTSO: oracle.ComposeTS(20, 0)},
			tableUpdate: oracle.ComposeTS(10, 0),
			filter: mlogCommitTSFilterWindow{
				lowerTSO: oracle.ComposeTS(15, 0), hasLower: true,
				upperTSO: oracle.ComposeTS(14, 0), hasUpper: true,
			},
			expectedOK: true,
		},
		{
			name:        "fallback lower equal upper is not proven empty",
			estimation:  planctx.MLogCommitTSEstimation{RetainedUpperTSO: oracle.ComposeTS(20, 0)},
			tableUpdate: oracle.ComposeTS(15, 0),
			filter:      mlogCommitTSFilterWindow{upperTSO: oracle.ComposeTS(15, 0), hasUpper: true},
			expectedOK:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableInfo := &model.TableInfo{UpdateTS: tc.tableUpdate}
			selectivity, ok := estimateMLogCommitTSSelectivity(&tc.estimation, tableInfo, tc.filter)
			require.Equal(t, tc.expectedOK, ok)
			if tc.expectedOK {
				require.Equal(t, tc.expected, selectivity)
			}
		})
	}
}
