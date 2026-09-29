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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
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
