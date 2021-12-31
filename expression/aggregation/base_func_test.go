// Copyright 2021 PingCAP, Inc.
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

package aggregation

import (
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestClone(t *testing.T) {
	ctx := mock.NewContext()
	col := &expression.Column{
		UniqueID: 0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	desc, err := newBaseFuncDesc(ctx, ast.AggFuncFirstRow, []expression.Expression{col})
	require.NoError(t, err)
	cloned := desc.clone()
	require.True(t, desc.equal(ctx, cloned))

	col1 := &expression.Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeVarchar),
	}
	cloned.Args[0] = col1

	require.Equal(t, col, desc.Args[0])
	require.False(t, desc.equal(ctx, cloned))
}
