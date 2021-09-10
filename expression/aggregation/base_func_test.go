package aggregation

import (
	"testing"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
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
