package expression

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestPushDownNotDoubleNegation(t *testing.T) {
	ctx := mock.NewContext()
	col := &Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}

	// Build: NOT NOT (a = 1)
	eqFunc := newFunctionWithMockCtx(ast.EQ, col, NewOne())
	notInner := newFunctionWithMockCtx(ast.UnaryNot, eqFunc)
	notOuter := newFunctionWithMockCtx(ast.UnaryNot, notInner)

	// PushDownNot should simplify NOT NOT (a=1) -> a=1
	ret := PushDownNot(ctx, notOuter)
	require.True(t, ret.Equal(ctx, eqFunc), "NOT NOT (a=1) should simplify to a=1")

	// Also verify: NOT NOT NOT (a=1) -> NOT (a=1) = (a!=1)
	notOuter3 := newFunctionWithMockCtx(ast.UnaryNot, notOuter)
	ret3 := PushDownNot(ctx, notOuter3)
	neFunc := newFunctionWithMockCtx(ast.NE, col, NewOne())
	require.True(t, ret3.Equal(ctx, neFunc), "NOT NOT NOT (a=1) should simplify to a!=1")
}
