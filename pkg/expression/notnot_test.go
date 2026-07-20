package expression

import (
	"testing"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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
	t.Logf("Input: %s", notOuter.StringWithCtx(ctx.GetEvalCtx(), errors.RedactLogDisable))
	t.Logf("Output: %s", ret.StringWithCtx(ctx.GetEvalCtx(), errors.RedactLogDisable))
	require.True(t, ret.Equal(ctx, eqFunc), "NOT NOT (a=1) should simplify to a=1")
}
