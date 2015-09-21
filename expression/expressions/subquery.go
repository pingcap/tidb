package expressions

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

type SubQuery interface {
	expression.Expression
	Value() interface{}
	SetValue(val interface{})
	UseOuterQuery() bool
	EvalRows(ctx context.Context, args map[interface{}]interface{}, count int) ([]interface{}, error)
	ColumnCount(ctx context.Context) (int, error)
}
