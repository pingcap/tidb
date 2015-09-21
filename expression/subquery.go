package expression

import (
	"github.com/pingcap/tidb/context"
)

type SubQuery interface {
	Expression
	Value() interface{}
	SetValue(val interface{})
	UseOuterQuery() bool
	EvalRows(ctx context.Context, args map[interface{}]interface{}, count int) ([]interface{}, error)
	ColumnCount(ctx context.Context) (int, error)
}
