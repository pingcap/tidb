package expression

import (
	"github.com/pingcap/tidb/context"
)

// SubQuery represents a sub query.
type SubQuery interface {
	Expression

	// Value returns the value of the SubQuery
	Value() interface{}

	// SetValue set val to SubQuery.
	SetValue(val interface{})

	// UseOuterQuery returns if sub query use outer query.
	UseOuterQuery() bool

	// EvalRows executes the subquery and returns the multi rows with rowCount.
	// rowCount < 0 means no limit.
	// If the ColumnCount is 1, we will return a column result like {1, 2, 3},
	// otherwise, we will return a table result like {{1, 1}, {2, 2}}.
	EvalRows(ctx context.Context, args map[interface{}]interface{}, count int) ([]interface{}, error)

	// ColumnCount returns column count for the sub query.
	ColumnCount(ctx context.Context) (int, error)
}
