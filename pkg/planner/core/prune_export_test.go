//go:build intest

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/util"
)

// PruneIndexesByWhereAndOrderForTest is an intest-only wrapper for testing.
func PruneIndexesByWhereAndOrderForTest(paths []*util.AccessPath, whereColumns, orderingColumns []*expression.Column, threshold int) []*util.AccessPath {
	return pruneIndexesByWhereAndOrder(paths, whereColumns, orderingColumns, threshold)
}
