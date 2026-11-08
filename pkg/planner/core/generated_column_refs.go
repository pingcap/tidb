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

package core

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table"
)

// Building the VirtualExpr of a virtual generated column re-parses its
// generation expression (table.ClonableExprNode.Clone) and rewrites it, for
// every DataSource. On wide tables with many generated columns this is a large
// share of planning time, even when the statement never touches them.
//
// A SELECT can only use a virtual generated column if it mentions the column by
// name, or mentions a column the generation expression depends on: generated
// column substitution (GcSubstituter) only replaces an expression over those
// base columns, and multi-valued, expression and tidb_shard index paths match on
// expressions over them too. So buildDataSource skips the VirtualExpr of any
// virtual generated column for which neither holds. Column pruning removes such
// a column before anything else reads its VirtualExpr.
//
// The column-name set is a conservative superset: names are collected from the
// whole statement regardless of which table they qualify. Whenever a statement
// can reference columns without naming them (`*`, `t.*`, NATURAL JOIN, TABLE t),
// or is not a SELECT, every VirtualExpr is built, as before.

// collectGeneratedColumnRefs returns the lower-case column names mentioned in a
// SELECT or set-operation statement, or nil when every VirtualExpr must be built.
func collectGeneratedColumnRefs(node ast.Node) map[string]struct{} {
	switch node.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
	default:
		return nil
	}
	c := &generatedColumnRefCollector{names: make(map[string]struct{})}
	node.Accept(c)
	if c.buildAll {
		return nil
	}
	return c.names
}

// addGeneratedColumnRefs merges the column names of a view definition into the
// statement's set. Views are parsed and built during planning, so their
// definitions are not in the statement's AST.
func (b *PlanBuilder) addGeneratedColumnRefs(node ast.Node) {
	if b.gcColumnRefs == nil {
		return
	}
	c := &generatedColumnRefCollector{names: b.gcColumnRefs}
	node.Accept(c)
	if c.buildAll {
		b.gcColumnRefs = nil
	}
}

type generatedColumnRefCollector struct {
	names    map[string]struct{}
	buildAll bool
}

// Enter implements ast.Visitor.
func (c *generatedColumnRefCollector) Enter(n ast.Node) (ast.Node, bool) {
	switch x := n.(type) {
	case *ast.ColumnName:
		c.names[x.Name.L] = struct{}{}
	case *ast.DefaultExpr:
		// DefaultExpr.Accept does not visit its column name.
		if x.Name != nil {
			c.names[x.Name.Name.L] = struct{}{}
		}
	case *ast.SelectField:
		if x.WildCard != nil {
			c.buildAll = true
		}
	case *ast.Join:
		if x.NaturalJoin {
			c.buildAll = true
		}
	case *ast.SelectStmt:
		if x.Kind == ast.SelectStmtKindTable {
			c.buildAll = true
		}
	}
	return n, c.buildAll
}

// Leave implements ast.Visitor.
func (*generatedColumnRefCollector) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

// virtualGeneratedColumnsToSkip returns, for each entry of columns, whether
// buildDataSource can skip building its VirtualExpr. It returns nil when every
// VirtualExpr must be built.
//
// A virtual generated column is built when any of these holds:
//   - its name is mentioned in the statement;
//   - one of the columns it depends on is mentioned, or is itself a built
//     virtual generated column (so substitution through a chain of generated
//     columns still sees every link);
//   - its dependencies are unknown (empty, or naming a column not in the table);
//   - it is a dependency of a built virtual generated column, which needs it to
//     be evaluated.
func (b *PlanBuilder) virtualGeneratedColumnsToSkip(tblInfo *model.TableInfo, columns []*table.Column) []bool {
	refs := b.gcColumnRefs
	// Partition expressions and DML read columns that the statement does not name.
	if refs == nil || b.inUpdateStmt || b.inDeleteStmt || tblInfo.Partition != nil {
		return nil
	}
	isVirtual := func(c *table.Column) bool { return c.IsGenerated() && !c.GeneratedStored }
	offsetByName := make(map[string]int, len(columns))
	hasVirtual := false
	for i, c := range columns {
		offsetByName[c.Name.L] = i
		hasVirtual = hasVirtual || isVirtual(c)
	}
	if !hasVirtual {
		return nil
	}

	build := make([]bool, len(columns))
	for i, c := range columns {
		if !isVirtual(c) {
			continue
		}
		if _, ok := refs[c.Name.L]; ok || len(c.Dependences) == 0 {
			build[i] = true
			continue
		}
		for dep := range c.Dependences {
			_, mentioned := refs[dep]
			_, exists := offsetByName[dep]
			if mentioned || !exists {
				build[i] = true
				break
			}
		}
	}
	// Generated columns that depend on a built generated column.
	for changed := true; changed; {
		changed = false
		for i, c := range columns {
			if build[i] || !isVirtual(c) {
				continue
			}
			for dep := range c.Dependences {
				if j := offsetByName[dep]; build[j] && isVirtual(columns[j]) {
					build[i], changed = true, true
					break
				}
			}
		}
	}
	// Generated columns that a built generated column needs for evaluation.
	for changed := true; changed; {
		changed = false
		for i, c := range columns {
			if !build[i] || !isVirtual(c) {
				continue
			}
			for dep := range c.Dependences {
				if j := offsetByName[dep]; !build[j] && isVirtual(columns[j]) {
					build[j], changed = true, true
				}
			}
		}
	}

	skip := build // reuse: invert in place
	anySkipped := false
	for i, c := range columns {
		skip[i] = isVirtual(c) && !build[i]
		anySkipped = anySkipped || skip[i]
	}
	if !anySkipped {
		return nil
	}
	return skip
}
