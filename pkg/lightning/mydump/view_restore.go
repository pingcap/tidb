// Copyright 2024 PingCAP, Inc.
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

package mydump

import (
	"context"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
)

// tableNameSet stores schema-qualified object names for dependency checks.
type tableNameSet map[filter.Table]struct{}

func (s tableNameSet) add(tbl filter.Table) {
	s[normalizeTableName(tbl.Schema, tbl.Name)] = struct{}{}
}

func (s tableNameSet) has(tbl filter.Table) bool {
	if s == nil {
		return false
	}
	_, ok := s[normalizeTableName(tbl.Schema, tbl.Name)]
	return ok
}

// parsedViewSchema keeps the normalized CREATE VIEW SQL together with the
// referenced objects extracted from the view query.
type parsedViewSchema struct {
	key       filter.Table
	deps      []filter.Table
	createSQL string
}

// viewNode is one vertex in the ordered view restore graph. Dependencies that
// are not restored by the dump stay in externalDeps and must already exist
// downstream.
type viewNode struct {
	key          filter.Table
	deps         []filter.Table
	externalDeps []filter.Table
	dependents   []filter.Table
	indegree     int
	createSQL    string
}

// viewRestorePlan keeps both the dependency graph and the topologically sorted
// restore order used during schema import.
type viewRestorePlan struct {
	nodes   map[filter.Table]*viewNode
	ordered []*viewNode
}

func lessTableName(left, right filter.Table) bool {
	if left.Schema != right.Schema {
		return left.Schema < right.Schema
	}
	return left.Name < right.Name
}

func sortViewNodes(nodes []*viewNode) {
	sort.Slice(nodes, func(i, j int) bool {
		return lessTableName(nodes[i].key, nodes[j].key)
	})
}

// SchemaImportPlan describes the schema objects that should be restored from a dump.
type SchemaImportPlan struct {
	dbMetas  []*MDDatabaseMeta
	viewPlan *viewRestorePlan
}

// viewDependencyCollector walks a CREATE VIEW query and collects referenced
// tables/views with the current schema filled in for unqualified names.
type viewDependencyCollector struct {
	currentSchema string
	deps          tableNameSet
	cteNameScopes []map[string]struct{}
}

func (c *viewDependencyCollector) pushCTEScope() {
	c.cteNameScopes = append(c.cteNameScopes, make(map[string]struct{}))
}

func (c *viewDependencyCollector) popCTEScope() {
	if len(c.cteNameScopes) == 0 {
		return
	}
	c.cteNameScopes = c.cteNameScopes[:len(c.cteNameScopes)-1]
}

func (c *viewDependencyCollector) recordCTEName(name string) {
	if len(c.cteNameScopes) == 0 {
		return
	}
	c.cteNameScopes[len(c.cteNameScopes)-1][name] = struct{}{}
}

func (c *viewDependencyCollector) isCTEName(name string) bool {
	for i := len(c.cteNameScopes) - 1; i >= 0; i-- {
		if _, ok := c.cteNameScopes[i][name]; ok {
			return true
		}
	}
	return false
}

func (c *viewDependencyCollector) Enter(n ast.Node) (ast.Node, bool) {
	switch node := n.(type) {
	case *ast.SelectStmt:
		if node.With != nil {
			c.pushCTEScope()
		}
		return n, false
	case *ast.CommonTableExpression:
		if node.IsRecursive {
			// Recursive CTE can reference itself, so expose the name before
			// traversing Query.
			c.recordCTEName(node.Name.L)
		}
		return n, false
	case *ast.TableName:
		if node.Schema.O == "" && c.isCTEName(node.Name.L) {
			return n, true
		}

		schema := node.Schema.L
		if schema == "" {
			// Dumpling may omit the schema for same-database references.
			schema = strings.ToLower(c.currentSchema)
		}
		c.deps.add(filterTableName(schema, node.Name.L))
		return n, true
	default:
		return n, false
	}
}

func (c *viewDependencyCollector) Leave(n ast.Node) (ast.Node, bool) {
	switch node := n.(type) {
	case *ast.CommonTableExpression:
		if !node.IsRecursive {
			// Non-recursive CTE becomes visible only after its definition has
			// been fully traversed.
			c.recordCTEName(node.Name.L)
		}
	case *ast.SelectStmt:
		if node.With != nil {
			c.popCTEScope()
		}
	}
	return n, true
}

// NewSchemaImportPlan builds a schema restore plan, including ordered view restoration when needed.
func NewSchemaImportPlan(ctx context.Context, store storeapi.Storage, sqlMode mysql.SQLMode, dbMetas []*MDDatabaseMeta) (*SchemaImportPlan, error) {
	plan := &SchemaImportPlan{dbMetas: dbMetas}
	if len(dbMetas) == 0 {
		return plan, nil
	}

	p := parser.New()
	p.SetSQLMode(sqlMode)

	parsedViews := make([]*parsedViewSchema, 0)
	for _, dbMeta := range dbMetas {
		for _, viewMeta := range dbMeta.Views {
			sqlStr, err := viewMeta.GetSchema(ctx, store)
			if err != nil {
				return nil, err
			}

			parsed, err := parseViewSchemaSQL(p, filterTableName(viewMeta.DB, viewMeta.Name), sqlStr)
			if err != nil {
				return nil, err
			}
			parsedViews = append(parsedViews, parsed)
		}
	}
	if len(parsedViews) == 0 {
		return plan, nil
	}

	viewPlan, err := buildViewRestorePlan(parsedViews, collectDumpTables(dbMetas))
	if err != nil {
		return nil, err
	}
	plan.viewPlan = viewPlan
	return plan, nil
}

// parseViewSchemaSQL removes dumpling's placeholder cleanup DDL, normalizes the
// CREATE VIEW target name, and records the referenced objects used for
// dependency planning.
func parseViewSchemaSQL(p *parser.Parser, currentView filter.Table, sql string) (*parsedViewSchema, error) {
	stmts, _, err := p.ParseSQL(sql)
	if err != nil {
		return nil, common.ErrInvalidSchemaStmt.Wrap(err).GenWithStackByArgs(sql)
	}

	var (
		res        strings.Builder
		restoreCtx = format.NewRestoreCtx(format.DefaultRestoreFlags, &res)
		createStmt *ast.CreateViewStmt
	)

	keptStatements := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		switch node := stmt.(type) {
		case *ast.DropTableStmt:
			// Dumpling emits placeholder cleanup DDL for views. We only keep the
			// statements needed to recreate the final view definition.
			continue
		case *ast.CreateViewStmt:
			if createStmt != nil {
				return nil, common.ErrInvalidSchemaStmt.GenWithStackByArgs("multiple create view statements found")
			}
			createStmt = node
		case *ast.SetStmt:
			// keep session setup statements
		default:
			// Preserve any additional parseable statements for compatibility with
			// the old view restore path, which tolerated them as long as the file
			// still contained a valid CREATE VIEW statement.
		}

		if err := stmt.Restore(restoreCtx); err != nil {
			return nil, common.ErrInvalidSchemaStmt.Wrap(err).GenWithStackByArgs(sql)
		}
		restoreCtx.WritePlain(";")
		keptStatements = append(keptStatements, res.String())
		res.Reset()
	}

	if createStmt == nil {
		return nil, common.ErrInvalidSchemaStmt.GenWithStackByArgs(
			"missing create view statement for " + currentView.String(),
		)
	}

	// Extract referenced objects from the SELECT body so the restore step can
	// build a dependency-aware creation order.
	collector := &viewDependencyCollector{
		currentSchema: currentView.Schema,
		deps:          make(tableNameSet),
	}
	createStmt.Select.Accept(collector)

	deps := slices.Collect(maps.Keys(collector.deps))

	return &parsedViewSchema{
		key:       currentView,
		deps:      deps,
		createSQL: strings.Join(keptStatements, "\n"),
	}, nil
}

// buildViewRestorePlan classifies each view dependency as:
//   - another dumped view, which becomes an edge in the topo-sort graph
//   - a dumped base table, which is already restored before views
//   - an external object, which must exist downstream before restore starts
func buildViewRestorePlan(parsedViews []*parsedViewSchema, dumpTables tableNameSet) (*viewRestorePlan, error) {
	plan := &viewRestorePlan{
		nodes: make(map[filter.Table]*viewNode, len(parsedViews)),
	}

	for _, parsed := range parsedViews {
		normalizedKey := normalizeTableName(parsed.key.Schema, parsed.key.Name)
		if _, exists := plan.nodes[normalizedKey]; exists {
			return nil, errors.Errorf("duplicate view definition for %s", parsed.key.String())
		}
		plan.nodes[normalizedKey] = &viewNode{
			key:       parsed.key,
			deps:      parsed.deps,
			createSQL: parsed.createSQL,
		}
	}

	for _, node := range plan.nodes {
		nodeKey := normalizeTableName(node.key.Schema, node.key.Name)
		for _, dep := range node.deps {
			normalizedDep := normalizeTableName(dep.Schema, dep.Name)
			if normalizedDep == nodeKey {
				return nil, errors.Errorf("cyclic view dependency detected for %s", node.key.String())
			}
			if depNode, ok := plan.nodes[normalizedDep]; ok {
				node.indegree++
				depNode.dependents = append(depNode.dependents, nodeKey)
				continue
			}
			if dumpTables.has(normalizedDep) {
				continue
			}
			node.externalDeps = append(node.externalDeps, normalizedDep)
		}
	}

	// Kahn's algorithm with a sorted initial ready set and sorted dependent
	// lists keeps view creation order stable across runs.
	ready := make([]*viewNode, 0, len(plan.nodes))
	for _, node := range plan.nodes {
		if node.indegree == 0 {
			ready = append(ready, node)
		}
	}
	sortViewNodes(ready)

	for len(ready) > 0 {
		node := ready[0]
		ready = ready[1:]
		plan.ordered = append(plan.ordered, node)

		for _, dependent := range node.dependents {
			dependentNode := plan.nodes[dependent]
			dependentNode.indegree--
			if dependentNode.indegree == 0 {
				ready = append(ready, dependentNode)
			}
		}
	}

	if len(plan.ordered) != len(plan.nodes) {
		cycleNodes := make([]filter.Table, 0, len(plan.nodes)-len(plan.ordered))
		for key, node := range plan.nodes {
			if node.indegree > 0 {
				cycleNodes = append(cycleNodes, key)
			}
		}
		cycleNames := make([]string, 0, len(cycleNodes))
		for _, key := range cycleNodes {
			cycleNames = append(cycleNames, key.String())
		}
		return nil, errors.Errorf("cyclic view dependency detected among %s", strings.Join(cycleNames, ", "))
	}

	return plan, nil
}

// validateViewRestorePlan checks the external dependencies collected during
// planning against the objects that already exist downstream.
func validateViewRestorePlan(plan *viewRestorePlan, existingObjects tableNameSet) error {
	for _, node := range plan.ordered {
		for _, dep := range node.externalDeps {
			if existingObjects.has(dep) {
				continue
			}
			return errors.Errorf("missing dependency %s referenced by %s", dep.String(), node.key.String())
		}
	}
	return nil
}
