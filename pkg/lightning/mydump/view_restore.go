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
	if s == nil {
		return
	}
	s[tbl] = struct{}{}
}

func (s tableNameSet) has(tbl filter.Table) bool {
	if s == nil {
		return false
	}
	_, ok := s[tbl]
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
}

func (c *viewDependencyCollector) Enter(n ast.Node) (ast.Node, bool) {
	tbl, ok := n.(*ast.TableName)
	if !ok {
		return n, false
	}

	schema := tbl.Schema.O
	if schema == "" {
		// Dumpling may omit the schema for same-database references.
		schema = c.currentSchema
	}
	c.deps.add(filter.Table{Schema: schema, Name: tbl.Name.O})
	return n, true
}

func (*viewDependencyCollector) Leave(n ast.Node) (ast.Node, bool) {
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
			schemaMeta := viewMeta
			if schemaMeta.charSet == "" {
				cloned := *schemaMeta
				cloned.charSet = "auto"
				schemaMeta = &cloned
			}
			sqlStr, err := schemaMeta.GetSchema(ctx, store)
			if err != nil {
				return nil, err
			}
			if strings.TrimSpace(sqlStr) == "" {
				return nil, errors.Errorf("empty schema for view %s.%s", viewMeta.DB, viewMeta.Name)
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
		restoreCtx = format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreTiDBSpecialComment|format.RestoreWithTTLEnableOff, &res)
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
			node.ViewName.Schema = ast.NewCIStr(currentView.Schema)
			node.ViewName.Name = ast.NewCIStr(currentView.Name)
			createStmt = node
		case *ast.SetStmt:
			// keep session setup statements
		default:
			return nil, common.ErrInvalidSchemaStmt.GenWithStackByArgs("unsupported statement in view schema")
		}

		if err := stmt.Restore(restoreCtx); err != nil {
			return nil, common.ErrInvalidSchemaStmt.Wrap(err).GenWithStackByArgs(sql)
		}
		restoreCtx.WritePlain(";")
		keptStatements = append(keptStatements, res.String())
		res.Reset()
	}

	if createStmt == nil {
		return nil, common.ErrInvalidSchemaStmt.GenWithStackByArgs("missing create view statement")
	}

	// Extract referenced objects from the SELECT body so the restore step can
	// build a dependency-aware creation order.
	collector := &viewDependencyCollector{
		currentSchema: currentView.Schema,
		deps:          make(tableNameSet),
	}
	createStmt.Select.Accept(collector)

	deps := make([]filter.Table, 0, len(collector.deps))
	for dep := range collector.deps {
		deps = append(deps, dep)
	}
	sort.Slice(deps, func(i, j int) bool {
		if deps[i].Schema != deps[j].Schema {
			return deps[i].Schema < deps[j].Schema
		}
		return deps[i].Name < deps[j].Name
	})

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
		if _, exists := plan.nodes[parsed.key]; exists {
			return nil, errors.Errorf("duplicate view definition for %s", parsed.key.String())
		}
		plan.nodes[parsed.key] = &viewNode{
			key:       parsed.key,
			deps:      append([]filter.Table(nil), parsed.deps...),
			createSQL: parsed.createSQL,
		}
	}

	for _, node := range plan.nodes {
		for _, dep := range node.deps {
			if dep == node.key {
				return nil, errors.Errorf("cyclic view dependency detected for %s", node.key.String())
			}
			if depNode, ok := plan.nodes[dep]; ok {
				node.indegree++
				depNode.dependents = append(depNode.dependents, node.key)
				continue
			}
			if dumpTables.has(dep) {
				continue
			}
			node.externalDeps = append(node.externalDeps, dep)
		}
		sort.Slice(node.dependents, func(i, j int) bool {
			if node.dependents[i].Schema != node.dependents[j].Schema {
				return node.dependents[i].Schema < node.dependents[j].Schema
			}
			return node.dependents[i].Name < node.dependents[j].Name
		})
		sort.Slice(node.externalDeps, func(i, j int) bool {
			if node.externalDeps[i].Schema != node.externalDeps[j].Schema {
				return node.externalDeps[i].Schema < node.externalDeps[j].Schema
			}
			return node.externalDeps[i].Name < node.externalDeps[j].Name
		})
	}

	// Kahn's algorithm with a deterministic ready queue keeps view creation
	// order stable across runs when multiple nodes become ready together.
	remainingIndegree := make(map[filter.Table]int, len(plan.nodes))
	ready := make([]filter.Table, 0, len(plan.nodes))
	for key, node := range plan.nodes {
		remainingIndegree[key] = node.indegree
		if node.indegree == 0 {
			ready = append(ready, key)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Schema != ready[j].Schema {
			return ready[i].Schema < ready[j].Schema
		}
		return ready[i].Name < ready[j].Name
	})

	for len(ready) > 0 {
		key := ready[0]
		ready = ready[1:]
		node := plan.nodes[key]
		plan.ordered = append(plan.ordered, node)

		for _, dependent := range node.dependents {
			remainingIndegree[dependent]--
			if remainingIndegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
		sort.Slice(ready, func(i, j int) bool {
			if ready[i].Schema != ready[j].Schema {
				return ready[i].Schema < ready[j].Schema
			}
			return ready[i].Name < ready[j].Name
		})
	}

	if len(plan.ordered) != len(plan.nodes) {
		return nil, errors.New("cyclic view dependency detected")
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
