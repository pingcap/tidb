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

package restore

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/util/set"
	filter "github.com/pingcap/tidb/util/table-filter"
	"go.uber.org/zap"
)

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

type parsedViewSchema struct {
	key       filter.Table
	deps      []filter.Table
	createSQL string
}

type viewNode struct {
	key          filter.Table
	deps         []filter.Table
	externalDeps []filter.Table
	dependents   []filter.Table
	indegree     int
	createSQL    string
}

type viewRestorePlan struct {
	nodes   map[filter.Table]*viewNode
	ordered []*viewNode
}

type viewDependencyCollector struct {
	currentSchema string
	deps          tableNameSet
	cteNameScopes []map[string]struct{}
}

func tableKey(dbName, tblName string) filter.Table {
	return filter.Table{Schema: dbName, Name: tblName}
}

func normalizeTableName(dbName, tblName string) filter.Table {
	return filter.Table{Schema: strings.ToLower(dbName), Name: strings.ToLower(tblName)}
}

func formatTableName(tbl filter.Table) string {
	return common.UniqueTable(tbl.Schema, tbl.Name)
}

func lessTableName(left, right filter.Table) bool {
	if left.Schema != right.Schema {
		return left.Schema < right.Schema
	}
	return left.Name < right.Name
}

func sortTableNames(tables []filter.Table) {
	sort.Slice(tables, func(i, j int) bool {
		return lessTableName(tables[i], tables[j])
	})
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

func hasWithClause(n ast.Node) bool {
	switch node := n.(type) {
	case *ast.SelectStmt:
		return node.With != nil
	case *ast.SetOprStmt:
		return node.With != nil
	case *ast.SetOprSelectList:
		return node.With != nil
	default:
		return false
	}
}

func (c *viewDependencyCollector) Enter(n ast.Node) (ast.Node, bool) {
	if hasWithClause(n) {
		c.pushCTEScope()
		return n, false
	}

	switch node := n.(type) {
	case *ast.CommonTableExpression:
		if node.IsRecursive {
			c.recordCTEName(node.Name.L)
		}
		return n, false
	case *ast.TableName:
		if node.Schema.O == "" && c.isCTEName(node.Name.L) {
			return n, true
		}

		schema := node.Schema.L
		if schema == "" {
			schema = strings.ToLower(c.currentSchema)
		}
		c.deps.add(tableKey(schema, node.Name.L))
		return n, true
	default:
		return n, false
	}
}

func (c *viewDependencyCollector) Leave(n ast.Node) (ast.Node, bool) {
	if node, ok := n.(*ast.CommonTableExpression); ok && !node.IsRecursive {
		c.recordCTEName(node.Name.L)
	}
	if hasWithClause(n) {
		c.popCTEScope()
	}
	return n, true
}

func collectDumpTables(dbMetas []*mydump.MDDatabaseMeta) tableNameSet {
	tables := make(tableNameSet)
	for _, dbMeta := range dbMetas {
		for _, tableMeta := range dbMeta.Tables {
			tables.add(tableKey(tableMeta.DB, tableMeta.Name))
		}
	}
	return tables
}

func mergeTableNameSets(sets ...tableNameSet) tableNameSet {
	merged := make(tableNameSet)
	for _, names := range sets {
		for name := range names {
			merged.add(name)
		}
	}
	return merged
}

func parseViewSchemaSQL(p *parser.Parser, currentView filter.Table, sqlStr string) (*parsedViewSchema, error) {
	stmts, _, err := p.ParseSQL(sqlStr)
	if err != nil {
		return nil, common.ErrInvalidSchemaStmt.Wrap(err).GenWithStackByArgs(sqlStr)
	}

	var (
		res        strings.Builder
		formatCtx  = format.NewRestoreCtx(format.DefaultRestoreFlags, &res)
		createStmt *ast.CreateViewStmt
	)

	keptStatements := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		switch node := stmt.(type) {
		case *ast.DropTableStmt:
			// Dumpling emits placeholder cleanup DDL for views. Keep only the
			// statements required to recreate the final view definition.
			_ = node
			continue
		case *ast.CreateViewStmt:
			if createStmt != nil {
				return nil, common.ErrInvalidSchemaStmt.GenWithStackByArgs("multiple create view statements found")
			}
			createStmt = node
		}

		if err := stmt.Restore(formatCtx); err != nil {
			return nil, common.ErrInvalidSchemaStmt.Wrap(err).GenWithStackByArgs(sqlStr)
		}
		formatCtx.WritePlain(";")
		keptStatements = append(keptStatements, res.String())
		res.Reset()
	}

	if createStmt == nil {
		return nil, common.ErrInvalidSchemaStmt.GenWithStackByArgs("missing create view statement for " + formatTableName(currentView))
	}

	collector := &viewDependencyCollector{
		currentSchema: currentView.Schema,
		deps:          make(tableNameSet),
	}
	createStmt.Select.Accept(collector)

	deps := make([]filter.Table, 0, len(collector.deps))
	for dep := range collector.deps {
		deps = append(deps, dep)
	}
	sortTableNames(deps)

	return &parsedViewSchema{
		key:       currentView,
		deps:      deps,
		createSQL: strings.Join(keptStatements, "\n"),
	}, nil
}

func buildViewRestorePlan(
	ctx context.Context,
	store storage.ExternalStorage,
	p *parser.Parser,
	dbMetas []*mydump.MDDatabaseMeta,
) (*viewRestorePlan, error) {
	parsedViews := make([]*parsedViewSchema, 0)
	for _, dbMeta := range dbMetas {
		for _, viewMeta := range dbMeta.Views {
			sqlStr, err := viewMeta.GetSchema(ctx, store)
			if err != nil {
				return nil, err
			}

			parsed, err := parseViewSchemaSQL(p, tableKey(viewMeta.DB, viewMeta.Name), sqlStr)
			if err != nil {
				return nil, err
			}
			parsedViews = append(parsedViews, parsed)
		}
	}
	if len(parsedViews) == 0 {
		return nil, nil
	}
	return buildViewImportPlan(parsedViews, collectDumpTables(dbMetas))
}

func buildViewImportPlan(parsedViews []*parsedViewSchema, dumpTables tableNameSet) (*viewRestorePlan, error) {
	plan := &viewRestorePlan{
		nodes: make(map[filter.Table]*viewNode, len(parsedViews)),
	}

	for _, parsed := range parsedViews {
		normalizedKey := normalizeTableName(parsed.key.Schema, parsed.key.Name)
		if _, exists := plan.nodes[normalizedKey]; exists {
			return nil, errors.Errorf("duplicate view definition for %s", formatTableName(parsed.key))
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
				return nil, errors.Errorf("cyclic view dependency detected for %s", formatTableName(node.key))
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
		sortTableNames(node.externalDeps)
		sortTableNames(node.dependents)
	}

	ready := make([]*viewNode, 0, len(plan.nodes))
	for _, node := range plan.nodes {
		if node.indegree == 0 {
			ready = append(ready, node)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		return lessTableName(ready[i].key, ready[j].key)
	})

	for len(ready) > 0 {
		node := ready[0]
		ready = ready[1:]
		plan.ordered = append(plan.ordered, node)

		newReady := false
		for _, dependent := range node.dependents {
			dependentNode := plan.nodes[dependent]
			dependentNode.indegree--
			if dependentNode.indegree == 0 {
				ready = append(ready, dependentNode)
				newReady = true
			}
		}
		if newReady {
			sort.Slice(ready, func(i, j int) bool {
				return lessTableName(ready[i].key, ready[j].key)
			})
		}
	}

	if len(plan.ordered) != len(plan.nodes) {
		cycleNames := make([]string, 0, len(plan.nodes)-len(plan.ordered))
		for key, node := range plan.nodes {
			if node.indegree > 0 {
				cycleNames = append(cycleNames, formatTableName(key))
			}
		}
		sort.Strings(cycleNames)
		return nil, errors.Errorf("cyclic view dependency detected among %s", strings.Join(cycleNames, ", "))
	}

	return plan, nil
}

func validateViewRestorePlan(plan *viewRestorePlan, existingObjects tableNameSet) error {
	for _, node := range plan.ordered {
		for _, dep := range node.externalDeps {
			if existingObjects.has(dep) {
				continue
			}
			return errors.Errorf("missing dependency %s referenced by %s", formatTableName(dep), formatTableName(node.key))
		}
	}
	return nil
}

func (worker *restoreSchemaWorker) enqueueViewJobs(dbMetas []*mydump.MDDatabaseMeta) error {
	plan, err := buildViewRestorePlan(worker.ctx, worker.store, worker.glue.GetParser(), dbMetas)
	if err != nil {
		return err
	}
	if plan == nil {
		return nil
	}

	existingNonViews, existingViews, err := worker.loadExistingViewDependencies(plan)
	if err != nil {
		return err
	}
	if err := validateViewRestorePlan(plan, mergeTableNameSets(existingNonViews, existingViews)); err != nil {
		return err
	}

	for _, node := range plan.ordered {
		normalizedKey := normalizeTableName(node.key.Schema, node.key.Name)
		if existingViews.has(normalizedKey) {
			worker.logger.Info("view already exists in downstream, skip processing the source file",
				zap.String("db", node.key.Schema),
				zap.String("view-name", node.key.Name))
			continue
		}
		if existingNonViews.has(normalizedKey) {
			return common.ErrCreateSchema.GenWithStack("downstream non-view object already exists for view '%s'", formatTableName(node.key))
		}

		if err := worker.addJob(node.createSQL, &schemaJob{
			dbName:   node.key.Schema,
			tblName:  node.key.Name,
			stmtType: schemaCreateView,
		}); err != nil {
			return err
		}
		// We don't support restoring views concurrently because the dump may
		// contain cross-schema dependencies between them.
		if err := worker.wait(); err != nil {
			return err
		}
	}
	return nil
}

func (worker *restoreSchemaWorker) loadExistingViewDependencies(plan *viewRestorePlan) (tableNameSet, tableNameSet, error) {
	schemas := make(set.StringSet)
	for _, node := range plan.nodes {
		schemas.Insert(strings.ToLower(node.key.Schema))
		for _, dep := range node.deps {
			schemas.Insert(strings.ToLower(dep.Schema))
		}
	}
	orderedSchemas := make([]string, 0, len(schemas))
	for schema := range schemas {
		orderedSchemas = append(orderedSchemas, schema)
	}
	sort.Strings(orderedSchemas)

	existingNonViews := make(tableNameSet)
	existingViews := make(tableNameSet)
	for _, schema := range orderedSchemas {
		objectTypes, err := worker.getExistingObjectTypes(worker.ctx, schema)
		if err != nil {
			return nil, nil, err
		}
		for objectName, isView := range objectTypes {
			key := filter.Table{Schema: schema, Name: objectName}
			if isView {
				existingViews.add(key)
				continue
			}
			existingNonViews.add(key)
		}
	}
	return existingNonViews, existingViews, nil
}

func (worker *restoreSchemaWorker) getExistingObjectTypes(ctx context.Context, schema string) (map[string]bool, error) {
	rows, err := worker.queryStringRows(
		ctx,
		"SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
		schema,
	)
	if err != nil {
		return nil, err
	}

	objectTypes := make(map[string]bool, len(rows))
	for _, row := range rows {
		objectTypes[strings.ToLower(row[0])] = strings.EqualFold(row[1], "VIEW")
	}
	return objectTypes, nil
}

func (worker *restoreSchemaWorker) queryStringRows(ctx context.Context, query string, args ...interface{}) ([][]string, error) {
	db, err := worker.glue.GetDB()
	if err != nil {
		return nil, errors.Trace(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make([][]string, 0)
	for rows.Next() {
		values := make([]sql.NullString, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, errors.Trace(err)
		}
		row := make([]string, len(cols))
		for i := range values {
			row[i] = values[i].String
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}
