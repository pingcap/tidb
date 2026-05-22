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
	"database/sql"
	"fmt"
	"sort"
	"strings"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

type schemaStmtType int

// String implements fmt.Stringer interface.
func (stmtType schemaStmtType) String() string {
	switch stmtType {
	case schemaCreateDatabase:
		return "restore database schema"
	case schemaCreateTable:
		return "restore table schema"
	case schemaCreateView:
		return "restore view schema"
	}
	return "unknown statement of schema"
}

const (
	schemaCreateDatabase schemaStmtType = iota
	schemaCreateTable
	schemaCreateView
)

type schemaJob struct {
	dbName   string
	tblName  string // empty for create db jobs
	stmtType schemaStmtType
	sqlStr   string
}

// SchemaImporter is used to import schema from dump files.
type SchemaImporter struct {
	logger      log.Logger
	db          *sql.DB
	sqlMode     mysql.SQLMode
	store       storeapi.Storage
	concurrency int
}

// NewSchemaImporter creates a new SchemaImporter instance.
func NewSchemaImporter(logger log.Logger, sqlMode mysql.SQLMode, db *sql.DB, store storeapi.Storage, concurrency int) *SchemaImporter {
	return &SchemaImporter{
		logger:      logger,
		db:          db,
		sqlMode:     sqlMode,
		store:       store,
		concurrency: concurrency,
	}
}

// Run imports all schemas from the given database metas.
func (si *SchemaImporter) Run(ctx context.Context, dbMetas []*MDDatabaseMeta) (err error) {
	plan, err := NewSchemaImportPlan(ctx, si.store, si.sqlMode, dbMetas)
	if err != nil {
		return err
	}
	logTask := si.logger.Begin(zap.InfoLevel, "restore all schema")
	defer func() {
		logTask.End(zap.ErrorLevel, err)
	}()

	if plan == nil || len(plan.dbMetas) == 0 {
		return nil
	}

	if err = si.importDatabases(ctx, plan.dbMetas); err != nil {
		return errors.Trace(err)
	}
	if err = si.importTables(ctx, plan.dbMetas); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(si.importViews(ctx, plan))
}

func (si *SchemaImporter) importDatabases(ctx context.Context, dbMetas []*MDDatabaseMeta) error {
	existingSchemas, err := si.getExistingDatabases(ctx)
	if err != nil {
		return err
	}

	ch := make(chan *MDDatabaseMeta)
	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	for range si.concurrency {
		eg.Go(func() error {
			p := parser.New()
			p.SetSQLMode(si.sqlMode)
			for dbMeta := range ch {
				sqlStr := dbMeta.GetSchema(egCtx, si.store)
				if err2 := si.runCommonJob(egCtx, p, &schemaJob{
					dbName:   dbMeta.Name,
					stmtType: schemaCreateDatabase,
					sqlStr:   sqlStr,
				}); err2 != nil {
					return err2
				}
			}
			return nil
		})
	}
	eg.Go(func() error {
		defer close(ch)
		for i := range dbMetas {
			dbMeta := dbMetas[i]
			// if downstream already has this database, we can skip ddl job
			if existingSchemas.Exist(strings.ToLower(dbMeta.Name)) {
				si.logger.Info("database already exists in downstream, skip",
					zap.String("db", dbMeta.Name),
				)
				continue
			}
			select {
			case ch <- dbMeta:
			case <-egCtx.Done():
			}
		}
		return nil
	})

	return eg.Wait()
}

func (si *SchemaImporter) importTables(ctx context.Context, dbMetas []*MDDatabaseMeta) error {
	ch := make(chan *MDTableMeta)
	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	for range si.concurrency {
		eg.Go(func() error {
			p := parser.New()
			p.SetSQLMode(si.sqlMode)
			for tableMeta := range ch {
				if tableMeta.SchemaFile.FileMeta.Path == "" {
					exist, err := si.isTableExist(egCtx, tableMeta.DB, tableMeta.Name)
					if err != nil {
						return err
					}
					if exist {
						// we already has this table in TiDB.
						// we should skip ddl job and let SchemaValid check.
						si.logger.Info("table already exists in downstream, skip",
							zap.String("db", tableMeta.DB), zap.String("table", tableMeta.Name))
						continue
					}
					return common.ErrSchemaNotExists.GenWithStackByArgs(tableMeta.DB, tableMeta.Name)
				}
				sqlStr, err := tableMeta.GetSchema(egCtx, si.store)
				if err != nil {
					return err
				}
				if err = si.runCreateTableJob(egCtx, p, &schemaJob{
					dbName:   tableMeta.DB,
					tblName:  tableMeta.Name,
					stmtType: schemaCreateTable,
					sqlStr:   sqlStr,
				}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	eg.Go(func() error {
		defer close(ch)
		for _, dbMeta := range dbMetas {
			if len(dbMeta.Tables) == 0 {
				continue
			}
			for i := range dbMeta.Tables {
				tblMeta := dbMeta.Tables[i]
				select {
				case ch <- tblMeta:
				case <-egCtx.Done():
					return egCtx.Err()
				}
			}
		}
		return nil
	})

	return eg.Wait()
}

// dumpling dump a view as a table-schema sql file which creates a table of same name
// as the view, and a view-schema sql file which drops the table and creates the view.
func (si *SchemaImporter) importViews(ctx context.Context, plan *SchemaImportPlan) error {
	// 3. restore views. Since views can cross database we must restore views after all table schemas are restored.
	if plan.viewPlan == nil {
		return nil
	}
	existingNonViews, existingViews, err := si.loadExistingViewDependencies(ctx, plan.viewPlan)
	if err != nil {
		return err
	}
	if err := validateViewRestorePlan(plan.viewPlan, unionTableNames(existingNonViews, existingViews)); err != nil {
		return err
	}

	p := parser.New()
	p.SetSQLMode(si.sqlMode)

	// TODO: Parallelize independent views in the same topo layer instead of
	// executing the whole ordered list one by one.
	for _, node := range plan.viewPlan.ordered {
		normalizedKey := normalizeTableName(node.key.Schema, node.key.Name)
		if existingViews.has(normalizedKey) {
			si.logger.Info("view already exists in downstream, skip",
				zap.String("db", node.key.Schema),
				zap.String("view-name", node.key.Name))
			continue
		}
		if existingNonViews.has(normalizedKey) {
			return common.ErrCreateSchema.GenWithStack("downstream non-view object already exists for view '%s'", node.key.String())
		}
		if err := si.runCommonJob(ctx, p, &schemaJob{
			dbName:   node.key.Schema,
			tblName:  node.key.Name,
			stmtType: schemaCreateView,
			sqlStr:   node.createSQL,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (si *SchemaImporter) runCreateTableJob(ctx context.Context, p *parser.Parser, job *schemaJob) error {
	// Table schema restore should preserve session directives but must not drop
	// downstream objects from source schema files.
	stmts, err := createIfNotExistsStmtWithMode(p, job.sqlStr, job.dbName, job.tblName, true)
	if err != nil {
		// if the schema supplied by the user is un-parsable by TiDB, we allow
		// user to create the table by themselves, then import data.
		exist, err2 := si.isTableExist(ctx, job.dbName, job.tblName)
		if err2 != nil {
			return err2
		}
		if exist {
			// we already has this table in TiDB.
			// we should skip ddl job and let SchemaValid check.
			si.logger.Info("table already exists in downstream, skip",
				zap.String("db", job.dbName), zap.String("table", job.tblName))
			return nil
		}
		return errors.Trace(err)
	}
	return si.runJob(ctx, job, stmts)
}

func filterTableName(dbName, tblName string) filter.Table {
	return filter.Table{Schema: dbName, Name: tblName}
}

func normalizeTableName(dbName, tblName string) filter.Table {
	return filter.Table{Schema: strings.ToLower(dbName), Name: strings.ToLower(tblName)}
}

// collectDumpTables returns only the physical tables restored before the view
// phase. View dependencies are tracked separately in viewRestorePlan.
func collectDumpTables(dbMetas []*MDDatabaseMeta) tableNameSet {
	tables := make(tableNameSet)
	for _, dbMeta := range dbMetas {
		for _, tableMeta := range dbMeta.Tables {
			tables.add(filterTableName(tableMeta.DB, tableMeta.Name))
		}
	}
	return tables
}

// unionTableNames merges downstream tables/views into one lookup set for
// external dependency validation.
func unionTableNames(sets ...tableNameSet) tableNameSet {
	merged := make(tableNameSet)
	for _, set := range sets {
		for key := range set {
			merged.add(key)
		}
	}
	return merged
}

// loadExistingViewDependencies fetches downstream tables and views for every
// object referenced by the plan so validation can distinguish missing external
// objects from already satisfied dependencies or name collisions without
// scanning whole schemas.
func (si *SchemaImporter) loadExistingViewDependencies(
	ctx context.Context,
	plan *viewRestorePlan,
) (existingNonViews tableNameSet, existingViews tableNameSet, err error) {
	schemas := make(set.StringSet)
	for _, node := range plan.nodes {
		schemas.Insert(strings.ToLower(node.key.Schema))
		for _, dep := range node.deps {
			schemas.Insert(strings.ToLower(dep.Schema))
		}
	}

	existingNonViews = make(tableNameSet)
	existingViews = make(tableNameSet)
	// existingNonViews tracks all downstream TABLE_TYPE != VIEW objects. In
	// TiDB today this effectively means tables and sequences.
	schemaNames := make([]string, 0, len(schemas))
	for schema := range schemas {
		schemaNames = append(schemaNames, schema)
	}
	sort.Strings(schemaNames)
	for _, schema := range schemaNames {
		var objectTypes map[string]bool
		objectTypes, err = si.getExistingObjectTypes(ctx, schema)
		if err != nil {
			return nil, nil, err
		}
		for objectName, isView := range objectTypes {
			if isView {
				existingViews.add(filter.Table{Schema: schema, Name: objectName})
				continue
			}
			existingNonViews.add(filter.Table{Schema: schema, Name: objectName})
		}
	}
	return existingNonViews, existingViews, nil
}

func (si *SchemaImporter) runCommonJob(ctx context.Context, p *parser.Parser, job *schemaJob) error {
	stmts, err := createIfNotExistsStmt(p, job.sqlStr, job.dbName, job.tblName)
	if err != nil {
		return errors.Trace(err)
	}
	return si.runJob(ctx, job, stmts)
}

func (si *SchemaImporter) runJob(ctx context.Context, job *schemaJob, stmts []string) error {
	conn, err := si.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	logger := si.logger.With(zap.String("db", job.dbName), zap.String("table", job.tblName))
	sqlWithRetry := common.SQLWithRetry{
		Logger: logger,
		DB:     conn,
	}
	for _, stmt := range stmts {
		task := logger.Begin(zap.DebugLevel, fmt.Sprintf("execute SQL: %s", stmt))
		err = sqlWithRetry.Exec(ctx, "run create schema job", stmt)
		task.End(zap.ErrorLevel, err)

		if err != nil {
			return common.ErrCreateSchema.Wrap(err).GenWithStackByArgs(common.UniqueTable(job.dbName, job.tblName), job.stmtType.String())
		}
	}
	return nil
}

func (si *SchemaImporter) getExistingDatabases(ctx context.Context) (set.StringSet, error) {
	return si.getExistingSchemas(ctx, `SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`)
}

// isTableExist checks whether the table exists in the downstream database, it
// works for view too.
// info schema V2 only store one copy of schema object in memory, so read/write
// need to lock, if we read too much and takes too long, it affects write, i.e.
// schema reloading during DDL execution, we can mitigate this by using
// finer-grained lock, but we cannot avoid it completely with current strategy.
// that's why we don't check table existence in batch by
// 'select table_name information_schema.tables where schema=xxx', and uses a
// 'show create table' to check table by table instead.
// 'select table_name information_schema.tables where schema=xxx and table_name=xxx'
// should be fine too, but that depends on how memory table is implemented, so we
// stick to 'show create table' for now.
func (si *SchemaImporter) isTableExist(ctx context.Context, dbName, tableName string) (bool, error) {
	sb := new(strings.Builder)
	sqlescape.MustFormatSQL(sb, `SHOW CREATE TABLE %n.%n`, dbName, tableName)
	_, err := si.getExistingSchemas(ctx, sb.String())
	if err != nil {
		cause := errors.Cause(err)
		if driverErr, ok := cause.(*dmysql.MySQLError); ok && driverErr.Number == errno.ErrNoSuchTable {
			return false, nil
		}
		return false, err
	}
	// show create table always return the table if no error, so no need to check
	// the result row count.
	return true, nil
}

func (si *SchemaImporter) getExistingObjectTypes(ctx context.Context, dbName string) (map[string]bool, error) {
	sb := new(strings.Builder)
	sqlescape.MustFormatSQL(sb, `SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = %?`, dbName)
	rows, err := si.queryStringRows(ctx, sb.String())
	if err != nil {
		return nil, err
	}
	objectTypes := make(map[string]bool, len(rows))
	for _, row := range rows {
		objectTypes[strings.ToLower(row[0])] = strings.EqualFold(row[1], "VIEW")
	}
	return objectTypes, nil
}

// get existing databases/tables/views using the given query, the first column of
// the query result should be the name.
// The returned names are convert to lower case.
func (si *SchemaImporter) getExistingSchemas(ctx context.Context, query string) (set.StringSet, error) {
	stringRows, err := si.queryStringRows(ctx, query)
	if err != nil {
		return nil, err
	}
	res := make(set.StringSet, len(stringRows))
	for _, row := range stringRows {
		res.Insert(strings.ToLower(row[0]))
	}
	return res, nil
}

func (si *SchemaImporter) queryStringRows(ctx context.Context, query string) ([][]string, error) {
	conn, err := si.db.Conn(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		_ = conn.Close()
	}()
	sqlWithRetry := common.SQLWithRetry{
		Logger: si.logger,
		DB:     conn,
	}
	stringRows, err := sqlWithRetry.QueryStringRows(ctx, "get existing schemas", query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return stringRows, nil
}

func createIfNotExistsStmt(p *parser.Parser, createTable, dbName, tblName string) ([]string, error) {
	return createIfNotExistsStmtWithMode(p, createTable, dbName, tblName, false)
}

func createIfNotExistsStmtWithMode(
	p *parser.Parser,
	createTable, dbName, tblName string,
	ignoreDestructiveDDL bool,
) ([]string, error) {
	stmts, _, err := p.ParseSQL(createTable)
	if err != nil {
		return []string{}, common.ErrInvalidSchemaStmt.Wrap(err).GenWithStackByArgs(createTable)
	}

	var res strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreTiDBSpecialComment|format.RestoreWithTTLEnableOff, &res)

	retStmts := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		switch node := stmt.(type) {
		case *ast.CreateDatabaseStmt:
			node.Name = ast.NewCIStr(dbName)
			node.IfNotExists = true
		case *ast.DropDatabaseStmt:
			if ignoreDestructiveDDL {
				continue
			}
			node.Name = ast.NewCIStr(dbName)
			node.IfExists = true
		case *ast.CreateTableStmt:
			node.Table.Schema = ast.NewCIStr(dbName)
			node.Table.Name = ast.NewCIStr(tblName)
			node.IfNotExists = true
		case *ast.CreateViewStmt:
			node.ViewName.Schema = ast.NewCIStr(dbName)
			node.ViewName.Name = ast.NewCIStr(tblName)
		case *ast.DropTableStmt:
			if ignoreDestructiveDDL {
				continue
			}
			node.Tables[0].Schema = ast.NewCIStr(dbName)
			node.Tables[0].Name = ast.NewCIStr(tblName)
			node.IfExists = true
		}
		if err := stmt.Restore(ctx); err != nil {
			return []string{}, common.ErrInvalidSchemaStmt.Wrap(err).GenWithStackByArgs(createTable)
		}
		ctx.WritePlain(";")
		retStmts = append(retStmts, res.String())
		res.Reset()
	}

	return retStmts, nil
}
