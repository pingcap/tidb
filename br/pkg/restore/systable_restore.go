// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const (
	sysUserTableName = "user"
)

var statsTables = map[string]map[string]struct{}{
	"mysql": {
		"stats_buckets":      {},
		"stats_extended":     {},
		"stats_feedback":     {},
		"stats_fm_sketch":    {},
		"stats_histograms":   {},
		"stats_history":      {},
		"stats_meta":         {},
		"stats_meta_history": {},
		"stats_table_locked": {},
		"stats_top_n":        {},
	},
}

var unRecoverableTable = map[string]map[string]struct{}{
	"mysql": {
		// some variables in tidb (e.g. gc_safe_point) cannot be recovered.
		"tidb":             {},
		"global_variables": {},

		"column_stats_usage":               {},
		"capture_plan_baselines_blacklist": {},
		// gc info don't need to recover.
		"gc_delete_range":      {},
		"gc_delete_range_done": {},

		// replace into view is not supported now
		"tidb_mdl_view": {},
	},
	"sys": {
		// replace into view is not supported now
		"schema_unused_indexes": {},
	},
}

type checkPrivilegeTableRowsCollateCompatibilitySQLPair struct {
	upstreamCollateSQL   string
	downstreamCollateSQL string
	columns              map[string]struct{}
}

var collateCompatibilityTables = map[string]map[string]checkPrivilegeTableRowsCollateCompatibilitySQLPair{
	"mysql": {
		"db": {
			upstreamCollateSQL:   "SELECT COUNT(1) FROM __TiDB_BR_Temporary_mysql.db",
			downstreamCollateSQL: "SELECT COUNT(1) FROM (SELECT Host, DB COLLATE utf8mb4_general_ci, User FROM __TiDB_BR_Temporary_mysql.db GROUP BY Host, DB COLLATE utf8mb4_general_ci, User) as a",
			columns:              map[string]struct{}{"db": {}},
		},
		"tables_priv": {
			upstreamCollateSQL:   "SELECT COUNT(1) FROM __TiDB_BR_Temporary_mysql.tables_priv",
			downstreamCollateSQL: "SELECT COUNT(1) FROM (SELECT Host, DB COLLATE utf8mb4_general_ci, User, Table_name COLLATE utf8mb4_general_ci FROM __TiDB_BR_Temporary_mysql.tables_priv GROUP BY Host, DB COLLATE utf8mb4_general_ci, User, Table_name COLLATE utf8mb4_general_ci) as a",
			columns:              map[string]struct{}{"db": {}, "table_name": {}},
		},
		"columns_priv": {
			upstreamCollateSQL:   "SELECT COUNT(1) FROM __TiDB_BR_Temporary_mysql.columns_priv",
			downstreamCollateSQL: "SELECT COUNT(1) FROM (SELECT Host, DB COLLATE utf8mb4_general_ci, User, Table_name COLLATE utf8mb4_general_ci, Column_name COLLATE utf8mb4_general_ci FROM __TiDB_BR_Temporary_mysql.columns_priv GROUP BY Host, DB COLLATE utf8mb4_general_ci, User, Table_name COLLATE utf8mb4_general_ci, Column_name COLLATE utf8mb4_general_ci) as a",
			columns:              map[string]struct{}{"db": {}, "table_name": {}, "column_name": {}},
		},
	},
}

func isUnrecoverableTable(schemaName string, tableName string) bool {
	tableMap, ok := unRecoverableTable[schemaName]
	if !ok {
		return false
	}
	_, ok = tableMap[tableName]
	return ok
}

func isStatsTable(schemaName string, tableName string) bool {
	tableMap, ok := statsTables[schemaName]
	if !ok {
		return false
	}
	_, ok = tableMap[tableName]
	return ok
}

// RestoreSystemSchemas restores the system schema(i.e. the `mysql` schema).
// Detail see https://github.com/pingcap/br/issues/679#issuecomment-762592254.
func (rc *Client) RestoreSystemSchemas(ctx context.Context, f filter.Filter) (rerr error) {
	sysDBs := []string{mysql.SystemDB, mysql.SysDB}
	for _, sysDB := range sysDBs {
		err := rc.restoreSystemSchema(ctx, f, sysDB)
		if err != nil {
			return err
		}
	}
	return nil
}

// restoreSystemSchema restores a system schema(i.e. the `mysql` or `sys` schema).
// Detail see https://github.com/pingcap/br/issues/679#issuecomment-762592254.
func (rc *Client) restoreSystemSchema(ctx context.Context, f filter.Filter, sysDB string) (rerr error) {
	temporaryDB := utils.TemporaryDBName(sysDB)
	defer func() {
		// Don't clean the temporary database for next restore with checkpoint.
		if rerr == nil {
			rc.cleanTemporaryDatabase(ctx, sysDB)
		}
	}()

	if !f.MatchSchema(sysDB) || !rc.withSysTable {
		log.Debug("system database filtered out", zap.String("database", sysDB))
		return nil
	}
	originDatabase, ok := rc.databases[temporaryDB.O]
	if !ok {
		log.Info("system database not backed up, skipping", zap.String("database", sysDB))
		return nil
	}
	db, ok := rc.getDatabaseByName(sysDB)
	if !ok {
		// Or should we create the database here?
		log.Warn("target database not exist, aborting", zap.String("database", sysDB))
		return nil
	}

	tablesRestored := make([]string, 0, len(originDatabase.Tables))
	for _, table := range originDatabase.Tables {
		tableName := table.Info.Name
		if f.MatchTable(sysDB, tableName.O) {
			if err := rc.replaceTemporaryTableToSystable(ctx, table.Info, db); err != nil {
				log.Warn("error during merging temporary tables into system tables",
					logutil.ShortError(err),
					zap.Stringer("table", tableName),
				)
				return errors.Annotatef(err, "error during merging temporary tables into system tables, table: %s", tableName)
			}
			tablesRestored = append(tablesRestored, tableName.L)
		}
	}
	if err := rc.afterSystemTablesReplaced(ctx, sysDB, tablesRestored); err != nil {
		return errors.Annotate(err, "error during extra works after system tables replaced")
	}
	return nil
}

// database is a record of a database.
// For fast querying whether a table exists and the temporary database of it.
type database struct {
	ExistingTables map[string]*model.TableInfo
	Name           model.CIStr
	TemporaryName  model.CIStr
}

// getDatabaseByName make a record of a database from info schema by its name.
func (rc *Client) getDatabaseByName(name string) (*database, bool) {
	infoSchema := rc.dom.InfoSchema()
	schema, ok := infoSchema.SchemaByName(model.NewCIStr(name))
	if !ok {
		return nil, false
	}
	db := &database{
		ExistingTables: map[string]*model.TableInfo{},
		Name:           model.NewCIStr(name),
		TemporaryName:  utils.TemporaryDBName(name),
	}
	for _, t := range schema.Tables {
		db.ExistingTables[t.Name.L] = t
	}
	return db, true
}

// afterSystemTablesReplaced do some extra work for special system tables.
// e.g. after inserting to the table mysql.user, we must execute `FLUSH PRIVILEGES` to allow it take effect.
func (rc *Client) afterSystemTablesReplaced(ctx context.Context, db string, tables []string) error {
	if db != mysql.SystemDB {
		return nil
	}

	var err error
	for _, table := range tables {
		if table == "user" {
			if serr := rc.dom.NotifyUpdatePrivilege(); serr != nil {
				log.Warn("failed to flush privileges, please manually execute `FLUSH PRIVILEGES`")
				err = multierr.Append(err, berrors.ErrUnknown.Wrap(serr).GenWithStack("failed to flush privileges"))
			} else {
				log.Info("privilege system table restored, please reconnect to make it effective")
			}
		} else if table == "bind_info" {
			if serr := rc.db.se.Execute(ctx, bindinfo.StmtRemoveDuplicatedPseudoBinding); serr != nil {
				log.Warn("failed to delete duplicated pseudo binding", zap.Error(serr))
				err = multierr.Append(err,
					berrors.ErrUnknown.Wrap(serr).GenWithStack("failed to delete duplicated pseudo binding %s", bindinfo.StmtRemoveDuplicatedPseudoBinding))
			} else {
				log.Info("success to remove duplicated pseudo binding")
			}
		}
	}
	return err
}

// replaceTemporaryTableToSystable replaces the temporary table to real system table.
func (rc *Client) replaceTemporaryTableToSystable(ctx context.Context, ti *model.TableInfo, db *database) error {
	dbName := db.Name.L
	tableName := ti.Name.L
	execSQL := func(sql string) error {
		// SQLs here only contain table name and database name, seems it is no need to redact them.
		if err := rc.db.se.Execute(ctx, sql); err != nil {
			log.Warn("failed to execute SQL restore system database",
				zap.String("table", tableName),
				zap.Stringer("database", db.Name),
				zap.String("sql", sql),
				zap.Error(err),
			)
			return berrors.ErrUnknown.Wrap(err).GenWithStack("failed to execute %s", sql)
		}
		log.Info("successfully restore system database",
			zap.String("table", tableName),
			zap.Stringer("database", db.Name),
			zap.String("sql", sql),
		)
		return nil
	}

	// The newly created tables have different table IDs with original tables,
	// 	hence the old statistics are invalid.
	//
	// TODO:
	// 	1   ) Rewrite the table IDs via `UPDATE _temporary_mysql.stats_xxx SET table_id = new_table_id WHERE table_id = old_table_id`
	//		BEFORE replacing into and then execute `rc.statsHandler.Update(rc.dom.InfoSchema())`.
	//  1.5 ) (Optional) The UPDATE statement sometimes costs, the whole system tables restore step can be place into the restore pipeline.
	//  2   ) Deprecate the origin interface for backing up statistics.
	if isStatsTable(dbName, tableName) {
		return nil
	}

	if isUnrecoverableTable(dbName, tableName) {
		return nil
	}

	// Currently, we don't support restore resource group metadata, so we need to
	// remove the resource group related metadata in mysql.user.
	// TODO: this function should be removed when we support backup and restore
	// resource group.
	if dbName == mysql.SystemDB && tableName == sysUserTableName {
		sql := fmt.Sprintf("UPDATE %s SET User_attributes = JSON_REMOVE(User_attributes, '$.resource_group');",
			utils.EncloseDBAndTable(db.TemporaryName.L, sysUserTableName))
		if err := execSQL(sql); err != nil {
			// FIXME: find a better way to check the error or we should check the version here instead.
			if !strings.Contains(err.Error(), "Unknown column 'User_attributes' in 'field list'") {
				return err
			}
			log.Warn("remove resource group meta failed, please ensure target cluster is newer than v6.6.0", logutil.ShortError(err))
		}
	}

	if db.ExistingTables[tableName] != nil {
		log.Info("replace into existing table",
			zap.String("table", tableName),
			zap.Stringer("schema", db.Name))
		if rc.privilegeTableRowsCollateCompatibility {
			if err := rc.checkPrivilegeTableRowsCollateCompatibility(ctx, dbName, tableName, ti, db.ExistingTables[tableName]); err != nil {
				return err
			}
		}
		// target column order may different with source cluster
		columnNames := make([]string, 0, len(ti.Columns))
		for _, col := range ti.Columns {
			columnNames = append(columnNames, utils.EncloseName(col.Name.L))
		}
		colListStr := strings.Join(columnNames, ",")
		replaceIntoSQL := fmt.Sprintf("REPLACE INTO %s(%s) SELECT %s FROM %s;",
			utils.EncloseDBAndTable(db.Name.L, tableName),
			colListStr, colListStr,
			utils.EncloseDBAndTable(db.TemporaryName.L, tableName))
		return execSQL(replaceIntoSQL)
	}

	renameSQL := fmt.Sprintf("RENAME TABLE %s TO %s;",
		utils.EncloseDBAndTable(db.TemporaryName.L, tableName),
		utils.EncloseDBAndTable(db.Name.L, tableName),
	)
	return execSQL(renameSQL)
}

func (rc *Client) cleanTemporaryDatabase(ctx context.Context, originDB string) {
	database := utils.TemporaryDBName(originDB)
	log.Debug("dropping temporary database", zap.Stringer("database", database))
	sql := fmt.Sprintf("DROP DATABASE IF EXISTS %s", utils.EncloseName(database.L))
	if err := rc.db.se.Execute(ctx, sql); err != nil {
		logutil.WarnTerm("failed to drop temporary database, it should be dropped manually",
			zap.Stringer("database", database),
			logutil.ShortError(err),
		)
	}
}

func checkSysTableColumnCollateCompatibility(dbNameL, tableNameL, columnNameL, upstreamCollate, downstreamCollate string) bool {
	if upstreamCollate != "utf8mb4_bin" || downstreamCollate != "utf8mb4_general_ci" {
		return false
	}
	collateCompatibilityTableMap, exists := collateCompatibilityTables[dbNameL]
	if !exists {
		return false
	}
	collateCompatibilityColumnMap, exists := collateCompatibilityTableMap[tableNameL]
	if !exists {
		return false
	}
	_, exists = collateCompatibilityColumnMap.columns[columnNameL]
	return exists
}

func (rc *Client) checkPrivilegeTableRowsCollateCompatibility(
	ctx context.Context,
	dbNameL, tableNameL string,
	upstreamTable, downstreamTable *model.TableInfo,
) error {
	collateCompatibilityTableMap, exists := collateCompatibilityTables[dbNameL]
	if !exists {
		return nil
	}
	collateCompatibilityColumnMap, exists := collateCompatibilityTableMap[tableNameL]
	if !exists {
		return nil
	}
	colCount := 0
	for _, col := range upstreamTable.Columns {
		if _, exists := collateCompatibilityColumnMap.columns[col.Name.L]; exists {
			if col.GetCollate() != "utf8mb4_bin" && col.GetCollate() != "utf8mb4_general_ci" {
				return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
					"incompatible column collate, upstream table %s.%s column %s collate is %s but should be utf8mb4_bin or utf8mb4_general_ci",
					dbNameL, tableNameL, col.Name.L, col.GetCollate())
			}
			colCount += 1
		}
	}
	if colCount != len(collateCompatibilityColumnMap.columns) {
		return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
			"incompatible column collate, upstream table %s.%s has only %d compatible columns",
			dbNameL, tableNameL, colCount)
	}
	colCount = 0
	for _, col := range downstreamTable.Columns {
		if _, exists := collateCompatibilityColumnMap.columns[col.Name.L]; exists {
			if col.GetCollate() != "utf8mb4_general_ci" {
				return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
					"incompatible column collate, downstream table %s.%s column %s collate is %s but should be utf8mb4_general_ci",
					dbNameL, tableNameL, col.Name.L, col.GetCollate())
			}
			colCount += 1
		}
	}
	if colCount != len(collateCompatibilityColumnMap.columns) {
		return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
			"incompatible column collate, downstream table %s.%s has only %d compatible columns",
			dbNameL, tableNameL, colCount)
	}
	ectx := rc.db.se.GetSessionCtx().GetRestrictedSQLExecutor()
	rows, _, err := ectx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		collateCompatibilityColumnMap.upstreamCollateSQL,
	)
	if err != nil {
		return errors.Annotatef(err, "failed to get the count of privilege rows")
	}
	if len(rows) == 0 {
		return errors.Errorf("failed to get the count of privilege rows")
	}
	upstreamCount := rows[0].GetInt64(0)
	rows, _, err = ectx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		collateCompatibilityColumnMap.downstreamCollateSQL,
	)
	if err != nil {
		return errors.Annotatef(err, "failed to get the count of privilege rows")
	}
	if len(rows) == 0 {
		return errors.Errorf("failed to get the count of privilege rows")
	}
	downstreamCount := rows[0].GetInt64(0)
	if upstreamCount != downstreamCount {
		return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
			"there are duplicated privilege rows with collate utf8mb4_general_ci [upstream count %d != downstream count %d]",
			upstreamCount, downstreamCount)
	}
	return nil
}
