// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package snapclient

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const (
	sysUserTableName = "user"
)

var planPeplayerTables = map[string]map[string]struct{}{
	"mysql": {
		"plan_replayer_status": {},
		"plan_replayer_task":   {},
	},
}

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
		"column_stats_usage": {},
	},
}

var renameableSysTables = map[string]map[string]struct{}{
	"mysql": {
		"bind_info":     {},
		"user":          {},
		"db":            {},
		"tables_priv":   {},
		"columns_priv":  {},
		"default_roles": {},
		"role_edges":    {},
		"global_priv":   {},
		"global_grants": {},
	},
}

// tables in this map is restored when fullClusterRestore=true
var sysPrivilegeTableMap = map[string]string{
	"user":          "(user = '%s' and host = '%%')",       // since v1.0.0
	"db":            "(user = '%s' and host = '%%')",       // since v1.0.0
	"tables_priv":   "(user = '%s' and host = '%%')",       // since v1.0.0
	"columns_priv":  "(user = '%s' and host = '%%')",       // since v1.0.0
	"default_roles": "(user = '%s' and host = '%%')",       // since v3.0.0
	"role_edges":    "(to_user = '%s' and to_host = '%%')", // since v3.0.0
	"global_priv":   "(user = '%s' and host = '%%')",       // since v3.0.8
	"global_grants": "(user = '%s' and host = '%%')",       // since v5.0.3
}

var unRecoverableTable = map[string]map[string]struct{}{
	"mysql": {
		// some variables in tidb (e.g. gc_safe_point) cannot be recovered.
		"tidb":                             {},
		"global_variables":                 {},
		"capture_plan_baselines_blacklist": {},
		// GET_LOCK() or IS_USED_LOCK() try to insert a lock into the table in a pessimistic transaction but finally rollback.
		// Therefore actually the table is empty.
		"advisory_locks": {},
		// Table ID is recorded in the column `job_info` so that the table cannot be recovered simply.
		"analyze_jobs": {},
		// Table ID is recorded in the column `table_id` so that the table cannot be recovered simply.
		"analyze_options": {},
		// Distributed eXecution Framework
		// Records the tidb node information, no need to recovered.
		"dist_framework_meta":             {},
		"tidb_global_task":                {},
		"tidb_global_task_history":        {},
		"tidb_background_subtask":         {},
		"tidb_background_subtask_history": {},
		// DDL internal system tables.
		"tidb_ddl_history": {},
		"tidb_ddl_job":     {},
		"tidb_ddl_reorg":   {},
		// Table ID is recorded in the column `schema_change` so that the table cannot be recovered simply.
		"tidb_ddl_notifier": {},
		// v7.2.0. Based on Distributed eXecution Framework, records running import jobs.
		"tidb_import_jobs": {},

		"help_topic": {},
		// records the RU for each resource group temporary, no need to recovered.
		"request_unit_by_group": {},
		// load the table data into the memory.
		"table_cache_meta": {},

		// TiDB runaway internal information.
		"tidb_runaway_queries":    {},
		"tidb_runaway_watch":      {},
		"tidb_runaway_watch_done": {},

		// TiDB internal ttl information.
		"tidb_ttl_job_history":  {},
		"tidb_ttl_table_status": {},
		"tidb_ttl_task":         {},

		// TiDB internal timers.
		"tidb_timers": {},

		// gc info don't need to recover.
		"gc_delete_range":       {},
		"gc_delete_range_done":  {},
		"index_advisor_results": {},

		// TiDB internal system table to synchronize metadata locks across nodes.
		"tidb_mdl_info": {},
		// replace into view is not supported now
		"tidb_mdl_view": {},

		"tidb_pitr_id_map":      {},
		"tidb_restore_registry": {},
	},
	"sys": {
		// replace into view is not supported now
		"schema_unused_indexes": {},
	},
}

type checkPrivilegeTableRowsCollateCompatiblitySQLPair struct {
	upstreamCollateSQL   string
	downstreamCollateSQL string
	columns              map[string]struct{}
}

var collateCompatibilityTables = map[string]map[string]checkPrivilegeTableRowsCollateCompatiblitySQLPair{
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

var unRecoverableSchema = map[string]struct{}{
	mysql.WorkloadSchema: {},
}

type SchemaVersionPairT struct {
	UpstreamVersionMajor   int64
	UpstreamVersionMinor   int64
	DownstreamVersionMajor int64
	DownstreamVersionMinor int64
}

func (t *SchemaVersionPairT) UpstreamVersion() string {
	return fmt.Sprintf("%d.%d", t.UpstreamVersionMajor, t.UpstreamVersionMinor)
}

func (t *SchemaVersionPairT) DownstreamVersion() string {
	return fmt.Sprintf("%d.%d", t.DownstreamVersionMajor, t.DownstreamVersionMinor)
}

type schemaUpdateSQL struct {
	schemaName string
	tableName  string
	sql        string
}

type versionSchemaUpdateSQL struct {
	versionMajor int64
	versionMinor int64
	updateSQLs   []schemaUpdateSQL
}

var upgradeStatsTableSchemaList = []versionSchemaUpdateSQL{
	// upgrade from v8.1.0 to v8.5.0, update stats table schema
	{versionMajor: 8, versionMinor: 1, updateSQLs: []schemaUpdateSQL{
		{
			schemaName: "mysql",
			tableName:  "stats_meta",
			sql:        "ALTER TABLE __TiDB_BR_Temporary_mysql.stats_meta ADD COLUMN IF NOT EXISTS last_stats_histograms_version bigint unsigned DEFAULT NULL",
		},
	}},
	// TODO: add new update here if version > 8.1.0
}

var downgradeStatsTableSchemaList = []versionSchemaUpdateSQL{
	// TODO: add new update here if version > 8.5.0
	// downgrade from v8.5.0 to v8.1.0, update stats table schema
	{versionMajor: 8, versionMinor: 5, updateSQLs: []schemaUpdateSQL{
		{
			schemaName: "mysql",
			tableName:  "stats_meta",
			sql:        "ALTER TABLE __TiDB_BR_Temporary_mysql.stats_meta DROP COLUMN IF EXISTS last_stats_histograms_version",
		},
	}},
}

func upgradeStatsTableSchema(
	ctx context.Context,
	renamedTables map[string]map[string]struct{},
	fromMajor, fromMinor, toMajor, toMinor int64,
	execution func(context.Context, string) error,
) error {
	for _, updateSQL := range upgradeStatsTableSchemaList {
		if fromMajor > updateSQL.versionMajor || (fromMajor == updateSQL.versionMajor && fromMinor > updateSQL.versionMinor) {
			continue
		}
		if toMajor < updateSQL.versionMajor || (toMajor == updateSQL.versionMajor && toMinor <= updateSQL.versionMinor) {
			break
		}
		for _, sql := range updateSQL.updateSQLs {
			if tables, ok := renamedTables[sql.schemaName]; ok {
				if _, ok := tables[sql.tableName]; ok {
					if err := execution(ctx, sql.sql); err != nil {
						return errors.Trace(err)
					}
				}
			}
		}
	}
	return nil
}

func downgradeStatsTableSchema(
	ctx context.Context,
	statisticTables map[string]map[string]struct{},
	fromMajor, fromMinor, toMajor, toMinor int64,
	execution func(context.Context, string) error,
) error {
	for _, updateSQL := range downgradeStatsTableSchemaList {
		if fromMajor < updateSQL.versionMajor || (fromMajor == updateSQL.versionMajor && fromMinor < updateSQL.versionMinor) {
			continue
		}
		if toMajor > updateSQL.versionMajor || (toMajor == updateSQL.versionMajor && toMinor >= updateSQL.versionMinor) {
			break
		}
		for _, sql := range updateSQL.updateSQLs {
			if tables, ok := statisticTables[sql.schemaName]; ok {
				if _, ok := tables[sql.tableName]; ok {
					if err := execution(ctx, sql.sql); err != nil {
						return errors.Trace(err)
					}
				}
			}
		}
	}
	return nil
}

func updateStatsTableSchema(
	ctx context.Context,
	renamedTables map[string]map[string]struct{},
	versionPair SchemaVersionPairT,
	execution func(context.Context, string) error,
) error {
	if versionPair.DownstreamVersionMajor == versionPair.UpstreamVersionMajor &&
		versionPair.DownstreamVersionMinor == versionPair.UpstreamVersionMinor {
		return nil
	}
	if versionPair.DownstreamVersionMajor > versionPair.UpstreamVersionMajor ||
		(versionPair.DownstreamVersionMajor == versionPair.UpstreamVersionMajor &&
			versionPair.DownstreamVersionMinor > versionPair.UpstreamVersionMinor) {
		return upgradeStatsTableSchema(ctx, renamedTables,
			versionPair.UpstreamVersionMajor, versionPair.UpstreamVersionMinor,
			versionPair.DownstreamVersionMajor, versionPair.DownstreamVersionMinor,
			execution,
		)
	}
	return downgradeStatsTableSchema(ctx, renamedTables,
		versionPair.UpstreamVersionMajor, versionPair.UpstreamVersionMinor,
		versionPair.DownstreamVersionMajor, versionPair.DownstreamVersionMinor,
		execution,
	)
}

func notifyUpdateAllUsersPrivilege(renamedTables map[string]map[string]struct{}, notifier func() error) error {
	for dbName, renamedTable := range renamedTables {
		if dbName != mysql.SystemDB {
			continue
		}
		for tableName := range renamedTable {
			if _, exists := sysPrivilegeTableMap[tableName]; exists {
				if err := notifier(); err != nil {
					log.Warn("failed to flush privileges, please manually execute `FLUSH PRIVILEGES`")
					return berrors.ErrUnknown.Wrap(err).GenWithStack("failed to flush privileges")
				}
				return nil
			}
		}
	}
	return nil
}

func isUnrecoverableTable(schemaName string, tableName string) bool {
	if _, ok := unRecoverableSchema[schemaName]; ok {
		return true
	}
	tableMap, ok := unRecoverableTable[schemaName]
	if !ok {
		return false
	}
	_, ok = tableMap[tableName]
	return ok
}

func IsStatsTemporaryTable(tempSchemaName, tableName string) bool {
	_, ok := GetDBNameIfStatsTemporaryTable(tempSchemaName, tableName)
	return ok
}

func GetDBNameIfStatsTemporaryTable(tempSchemaName, tableName string) (string, bool) {
	if dbName, ok := utils.StripTempDBPrefix(tempSchemaName); ok && isStatsTable(dbName, tableName) {
		return dbName, true
	}
	return "", false
}

func IsRenameableSysTemporaryTable(tempSchemaName, tableName string) bool {
	_, ok := GetDBNameIfRenameableSysTemporaryTable(tempSchemaName, tableName)
	return ok
}

func GetDBNameIfRenameableSysTemporaryTable(tempSchemaName, tableName string) (string, bool) {
	if dbName, ok := utils.StripTempDBPrefix(tempSchemaName); ok && isRenameableSysTable(dbName, tableName) {
		return dbName, true
	}
	return "", false
}

type TemporaryTableChecker struct {
	loadStatsPhysical    bool
	loadSysTablePhysical bool
}

func (c *TemporaryTableChecker) CheckTemporaryTables(tempSchemaName, tableName string) (string, bool) {
	if c.loadStatsPhysical {
		if dbName, ok := GetDBNameIfStatsTemporaryTable(tempSchemaName, tableName); ok {
			return dbName, true
		}
	}
	if c.loadSysTablePhysical {
		if dbName, ok := GetDBNameIfRenameableSysTemporaryTable(tempSchemaName, tableName); ok {
			return dbName, true
		}
	}
	return "", false
}

func GenerateMoveRenamedTableSQLPair(restoreTS uint64, statisticTables map[string]map[string]struct{}) string {
	renameBuffer := make([]string, 0, 32)
	for dbName, tableNames := range statisticTables {
		for tableName := range tableNames {
			renameToTemp := fmt.Sprintf("%s.%s TO %s.%s_deleted_%d", dbName, tableName, utils.TemporaryDBName(dbName), tableName, restoreTS)
			renameFromTemp := fmt.Sprintf("%s.%s TO %s.%s", utils.TemporaryDBName(dbName), tableName, dbName, tableName)
			renameBuffer = append(renameBuffer, renameToTemp, renameFromTemp)
		}
	}
	return fmt.Sprintf("RENAME TABLE %s", strings.Join(renameBuffer, ","))
}

func isStatsTable(schemaName string, tableName string) bool {
	tableMap, ok := statsTables[schemaName]
	if !ok {
		return false
	}
	_, ok = tableMap[tableName]
	return ok
}

func isRenameableSysTable(schemaName string, tableName string) bool {
	tableMap, ok := renameableSysTables[schemaName]
	if !ok {
		return false
	}
	_, ok = tableMap[tableName]
	return ok
}

func isPlanReplayerTables(schemaName string, tableName string) bool {
	tableMap, ok := planPeplayerTables[schemaName]
	if !ok {
		return false
	}
	_, ok = tableMap[tableName]
	return ok
}

func removeUserResourceGroup(ctx context.Context, dbName string, execSQL func(context.Context, string) error) error {
	sql := fmt.Sprintf("UPDATE %s SET User_attributes = JSON_REMOVE(User_attributes, '$.resource_group');",
		utils.EncloseDBAndTable(dbName, sysUserTableName))
	if err := execSQL(ctx, sql); err != nil {
		// FIXME: find a better way to check the error or we should check the version here instead.
		if !strings.Contains(err.Error(), "Unknown column 'User_attributes' in 'field list'") {
			return err
		}
		log.Warn("remove resource group meta failed, please ensure target cluster is newer than v6.6.0", logutil.ShortError(err))
	}
	return nil
}

// RestoreSystemSchemas restores the system schema(i.e. the `mysql` schema).
// Detail see https://github.com/pingcap/br/issues/679#issuecomment-762592254.
func (rc *SnapClient) RestoreSystemSchemas(ctx context.Context, f filter.Filter, loadSysTablePhysical bool) (rerr error) {
	sysDBs := []string{mysql.SystemDB, mysql.SysDB, mysql.WorkloadSchema}
	for _, sysDB := range sysDBs {
		err := rc.restoreSystemSchema(ctx, f, sysDB, loadSysTablePhysical)
		if err != nil {
			return err
		}
	}
	return nil
}

// restoreSystemSchema restores a system schema(i.e. the `mysql` or `sys` schema).
// Detail see https://github.com/pingcap/br/issues/679#issuecomment-762592254.
func (rc *SnapClient) restoreSystemSchema(ctx context.Context, f filter.Filter, sysDB string, loadSysTablePhysical bool) (rerr error) {
	temporaryDB := utils.TemporaryDBName(sysDB)
	defer func() {
		// Don't clean the temporary database for next restore with checkpoint.
		if rerr == nil {
			rc.cleanTemporaryDatabase(ctx, sysDB)
		}
	}()

	if !f.MatchSchema(sysDB) || !rc.withSysTable {
		log.Info("system database filtered out", zap.String("database", sysDB))
		return nil
	}
	originDatabase, ok := rc.databases[temporaryDB.O]
	if !ok {
		log.Info("system database not backed up, skipping", zap.String("database", sysDB))
		return nil
	}
	db, ok, err := rc.getSystemDatabaseByName(ctx, sysDB)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		// Or should we create the database here?
		log.Warn("target database not exist, aborting", zap.String("database", sysDB))
		return nil
	}

	tablesRestored := make([]string, 0, len(originDatabase.Tables))
	for _, table := range originDatabase.Tables {
		tableName := table.Info.Name
		if f.MatchTable(sysDB, tableName.O) {
			if loadSysTablePhysical && isRenameableSysTable(sysDB, tableName.O) {
				continue
			}
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
	Name           ast.CIStr
	TemporaryName  ast.CIStr
}

// getSystemDatabaseByName make a record of a system database, such as mysql and sys, from info schema by its name.
func (rc *SnapClient) getSystemDatabaseByName(ctx context.Context, name string) (*database, bool, error) {
	infoSchema := rc.dom.InfoSchema()
	schema, ok := infoSchema.SchemaByName(ast.NewCIStr(name))
	if !ok {
		return nil, false, nil
	}
	db := &database{
		ExistingTables: map[string]*model.TableInfo{},
		Name:           ast.NewCIStr(name),
		TemporaryName:  utils.TemporaryDBName(name),
	}
	// It's OK to get all the tables from system tables.
	tableInfos, err := infoSchema.SchemaTableInfos(ctx, schema.Name)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	for _, tbl := range tableInfos {
		db.ExistingTables[tbl.Name.L] = tbl
	}
	return db, true, nil
}

// afterSystemTablesReplaced do some extra work for special system tables.
// e.g. after inserting to the table mysql.user, we must execute `FLUSH PRIVILEGES` to allow it take effect.
func (rc *SnapClient) afterSystemTablesReplaced(ctx context.Context, db string, tables []string) error {
	if db != mysql.SystemDB {
		return nil
	}

	var err error
	for _, table := range tables {
		if table == "user" {
			if serr := rc.dom.NotifyUpdateAllUsersPrivilege(); serr != nil {
				log.Warn("failed to flush privileges, please manually execute `FLUSH PRIVILEGES`")
				err = multierr.Append(err, berrors.ErrUnknown.Wrap(serr).GenWithStack("failed to flush privileges"))
			} else {
				log.Info("privilege system table restored, please reconnect to make it effective")
			}
		} else if table == "bind_info" {
			if serr := rc.db.Session().Execute(ctx, bindinfo.StmtRemoveDuplicatedPseudoBinding); serr != nil {
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
func (rc *SnapClient) replaceTemporaryTableToSystable(ctx context.Context, ti *model.TableInfo, db *database) error {
	dbName := db.Name.L
	tableName := ti.Name.L
	execSQL := func(ctx context.Context, sql string) error {
		// SQLs here only contain table name and database name, seems it is no need to redact them.
		if err := rc.db.Session().Execute(ctx, sql); err != nil {
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

	if isPlanReplayerTables(dbName, tableName) {
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
		if err := removeUserResourceGroup(ctx, db.TemporaryName.L, execSQL); err != nil {
			return err
		}
	}

	if db.ExistingTables[tableName] != nil {
		log.Info("replace into existing table",
			zap.String("table", tableName),
			zap.Stringer("schema", db.Name))
		if rc.checkPrivilegeTableRowsCollateCompatiblity {
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
		return execSQL(ctx, replaceIntoSQL)
	}

	renameSQL := fmt.Sprintf("RENAME TABLE %s TO %s;",
		utils.EncloseDBAndTable(db.TemporaryName.L, tableName),
		utils.EncloseDBAndTable(db.Name.L, tableName),
	)
	return execSQL(ctx, renameSQL)
}

func (rc *SnapClient) cleanTemporaryDatabase(ctx context.Context, originDB string) {
	database := utils.TemporaryDBName(originDB)
	log.Debug("dropping temporary database", zap.Stringer("database", database))
	sql := fmt.Sprintf("DROP DATABASE IF EXISTS %s", utils.EncloseName(database.L))
	if err := rc.db.Session().Execute(ctx, sql); err != nil {
		logutil.WarnTerm("failed to drop temporary database, it should be dropped manually",
			zap.Stringer("database", database),
			logutil.ShortError(err),
		)
	}
}

func CheckSysTableCompatibility(dom *domain.Domain, tables []*metautil.Table, collationCheck bool) (canLoadSysTablePhysical bool, err error) {
	log.Info("checking target cluster system table compatibility with backed up data")
	canLoadSysTablePhysical = true
	privilegeTablesInBackup := make([]*metautil.Table, 0)
	for _, table := range tables {
		decodedSysDBName, ok := utils.GetSysDBCIStrName(table.DB.Name)
		if ok && decodedSysDBName.L == mysql.SystemDB && sysPrivilegeTableMap[table.Info.Name.L] != "" {
			privilegeTablesInBackup = append(privilegeTablesInBackup, table)
		}
	}
	sysDB := ast.NewCIStr(mysql.SystemDB)
	for _, table := range privilegeTablesInBackup {
		ti, err := restore.GetTableSchema(dom, sysDB, table.Info.Name)
		if err != nil {
			log.Error("missing table on target cluster", zap.Stringer("table", table.Info.Name))
			return false, errors.Annotate(berrors.ErrRestoreIncompatibleSys, "missed system table: "+table.Info.Name.O)
		}
		backupTi := table.Info
		// skip checking the number of columns in mysql.user table,
		// because higher versions of TiDB may add new columns.
		if len(ti.Columns) != len(backupTi.Columns) && backupTi.Name.L != sysUserTableName {
			log.Error("column count mismatch",
				zap.Stringer("table", table.Info.Name),
				zap.Int("col in cluster", len(ti.Columns)),
				zap.Int("col in backup", len(backupTi.Columns)))
			return false, errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
				"column count mismatch, table: %s, col in cluster: %d, col in backup: %d",
				table.Info.Name.O, len(ti.Columns), len(backupTi.Columns))
		}
		backupColMap := make(map[string]*model.ColumnInfo)
		for i := range backupTi.Columns {
			col := backupTi.Columns[i]
			backupColMap[col.Name.L] = col
		}
		// order can be different but type must compatible
		for i := range ti.Columns {
			col := ti.Columns[i]
			backupCol := backupColMap[col.Name.L]
			if backupCol == nil {
				// skip when the backed up mysql.user table is missing columns.
				if backupTi.Name.L == sysUserTableName {
					log.Warn("missing column in backup data",
						zap.Stringer("table", table.Info.Name),
						zap.String("col", fmt.Sprintf("%s %s", col.Name, col.FieldType.String())))
					continue
				}
				log.Error("missing column in backup data",
					zap.Stringer("table", table.Info.Name),
					zap.String("col", fmt.Sprintf("%s %s", col.Name, col.FieldType.String())))
				return false, errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
					"missing column in backup data, table: %s, col: %s %s",
					table.Info.Name.O,
					col.Name, col.FieldType.String())
			}
			typeEq, collateEq := utils.IsTypeCompatible(backupCol.FieldType, col.FieldType)
			canLoadSysTablePhysical = canLoadSysTablePhysical && collateEq && typeEq
			collateCompatible := collateEq
			if typeEq && (!collateEq && collationCheck) {
				collateCompatible = checkSysTableColumnCollateCompatibility(mysql.SystemDB, table.Info.Name.L, col.Name.L, backupCol.GetCollate(), col.GetCollate())
			}
			if !(typeEq && collateCompatible) {
				log.Error("incompatible column",
					zap.Stringer("table", table.Info.Name),
					zap.String("col in cluster", fmt.Sprintf("%s %s", col.Name, col.FieldType.String())),
					zap.String("col in backup", fmt.Sprintf("%s %s", backupCol.Name, backupCol.FieldType.String())))
				return false, errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
					"incompatible column, table: %s, col in cluster: %s %s, col in backup: %s %s",
					table.Info.Name.O,
					col.Name, col.FieldType.String(),
					backupCol.Name, backupCol.FieldType.String())
			}
		}

		if backupTi.Name.L == sysUserTableName {
			// check whether the columns of table in cluster are less than the backup data
			clusterColMap := make(map[string]*model.ColumnInfo)
			for i := range ti.Columns {
				col := ti.Columns[i]
				clusterColMap[col.Name.L] = col
			}
			// order can be different
			for i := range backupTi.Columns {
				col := backupTi.Columns[i]
				clusterCol := clusterColMap[col.Name.L]
				if clusterCol == nil {
					log.Error("missing column in cluster data",
						zap.Stringer("table", table.Info.Name),
						zap.String("col", fmt.Sprintf("%s %s", col.Name, col.FieldType.String())))
					return false, errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
						"missing column in cluster data, table: %s, col: %s %s",
						table.Info.Name.O,
						col.Name, col.FieldType.String())
				}
			}
		}
	}
	return canLoadSysTablePhysical, nil
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

func (rc *SnapClient) checkPrivilegeTableRowsCollateCompatibility(
	ctx context.Context,
	dbNameL, tableNameL string,
	upstreamTable, downstreamTable *model.TableInfo,
) error {
	collateCompatiblityTableMap, exists := collateCompatibilityTables[dbNameL]
	if !exists {
		return nil
	}
	collateCompatibilityColumnMap, exists := collateCompatiblityTableMap[tableNameL]
	if !exists {
		return nil
	}
	colCount := 0
	for _, col := range upstreamTable.Columns {
		if _, exists := collateCompatibilityColumnMap.columns[col.Name.L]; exists {
			if col.GetCollate() != "utf8mb4_bin" && col.GetCollate() != "utf8mb4_general_ci" {
				return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
					"incompatible column collate, upstream table %s.%s column %s collate is %s but should be utf8mb4_bin",
					dbNameL, tableNameL, col.Name.L, col.GetCollate())
			}
			colCount += 1
		}
	}
	if colCount != len(collateCompatibilityColumnMap.columns) {
		return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
			"incompatible column collate, upstream table %s.%s has only %d columns with collate utf8mb4_bin",
			dbNameL, tableNameL, colCount)
	}
	colCount = 0
	for _, col := range downstreamTable.Columns {
		if _, exists := collateCompatibilityColumnMap.columns[col.Name.L]; exists {
			if col.GetCollate() != "utf8mb4_general_ci" {
				return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
					"incompatible column collate, downstream table %s.%s column %s collate should be %s",
					dbNameL, tableNameL, col.Name.L, col.GetCollate())
			}
			colCount += 1
		}
	}
	if colCount != len(collateCompatibilityColumnMap.columns) {
		return errors.Annotatef(berrors.ErrRestoreIncompatibleSys,
			"incompatible column collate, downstream table %s.%s has only %d columns with collate utf8mb4_bin",
			dbNameL, tableNameL, colCount)
	}
	ectx := rc.db.Session().GetSessionCtx().GetRestrictedSQLExecutor()
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
		return errors.Errorf("there are duplicated rows with collate utf8mb4_general_ci [upstream count %d != downstream count %d]",
			upstreamCount, downstreamCount)
	}
	return nil
}
