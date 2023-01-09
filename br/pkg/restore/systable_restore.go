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
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	filter "github.com/pingcap/tidb/util/table-filter"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const (
	rootUser         = "root"
	sysUserTableName = "user"
	cloudAdminUser   = "cloud_admin"
)

var statsTables = map[string]struct{}{
	"stats_buckets":    {},
	"stats_extended":   {},
	"stats_feedback":   {},
	"stats_fm_sketch":  {},
	"stats_histograms": {},
	"stats_meta":       {},
	"stats_top_n":      {},
}

var unRecoverableTable = map[string]struct{}{
	// some variables in tidb (e.g. gc_safe_point) cannot be recovered.
	"tidb":             {},
	"global_variables": {},

	"column_stats_usage":               {},
	"capture_plan_baselines_blacklist": {},
	// gc info don't need to recover.
	"gc_delete_range":      {},
	"gc_delete_range_done": {},

	// schema_index_usage has table id need to be rewrite.
	"schema_index_usage": {},
}

// tables in this map is restored when fullClusterRestore=true
// the value part is the filter in SQL where clause which is used to
// skip clearing or restoring 'cloud_admin'@'%' which is a special
// user on TiDB Cloud
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

func isUnrecoverableTable(tableName string) bool {
	_, ok := unRecoverableTable[tableName]
	return ok
}

func isStatsTable(tableName string) bool {
	_, ok := statsTables[tableName]
	return ok
}

func generateResetSQLs(db *database, resetUsers []string) []string {
	if db.Name.L != mysql.SystemDB {
		return nil
	}
	sqls := make([]string, 0, 10)
	// we only need reset root password once
	rootReset := false
	for tableName := range db.ExistingTables {
		if sysPrivilegeTableMap[tableName] != "" {
			for _, name := range resetUsers {
				if strings.ToLower(name) == rootUser {
					if !rootReset {
						updateSQL := fmt.Sprintf("UPDATE %s.%s SET authentication_string='',"+
							" Shutdown_priv='Y',"+
							" Config_priv='Y'"+
							" WHERE USER='root' AND Host='%%';",
							db.Name.L, sysUserTableName)
						sqls = append(sqls, updateSQL)
						rootReset = true
					} else {
						continue
					}
				} else {
					/* #nosec G202: SQL string concatenation */
					whereClause := fmt.Sprintf("WHERE "+sysPrivilegeTableMap[tableName], name)
					deleteSQL := fmt.Sprintf("DELETE FROM %s %s;",
						utils.EncloseDBAndTable(db.Name.L, tableName), whereClause)
					sqls = append(sqls, deleteSQL)
				}
			}
		}
	}
	return sqls
}

// ClearSystemUsers is used for volume-snapshot restoration.
// because we can not support restore user in some scenarios, for example in cloud.
// we'd better use this function to drop cloud_admin user after volume-snapshot restore.
func (rc *Client) ClearSystemUsers(ctx context.Context, resetUsers []string) error {
	sysDB := mysql.SystemDB
	db, ok := rc.getDatabaseByName(sysDB)
	if !ok {
		log.Warn("target database not exist, aborting", zap.String("database", sysDB))
		return nil
	}
	execSQL := func(sql string) error {
		// SQLs here only contain table name and database name, seems it is no need to redact them.
		if err := rc.db.se.Execute(ctx, sql); err != nil {
			log.Warn("failed to clear system users",
				zap.Stringer("database", db.Name),
				zap.String("sql", sql),
				zap.Error(err),
			)
			return berrors.ErrUnknown.Wrap(err).GenWithStack("failed to execute %s", sql)
		}
		log.Info("successfully clear system users after restoration",
			zap.Stringer("database", db.Name),
			zap.String("sql", sql),
		)
		return nil
	}

	sqls := generateResetSQLs(db, resetUsers)
	for _, sql := range sqls {
		log.Info("reset system user for cloud", zap.String("sql", sql))
		if err := execSQL(sql); err != nil {
			return err
		}
	}
	return nil
}

// RestoreSystemSchemas restores the system schema(i.e. the `mysql` schema).
// Detail see https://github.com/pingcap/br/issues/679#issuecomment-762592254.
func (rc *Client) RestoreSystemSchemas(ctx context.Context, f filter.Filter) {
	sysDB := mysql.SystemDB

	temporaryDB := utils.TemporaryDBName(sysDB)
	defer rc.cleanTemporaryDatabase(ctx, sysDB)

	if !f.MatchSchema(sysDB) || !rc.withSysTable {
		log.Debug("system database filtered out", zap.String("database", sysDB))
		return
	}
	originDatabase, ok := rc.databases[temporaryDB.O]
	if !ok {
		log.Info("system database not backed up, skipping", zap.String("database", sysDB))
		return
	}
	db, ok := rc.getDatabaseByName(sysDB)
	if !ok {
		// Or should we create the database here?
		log.Warn("target database not exist, aborting", zap.String("database", sysDB))
		return
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
			}
			tablesRestored = append(tablesRestored, tableName.L)
		}
	}
	if err := rc.afterSystemTablesReplaced(tablesRestored); err != nil {
		for _, e := range multierr.Errors(err) {
			log.Warn("error during reconfigurating the system tables", zap.String("database", sysDB), logutil.ShortError(e))
		}
	}
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
func (rc *Client) afterSystemTablesReplaced(tables []string) error {
	var err error
	for _, table := range tables {
		switch {
		case table == "user":
			if rc.fullClusterRestore {
				log.Info("privilege system table restored, please reconnect to make it effective")
				err = rc.dom.NotifyUpdatePrivilege()
			} else {
				// to make it compatible with older version
				// todo: should we allow restore system table in non-fresh cluster in later br version?
				// if we don't, we can check it at first place.
				err = multierr.Append(err, errors.Annotatef(berrors.ErrUnsupportedSystemTable,
					"restored user info may not take effect, until you should execute `FLUSH PRIVILEGES` manually"))
			}
		}
	}
	return err
}

// replaceTemporaryTableToSystable replaces the temporary table to real system table.
func (rc *Client) replaceTemporaryTableToSystable(ctx context.Context, ti *model.TableInfo, db *database) error {
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
	if isStatsTable(tableName) {
		return berrors.ErrUnsupportedSystemTable.GenWithStack("restoring stats via `mysql` schema isn't support yet: " +
			"the table ID is out-of-date and may corrupt existing statistics")
	}

	if isUnrecoverableTable(tableName) {
		return berrors.ErrUnsupportedSystemTable.GenWithStack("restoring unsupported `mysql` schema table")
	}

	if db.ExistingTables[tableName] != nil {
		whereNotClause := ""
		if rc.fullClusterRestore && sysPrivilegeTableMap[tableName] != "" {
			// cloud_admin is a special user on tidb cloud, need to skip it.
			/* #nosec G202: SQL string concatenation */
			whereNotClause = fmt.Sprintf("WHERE NOT "+sysPrivilegeTableMap[tableName], cloudAdminUser)
			log.Info("full cluster restore, delete existing data",
				zap.String("table", tableName), zap.Stringer("schema", db.Name))
			deleteSQL := fmt.Sprintf("DELETE FROM %s %s;",
				utils.EncloseDBAndTable(db.Name.L, tableName), whereNotClause)
			if err := execSQL(deleteSQL); err != nil {
				return err
			}
		}
		log.Info("replace into existing table",
			zap.String("table", tableName),
			zap.Stringer("schema", db.Name))
		// target column order may different with source cluster
		columnNames := make([]string, 0, len(ti.Columns))
		for _, col := range ti.Columns {
			columnNames = append(columnNames, utils.EncloseName(col.Name.L))
		}
		colListStr := strings.Join(columnNames, ",")
		replaceIntoSQL := fmt.Sprintf("REPLACE INTO %s(%s) SELECT %s FROM %s %s;",
			utils.EncloseDBAndTable(db.Name.L, tableName),
			colListStr, colListStr,
			utils.EncloseDBAndTable(db.TemporaryName.L, tableName),
			whereNotClause)
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
