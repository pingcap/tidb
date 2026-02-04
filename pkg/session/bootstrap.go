// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	osuser "os/user"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storepkg "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// bootstrapOwnerKey is the key used by ddl owner mutex during boostrap.
var bootstrapOwnerKey = "/tidb/distributeDDLOwnerLock/"

// bootstrap initiates system DB for a store.
func bootstrap(s sessionapi.Session) {
	startTime := time.Now()
	err := InitMDLVariableForBootstrap(s.GetStore())
	if err != nil {
		logutil.BgLogger().Fatal("init metadata lock failed during bootstrap", zap.Error(err))
	}
	dom := domain.GetDomain(s)
	bootLogger := logutil.SampleLoggerFactory(30*time.Second, 1)()
	for {
		b, err := checkBootstrapped(s)
		if err != nil {
			logutil.BgLogger().Fatal("check bootstrap error",
				zap.Error(err))
		}
		// For rolling upgrade, we can't do upgrade only in the owner.
		if b {
			upgrade(s)
			logutil.BgLogger().Info("upgrade successful in bootstrap",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		// To reduce conflict when multiple TiDB-server start at the same time.
		// Actually only one server need to do the bootstrap. So we chose DDL owner to do this.
		if dom.DDL().OwnerManager().IsOwner() {
			doDDLWorks(s)
			doDMLWorks(s)
			runBootstrapSQLFile = true
			logutil.BgLogger().Info("bootstrap successful",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		bootLogger.Info("bootstrap not done yet, waiting for owner to finish")
		time.Sleep(200 * time.Millisecond)
	}
}

const (
	// varTrue is the true value in mysql.TiDB table for boolean columns.
	varTrue = "True"
	// varFalse is the false value in mysql.TiDB table for boolean columns.
	varFalse = "False"
	// The variable name in mysql.TiDB table.
	// It is used for checking if the store is bootstrapped by any TiDB server.
	// If the value is `True`, the store is already bootstrapped by a TiDB server.
	bootstrappedVar = "bootstrapped"
	// The variable name in mysql.TiDB table.
	// It is used for getting the version of the TiDB server which bootstrapped the store.
	tidbServerVersionVar = "tidb_server_version"
	// The variable name in mysql.tidb table and it will be used when we want to know
	// system timezone.
	tidbSystemTZ = "system_tz"
	// TidbNewCollationEnabled The variable name in mysql.tidb table and it will indicate if the new collations are enabled in the TiDB cluster.
	TidbNewCollationEnabled = "new_collation_enabled"
	// The variable name in mysql.tidb table and it records the default value of
	// mem-quota-query when upgrade from v3.0.x to v4.0.9+.
	tidbDefMemoryQuotaQuery = "default_memory_quota_query"
	// The variable name in mysql.tidb table and it records the default value of
	// oom-action when upgrade from v3.0.x to v4.0.11+.
	tidbDefOOMAction = "default_oom_action"
	// The variable name in mysql.tidb table and it records the current DDLTableVersion
	tidbDDLTableVersion = "ddl_table_version"
	// The variable name in mysql.tidb table and it records the cluster id of this cluster
	tidbClusterID = "cluster_id"
)

// DDL owner key's expired time is ManagerSessionTTL seconds, we should wait the time and give more time to have a chance to finish it.
var internalSQLTimeout = owner.ManagerSessionTTL + 15

// whether to run the sql file in bootstrap.
var runBootstrapSQLFile = false

// DisableRunBootstrapSQLFileInTest only used for test
func DisableRunBootstrapSQLFileInTest() {
	if intest.InTest {
		runBootstrapSQLFile = false
	}
}

func checkBootstrapped(s sessionapi.Session) (bool, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	//  Check if system db exists.
	_, err := s.ExecuteInternal(ctx, "USE %n", mysql.SystemDB)
	if err != nil && infoschema.ErrDatabaseNotExists.NotEqual(err) {
		logutil.BgLogger().Fatal("check bootstrap error",
			zap.Error(err))
	}
	// Check bootstrapped variable value in TiDB table.
	sVal, _, err := getTiDBVar(s, bootstrappedVar)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	isBootstrapped := sVal == varTrue
	if isBootstrapped {
		// Make sure that doesn't affect the following operations.
		if err = s.CommitTxn(ctx); err != nil {
			return false, errors.Trace(err)
		}
	}
	return isBootstrapped, nil
}

// getTiDBVar gets variable value from mysql.tidb table.
// Those variables are used by TiDB server.
func getTiDBVar(s sessionapi.Session, name string) (sVal string, isNull bool, e error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	rs, err := s.ExecuteInternal(ctx, `SELECT HIGH_PRIORITY VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME= %?`,
		mysql.SystemDB,
		mysql.TiDBTable,
		name,
	)
	if err != nil {
		return "", true, errors.Trace(err)
	}
	if rs == nil {
		return "", true, errors.New("Wrong number of Recordset")
	}
	defer terror.Call(rs.Close)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	if err != nil || req.NumRows() == 0 {
		return "", true, errors.Trace(err)
	}
	row := req.GetRow(0)
	if row.IsNull(0) {
		return "", true, nil
	}
	return row.GetString(0), false, nil
}

var (
	// SupportUpgradeHTTPOpVer is exported for testing.
	// The minimum version of the upgrade by paused user DDL can be notified through the HTTP API.
	SupportUpgradeHTTPOpVer int64 = version174
)

func acquireLock(store kv.Storage) (func(), error) {
	etcdCli, err := storepkg.NewEtcdCli(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if etcdCli == nil {
		// Special handling for test.
		logutil.BgLogger().Warn("skip acquire ddl owner lock for uni-store")
		return func() {
			// do nothing
		}, nil
	}
	releaseFn, err := owner.AcquireDistributedLock(context.Background(), etcdCli, bootstrapOwnerKey, 10)
	if err != nil {
		if err2 := etcdCli.Close(); err2 != nil {
			logutil.BgLogger().Error("failed to close etcd client", zap.Error(err2))
		}
		return nil, errors.Trace(err)
	}
	return func() {
		releaseFn()
		if err2 := etcdCli.Close(); err2 != nil {
			logutil.BgLogger().Error("failed to close etcd client", zap.Error(err2))
		}
	}, nil
}

// initGlobalVariableIfNotExists initialize a global variable with specific val if it does not exist.
func initGlobalVariableIfNotExists(s sessionapi.Session, name string, val any) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	rows, err := sqlexec.ExecSQL(ctx, s, "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?;", mysql.SystemDB, mysql.GlobalVariablesTable, name)
	terror.MustNil(err)
	if len(rows) != 0 {
		return
	}

	mustExecute(s, "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
		mysql.SystemDB, mysql.GlobalVariablesTable, name, val)
}

func writeOOMAction(s sessionapi.Session) {
	comment := "oom-action is `log` by default in v3.0.x, `cancel` by default in v4.0.11+"
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?`,
		mysql.SystemDB, mysql.TiDBTable, tidbDefOOMAction, vardef.OOMActionLog, comment, vardef.OOMActionLog,
	)
}

// updateBootstrapVer updates bootstrap version variable in mysql.TiDB table.
func updateBootstrapVer(s sessionapi.Session) {
	// Update bootstrap version.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion, currentBootstrapVersion,
	)
}

// getBootstrapVersion gets bootstrap version from mysql.tidb table;
func getBootstrapVersion(s sessionapi.Session) (int64, error) {
	sVal, isNull, err := getTiDBVar(s, tidbServerVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

var systemDatabases = []DatabaseBasicInfo{
	{ID: metadef.SystemDatabaseID, Name: mysql.SystemDB},
	{ID: metadef.SysDatabaseID, Name: mysql.SysDB},
}

// tablesInSystemDatabase contains the definitions of system tables in the mysql
// database, or the system database, except DDL related tables, see ddlTableVersionTables.
var tablesInSystemDatabase = []TableBasicInfo{
	{ID: metadef.UserTableID, Name: "user", SQL: metadef.CreateUserTable},
	{ID: metadef.PasswordHistoryTableID, Name: "password_history", SQL: metadef.CreatePasswordHistoryTable},
	{ID: metadef.GlobalPrivTableID, Name: "global_priv", SQL: metadef.CreateGlobalPrivTable},
	{ID: metadef.DBTableID, Name: "db", SQL: metadef.CreateDBTable},
	{ID: metadef.TablesPrivTableID, Name: "tables_priv", SQL: metadef.CreateTablesPrivTable},
	{ID: metadef.ColumnsPrivTableID, Name: "columns_priv", SQL: metadef.CreateColumnsPrivTable},
	{ID: metadef.GlobalVariablesTableID, Name: "global_variables", SQL: metadef.CreateGlobalVariablesTable},
	{ID: metadef.TiDBTableID, Name: "tidb", SQL: metadef.CreateTiDBTable},
	{ID: metadef.HelpTopicTableID, Name: "help_topic", SQL: metadef.CreateHelpTopicTable},
	{ID: metadef.StatsMetaTableID, Name: "stats_meta", SQL: metadef.CreateStatsMetaTable},
	{ID: metadef.StatsHistogramsTableID, Name: "stats_histograms", SQL: metadef.CreateStatsHistogramsTable},
	{ID: metadef.StatsBucketsTableID, Name: "stats_buckets", SQL: metadef.CreateStatsBucketsTable},
	{ID: metadef.GCDeleteRangeTableID, Name: "gc_delete_range", SQL: metadef.CreateGCDeleteRangeTable},
	{ID: metadef.GCDeleteRangeDoneTableID, Name: "gc_delete_range_done", SQL: metadef.CreateGCDeleteRangeDoneTable},
	{ID: metadef.StatsFeedbackTableID, Name: "stats_feedback", SQL: metadef.CreateStatsFeedbackTable},
	{ID: metadef.RoleEdgesTableID, Name: "role_edges", SQL: metadef.CreateRoleEdgesTable},
	{ID: metadef.DefaultRolesTableID, Name: "default_roles", SQL: metadef.CreateDefaultRolesTable},
	{ID: metadef.BindInfoTableID, Name: "bind_info", SQL: metadef.CreateBindInfoTable},
	{ID: metadef.StatsTopNTableID, Name: "stats_top_n", SQL: metadef.CreateStatsTopNTable},
	{ID: metadef.ExprPushdownBlacklistTableID, Name: "expr_pushdown_blacklist", SQL: metadef.CreateExprPushdownBlacklistTable},
	{ID: metadef.OptRuleBlacklistTableID, Name: "opt_rule_blacklist", SQL: metadef.CreateOptRuleBlacklistTable},
	{ID: metadef.StatsExtendedTableID, Name: "stats_extended", SQL: metadef.CreateStatsExtendedTable},
	{ID: metadef.StatsFMSketchTableID, Name: "stats_fm_sketch", SQL: metadef.CreateStatsFMSketchTable},
	{ID: metadef.GlobalGrantsTableID, Name: "global_grants", SQL: metadef.CreateGlobalGrantsTable},
	{ID: metadef.CapturePlanBaselinesBlacklistTableID, Name: "capture_plan_baselines_blacklist", SQL: metadef.CreateCapturePlanBaselinesBlacklistTable},
	{ID: metadef.ColumnStatsUsageTableID, Name: "column_stats_usage", SQL: metadef.CreateColumnStatsUsageTable},
	{ID: metadef.TableCacheMetaTableID, Name: "table_cache_meta", SQL: metadef.CreateTableCacheMetaTable},
	{ID: metadef.AnalyzeOptionsTableID, Name: "analyze_options", SQL: metadef.CreateAnalyzeOptionsTable},
	{ID: metadef.StatsHistoryTableID, Name: "stats_history", SQL: metadef.CreateStatsHistoryTable},
	{ID: metadef.StatsMetaHistoryTableID, Name: "stats_meta_history", SQL: metadef.CreateStatsMetaHistoryTable},
	{ID: metadef.AnalyzeJobsTableID, Name: "analyze_jobs", SQL: metadef.CreateAnalyzeJobsTable},
	{ID: metadef.AdvisoryLocksTableID, Name: "advisory_locks", SQL: metadef.CreateAdvisoryLocksTable},
	{ID: metadef.PlanReplayerStatusTableID, Name: "plan_replayer_status", SQL: metadef.CreatePlanReplayerStatusTable},
	{ID: metadef.PlanReplayerTaskTableID, Name: "plan_replayer_task", SQL: metadef.CreatePlanReplayerTaskTable},
	{ID: metadef.StatsTableLockedTableID, Name: "stats_table_locked", SQL: metadef.CreateStatsTableLockedTable},
	{ID: metadef.TiDBTTLTableStatusTableID, Name: "tidb_ttl_table_status", SQL: metadef.CreateTiDBTTLTableStatusTable},
	{ID: metadef.TiDBTTLTaskTableID, Name: "tidb_ttl_task", SQL: metadef.CreateTiDBTTLTaskTable},
	{ID: metadef.TiDBTTLJobHistoryTableID, Name: "tidb_ttl_job_history", SQL: metadef.CreateTiDBTTLJobHistoryTable},
	{ID: metadef.TiDBGlobalTaskTableID, Name: "tidb_global_task", SQL: metadef.CreateTiDBGlobalTaskTable},
	{ID: metadef.TiDBGlobalTaskHistoryTableID, Name: "tidb_global_task_history", SQL: metadef.CreateTiDBGlobalTaskHistoryTable},
	{ID: metadef.TiDBImportJobsTableID, Name: "tidb_import_jobs", SQL: metadef.CreateTiDBImportJobsTable},
	{ID: metadef.TiDBRunawayWatchTableID, Name: "tidb_runaway_watch", SQL: metadef.CreateTiDBRunawayWatchTable},
	{ID: metadef.TiDBRunawayQueriesTableID, Name: "tidb_runaway_queries", SQL: metadef.CreateTiDBRunawayQueriesTable},
	{ID: metadef.TiDBTimersTableID, Name: "tidb_timers", SQL: metadef.CreateTiDBTimersTable},
	{ID: metadef.TiDBRunawayWatchDoneTableID, Name: "tidb_runaway_watch_done", SQL: metadef.CreateTiDBRunawayWatchDoneTable},
	{ID: metadef.DistFrameworkMetaTableID, Name: "dist_framework_meta", SQL: metadef.CreateDistFrameworkMetaTable},
	{ID: metadef.RequestUnitByGroupTableID, Name: "request_unit_by_group", SQL: metadef.CreateRequestUnitByGroupTable},
	{ID: metadef.TiDBPITRIDMapTableID, Name: "tidb_pitr_id_map", SQL: metadef.CreateTiDBPITRIDMapTable},
	{ID: metadef.TiDBRestoreRegistryTableID, Name: "tidb_restore_registry", SQL: metadef.CreateTiDBRestoreRegistryTable},
	{ID: metadef.IndexAdvisorResultsTableID, Name: "index_advisor_results", SQL: metadef.CreateIndexAdvisorResultsTable},
	{ID: metadef.TiDBKernelOptionsTableID, Name: "tidb_kernel_options", SQL: metadef.CreateTiDBKernelOptionsTable},
	{ID: metadef.TiDBWorkloadValuesTableID, Name: "tidb_workload_values", SQL: metadef.CreateTiDBWorkloadValuesTable},
	{ID: metadef.TiDBSoftDeleteTableStatusTableID, Name: "tidb_softdelete_table_status", SQL: metadef.CreateTiDBSoftDeleteTableStatusTable},

	// NOTE: if you need to add more tables to 'mysql' database, please also add
	// an entry to versionedBootstrapSchemas, to make sure the table is created
	// correctly in nextgen kennel.
}

type versionedBootstrapSchema struct {
	ver       meta.NextGenBootTableVersion
	databases []DatabaseBasicInfo
}

const (
	// 52 is the number of system tables as we do this change.
	// as tablesInSystemDatabase is shared with classic kernel, it's simple to
	// use a slice to hold all system tables in classic kernel. but in nextgen,
	// we need to make those tables versioned, as we don't create system tables
	// through DDL, we need this version to avoid create tables again.
	// if we add more system tables later, we should increase the version, and
	// add another versionedBootstrapSchema entry.
	tableCountInFirstVerOnNextGen = 52
	// added tidb_softdelete_table_status
	tableCountInSecondVerOnNextGen = 53
)

// used in nextgen, to create system tables directly through meta kv, without
// going through DDL, so we can create them with reversed ID range.
var versionedBootstrapSchemas = []versionedBootstrapSchema{
	{ver: meta.BaseNextGenBootTableVersion, databases: []DatabaseBasicInfo{
		{ID: metadef.SystemDatabaseID, Name: mysql.SystemDB, Tables: tablesInSystemDatabase[:tableCountInFirstVerOnNextGen]},
		{ID: metadef.SysDatabaseID, Name: mysql.SysDB},
	}},
	{ver: meta.SecondNextGenBootTableVersion, databases: []DatabaseBasicInfo{
		{ID: metadef.SystemDatabaseID, Name: mysql.SystemDB, Tables: tablesInSystemDatabase[tableCountInFirstVerOnNextGen:tableCountInSecondVerOnNextGen]},
	}},
}

func bootstrapSchemas(store kv.Storage) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	return kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		currVer, err := m.GetNextGenBootTableVersion()
		if err != nil {
			return errors.Trace(err)
		}

		largestVer := currVer
		for _, vt := range versionedBootstrapSchemas {
			if currVer >= vt.ver {
				continue
			}
			logutil.BgLogger().Info("bootstrap tables", zap.Int("currVer", int(currVer)),
				zap.Int("targetVer", int(vt.ver)))
			for _, bdb := range vt.databases {
				if err = m.CreateSysDatabaseByIDIfNotExists(bdb.Name, bdb.ID); err != nil {
					return err
				}
				if len(bdb.Tables) > 0 {
					if err = createAndSplitTables(store, m, bdb.ID, bdb.Tables); err != nil {
						return err
					}
				}
			}
			largestVer = max(largestVer, vt.ver)
		}
		if largestVer > currVer {
			return m.SetNextGenBootTableVersion(largestVer)
		}
		return nil
	})
}

// doDDLWorks executes DDL statements in bootstrap stage.
func doDDLWorks(s sessionapi.Session) {
	// for nextgen, system schemas are created in bootstrapSessionImpl
	if kerneltype.IsClassic() {
		for _, db := range systemDatabases {
			mustExecute(s, "CREATE DATABASE IF NOT EXISTS %n", db.Name)
		}
		for _, tbl := range tablesInSystemDatabase {
			mustExecute(s, tbl.SQL)
		}
	}
	// Create bind_info table.
	insertBuiltinBindInfoRow(s)
	// Create `mysql.tidb_mdl_view` view.
	mustExecute(s, metadef.CreateTiDBMDLView)
	// create `sys.schema_unused_indexes` view
	mustExecute(s, metadef.CreateSchemaUnusedIndexesView)
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
}

func checkSystemTableConstraint(tblInfo *model.TableInfo) error {
	if tblInfo.Partition != nil {
		return errors.New("system table should not be partitioned table")
	}
	if tblInfo.SepAutoInc() {
		// AUTO_ID_CACHE=1 is implemented through GRPC service and requires owner
		// election, system tables should not depend on that.
		return errors.New("system table should not use AUTO_ID_CACHE=1")
	}
	return nil
}

// doBootstrapSQLFile executes SQL commands in a file as the last stage of bootstrap.
// It is useful for setting the initial value of GLOBAL variables.
func doBootstrapSQLFile(s sessionapi.Session) error {
	sqlFile := config.GetGlobalConfig().InitializeSQLFile
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	if sqlFile == "" {
		return nil
	}
	logutil.BgLogger().Info("executing -initialize-sql-file", zap.String("file", sqlFile))
	b, err := os.ReadFile(sqlFile) //nolint:gosec
	if err != nil {
		if intest.InTest {
			return err
		}
		logutil.BgLogger().Fatal("unable to read InitializeSQLFile", zap.Error(err))
	}
	stmts, err := s.Parse(ctx, string(b))
	if err != nil {
		if intest.InTest {
			return err
		}
		logutil.BgLogger().Fatal("unable to parse InitializeSQLFile", zap.Error(err))
	}
	for _, stmt := range stmts {
		rs, err := s.ExecuteStmt(ctx, stmt)
		if err != nil {
			logutil.BgLogger().Warn("InitializeSQLFile error", zap.Error(err))
		}
		if rs != nil {
			// I don't believe we need to drain the result-set in bootstrap mode
			// but if required we can do this here in future.
			if err := rs.Close(); err != nil {
				logutil.BgLogger().Fatal("unable to close result", zap.Error(err))
			}
		}
	}
	return nil
}

// doDMLWorks executes DML statements in bootstrap stage.
// All the statements run in a single transaction.
func doDMLWorks(s sessionapi.Session) {
	mustExecute(s, "BEGIN")
	if config.GetGlobalConfig().Security.SecureBootstrap {
		// If secure bootstrap is enabled, we create a root@localhost account which can login with auth_socket.
		// i.e. mysql -S /tmp/tidb.sock -uroot
		// The auth_socket plugin will validate that the user matches $USER.
		u, err := osuser.Current()
		if err != nil {
			logutil.BgLogger().Fatal("failed to read current user. unable to secure bootstrap.", zap.Error(err))
		}
		mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.user (Host,User,authentication_string,plugin,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Process_priv,Grant_priv,References_priv,Alter_priv,Show_db_priv,
			Super_priv,Create_tmp_table_priv,Lock_tables_priv,Execute_priv,Create_view_priv,Show_view_priv,Create_routine_priv,Alter_routine_priv,Index_priv,Create_user_priv,Event_priv,Repl_slave_priv,Repl_client_priv,Trigger_priv,Create_role_priv,Drop_role_priv,Account_locked,
		    Shutdown_priv,Reload_priv,FILE_priv,Config_priv,Create_Tablespace_Priv,User_attributes,Token_issuer) VALUES
		("localhost", "root", %?, "auth_socket", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", null, "")`, u.Username)
	} else {
		mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.user (Host,User,authentication_string,plugin,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Process_priv,Grant_priv,References_priv,Alter_priv,Show_db_priv,
			Super_priv,Create_tmp_table_priv,Lock_tables_priv,Execute_priv,Create_view_priv,Show_view_priv,Create_routine_priv,Alter_routine_priv,Index_priv,Create_user_priv,Event_priv,Repl_slave_priv,Repl_client_priv,Trigger_priv,Create_role_priv,Drop_role_priv,Account_locked,
		    Shutdown_priv,Reload_priv,FILE_priv,Config_priv,Create_Tablespace_Priv,User_attributes,Token_issuer) VALUES
		("%", "root", "", "mysql_native_password", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y", null, "")`)
	}

	// For GLOBAL scoped system variables, insert the initial value
	// into the mysql.global_variables table. This is only run on initial
	// bootstrap, and in some cases we will use a different default value
	// for new installs versus existing installs.

	values := make([]string, 0, len(variable.GetSysVars()))
	for k, v := range variable.GetSysVars() {
		if !v.HasGlobalScope() {
			continue
		}
		vVal := variable.GlobalSystemVariableInitialValue(v.Name, v.Value)

		// sanitize k and vVal
		value := fmt.Sprintf(`("%s", "%s")`, sqlescape.EscapeString(k), sqlescape.EscapeString(vVal))
		values = append(values, value)
	}
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)

	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?, "Bootstrap flag. Do not delete.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, bootstrappedVar, varTrue, varTrue,
	)

	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?, "Bootstrap version. Do not delete.")`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion,
	)
	writeSystemTZ(s)

	writeNewCollationParameter(s, config.GetGlobalConfig().NewCollationsEnabledOnFirstBootstrap)

	writeStmtSummaryVars(s)

	writeDDLTableVersion(s)

	writeClusterID(s)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err := s.ExecuteInternal(ctx, "COMMIT")
	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("doDMLWorks failed", zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if TiDB is already bootstrapped.
		b, err1 := checkBootstrapped(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("doDMLWorks failed", zap.Error(err1))
		}
		if b {
			return
		}
		logutil.BgLogger().Fatal("doDMLWorks failed", zap.Error(err))
	}
}

func mustExecute(s sessionapi.Session, sql string, args ...any) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(internalSQLTimeout)*time.Second)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	_, err := s.ExecuteInternal(ctx, sql, args...)
	defer cancel()
	if err != nil {
		logutil.BgLogger().Fatal("mustExecute error", zap.Error(err), zap.Stack("stack"))
	}
}

// oldPasswordUpgrade upgrade password to MySQL compatible format
func oldPasswordUpgrade(pass string) (string, error) {
	hash1, err := hex.DecodeString(pass)
	if err != nil {
		return "", errors.Trace(err)
	}

	hash2 := auth.Sha1Hash(hash1)
	newpass := fmt.Sprintf("*%X", hash2)
	return newpass, nil
}

// rebuildAllPartitionValueMapAndSorted rebuilds all value map and sorted info for list column partitions with InfoSchema.
func rebuildAllPartitionValueMapAndSorted(ctx context.Context, s *session) {
	type partitionExpr interface {
		PartitionExpr() *tables.PartitionExpr
	}

	p := parser.New()
	is := s.GetInfoSchema().(infoschema.InfoSchema)
	dbs := is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute)
	for _, db := range dbs {
		for _, t := range db.TableInfos {
			pi := t.GetPartitionInfo()
			if pi == nil || pi.Type != ast.PartitionTypeList {
				continue
			}
			tbl, ok := is.TableByID(ctx, t.ID)
			intest.Assert(ok, "table not found in infoschema")
			pe := tbl.(partitionExpr).PartitionExpr()
			for _, cp := range pe.ColPrunes {
				if err := cp.RebuildPartitionValueMapAndSorted(p, pi.Definitions); err != nil {
					logutil.BgLogger().Warn("build list column partition value map and sorted failed")
					break
				}
			}
		}
	}
}
