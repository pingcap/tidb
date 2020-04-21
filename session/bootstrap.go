// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"context"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

const (
	// CreateUserTable is the SQL statement creates User table in system db.
	CreateUserTable = `CREATE TABLE if not exists mysql.user (
		Host				CHAR(64),
		User				CHAR(32),
		authentication_string	TEXT,
		Select_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Insert_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Update_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Delete_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Process_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Grant_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		References_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_db_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Super_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_tmp_table_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_tables_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Index_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_user_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Event_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_role_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_role_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Account_locked			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Shutdown_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Reload_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		FILE_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, User));`
	// CreateGlobalPrivTable is the SQL statement creates Global scope privilege table in system db.
	CreateGlobalPrivTable = "CREATE TABLE if not exists mysql.global_priv (" +
		"Host char(60) NOT NULL DEFAULT ''," +
		"User char(80) NOT NULL DEFAULT ''," +
		"Priv longtext NOT NULL DEFAULT ''," +
		"PRIMARY KEY (Host, User)" +
		")"
	// CreateDBPrivTable is the SQL statement creates DB scope privilege table in system db.
	CreateDBPrivTable = `CREATE TABLE if not exists mysql.db (
		Host			CHAR(60),
		DB			CHAR(64),
		User			CHAR(32),
		Select_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Insert_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Update_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Delete_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Create_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Drop_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Grant_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		References_priv 	ENUM('N','Y') Not Null DEFAULT 'N',
		Index_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Alter_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Create_tmp_table_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_tables_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Event_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, DB, User));`
	// CreateTablePrivTable is the SQL statement creates table scope privilege table in system db.
	CreateTablePrivTable = `CREATE TABLE if not exists mysql.tables_priv (
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(32),
		Table_name	CHAR(64),
		Grantor		CHAR(77),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Table_priv	SET('Select','Insert','Update','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References'),
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Table_name));`
	// CreateColumnPrivTable is the SQL statement creates column scope privilege table in system db.
	CreateColumnPrivTable = `CREATE TABLE if not exists mysql.columns_priv(
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(32),
		Table_name	CHAR(64),
		Column_name	CHAR(64),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Table_name, Column_name));`
	// CreateGlobalVariablesTable is the SQL statement creates global variable table in system db.
	// TODO: MySQL puts GLOBAL_VARIABLES table in INFORMATION_SCHEMA db.
	// INFORMATION_SCHEMA is a virtual db in TiDB. So we put this table in system db.
	// Maybe we will put it back to INFORMATION_SCHEMA.
	CreateGlobalVariablesTable = `CREATE TABLE if not exists mysql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null);`
	// CreateTiDBTable is the SQL statement creates a table in system db.
	// This table is a key-value struct contains some information used by TiDB.
	// Currently we only put bootstrapped in it which indicates if the system is already bootstrapped.
	CreateTiDBTable = `CREATE TABLE if not exists mysql.tidb(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null,
		COMMENT VARCHAR(1024));`

	// CreateHelpTopic is the SQL statement creates help_topic table in system db.
	// See: https://dev.mysql.com/doc/refman/5.5/en/system-database.html#system-database-help-tables
	CreateHelpTopic = `CREATE TABLE if not exists mysql.help_topic (
  		help_topic_id int(10) unsigned NOT NULL,
  		name char(64) NOT NULL,
  		help_category_id smallint(5) unsigned NOT NULL,
  		description text NOT NULL,
  		example text NOT NULL,
  		url text NOT NULL,
  		PRIMARY KEY (help_topic_id),
  		UNIQUE KEY name (name)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='help topics';`

	// CreateStatsMetaTable stores the meta of table statistics.
	CreateStatsMetaTable = `CREATE TABLE if not exists mysql.stats_meta (
		version bigint(64) unsigned NOT NULL,
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		count bigint(64) unsigned NOT NULL DEFAULT 0,
		index idx_ver(version),
		unique index tbl(table_id)
	);`

	// CreateStatsColsTable stores the statistics of table columns.
	CreateStatsColsTable = `CREATE TABLE if not exists mysql.stats_histograms (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		distinct_count bigint(64) NOT NULL,
		null_count bigint(64) NOT NULL DEFAULT 0,
		tot_col_size bigint(64) NOT NULL DEFAULT 0,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		version bigint(64) unsigned NOT NULL DEFAULT 0,
		cm_sketch blob,
		stats_ver bigint(64) NOT NULL DEFAULT 0,
		flag bigint(64) NOT NULL DEFAULT 0,
		correlation double NOT NULL DEFAULT 0,
		last_analyze_pos blob DEFAULT NULL,
		unique index tbl(table_id, is_index, hist_id)
	);`

	// CreateStatsBucketsTable stores the histogram info for every table columns.
	CreateStatsBucketsTable = `CREATE TABLE if not exists mysql.stats_buckets (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		bucket_id bigint(64) NOT NULL,
		count bigint(64) NOT NULL,
		repeats bigint(64) NOT NULL,
		upper_bound blob NOT NULL,
		lower_bound blob ,
		unique index tbl(table_id, is_index, hist_id, bucket_id)
	);`

	// CreateGCDeleteRangeTable stores schemas which can be deleted by DeleteRange.
	CreateGCDeleteRangeTable = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range (
		job_id BIGINT NOT NULL COMMENT "the DDL job ID",
		element_id BIGINT NOT NULL COMMENT "the schema element ID",
		start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_index (job_id, element_id)
	);`

	// CreateGCDeleteRangeDoneTable stores schemas which are already deleted by DeleteRange.
	CreateGCDeleteRangeDoneTable = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range_done (
		job_id BIGINT NOT NULL COMMENT "the DDL job ID",
		element_id BIGINT NOT NULL COMMENT "the schema element ID",
		start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_done_index (job_id, element_id)
	);`

	// CreateStatsFeedbackTable stores the feedback info which is used to update stats.
	CreateStatsFeedbackTable = `CREATE TABLE IF NOT EXISTS mysql.stats_feedback (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		feedback blob NOT NULL,
		index hist(table_id, is_index, hist_id)
	);`

	// CreateBindInfoTable stores the sql bind info which is used to update globalBindCache.
	CreateBindInfoTable = `CREATE TABLE IF NOT EXISTS mysql.bind_info (
		original_sql text NOT NULL  ,
      	bind_sql text NOT NULL ,
      	default_db text  NOT NULL,
		status text NOT NULL,
		create_time timestamp(3) NOT NULL,
		update_time timestamp(3) NOT NULL,
		charset text NOT NULL,
		collation text NOT NULL,
		INDEX sql_index(original_sql(1024),default_db(1024)) COMMENT "accelerate the speed when add global binding query",
		INDEX time_index(update_time) COMMENT "accelerate the speed when querying with last update time"
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

	// CreateRoleEdgesTable stores the role and user relationship information.
	CreateRoleEdgesTable = `CREATE TABLE IF NOT EXISTS mysql.role_edges (
		FROM_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		FROM_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		WITH_ADMIN_OPTION enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',
		PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
	);`

	// CreateDefaultRolesTable stores the active roles for a user.
	CreateDefaultRolesTable = `CREATE TABLE IF NOT EXISTS mysql.default_roles (
		HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		DEFAULT_ROLE_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '%',
		DEFAULT_ROLE_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		PRIMARY KEY (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER)
	)`

	// CreateStatsTopNTable stores topn data of a cmsketch with top n.
	CreateStatsTopNTable = `CREATE TABLE if not exists mysql.stats_top_n (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		value longblob,
		count bigint(64) UNSIGNED NOT NULL,
		index tbl(table_id, is_index, hist_id)
	);`

	// CreateExprPushdownBlacklist stores the expressions which are not allowed to be pushed down.
	CreateExprPushdownBlacklist = `CREATE TABLE IF NOT EXISTS mysql.expr_pushdown_blacklist (
		name char(100) NOT NULL,
		store_type char(100) NOT NULL DEFAULT 'tikv,tiflash,tidb',
		reason varchar(200)
	);`

	// CreateOptRuleBlacklist stores the list of disabled optimizing operations.
	CreateOptRuleBlacklist = `CREATE TABLE IF NOT EXISTS mysql.opt_rule_blacklist (
		name char(100) NOT NULL
	);`
)

// bootstrap initiates system DB for a store.
func bootstrap(s Session) {
	startTime := time.Now()
	dom := domain.GetDomain(s)
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
			logutil.BgLogger().Info("bootstrap successful",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
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
	// The variable name in mysql.tidb table and it will indicate if the new collations are enabled in the TiDB cluster.
	tidbNewCollationEnabled = "new_collation_enabled"
	// Const for TiDB server version 2.
	version2  = 2
	version3  = 3
	version4  = 4
	version5  = 5
	version6  = 6
	version7  = 7
	version8  = 8
	version9  = 9
	version10 = 10
	version11 = 11
	version12 = 12
	version13 = 13
	version14 = 14
	version15 = 15
	version16 = 16
	version17 = 17
	version18 = 18
	version19 = 19
	version20 = 20
	version21 = 21
	version22 = 22
	version23 = 23
	version24 = 24
	version25 = 25
	version26 = 26
	version27 = 27
	version28 = 28
	// version29 is not needed.
	version30 = 30
	version31 = 31
	version32 = 32
	version33 = 33
	version34 = 34
	version35 = 35
	version36 = 36
	version37 = 37
	version38 = 38
	version39 = 39
	// version40 is the version that introduce new collation in TiDB,
	// see https://github.com/pingcap/tidb/pull/14574 for more details.
	version40 = 40
	version41 = 41
	// version42 add storeType and reason column in expr_pushdown_blacklist
	version42 = 42
	// version43 delete tidb_isolation_read_engines from mysql.global_variables to avoid unexpected behavior after upgrade.
	version43 = 43
)

var bootstrapVersion map[int64]func(Session)

func checkBootstrapped(s Session) (bool, error) {
	//  Check if system db exists.
	_, err := s.Execute(context.Background(), fmt.Sprintf("USE %s;", mysql.SystemDB))
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
		if err = s.CommitTxn(context.Background()); err != nil {
			return false, errors.Trace(err)
		}
	}
	return isBootstrapped, nil
}

// getTiDBVar gets variable value from mysql.tidb table.
// Those variables are used by TiDB server.
func getTiDBVar(s Session, name string) (sVal string, isNull bool, e error) {
	sql := fmt.Sprintf(`SELECT HIGH_PRIORITY VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s"`,
		mysql.SystemDB, mysql.TiDBTable, name)
	ctx := context.Background()
	rs, err := s.Execute(ctx, sql)
	if err != nil {
		return "", true, errors.Trace(err)
	}
	if len(rs) != 1 {
		return "", true, errors.New("Wrong number of Recordset")
	}
	r := rs[0]
	defer terror.Call(r.Close)
	req := r.NewChunk()
	err = r.Next(ctx, req)
	if err != nil || req.NumRows() == 0 {
		return "", true, errors.Trace(err)
	}
	row := req.GetRow(0)
	if row.IsNull(0) {
		return "", true, nil
	}
	return row.GetString(0), false, nil
}

// upgrade function  will do some upgrade works, when the system is bootstrapped by low version TiDB server
// For example, add new system variables into mysql.global_variables table.
func upgrade(s Session) {
	ver, err := getBootstrapVersion(s)
	terror.MustNil(err)
	if ver >= currentBootstrapVersion {
		// It is already bootstrapped/upgraded by a higher version TiDB server.
		return
	}
	// Do upgrade works then update bootstrap version.
	for version, upgradeFunc := range bootstrapVersion {
		if ver < version {
			upgradeFunc(s)
		}
	}

	// upgradeToVer29 only need to be run when the current version is 28.
	if ver == version28 {
		upgradeToVer29(s)
	}

	updateBootstrapVer(s)
	_, err = s.Execute(context.Background(), "COMMIT")

	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("update bootstrap ver failed",
			zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if TiDB is already upgraded.
		v, err1 := getBootstrapVersion(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("upgrade failed", zap.Error(err1))
		}
		if v >= currentBootstrapVersion {
			// It is already bootstrapped/upgraded by a higher version TiDB server.
			return
		}
		logutil.BgLogger().Fatal("[Upgrade] upgrade failed",
			zap.Int64("from", ver),
			zap.Int("to", currentBootstrapVersion),
			zap.Error(err))
	}
}

// upgradeToVer2 updates to version 2.
func upgradeToVer2(s Session) {
	// Version 2 add two system variable for DistSQL concurrency controlling.
	// Insert distsql related system variable.
	distSQLVars := []string{variable.TiDBDistSQLScanConcurrency}
	values := make([]string, 0, len(distSQLVars))
	for _, v := range distSQLVars {
		value := fmt.Sprintf(`("%s", "%s")`, v, variable.SysVars[v].Value)
		values = append(values, value)
	}
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY IGNORE INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)
}

// upgradeToVer3 updates to version 3.
func upgradeToVer3(s Session) {
	// Version 3 fix tx_read_only variable value.
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %s.%s set variable_value = '0' where variable_name = 'tx_read_only';",
		mysql.SystemDB, mysql.GlobalVariablesTable)
	mustExecute(s, sql)
}

// upgradeToVer4 updates to version 4.
func upgradeToVer4(s Session) {
	sql := CreateStatsMetaTable
	mustExecute(s, sql)
}

func upgradeToVer5(s Session) {
	mustExecute(s, CreateStatsColsTable)
	mustExecute(s, CreateStatsBucketsTable)
}

func upgradeToVer6(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Super_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_db_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Super_priv='Y'")
}

func upgradeToVer7(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Process_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Drop_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Process_priv='Y'")
}

func upgradeToVer8(s Session) {
	// This is a dummy upgrade, it checks whether upgradeToVer7 success, if not, do it again.
	if _, err := s.Execute(context.Background(), "SELECT HIGH_PRIORITY `Process_priv` from mysql.user limit 0"); err == nil {
		return
	}
	upgradeToVer7(s)
}

func upgradeToVer9(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Trigger_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`", infoschema.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Trigger_priv='Y'")
}

func doReentrantDDL(s Session, sql string, ignorableErrs ...error) {
	_, err := s.Execute(context.Background(), sql)
	for _, ignorableErr := range ignorableErrs {
		if terror.ErrorEqual(err, ignorableErr) {
			return
		}
	}
	if err != nil {
		logutil.BgLogger().Fatal("doReentrantDDL error", zap.Error(err))
	}
}

func upgradeToVer10(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets CHANGE COLUMN `value` `upper_bound` BLOB NOT NULL", infoschema.ErrColumnNotExists, infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `lower_bound` BLOB", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `null_count` bigint(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN distinct_ratio", ddl.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN use_count_to_estimate", ddl.ErrCantDropFieldOrKey)
}

func upgradeToVer11(s Session) {
	_, err := s.Execute(context.Background(), "ALTER TABLE mysql.user ADD COLUMN `References_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`")
	if err != nil {
		if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
			return
		}
		logutil.BgLogger().Fatal("upgradeToVer11 error", zap.Error(err))
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET References_priv='Y'")
}

func upgradeToVer12(s Session) {
	ctx := context.Background()
	_, err := s.Execute(ctx, "BEGIN")
	terror.MustNil(err)
	sql := "SELECT HIGH_PRIORITY user, host, password FROM mysql.user WHERE password != ''"
	rs, err := s.Execute(ctx, sql)
	if terror.ErrorEqual(err, core.ErrUnknownColumn) {
		sql := "SELECT HIGH_PRIORITY user, host, authentication_string FROM mysql.user WHERE authentication_string != ''"
		rs, err = s.Execute(ctx, sql)
	}
	terror.MustNil(err)
	r := rs[0]
	sqls := make([]string, 0, 1)
	defer terror.Call(r.Close)
	req := r.NewChunk()
	it := chunk.NewIterator4Chunk(req)
	err = r.Next(ctx, req)
	for err == nil && req.NumRows() != 0 {
		for row := it.Begin(); row != it.End(); row = it.Next() {
			user := row.GetString(0)
			host := row.GetString(1)
			pass := row.GetString(2)
			var newPass string
			newPass, err = oldPasswordUpgrade(pass)
			terror.MustNil(err)
			updateSQL := fmt.Sprintf(`UPDATE HIGH_PRIORITY mysql.user set password = "%s" where user="%s" and host="%s"`, newPass, user, host)
			sqls = append(sqls, updateSQL)
		}
		err = r.Next(ctx, req)
	}
	terror.MustNil(err)

	for _, sql := range sqls {
		mustExecute(s, sql)
	}

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%d", "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%d"`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, version12, version12)
	mustExecute(s, sql)

	mustExecute(s, "COMMIT")
}

func upgradeToVer13(s Session) {
	sqls := []string{
		"ALTER TABLE mysql.user ADD COLUMN `Create_tmp_table_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Super_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Lock_tables_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Show_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Alter_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Event_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`",
	}
	ctx := context.Background()
	for _, sql := range sqls {
		_, err := s.Execute(ctx, sql)
		if err != nil {
			if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
				continue
			}
			logutil.BgLogger().Fatal("upgradeToVer13 error", zap.Error(err))
		}
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer14(s Session) {
	sqls := []string{
		"ALTER TABLE mysql.db ADD COLUMN `References_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Create_tmp_table_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Alter_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Lock_tables_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Create_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Lock_tables_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Show_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Create_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Alter_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Event_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE mysql.db ADD COLUMN `Trigger_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Event_priv`",
	}
	ctx := context.Background()
	for _, sql := range sqls {
		_, err := s.Execute(ctx, sql)
		if err != nil {
			if terror.ErrorEqual(err, infoschema.ErrColumnExists) {
				continue
			}
			logutil.BgLogger().Fatal("upgradeToVer14 error", zap.Error(err))
		}
	}
}

func upgradeToVer15(s Session) {
	var err error
	_, err = s.Execute(context.Background(), CreateGCDeleteRangeTable)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer15 error", zap.Error(err))
	}
}

func upgradeToVer16(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `cm_sketch` blob", infoschema.ErrColumnExists)
}

func upgradeToVer17(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.user MODIFY User CHAR(32)")
}

func upgradeToVer18(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `tot_col_size` bigint(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer19(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.db MODIFY User CHAR(32)")
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY User CHAR(32)")
	doReentrantDDL(s, "ALTER TABLE mysql.columns_priv MODIFY User CHAR(32)")
}

func upgradeToVer20(s Session) {
	doReentrantDDL(s, CreateStatsFeedbackTable)
}

func upgradeToVer21(s Session) {
	mustExecute(s, CreateGCDeleteRangeDoneTable)

	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX job_id", ddl.ErrCantDropFieldOrKey)
	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range ADD UNIQUE INDEX delete_range_index (job_id, element_id)", ddl.ErrDupKeyName)
	doReentrantDDL(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX element_id", ddl.ErrCantDropFieldOrKey)
}

func upgradeToVer22(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `stats_ver` bigint(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer23(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `flag` bigint(64) NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

// writeSystemTZ writes system timezone info into mysql.tidb
func writeSystemTZ(s Session) {
	sql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%s", "TiDB Global System Timezone.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%s"`,
		mysql.SystemDB, mysql.TiDBTable, tidbSystemTZ, timeutil.InferSystemTZ(), timeutil.InferSystemTZ())
	mustExecute(s, sql)
}

// upgradeToVer24 initializes `System` timezone according to docs/design/2018-09-10-adding-tz-env.md
func upgradeToVer24(s Session) {
	writeSystemTZ(s)
}

// upgradeToVer25 updates tidb_max_chunk_size to new low bound value 32 if previous value is small than 32.
func upgradeToVer25(s Session) {
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %[1]s.%[2]s SET VARIABLE_VALUE = '%[4]d' WHERE VARIABLE_NAME = '%[3]s' AND VARIABLE_VALUE < %[4]d",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBMaxChunkSize, variable.DefInitChunkSize)
	mustExecute(s, sql)
}

func upgradeToVer26(s Session) {
	mustExecute(s, CreateRoleEdgesTable)
	mustExecute(s, CreateDefaultRolesTable)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Create_role_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Drop_role_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Account_locked` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	// user with Create_user_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_role_priv='Y',Drop_role_priv='Y' WHERE Create_user_priv='Y'")
	// user with Create_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer27(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `correlation` double NOT NULL DEFAULT 0", infoschema.ErrColumnExists)
}

func upgradeToVer28(s Session) {
	doReentrantDDL(s, CreateBindInfoTable)
}

func upgradeToVer29(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info change create_time create_time timestamp(3)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info change update_time update_time timestamp(3)")
	doReentrantDDL(s, "ALTER TABLE mysql.bind_info add index sql_index (original_sql(1024),default_db(1024))", ddl.ErrDupKeyName)
}

func upgradeToVer30(s Session) {
	mustExecute(s, CreateStatsTopNTable)
}

func upgradeToVer31(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `last_analyze_pos` blob default null", infoschema.ErrColumnExists)
}

func upgradeToVer32(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.tables_priv MODIFY table_priv SET('Select','Insert','Update','Delete','Create','Drop','Grant', 'Index', 'Alter', 'Create View', 'Show View', 'Trigger', 'References')")
}

func upgradeToVer33(s Session) {
	doReentrantDDL(s, CreateExprPushdownBlacklist)
}

func upgradeToVer34(s Session) {
	doReentrantDDL(s, CreateOptRuleBlacklist)
}

func upgradeToVer35(s Session) {
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %s.%s SET VARIABLE_NAME = '%s' WHERE VARIABLE_NAME = 'tidb_back_off_weight'",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBBackOffWeight)
	mustExecute(s, sql)
}

func upgradeToVer36(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Shutdown_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	// A root user will have those privileges after upgrading.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Shutdown_priv='Y' where Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
}

func upgradeToVer37(s Session) {
	// when upgrade from old tidb and no 'tidb_enable_window_function' in GLOBAL_VARIABLES, init it with 0.
	sql := fmt.Sprintf("INSERT IGNORE INTO  %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%d')",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBEnableWindowFunction, 0)
	mustExecute(s, sql)
}

func upgradeToVer38(s Session) {
	var err error
	_, err = s.Execute(context.Background(), CreateGlobalPrivTable)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer38 error", zap.Error(err))
	}
}

func upgradeToVer39(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `Reload_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `File_priv` ENUM('N','Y') DEFAULT 'N'", infoschema.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Reload_priv='Y' where Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET File_priv='Y' where Super_priv='Y'")
}

func writeNewCollationParameter(s Session, flag bool) {
	comment := "If the new collations are enabled. Do not edit it."
	b := varFalse
	if flag {
		b = varTrue
	}
	sql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", '%s', '%s') ON DUPLICATE KEY UPDATE VARIABLE_VALUE='%s'`,
		mysql.SystemDB, mysql.TiDBTable, tidbNewCollationEnabled, b, comment, b)
	mustExecute(s, sql)
}

func upgradeToVer40(s Session) {
	// There is no way to enable new collation for an existing TiDB cluster.
	writeNewCollationParameter(s, false)
}

func upgradeToVer41(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.user CHANGE `password` `authentication_string` TEXT", infoschema.ErrColumnExists, infoschema.ErrColumnNotExists)
	doReentrantDDL(s, "ALTER TABLE mysql.user ADD COLUMN `password` TEXT as (`authentication_string`)", infoschema.ErrColumnExists)
}

// writeDefaultExprPushDownBlacklist writes default expr pushdown blacklist into mysql.expr_pushdown_blacklist
func writeDefaultExprPushDownBlacklist(s Session) {
	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES"+
		"('date_add','tiflash', 'DST(daylight saving time) does not take effect in TiFlash date_add'),"+
		"('cast','tiflash', 'Behavior of some corner cases(overflow, truncate etc) is different in TiFlash and TiDB')")
}

func upgradeToVer42(s Session) {
	doReentrantDDL(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `store_type` char(100) NOT NULL DEFAULT 'tikv,tiflash,tidb'", infoschema.ErrColumnExists)
	doReentrantDDL(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `reason` varchar(200)", infoschema.ErrColumnExists)
	writeDefaultExprPushDownBlacklist(s)
}

func upgradeToVer43(s Session) {
	mustExecute(s, "DELETE FROM mysql.global_variables where variable_name = \"tidb_isolation_read_engines\"")
}

// updateBootstrapVer updates bootstrap version variable in mysql.TiDB table.
func updateBootstrapVer(s Session) {
	// Update bootstrap version.
	sql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%d", "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%d"`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion, currentBootstrapVersion)
	mustExecute(s, sql)
}

// getBootstrapVersion gets bootstrap version from mysql.tidb table;
func getBootstrapVersion(s Session) (int64, error) {
	sVal, isNull, err := getTiDBVar(s, tidbServerVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

// doDDLWorks executes DDL statements in bootstrap stage.
func doDDLWorks(s Session) {
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
	// Create system db.
	mustExecute(s, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", mysql.SystemDB))
	// Create user table.
	mustExecute(s, CreateUserTable)
	// Create privilege tables.
	mustExecute(s, CreateGlobalPrivTable)
	mustExecute(s, CreateDBPrivTable)
	mustExecute(s, CreateTablePrivTable)
	mustExecute(s, CreateColumnPrivTable)
	// Create global system variable table.
	mustExecute(s, CreateGlobalVariablesTable)
	// Create TiDB table.
	mustExecute(s, CreateTiDBTable)
	// Create help table.
	mustExecute(s, CreateHelpTopic)
	// Create stats_meta table.
	mustExecute(s, CreateStatsMetaTable)
	// Create stats_columns table.
	mustExecute(s, CreateStatsColsTable)
	// Create stats_buckets table.
	mustExecute(s, CreateStatsBucketsTable)
	// Create gc_delete_range table.
	mustExecute(s, CreateGCDeleteRangeTable)
	// Create gc_delete_range_done table.
	mustExecute(s, CreateGCDeleteRangeDoneTable)
	// Create stats_feedback table.
	mustExecute(s, CreateStatsFeedbackTable)
	// Create role_edges table.
	mustExecute(s, CreateRoleEdgesTable)
	// Create default_roles table.
	mustExecute(s, CreateDefaultRolesTable)
	// Create bind_info table.
	mustExecute(s, CreateBindInfoTable)
	// Create stats_topn_store table.
	mustExecute(s, CreateStatsTopNTable)
	// Create expr_pushdown_blacklist table.
	mustExecute(s, CreateExprPushdownBlacklist)
	// Create opt_rule_blacklist table.
	mustExecute(s, CreateOptRuleBlacklist)
}

// doDMLWorks executes DML statements in bootstrap stage.
// All the statements run in a single transaction.
func doDMLWorks(s Session) {
	mustExecute(s, "BEGIN")

	// Insert a default user with empty password.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.user VALUES
		("%", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y")`)

	// Init global system variables table.
	values := make([]string, 0, len(variable.SysVars))
	for k, v := range variable.SysVars {
		// Session only variable should not be inserted.
		if v.Scope != variable.ScopeSession {
			vVal := v.Value
			if v.Name == variable.TiDBTxnMode && config.GetGlobalConfig().Store == "tikv" {
				vVal = "pessimistic"
			}
			if v.Name == variable.TiDBRowFormatVersion {
				vVal = strconv.Itoa(variable.DefTiDBRowFormatV2)
			}
			value := fmt.Sprintf(`("%s", "%s")`, strings.ToLower(k), vVal)
			values = append(values, value)
		}
	}
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY INTO %s.%s VALUES %s;", mysql.SystemDB, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%s", "Bootstrap flag. Do not delete.")
		ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%s"`,
		mysql.SystemDB, mysql.TiDBTable, bootstrappedVar, varTrue, varTrue)
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%d", "Bootstrap version. Do not delete.")`,
		mysql.SystemDB, mysql.TiDBTable, tidbServerVersionVar, currentBootstrapVersion)
	mustExecute(s, sql)

	writeSystemTZ(s)

	writeNewCollationParameter(s, config.GetGlobalConfig().NewCollationsEnabledOnFirstBootstrap)

	writeDefaultExprPushDownBlacklist(s)

	_, err := s.Execute(context.Background(), "COMMIT")
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

func mustExecute(s Session, sql string) {
	_, err := s.Execute(context.Background(), sql)
	if err != nil {
		debug.PrintStack()
		logutil.BgLogger().Fatal("mustExecute error", zap.Error(err))
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

func init() {
	bootstrapVersion = map[int64]func(Session){
		version2:  upgradeToVer2,
		version3:  upgradeToVer3,
		version4:  upgradeToVer4,
		version5:  upgradeToVer5,
		version6:  upgradeToVer6,
		version7:  upgradeToVer7,
		version8:  upgradeToVer8,
		version9:  upgradeToVer9,
		version10: upgradeToVer10,
		version11: upgradeToVer11,
		version12: upgradeToVer12,
		version13: upgradeToVer13,
		version14: upgradeToVer14,
		version15: upgradeToVer15,
		version16: upgradeToVer16,
		version17: upgradeToVer17,
		version18: upgradeToVer18,
		version19: upgradeToVer19,
		version20: upgradeToVer20,
		version21: upgradeToVer21,
		version22: upgradeToVer22,
		version23: upgradeToVer23,
		version24: upgradeToVer24,
		version25: upgradeToVer25,
		version26: upgradeToVer26,
		version27: upgradeToVer27,
		version28: upgradeToVer28,
		version30: upgradeToVer30,
		version31: upgradeToVer31,
		version32: upgradeToVer32,
		version33: upgradeToVer33,
		version34: upgradeToVer34,
		version35: upgradeToVer35,
		version36: upgradeToVer36,
		version37: upgradeToVer37,
		version38: upgradeToVer38,
		version39: upgradeToVer39,
		version40: upgradeToVer40,
		version41: upgradeToVer41,
		version42: upgradeToVer42,
		version43: upgradeToVer43,
	}
}
