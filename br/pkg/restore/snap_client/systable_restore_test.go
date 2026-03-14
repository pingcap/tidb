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

package snapclient_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/stretchr/testify/require"
)

func TestCheckSysTableCompatibility(t *testing.T) {
	cluster := mc
	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, split.DefaultTestKeepaliveCfg)
	err := client.InitConnections(g, cluster.Storage)
	require.NoError(t, err)

	info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(ast.NewCIStr(mysql.SystemDB))
	require.True(t, isExist)
	tmpSysDB := dbSchema.Clone()
	tmpSysDB.Name = utils.TemporaryDBName(mysql.SystemDB)
	sysDB := ast.NewCIStr(mysql.SystemDB)
	userTI, err := restore.GetTableSchema(cluster.Domain, sysDB, ast.NewCIStr("user"))
	require.NoError(t, err)

	var canLoadSysTablePhysical bool
	// user table in cluster have more columns(success)
	mockedUserTI := userTI.Clone()
	userTI.Columns = append(userTI.Columns, &model.ColumnInfo{Name: ast.NewCIStr("new-name")})
	canLoadSysTablePhysical, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}}, false)
	require.NoError(t, err)
	require.True(t, canLoadSysTablePhysical)
	userTI.Columns = userTI.Columns[:len(userTI.Columns)-1]

	// user table in cluster have less columns(failed)
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns = append(mockedUserTI.Columns, &model.ColumnInfo{Name: ast.NewCIStr("new-name")})
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}}, false)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// column order mismatch(success)
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[4], mockedUserTI.Columns[5] = mockedUserTI.Columns[5], mockedUserTI.Columns[4]
	canLoadSysTablePhysical, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}}, false)
	require.NoError(t, err)
	require.True(t, canLoadSysTablePhysical)

	// incompatible column type
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[0].FieldType.SetFlen(2000) // Columns[0] is `Host` char(255)
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}}, false)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// compatible
	mockedUserTI = userTI.Clone()
	canLoadSysTablePhysical, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}}, false)
	require.NoError(t, err)
	require.True(t, canLoadSysTablePhysical)

	// use the mysql.db table to test for column count mismatch.
	dbTI, err := restore.GetTableSchema(cluster.Domain, sysDB, ast.NewCIStr("db"))
	require.NoError(t, err)

	// other system tables in cluster have more columns(failed)
	mockedDBTI := dbTI.Clone()
	//dbTI.Columns = append(dbTI.Columns, &model.ColumnInfo{Name: ast.NewCIStr("new-name")})
	mockedDBTI.Columns = append(dbTI.Columns, &model.ColumnInfo{Name: ast.NewCIStr("new-name")})
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}}, false)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// collate mismatch
	mockedDBTI = dbTI.Clone()
	mockedDBTI.Columns[1].SetCollate("utf8mb4_bin")
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}}, false)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// skip check collate
	mockedDBTI = dbTI.Clone()
	mockedDBTI.Columns[1].SetCollate("utf8mb4_bin")
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}}, true)
	require.NoError(t, err)

	// skip check collate but type mismatch
	mockedDBTI = dbTI.Clone()
	mockedDBTI.Columns[1].SetCollate("utf8mb4_bin")
	mockedUserTI.Columns[1].FieldType.SetFlen(2000) // Columns[1] is `DB` char(64)
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}}, true)
	require.NoError(t, err)

	// another column collate mismatch
	mockedDBTI = dbTI.Clone()
	mockedDBTI.Columns[0].SetCollate("utf8mb4_general_ci")
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}}, true)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// another column collate mismatch
	mockedDBTI = dbTI.Clone()
	mockedDBTI.Columns[1].SetCollate("utf8mb4_unicode_ci")
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}}, true)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))
}

type mustExecuteSession struct {
	ctx context.Context
	se  glue.Session
	t   *testing.T
}

func (se *mustExecuteSession) MustExecute(sql string) {
	err := se.se.ExecuteInternal(se.ctx, sql)
	require.NoError(se.t, err)
}

const (
	CreateDBSQL = `CREATE TABLE __TiDB_BR_Temporary_mysql.db (
  Host char(255) NOT NULL,
  DB char(64) NOT NULL,
  User char(32) NOT NULL,
  Select_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Insert_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Update_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Delete_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Create_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Drop_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Grant_priv enum('N','Y') NOT NULL DEFAULT 'N',
  References_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Index_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Alter_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Create_tmp_table_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Lock_tables_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Create_view_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Show_view_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Create_routine_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Alter_routine_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Execute_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Event_priv enum('N','Y') NOT NULL DEFAULT 'N',
  Trigger_priv enum('N','Y') NOT NULL DEFAULT 'N',
  PRIMARY KEY (Host,DB,User) /*T![clustered_index] NONCLUSTERED */
)`

	CreateTableSQL = `CREATE TABLE __TiDB_BR_Temporary_mysql.tables_priv (
  Host char(255) NOT NULL,
  DB char(64) NOT NULL,
  User char(32) NOT NULL,
  Table_name char(64) NOT NULL,
  Grantor char(77) DEFAULT NULL,
  Timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
  Table_priv set('Select','Insert','Update','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References') DEFAULT NULL,
  Column_priv set('Select','Insert','Update','References') DEFAULT NULL,
  PRIMARY KEY (Host,DB,User,Table_name) /*T![clustered_index] NONCLUSTERED */
)`

	CreateColumnSQL = `CREATE TABLE __TiDB_BR_Temporary_mysql.columns_priv (
  Host char(255) NOT NULL,
  DB char(64) NOT NULL,
  User char(32) NOT NULL,
  Table_name char(64) NOT NULL,
  Column_name char(64) NOT NULL,
  Timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
  Column_priv set('Select','Insert','Update','References') DEFAULT NULL,
  PRIMARY KEY (Host,DB,User,Table_name,Column_name) /*T![clustered_index] NONCLUSTERED */
)`
)

func TestCheckPrivilegeTableRowsCollateCompatibility(t *testing.T) {
	cluster := mc
	ctx := context.Background()
	g := gluetidb.New()
	rc := snapclient.SnapClient{}
	defer rc.Close()
	err := rc.InitConnections(g, cluster.Storage)
	require.NoError(t, err)
	rc.SetCheckPrivilegeTableRowsCollateCompatiblity(true)

	se, err := g.CreateSession(cluster.Storage)
	require.NoError(t, err)
	defer se.Close()
	mse := &mustExecuteSession{ctx, se, t}
	mse.MustExecute("CREATE USER newroot")
	mse.MustExecute("CREATE USER oldroot")
	mse.MustExecute("CREATE DATABASE __TiDB_BR_Temporary_mysql")
	defer mse.MustExecute("DROP DATABASE __TiDB_BR_Temporary_mysql")

	downstreamDBTable, err := restore.GetTableSchema(cluster.Domain, ast.NewCIStr("mysql"), ast.NewCIStr("db"))
	require.NoError(t, err)
	downstreamTablesTable, err := restore.GetTableSchema(cluster.Domain, ast.NewCIStr("mysql"), ast.NewCIStr("tables_priv"))
	require.NoError(t, err)
	downstreamColumnsTable, err := restore.GetTableSchema(cluster.Domain, ast.NewCIStr("mysql"), ast.NewCIStr("columns_priv"))
	require.NoError(t, err)
	// case 1: privilege db
	mse.MustExecute(CreateDBSQL)
	backupTable, err := restore.GetTableSchema(cluster.Domain, ast.NewCIStr("__TiDB_BR_Temporary_mysql"), ast.NewCIStr("db"))
	require.NoError(t, err)
	mse.MustExecute("INSERT INTO __TiDB_BR_Temporary_mysql.db (Host,DB,User) VALUES ('%','test','newroot')")
	mse.MustExecute("INSERT INTO __TiDB_BR_Temporary_mysql.db (Host,DB,User) VALUES ('%','test','oldroot')")
	err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "db", backupTable, downstreamDBTable)
	require.NoError(t, err)
	mse.MustExecute("INSERT INTO __TiDB_BR_Temporary_mysql.db (Host,DB,User) VALUES ('%','Test','newroot')")
	err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "db", backupTable, downstreamDBTable)
	require.Error(t, err)
	mse.MustExecute("DELETE FROM __TiDB_BR_Temporary_mysql.db WHERE DB = 'Test'")
	mse.MustExecute("INSERT INTO __TiDB_BR_Temporary_mysql.db (Host,DB,User) VALUES ('%','cafe','newroot')")
	err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "db", backupTable, downstreamDBTable)
	require.NoError(t, err)
	mse.MustExecute("INSERT INTO __TiDB_BR_Temporary_mysql.db (Host,DB,User) VALUES ('%','café','newroot')")
	err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "db", backupTable, downstreamDBTable)
	require.Error(t, err)
	mse.MustExecute("DELETE FROM __TiDB_BR_Temporary_mysql.db WHERE DB = 'cafe'")
	err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "db", backupTable, downstreamDBTable)
	require.NoError(t, err)
	mse.MustExecute("DROP TABLE __TiDB_BR_Temporary_mysql.db")

	// case 2: privilege table
	type privCase struct {
		insertValues []string
		deleteCond   []string
	}
	mse.MustExecute(CreateTableSQL)
	backupTable, err = restore.GetTableSchema(cluster.Domain, ast.NewCIStr("__TiDB_BR_Temporary_mysql"), ast.NewCIStr("tables_priv"))
	require.NoError(t, err)
	mse.MustExecute("INSERT INTO __TiDB_BR_Temporary_mysql.tables_priv (Host,DB,User,Table_name) VALUES ('%','test','newroot','ta1')")
	mse.MustExecute("INSERT INTO __TiDB_BR_Temporary_mysql.tables_priv (Host,DB,User,Table_name) VALUES ('%','test','oldroot','ta1')")
	err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "tables_priv", backupTable, downstreamTablesTable)
	require.NoError(t, err)
	cases := []privCase{
		{
			insertValues: []string{"('%','test','newroot','Ta1')"},
			deleteCond:   []string{"Table_name = 'Ta1'"},
		},
		{
			insertValues: []string{"('%','tEst','newroot','ta1')"},
			deleteCond:   []string{"DB = 'tEst'"},
		},
		{
			insertValues: []string{"('%','tEst','newroot','Ta1')"},
			deleteCond:   []string{"DB = 'tEst'"},
		},
		{
			insertValues: []string{"('%','test','newroot','tá1')"},
			deleteCond:   []string{"Table_name = 'tá1'"},
		},
		{
			insertValues: []string{"('%','tést','newroot','ta1')"},
			deleteCond:   []string{"DB = 'tést'"},
		},
		{
			insertValues: []string{"('%','tést','newroot','tá1')"},
			deleteCond:   []string{"DB = 'tést'"},
		},
		{
			insertValues: []string{"('%','tést','newroot','tá1')", "('%','tEst','newroot','Ta1')"},
			deleteCond:   []string{"DB = 'tést'", "DB = 'tEst'"},
		},
	}
	for _, cs := range cases {
		for _, v := range cs.insertValues {
			mse.MustExecute(fmt.Sprintf("INSERT INTO __TiDB_BR_Temporary_mysql.tables_priv (Host,DB,User,Table_name) VALUES %s", v))
		}
		err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "tables_priv", backupTable, downstreamTablesTable)
		require.Error(t, err)
		for _, v := range cs.deleteCond {
			mse.MustExecute(fmt.Sprintf("DELETE FROM __TiDB_BR_Temporary_mysql.tables_priv WHERE %s", v))
		}
		err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "tables_priv", backupTable, downstreamTablesTable)
		require.NoError(t, err)
	}
	mse.MustExecute("DROP TABLE __TiDB_BR_Temporary_mysql.tables_priv")

	// case 3: privilege column
	mse.MustExecute(CreateColumnSQL)
	backupTable, err = restore.GetTableSchema(cluster.Domain, ast.NewCIStr("__TiDB_BR_Temporary_mysql"), ast.NewCIStr("columns_priv"))
	require.NoError(t, err)
	mse.MustExecute("INSERT INTO __TiDB_BR_Temporary_mysql.columns_priv (Host,DB,User,Table_name,Column_name) VALUES ('%','test','newroot','ta1','ca1')")
	mse.MustExecute("INSERT INTO __TiDB_BR_Temporary_mysql.columns_priv (Host,DB,User,Table_name,Column_name) VALUES ('%','test','oldroot','ta1','ca1')")
	err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "columns_priv", backupTable, downstreamColumnsTable)
	require.NoError(t, err)
	cases = []privCase{
		{
			insertValues: []string{"('%','test','newroot','ta1','Ca1')"},
			deleteCond:   []string{"Column_name = 'Ca1'"},
		},
		{
			insertValues: []string{"('%','test','newroot','Ta1','ca1')"},
			deleteCond:   []string{"Table_name = 'Ta1'"},
		},
		{
			insertValues: []string{"('%','Test','newroot','ta1','ca1')"},
			deleteCond:   []string{"DB = 'Test'"},
		},
		{
			insertValues: []string{"('%','test','newroot','ta1','cá1')"},
			deleteCond:   []string{"Column_name = 'cá1'"},
		},
		{
			insertValues: []string{"('%','test','newroot','tá1','ca1')"},
			deleteCond:   []string{"Table_name = 'tá1'"},
		},
		{
			insertValues: []string{"('%','tést','newroot','ta1','ca1')"},
			deleteCond:   []string{"DB = 'tést'"},
		},
		{
			insertValues: []string{"('%','tést','newroot','ta1','ca1')", "('%','Test','newroot','ta1','ca1')"},
			deleteCond:   []string{"DB = 'tést'", "DB = 'Test'"},
		},
	}
	for _, cs := range cases {
		for _, v := range cs.insertValues {
			mse.MustExecute(fmt.Sprintf("INSERT INTO __TiDB_BR_Temporary_mysql.columns_priv (Host,DB,User,Table_name,Column_name) VALUES %s", v))
		}
		err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "columns_priv", backupTable, downstreamColumnsTable)
		require.Error(t, err)
		for _, v := range cs.deleteCond {
			mse.MustExecute(fmt.Sprintf("DELETE FROM __TiDB_BR_Temporary_mysql.columns_priv WHERE %s", v))
		}
		err = rc.CheckPrivilegeTableRowsCollateCompatibility(ctx, "mysql", "columns_priv", backupTable, downstreamColumnsTable)
		require.NoError(t, err)
	}
	mse.MustExecute("DROP TABLE __TiDB_BR_Temporary_mysql.columns_priv")
}

// NOTICE: Once there is a new system table, BR needs to ensure that it is correctly classified:
//
// - IF it is an unrecoverable table, please add the table name into `unRecoverableTable`.
// - IF it is an system privilege table, please add the table name into `sysPrivilegeTableMap`.
// - IF it is an statistics table, please add the table name into `statsTables`.
//
// NOTICE: Once the schema of the statistics table updates, please update the `upgradeStatsTableSchemaList`
// and `downgradeStatsTableSchemaList`.
//
// The above variables are in the file br/pkg/restore/systable_restore.go
func TestMonitorTheSystemTableIncremental(t *testing.T) {
	require.Equal(t, int64(255), session.CurrentBootstrapVersion)
}

func TestIsStatsTemporaryTable(t *testing.T) {
	require.False(t, snapclient.IsStatsTemporaryTable("", ""))
	require.False(t, snapclient.IsStatsTemporaryTable("", "stats_meta"))
	require.False(t, snapclient.IsStatsTemporaryTable("mysql", "stats_meta"))
	require.False(t, snapclient.IsStatsTemporaryTable("__TiDB_BR_Temporary_test", "stats_meta"))
	require.True(t, snapclient.IsStatsTemporaryTable("__TiDB_BR_Temporary_mysql", "stats_meta"))
	require.False(t, snapclient.IsStatsTemporaryTable("__TiDB_BR_Temporary_mysql", "test"))
}

func TestGetDBNameIfStatsTemporaryTable(t *testing.T) {
	_, ok := snapclient.GetDBNameIfStatsTemporaryTable("", "")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfStatsTemporaryTable("", "stats_meta")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfStatsTemporaryTable("mysql", "stats_meta")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfStatsTemporaryTable("__TiDB_BR_Temporary_test", "stats_meta")
	require.False(t, ok)
	name, ok := snapclient.GetDBNameIfStatsTemporaryTable("__TiDB_BR_Temporary_mysql", "stats_meta")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = snapclient.GetDBNameIfStatsTemporaryTable("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)
}

func TestTemporaryTableCheckerForStatsTemporaryTable(t *testing.T) {
	checker := snapclient.NewTemporaryTableChecker(true, false)
	_, ok := checker.CheckTemporaryTables("", "")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "stats_meta")
	require.False(t, ok)
	name, ok := checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "stats_meta")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)

	_, ok = checker.CheckTemporaryTables("", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)
}

func TestIsRenameableSysTemporaryTable(t *testing.T) {
	require.False(t, snapclient.IsRenameableSysTemporaryTable("", ""))
	require.False(t, snapclient.IsRenameableSysTemporaryTable("", "user"))
	require.False(t, snapclient.IsRenameableSysTemporaryTable("mysql", "user"))
	require.False(t, snapclient.IsRenameableSysTemporaryTable("__TiDB_BR_Temporary_test", "user"))
	require.True(t, snapclient.IsRenameableSysTemporaryTable("__TiDB_BR_Temporary_mysql", "user"))
	require.False(t, snapclient.IsRenameableSysTemporaryTable("__TiDB_BR_Temporary_mysql", "test"))
}

func TestGetDBNameIfRenameableSysTemporaryTable(t *testing.T) {
	_, ok := snapclient.GetDBNameIfRenameableSysTemporaryTable("", "")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfRenameableSysTemporaryTable("", "user")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfRenameableSysTemporaryTable("mysql", "user")
	require.False(t, ok)
	_, ok = snapclient.GetDBNameIfRenameableSysTemporaryTable("__TiDB_BR_Temporary_test", "user")
	require.False(t, ok)
	name, ok := snapclient.GetDBNameIfRenameableSysTemporaryTable("__TiDB_BR_Temporary_mysql", "user")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = snapclient.GetDBNameIfRenameableSysTemporaryTable("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)
}

func TestTemporaryTableCheckerForRenameableSysTemporaryTable(t *testing.T) {
	checker := snapclient.NewTemporaryTableChecker(false, true)
	_, ok := checker.CheckTemporaryTables("", "")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "user")
	require.False(t, ok)
	name, ok := checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "user")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)

	_, ok = checker.CheckTemporaryTables("", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)
}

func TestTemporaryTableChecker(t *testing.T) {
	checker := snapclient.NewTemporaryTableChecker(true, true)
	_, ok := checker.CheckTemporaryTables("", "")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "user")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "user")
	require.False(t, ok)
	name, ok := checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "user")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)

	_, ok = checker.CheckTemporaryTables("", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("mysql", "stats_meta")
	require.False(t, ok)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_test", "stats_meta")
	require.False(t, ok)
	name, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "stats_meta")
	require.True(t, ok)
	require.Equal(t, "mysql", name)
	_, ok = checker.CheckTemporaryTables("__TiDB_BR_Temporary_mysql", "test")
	require.False(t, ok)
}

func TestGenerateMoveRenamedTableSQLPair(t *testing.T) {
	renameSQL := snapclient.GenerateMoveRenamedTableSQLPair(123, map[string]map[string]struct{}{
		"mysql": {"stats_meta": struct{}{}, "stats_buckets": struct{}{}, "stats_top_n": struct{}{}},
	})
	require.Contains(t, renameSQL, "mysql.stats_meta TO __TiDB_BR_Temporary_mysql.stats_meta_deleted_123")
	require.Contains(t, renameSQL, "__TiDB_BR_Temporary_mysql.stats_meta TO mysql.stats_meta")
	require.Contains(t, renameSQL, "mysql.stats_buckets TO __TiDB_BR_Temporary_mysql.stats_buckets_deleted_123")
	require.Contains(t, renameSQL, "__TiDB_BR_Temporary_mysql.stats_buckets TO mysql.stats_buckets")
	require.Contains(t, renameSQL, "mysql.stats_top_n TO __TiDB_BR_Temporary_mysql.stats_top_n_deleted_123")
	require.Contains(t, renameSQL, "__TiDB_BR_Temporary_mysql.stats_top_n TO mysql.stats_top_n")
}

func TestNotifyUpdateAllUsersPrivilege(t *testing.T) {
	notifier := func() error {
		return errors.Errorf("test")
	}
	err := snapclient.NotifyUpdateAllUsersPrivilege(map[string]map[string]struct{}{
		"test": {"user": {}},
	}, notifier)
	require.NoError(t, err)
	err = snapclient.NotifyUpdateAllUsersPrivilege(map[string]map[string]struct{}{
		"mysql": {"use": {}, "test": {}},
	}, notifier)
	require.NoError(t, err)
	err = snapclient.NotifyUpdateAllUsersPrivilege(map[string]map[string]struct{}{
		"mysql": {"test": {}, "user": {}, "db": {}},
	}, notifier)
	require.Error(t, err)
}
