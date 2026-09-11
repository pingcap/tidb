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
	"github.com/pingcap/tidb/pkg/testkit"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
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
	// user table in cluster has more unrecognized columns, so fall back to logical restore.
	mockedUserTI := userTI.Clone()
	userTI.Columns = append(userTI.Columns, &model.ColumnInfo{Name: ast.NewCIStr("new-name")})
	canLoadSysTablePhysical, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}}, false)
	require.NoError(t, err)
	require.False(t, canLoadSysTablePhysical)
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

	// mysql.user backup from older TiDB may miss Operate_view_priv. It cannot be
	// loaded physically, but logical restore can fill the target default value and
	// execute a fixed compatibility expression while copying rows into mysql.user.
	mockedUserTI = cloneTableInfoWithoutColumn(userTI, "Operate_view_priv")
	canLoadSysTablePhysical, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}}, false)
	require.NoError(t, err)
	require.False(t, canLoadSysTablePhysical)

	// recognized compatible columns still require their configured dependency columns.
	mockedUserTI = cloneTableInfoWithoutColumn(userTI, "Operate_view_priv")
	mockedUserTI = cloneTableInfoWithoutColumn(mockedUserTI, "Super_priv")
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}}, false)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// use the mysql.db table to test for column count mismatch.
	dbTI, err := restore.GetTableSchema(cluster.Domain, sysDB, ast.NewCIStr("db"))
	require.NoError(t, err)

	// mysql.db backup from older TiDB may miss Operate_view_priv. It cannot be
	// loaded physically, but logical restore can fill the missing column by the
	// configured compatibility expression.
	mockedDBTI := cloneTableInfoWithoutColumn(dbTI, "Operate_view_priv")
	canLoadSysTablePhysical, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}}, false)
	require.NoError(t, err)
	require.False(t, canLoadSysTablePhysical)

	tablesPrivTI, err := restore.GetTableSchema(cluster.Domain, sysDB, ast.NewCIStr("tables_priv"))
	require.NoError(t, err)
	mockedTablesPrivTI := tablesPrivTI.Clone()
	tablePrivCol := model.FindColumnInfo(mockedTablesPrivTI.Columns, "Table_priv")
	require.NotNil(t, tablePrivCol)
	tablePrivElems := make([]string, 0, len(tablePrivCol.GetElems()))
	for _, elem := range tablePrivCol.GetElems() {
		if elem != mysql.OperateViewPriv.SetString() {
			tablePrivElems = append(tablePrivElems, elem)
		}
	}
	tablePrivCol.SetElems(tablePrivElems)
	canLoadSysTablePhysical, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedTablesPrivTI,
	}}, false)
	require.NoError(t, err)
	require.True(t, canLoadSysTablePhysical)

	mockedTablesPrivTI = tablesPrivTI.Clone()
	tablePrivCol = model.FindColumnInfo(mockedTablesPrivTI.Columns, "Table_priv")
	require.NotNil(t, tablePrivCol)
	tablePrivElems = append(tablePrivCol.GetElems(), "Future Priv")
	tablePrivCol.SetElems(tablePrivElems)
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedTablesPrivTI,
	}}, false)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// other system tables in cluster have more unrecognized columns(failed)
	mockedDBTI = dbTI.Clone()
	dbTI.Columns = append(dbTI.Columns, &model.ColumnInfo{Name: ast.NewCIStr("new-name")})
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}}, false)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))
	dbTI.Columns = dbTI.Columns[:len(dbTI.Columns)-1]

	// bind_info is also a recoverable system table and should be checked.
	bindInfoTI, err := restore.GetTableSchema(cluster.Domain, sysDB, ast.NewCIStr("bind_info"))
	require.NoError(t, err)
	mockedBindInfoTI := cloneTableInfoWithoutColumn(bindInfoTI, "last_used_date")
	canLoadSysTablePhysical, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedBindInfoTI,
	}}, false)
	require.NoError(t, err)
	require.False(t, canLoadSysTablePhysical)

	mockedBindInfoTI = cloneTableInfoWithoutColumn(bindInfoTI, "source")
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedBindInfoTI,
	}}, false)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	mockedBindInfoTI = bindInfoTI.Clone()
	mockedBindInfoTI.Columns = append(bindInfoTI.Columns, &model.ColumnInfo{Name: ast.NewCIStr("new-name")})
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedBindInfoTI,
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
	mockedDBTI.Columns[1].FieldType.SetFlen(2000) // Columns[1] is `DB` char(64)
	_, err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}}, true)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

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

func TestBuildSystemTableReplaceColumns(t *testing.T) {
	cluster := mc
	sysDB := ast.NewCIStr(mysql.SystemDB)
	userTI, err := restore.GetTableSchema(cluster.Domain, sysDB, ast.NewCIStr("user"))
	require.NoError(t, err)

	oldUserTI := cloneTableInfoWithoutColumn(userTI, "Operate_view_priv")
	columnNames, columnExpressions, err := snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "user", oldUserTI, userTI)
	require.NoError(t, err)
	require.Contains(t, columnNames, "`operate_view_priv`")
	require.Len(t, columnExpressions, len(columnNames))
	require.Contains(t, columnExpressions, "IF(`Super_priv` = 'Y', 'Y', 'N')")

	oldUserTIWithoutOperateViewDependency := cloneTableInfoWithoutColumn(oldUserTI, "Super_priv")
	_, _, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "user", oldUserTIWithoutOperateViewDependency, userTI)
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	dbTI, err := restore.GetTableSchema(cluster.Domain, sysDB, ast.NewCIStr("db"))
	require.NoError(t, err)
	oldDBTI := cloneTableInfoWithoutColumn(dbTI, "Operate_view_priv")
	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "db", oldDBTI, dbTI)
	require.NoError(t, err)
	require.Contains(t, columnNames, "`operate_view_priv`")
	require.Len(t, columnExpressions, len(columnNames))
	require.Contains(t, columnExpressions, "'N'")

	bindInfoTI, err := restore.GetTableSchema(cluster.Domain, sysDB, ast.NewCIStr("bind_info"))
	require.NoError(t, err)
	oldBindInfoTI := cloneTableInfoWithoutColumn(bindInfoTI, "last_used_date")
	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "bind_info", oldBindInfoTI, bindInfoTI)
	require.NoError(t, err)
	require.Contains(t, columnNames, "`last_used_date`")
	require.Len(t, columnExpressions, len(columnNames))
	require.Contains(t, columnExpressions, "NULL")

	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "user", userTI, userTI)
	require.NoError(t, err)
	require.Contains(t, columnNames, "`operate_view_priv`")
	require.Contains(t, columnExpressions, "`operate_view_priv`")

	downstreamWithFutureColumn := userTI.Clone()
	downstreamWithFutureColumn.Columns = append(downstreamWithFutureColumn.Columns, &model.ColumnInfo{Name: ast.NewCIStr("future_priv")})
	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "user", oldUserTI, downstreamWithFutureColumn)
	require.NoError(t, err)
	require.Len(t, columnExpressions, len(columnNames))
	require.NotContains(t, columnNames, "`future_priv`")

	downstreamWithRequiredFutureColumn := userTI.Clone()
	requiredFutureColumn := &model.ColumnInfo{Name: ast.NewCIStr("future_required")}
	requiredFutureColumn.AddFlag(mysql.NotNullFlag)
	downstreamWithRequiredFutureColumn.Columns = append(downstreamWithRequiredFutureColumn.Columns, requiredFutureColumn)
	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "user", oldUserTI, downstreamWithRequiredFutureColumn)
	require.NoError(t, err)
	require.Len(t, columnExpressions, len(columnNames))
	require.NotContains(t, columnNames, "`future_required`")

	downstreamDBWithFutureColumn := dbTI.Clone()
	downstreamDBWithFutureColumn.Columns = append(downstreamDBWithFutureColumn.Columns, &model.ColumnInfo{Name: ast.NewCIStr("future_db_col")})
	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "db", oldDBTI, downstreamDBWithFutureColumn)
	require.NoError(t, err)
	require.Len(t, columnExpressions, len(columnNames))
	require.NotContains(t, columnNames, "`future_db_col`")
}

func cloneTableInfoWithoutColumn(ti *model.TableInfo, colName string) *model.TableInfo {
	clone := ti.Clone()
	colName = ast.NewCIStr(colName).L
	columns := make([]*model.ColumnInfo, 0, len(clone.Columns))
	for _, col := range clone.Columns {
		if col.Name.L == colName {
			continue
		}
		columns = append(columns, col)
	}
	clone.Columns = columns
	return clone
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
	rc.SetCheckPrivilegeTableRowsCollateCompatibility(true)

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

func TestRestoreSystemSchemasUpgradeOperateViewPrivilege(t *testing.T) {
	cluster := mc
	ctx := context.Background()
	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, split.DefaultTestKeepaliveCfg)
	err := client.InitConnections(g, cluster.Storage)
	require.NoError(t, err)
	defer client.Close()
	client.SetWithSysTable(true)

	tk := testkit.NewTestKit(t, cluster.Storage)
	const (
		userName     = "br_restore_operate_view"
		dbName       = "br_restore_operate_view_db"
		tableName    = "br_restore_operate_view_table"
		keepUserName = "br_restore_operate_keep"
		keepDBName   = "br_restore_operate_keep_db"
		extraColumn  = "br_extra_int"
		bindSQL      = "select /* br_restore_bind_info */ 1"
		sqlDigest    = "br_restore_bind_sql_digest"
		planDigest   = "br_restore_bind_plan_digest"
	)
	cleanup := func() {
		tk.MustExec("DROP DATABASE IF EXISTS __TiDB_BR_Temporary_mysql")
		tk.MustExec(fmt.Sprintf("ALTER TABLE mysql.user DROP COLUMN IF EXISTS %s", extraColumn))
		tk.MustExec(fmt.Sprintf("DELETE FROM mysql.bind_info WHERE original_sql='%s' OR sql_digest='%s' OR plan_digest='%s'", bindSQL, sqlDigest, planDigest))
		tk.MustExec(fmt.Sprintf("DELETE FROM mysql.tables_priv WHERE User='%s' AND Host='%%' AND DB='%s' AND Table_name='%s'", userName, dbName, tableName))
		tk.MustExec(fmt.Sprintf("DELETE FROM mysql.db WHERE User='%s' AND Host='%%' AND DB='%s'", userName, dbName))
		tk.MustExec(fmt.Sprintf("DELETE FROM mysql.user WHERE User='%s' AND Host='%%'", userName))
		tk.MustExec(fmt.Sprintf("DELETE FROM mysql.db WHERE User='%s' AND Host='%%' AND DB='%s'", keepUserName, keepDBName))
		tk.MustExec(fmt.Sprintf("DELETE FROM mysql.user WHERE User='%s' AND Host='%%'", keepUserName))
	}
	cleanup()
	defer cleanup()

	tk.MustExec(fmt.Sprintf("INSERT INTO mysql.user (Host, User, authentication_string, plugin, Super_priv, Operate_view_priv) VALUES ('%%', '%s', '', 'mysql_native_password', 'Y', 'N')", keepUserName))
	tk.MustExec(fmt.Sprintf("INSERT INTO mysql.db (Host, DB, User, Operate_view_priv) VALUES ('%%', '%s', '%s', 'Y')", keepDBName, keepUserName))

	tk.MustExec("CREATE DATABASE __TiDB_BR_Temporary_mysql")
	tk.MustExec("CREATE TABLE __TiDB_BR_Temporary_mysql.user LIKE mysql.user")
	tk.MustExec("ALTER TABLE __TiDB_BR_Temporary_mysql.user DROP COLUMN Operate_view_priv")
	tk.MustExec("CREATE TABLE __TiDB_BR_Temporary_mysql.db LIKE mysql.db")
	tk.MustExec("ALTER TABLE __TiDB_BR_Temporary_mysql.db DROP COLUMN Operate_view_priv")
	tk.MustExec("CREATE TABLE __TiDB_BR_Temporary_mysql.tables_priv LIKE mysql.tables_priv")
	tk.MustExec("ALTER TABLE __TiDB_BR_Temporary_mysql.tables_priv MODIFY COLUMN Table_priv SET('Select','Insert','Update','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References') DEFAULT NULL")
	tk.MustExec("CREATE TABLE __TiDB_BR_Temporary_mysql.bind_info LIKE mysql.bind_info")
	tk.MustExec("ALTER TABLE __TiDB_BR_Temporary_mysql.bind_info DROP COLUMN last_used_date")
	tk.MustExec(fmt.Sprintf("ALTER TABLE mysql.user ADD COLUMN %s int", extraColumn))

	tk.MustExec(fmt.Sprintf("INSERT INTO __TiDB_BR_Temporary_mysql.user (Host, User, authentication_string, plugin, Super_priv) VALUES ('%%', '%s', '', 'mysql_native_password', 'Y')", userName))
	tk.MustExec(fmt.Sprintf("INSERT INTO __TiDB_BR_Temporary_mysql.db (Host, DB, User, Show_view_priv) VALUES ('%%', '%s', '%s', 'Y')", dbName, userName))
	tk.MustExec(fmt.Sprintf("INSERT INTO __TiDB_BR_Temporary_mysql.tables_priv (Host, DB, User, Table_name, Table_priv) VALUES ('%%', '%s', '%s', '%s', 'Select,Show View')", dbName, userName, tableName))
	tk.MustExec(fmt.Sprintf(`INSERT INTO __TiDB_BR_Temporary_mysql.bind_info (
			original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest, plan_digest
		) VALUES ('%s', '%s', '%s', 'enabled', '2020-01-01 00:00:00', '2020-01-01 00:00:00', 'utf8mb4', 'utf8mb4_bin', 'manual', '%s', '%s')`,
		bindSQL, bindSQL, dbName, sqlDigest, planDigest))

	info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	tmpSysDB, ok := info.SchemaByName(utils.TemporaryDBName(mysql.SystemDB))
	require.True(t, ok)
	backupTables := make([]*metautil.Table, 0, 4)
	for _, name := range []string{"user", "db", "tables_priv", "bind_info"} {
		ti, err := restore.GetTableSchema(cluster.Domain, tmpSysDB.Name, ast.NewCIStr(name))
		require.NoError(t, err)
		backupTables = append(backupTables, &metautil.Table{DB: tmpSysDB, Info: ti})
	}
	client.SetDatabases(map[string]*metautil.Database{
		tmpSysDB.Name.O: {
			Info:   tmpSysDB,
			Tables: backupTables,
		},
	})

	err = client.RestoreSystemSchemas(ctx, filter.CaseInsensitive(filter.NewSchemasFilter(mysql.SystemDB)), false)
	require.NoError(t, err)

	tk.MustQuery(fmt.Sprintf("SELECT Operate_view_priv FROM mysql.user WHERE User='%s' AND Host='%%'", userName)).
		Check(testkit.Rows("Y"))
	tk.MustQuery(fmt.Sprintf("SELECT %s IS NULL FROM mysql.user WHERE User='%s' AND Host='%%'", extraColumn, userName)).
		Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("SELECT Operate_view_priv FROM mysql.db WHERE User='%s' AND Host='%%' AND DB='%s'", userName, dbName)).
		Check(testkit.Rows("N"))
	tk.MustQuery(fmt.Sprintf("SELECT Table_priv FROM mysql.tables_priv WHERE User='%s' AND Host='%%' AND DB='%s' AND Table_name='%s'", userName, dbName, tableName)).
		Check(testkit.Rows("Select,Show View"))
	tk.MustQuery(fmt.Sprintf("SELECT Operate_view_priv FROM mysql.user WHERE User='%s' AND Host='%%'", keepUserName)).
		Check(testkit.Rows("N"))
	tk.MustQuery(fmt.Sprintf("SELECT Operate_view_priv FROM mysql.db WHERE User='%s' AND Host='%%' AND DB='%s'", keepUserName, keepDBName)).
		Check(testkit.Rows("Y"))
	tk.MustQuery(fmt.Sprintf("SELECT COUNT(*) FROM mysql.bind_info WHERE original_sql='%s' AND last_used_date IS NULL", bindSQL)).
		Check(testkit.Rows("1"))
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
	require.Equal(t, int64(287), session.CurrentBootstrapVersion)
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
