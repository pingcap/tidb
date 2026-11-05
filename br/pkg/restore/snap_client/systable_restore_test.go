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

	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
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
	err := client.Init(g, cluster.Storage)
	require.NoError(t, err)

	info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(pmodel.NewCIStr(mysql.SystemDB))
	require.True(t, isExist)
	tmpSysDB := dbSchema.Clone()
	tmpSysDB.Name = utils.TemporaryDBName(mysql.SystemDB)
	sysDB := pmodel.NewCIStr(mysql.SystemDB)
	userTI, err := restore.GetTableSchema(cluster.Domain, sysDB, pmodel.NewCIStr("user"))
	require.NoError(t, err)

	// user table in cluster have more columns(success)
	mockedUserTI := userTI.Clone()
	userTI.Columns = append(userTI.Columns, &model.ColumnInfo{Name: pmodel.NewCIStr("new-name")})
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)
	userTI.Columns = userTI.Columns[:len(userTI.Columns)-1]

	// user table in cluster have less columns(failed)
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns = append(mockedUserTI.Columns, &model.ColumnInfo{Name: pmodel.NewCIStr("new-name")})
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// column order mismatch(success)
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[4], mockedUserTI.Columns[5] = mockedUserTI.Columns[5], mockedUserTI.Columns[4]
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)

	// incompatible column type
	mockedUserTI = userTI.Clone()
	mockedUserTI.Columns[0].FieldType.SetFlen(2000) // Columns[0] is `Host` char(255)
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))

	// compatible
	mockedUserTI = userTI.Clone()
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedUserTI,
	}})
	require.NoError(t, err)

	// Older privilege tables can omit Operate_view_priv when its configured
	// dependency columns are available.
	mockedUserTI = cloneTableInfoWithoutColumn(userTI, "Operate_view_priv")
	require.NoError(t, snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB: tmpSysDB, Info: mockedUserTI,
	}}))
	mockedUserTI = cloneTableInfoWithoutColumn(mockedUserTI, "Super_priv")
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB: tmpSysDB, Info: mockedUserTI,
	}})))

	// use the mysql.db table to test for column count mismatch.
	dbTI, err := restore.GetTableSchema(cluster.Domain, sysDB, pmodel.NewCIStr("db"))
	require.NoError(t, err)

	mockedDBWithoutOperateView := cloneTableInfoWithoutColumn(dbTI, "Operate_view_priv")
	require.NoError(t, snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB: tmpSysDB, Info: mockedDBWithoutOperateView,
	}}))

	// other system tables in cluster have more columns(failed)
	mockedDBTI := dbTI.Clone()
	originalDBColumns := dbTI.Columns
	t.Cleanup(func() { dbTI.Columns = originalDBColumns })
	dbTI.Columns = append(dbTI.Columns, &model.ColumnInfo{Name: pmodel.NewCIStr("new-name")})
	err = snapclient.CheckSysTableCompatibility(cluster.Domain, []*metautil.Table{{
		DB:   tmpSysDB,
		Info: mockedDBTI,
	}})
	require.True(t, berrors.ErrRestoreIncompatibleSys.Equal(err))
}

func TestBuildSystemTableReplaceColumns(t *testing.T) {
	cluster := mc
	sysDB := pmodel.NewCIStr(mysql.SystemDB)
	userTI, err := restore.GetTableSchema(cluster.Domain, sysDB, pmodel.NewCIStr("user"))
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

	dbTI, err := restore.GetTableSchema(cluster.Domain, sysDB, pmodel.NewCIStr("db"))
	require.NoError(t, err)
	oldDBTI := cloneTableInfoWithoutColumn(dbTI, "Operate_view_priv")
	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "db", oldDBTI, dbTI)
	require.NoError(t, err)
	require.Contains(t, columnNames, "`operate_view_priv`")
	require.Len(t, columnExpressions, len(columnNames))
	require.Contains(t, columnExpressions, "'N'")

	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "user", userTI, userTI)
	require.NoError(t, err)
	require.Contains(t, columnNames, "`operate_view_priv`")
	require.Contains(t, columnExpressions, "`operate_view_priv`")

	downstreamWithFutureColumn := userTI.Clone()
	downstreamWithFutureColumn.Columns = append(downstreamWithFutureColumn.Columns, &model.ColumnInfo{Name: pmodel.NewCIStr("future_priv")})
	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "user", oldUserTI, downstreamWithFutureColumn)
	require.NoError(t, err)
	require.Len(t, columnExpressions, len(columnNames))
	require.NotContains(t, columnNames, "`future_priv`")

	downstreamWithRequiredFutureColumn := userTI.Clone()
	requiredFutureColumn := &model.ColumnInfo{Name: pmodel.NewCIStr("future_required")}
	requiredFutureColumn.AddFlag(mysql.NotNullFlag)
	downstreamWithRequiredFutureColumn.Columns = append(downstreamWithRequiredFutureColumn.Columns, requiredFutureColumn)
	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "user", oldUserTI, downstreamWithRequiredFutureColumn)
	require.NoError(t, err)
	require.Len(t, columnExpressions, len(columnNames))
	require.NotContains(t, columnNames, "`future_required`")

	downstreamDBWithFutureColumn := dbTI.Clone()
	downstreamDBWithFutureColumn.Columns = append(downstreamDBWithFutureColumn.Columns, &model.ColumnInfo{Name: pmodel.NewCIStr("future_db_col")})
	columnNames, columnExpressions, err = snapclient.BuildSystemTableReplaceColumns(mysql.SystemDB, "db", oldDBTI, downstreamDBWithFutureColumn)
	require.NoError(t, err)
	require.Len(t, columnExpressions, len(columnNames))
	require.NotContains(t, columnNames, "`future_db_col`")
}

func cloneTableInfoWithoutColumn(ti *model.TableInfo, colName string) *model.TableInfo {
	clone := ti.Clone()
	colName = pmodel.NewCIStr(colName).L
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

func TestRestoreSystemSchemasUpgradeOperateViewPrivilege(t *testing.T) {
	cluster := mc
	ctx := context.Background()
	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, split.DefaultTestKeepaliveCfg)
	err := client.Init(g, cluster.Storage)
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
	)
	cleanup := func() {
		tk.MustExec("DROP DATABASE IF EXISTS __TiDB_BR_Temporary_mysql")
		tk.MustExec(fmt.Sprintf("ALTER TABLE mysql.user DROP COLUMN IF EXISTS %s", extraColumn))
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
	tk.MustExec(fmt.Sprintf("ALTER TABLE mysql.user ADD COLUMN %s int", extraColumn))

	tk.MustExec(fmt.Sprintf("INSERT INTO __TiDB_BR_Temporary_mysql.user (Host, User, authentication_string, plugin, Super_priv) VALUES ('%%', '%s', '', 'mysql_native_password', 'Y')", userName))
	tk.MustExec(fmt.Sprintf("INSERT INTO __TiDB_BR_Temporary_mysql.db (Host, DB, User, Show_view_priv) VALUES ('%%', '%s', '%s', 'Y')", dbName, userName))
	tk.MustExec(fmt.Sprintf("INSERT INTO __TiDB_BR_Temporary_mysql.tables_priv (Host, DB, User, Table_name, Table_priv) VALUES ('%%', '%s', '%s', '%s', 'Select,Show View')", dbName, userName, tableName))

	info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	tmpSysDB, ok := info.SchemaByName(utils.TemporaryDBName(mysql.SystemDB))
	require.True(t, ok)
	backupTables := make([]*metautil.Table, 0, 3)
	for _, name := range []string{"user", "db", "tables_priv"} {
		ti, err := restore.GetTableSchema(cluster.Domain, tmpSysDB.Name, pmodel.NewCIStr(name))
		require.NoError(t, err)
		backupTables = append(backupTables, &metautil.Table{DB: tmpSysDB, Info: ti})
	}
	require.NoError(t, snapclient.CheckSysTableCompatibility(cluster.Domain, backupTables))
	client.SetDatabases(map[string]*metautil.Database{
		tmpSysDB.Name.O: {
			Info:   tmpSysDB,
			Tables: backupTables,
		},
	})

	err = client.RestoreSystemSchemas(ctx, filter.CaseInsensitive(filter.NewSchemasFilter(mysql.SystemDB)))
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
}

// NOTICE: Once there is a new system table, BR needs to ensure that it is correctly classified:
//
// - IF it is an unrecoverable table, please add the table name into `unRecoverableTable`.
// - IF it is an system privilege table, please add the table name into `sysPrivilegeTableMap`.
// - IF it is an statistics table, please add the table name into `statsTables`.
//
// The above variables are in the file br/pkg/restore/systable_restore.go
func TestMonitorTheSystemTableIncremental(t *testing.T) {
	require.Equal(t, int64(223), session.CurrentBootstrapVersion)
}
