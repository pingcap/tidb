// Copyright 2026 PingCAP, Inc.
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

package restore_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

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
	rc := restore.Client{}
	defer rc.Close()
	err := rc.Init(g, cluster.Storage)
	require.NoError(t, err)

	se, err := g.CreateSession(cluster.Storage)
	require.NoError(t, err)
	defer se.Close()
	mse := &mustExecuteSession{ctx, se, t}
	mse.MustExecute("CREATE DATABASE __TiDB_BR_Temporary_mysql")
	defer mse.MustExecute("DROP DATABASE __TiDB_BR_Temporary_mysql")

	downstreamDBTable, err := rc.GetTableSchema(cluster.Domain, model.NewCIStr("mysql"), model.NewCIStr("db"))
	require.NoError(t, err)
	downstreamTablesTable, err := rc.GetTableSchema(cluster.Domain, model.NewCIStr("mysql"), model.NewCIStr("tables_priv"))
	require.NoError(t, err)
	downstreamColumnsTable, err := rc.GetTableSchema(cluster.Domain, model.NewCIStr("mysql"), model.NewCIStr("columns_priv"))
	require.NoError(t, err)
	// case 1: privilege db
	mse.MustExecute(CreateDBSQL)
	backupTable, err := rc.GetTableSchema(cluster.Domain, model.NewCIStr("__TiDB_BR_Temporary_mysql"), model.NewCIStr("db"))
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
	backupTable, err = rc.GetTableSchema(cluster.Domain, model.NewCIStr("__TiDB_BR_Temporary_mysql"), model.NewCIStr("tables_priv"))
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
	backupTable, err = rc.GetTableSchema(cluster.Domain, model.NewCIStr("__TiDB_BR_Temporary_mysql"), model.NewCIStr("columns_priv"))
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
