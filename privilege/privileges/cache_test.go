// Copyright 2016 PingCAP, Inc.
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

package privileges_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestLoadUserTable(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, "use mysql;")
	mustExec(t, se, "truncate table user;")

	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	require.Len(t, p.User, 0)

	// Host | User | authentication_string | Select_priv | Insert_priv | Update_priv | Delete_priv | Create_priv | Drop_priv | Process_priv | Grant_priv | References_priv | Alter_priv | Show_db_priv | Super_priv | Execute_priv | Index_priv | Create_user_priv | Trigger_priv
	mustExec(t, se, `INSERT INTO mysql.user (Host, User, authentication_string, Select_priv) VALUES ("%", "root", "", "Y")`)
	mustExec(t, se, `INSERT INTO mysql.user (Host, User, authentication_string, Insert_priv) VALUES ("%", "root1", "admin", "Y")`)
	mustExec(t, se, `INSERT INTO mysql.user (Host, User, authentication_string, Update_priv, Show_db_priv, References_priv) VALUES ("%", "root11", "", "Y", "Y", "Y")`)
	mustExec(t, se, `INSERT INTO mysql.user (Host, User, authentication_string, Create_user_priv, Index_priv, Execute_priv, Create_view_priv, Show_view_priv, Show_db_priv, Super_priv, Trigger_priv) VALUES ("%", "root111", "", "Y",  "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)

	p = privileges.MySQLPrivilege{}
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	require.Len(t, p.User, len(p.UserMap))

	user := p.User
	require.Equal(t, "root", user[0].User)
	require.Equal(t, mysql.SelectPriv, user[0].Privileges)
	require.Equal(t, mysql.InsertPriv, user[1].Privileges)
	require.Equal(t, mysql.UpdatePriv|mysql.ShowDBPriv|mysql.ReferencesPriv, user[2].Privileges)
	require.Equal(t, mysql.CreateUserPriv|mysql.IndexPriv|mysql.ExecutePriv|mysql.CreateViewPriv|mysql.ShowViewPriv|mysql.ShowDBPriv|mysql.SuperPriv|mysql.TriggerPriv, user[3].Privileges)
}

func TestLoadGlobalPrivTable(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, "use mysql;")
	mustExec(t, se, "truncate table global_priv")

	mustExec(t, se, `INSERT INTO mysql.global_priv VALUES ("%", "tu", "{\"access\":0,\"plugin\":\"mysql_native_password\",\"ssl_type\":3,
				\"ssl_cipher\":\"cipher\",\"x509_subject\":\"\C=ZH1\", \"x509_issuer\":\"\C=ZH2\", \"san\":\"\IP:127.0.0.1, IP:1.1.1.1, DNS:pingcap.com, URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me1\", \"password_last_changed\":1}")`)

	var p privileges.MySQLPrivilege
	err = p.LoadGlobalPrivTable(se)
	require.NoError(t, err)
	require.Equal(t, `%`, p.Global["tu"][0].Host)
	require.Equal(t, `tu`, p.Global["tu"][0].User)
	require.Equal(t, privileges.SslTypeSpecified, p.Global["tu"][0].Priv.SSLType)
	require.Equal(t, "C=ZH2", p.Global["tu"][0].Priv.X509Issuer)
	require.Equal(t, "C=ZH1", p.Global["tu"][0].Priv.X509Subject)
	require.Equal(t, "IP:127.0.0.1, IP:1.1.1.1, DNS:pingcap.com, URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me1", p.Global["tu"][0].Priv.SAN)
	require.Len(t, p.Global["tu"][0].Priv.SANs[util.IP], 2)
	require.Equal(t, "pingcap.com", p.Global["tu"][0].Priv.SANs[util.DNS][0])
	require.Equal(t, "spiffe://mesh.pingcap.com/ns/timesh/sa/me1", p.Global["tu"][0].Priv.SANs[util.URI][0])
}

func TestLoadDBTable(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, "use mysql;")
	mustExec(t, se, "truncate table db;")

	mustExec(t, se, `INSERT INTO mysql.db (Host, DB, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv) VALUES ("%", "information_schema", "root", "Y", "Y", "Y", "Y", "Y")`)
	mustExec(t, se, `INSERT INTO mysql.db (Host, DB, User, Drop_priv, Grant_priv, Index_priv, Alter_priv, Create_view_priv, Show_view_priv, Execute_priv) VALUES ("%", "mysql", "root1", "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)

	var p privileges.MySQLPrivilege
	err = p.LoadDBTable(se)
	require.NoError(t, err)
	require.Len(t, p.DB, len(p.DBMap))

	require.Equal(t, mysql.SelectPriv|mysql.InsertPriv|mysql.UpdatePriv|mysql.DeletePriv|mysql.CreatePriv, p.DB[0].Privileges)
	require.Equal(t, mysql.DropPriv|mysql.GrantPriv|mysql.IndexPriv|mysql.AlterPriv|mysql.CreateViewPriv|mysql.ShowViewPriv|mysql.ExecutePriv, p.DB[1].Privileges)
}

func TestLoadTablesPrivTable(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, "use mysql;")
	mustExec(t, se, "truncate table tables_priv")

	mustExec(t, se, `INSERT INTO mysql.tables_priv VALUES ("%", "db", "user", "table", "grantor", "2017-01-04 16:33:42.235831", "Grant,Index,Alter", "Insert,Update")`)

	var p privileges.MySQLPrivilege
	err = p.LoadTablesPrivTable(se)
	require.NoError(t, err)
	require.Len(t, p.TablesPriv, len(p.TablesPrivMap))

	require.Equal(t, `%`, p.TablesPriv[0].Host)
	require.Equal(t, "db", p.TablesPriv[0].DB)
	require.Equal(t, "user", p.TablesPriv[0].User)
	require.Equal(t, "table", p.TablesPriv[0].TableName)
	require.Equal(t, mysql.GrantPriv|mysql.IndexPriv|mysql.AlterPriv, p.TablesPriv[0].TablePriv)
	require.Equal(t, mysql.InsertPriv|mysql.UpdatePriv, p.TablesPriv[0].ColumnPriv)
}

func TestLoadColumnsPrivTable(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, "use mysql;")
	mustExec(t, se, "truncate table columns_priv")

	mustExec(t, se, `INSERT INTO mysql.columns_priv VALUES ("%", "db", "user", "table", "column", "2017-01-04 16:33:42.235831", "Insert,Update")`)
	mustExec(t, se, `INSERT INTO mysql.columns_priv VALUES ("127.0.0.1", "db", "user", "table", "column", "2017-01-04 16:33:42.235831", "Select")`)

	var p privileges.MySQLPrivilege
	err = p.LoadColumnsPrivTable(se)
	require.NoError(t, err)
	require.Equal(t, `%`, p.ColumnsPriv[0].Host)
	require.Equal(t, "db", p.ColumnsPriv[0].DB)
	require.Equal(t, "user", p.ColumnsPriv[0].User)
	require.Equal(t, "table", p.ColumnsPriv[0].TableName)
	require.Equal(t, "column", p.ColumnsPriv[0].ColumnName)
	require.Equal(t, mysql.InsertPriv|mysql.UpdatePriv, p.ColumnsPriv[0].ColumnPriv)
	require.Equal(t, mysql.SelectPriv, p.ColumnsPriv[1].ColumnPriv)
}

func TestLoadDefaultRoleTable(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, "use mysql;")
	mustExec(t, se, "truncate table default_roles")

	mustExec(t, se, `INSERT INTO mysql.default_roles VALUES ("%", "test_default_roles", "localhost", "r_1")`)
	mustExec(t, se, `INSERT INTO mysql.default_roles VALUES ("%", "test_default_roles", "localhost", "r_2")`)
	var p privileges.MySQLPrivilege
	err = p.LoadDefaultRoles(se)
	require.NoError(t, err)
	require.Equal(t, `%`, p.DefaultRoles[0].Host)
	require.Equal(t, "test_default_roles", p.DefaultRoles[0].User)
	require.Equal(t, "localhost", p.DefaultRoles[0].DefaultRoleHost)
	require.Equal(t, "r_1", p.DefaultRoles[0].DefaultRoleUser)
	require.Equal(t, "localhost", p.DefaultRoles[1].DefaultRoleHost)
}

func TestPatternMatch(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	activeRoles := make([]*auth.RoleIdentity, 0)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, "USE MYSQL;")
	mustExec(t, se, "TRUNCATE TABLE mysql.user")
	mustExec(t, se, `INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("10.0.%", "root", "Y", "Y")`)
	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	require.True(t, p.RequestVerification(activeRoles, "root", "10.0.1", "test", "", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "root", "10.0.1.118", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "114.114.114.114", "test", "", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "root", "114.114.114.114", "test", "", "", mysql.PrivilegeType(0)))
	require.True(t, p.RequestVerification(activeRoles, "root", "10.0.1.118", "test", "", "", mysql.ShutdownPriv))

	mustExec(t, se, "TRUNCATE TABLE mysql.user")
	mustExec(t, se, `INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("", "root", "Y", "N")`)
	p = privileges.MySQLPrivilege{}
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	require.True(t, p.RequestVerification(activeRoles, "root", "", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "notnull", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "", "test", "", "", mysql.ShutdownPriv))

	// Pattern match for DB.
	mustExec(t, se, "TRUNCATE TABLE mysql.user")
	mustExec(t, se, "TRUNCATE TABLE mysql.db")
	mustExec(t, se, `INSERT INTO mysql.db (user,host,db,select_priv) values ('genius', '%', 'te%', 'Y')`)
	err = p.LoadDBTable(se)
	require.NoError(t, err)
	require.True(t, p.RequestVerification(activeRoles, "genius", "127.0.0.1", "test", "", "", mysql.SelectPriv))
}

func TestHostMatch(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	activeRoles := make([]*auth.RoleIdentity, 0)
	require.NoError(t, err)
	defer se.Close()

	// Host name can be IPv4 address + netmask.
	mustExec(t, se, "USE MYSQL;")
	mustExec(t, se, "TRUNCATE TABLE mysql.user")
	mustExec(t, se, `INSERT INTO mysql.user (HOST, USER, authentication_string, Select_priv, Shutdown_priv) VALUES ("172.0.0.0/255.0.0.0", "root", "", "Y", "Y")`)
	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	require.True(t, p.RequestVerification(activeRoles, "root", "172.0.0.1", "test", "", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "root", "172.1.1.1", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "198.0.0.1", "test", "", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "root", "198.0.0.1", "test", "", "", mysql.PrivilegeType(0)))
	require.True(t, p.RequestVerification(activeRoles, "root", "172.0.0.1", "test", "", "", mysql.ShutdownPriv))
	mustExec(t, se, `TRUNCATE TABLE mysql.user`)

	// Invalid host name, the user can be created, but cannot login.
	cases := []string{
		"127.0.0.0/24",
		"127.0.0.1/255.0.0.0",
		"127.0.0.0/255.0.0",
		"127.0.0.0/255.0.0.0.0",
		"127%/255.0.0.0",
		"127.0.0.0/%",
		"127.0.0.%/%",
		"127%/%",
	}
	for _, IPMask := range cases {
		sql := fmt.Sprintf(`INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("%s", "root", "Y", "Y")`, IPMask)
		mustExec(t, se, sql)
		p = privileges.MySQLPrivilege{}
		err = p.LoadUserTable(se)
		require.NoError(t, err)
		require.False(t, p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", mysql.SelectPriv), fmt.Sprintf("test case: %s", IPMask))
		require.False(t, p.RequestVerification(activeRoles, "root", "127.0.0.0", "test", "", "", mysql.SelectPriv), fmt.Sprintf("test case: %s", IPMask))
		require.False(t, p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.ShutdownPriv), fmt.Sprintf("test case: %s", IPMask))
	}

	// Netmask notation cannot be used for IPv6 addresses.
	mustExec(t, se, `INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("2001:db8::/ffff:ffff::", "root", "Y", "Y")`)
	p = privileges.MySQLPrivilege{}
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	require.False(t, p.RequestVerification(activeRoles, "root", "2001:db8::1234", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "2001:db8::", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.ShutdownPriv))
}

func TestCaseInsensitive(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	activeRoles := make([]*auth.RoleIdentity, 0)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, "CREATE DATABASE TCTrain;")
	mustExec(t, se, "CREATE TABLE TCTrain.TCTrainOrder (id int);")
	mustExec(t, se, "TRUNCATE TABLE mysql.user")
	mustExec(t, se, `INSERT INTO mysql.db VALUES ("127.0.0.1", "TCTrain", "genius", "Y", "Y", "Y", "Y", "Y", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N")`)
	var p privileges.MySQLPrivilege
	err = p.LoadDBTable(se)
	require.NoError(t, err)
	// DB and Table names are case insensitive in MySQL.
	require.True(t, p.RequestVerification(activeRoles, "genius", "127.0.0.1", "TCTrain", "TCTrainOrder", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "genius", "127.0.0.1", "TCTRAIN", "TCTRAINORDER", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "genius", "127.0.0.1", "tctrain", "tctrainorder", "", mysql.SelectPriv))
}

func TestLoadRoleGraph(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, "use mysql;")
	mustExec(t, se, "truncate table user;")

	var p privileges.MySQLPrivilege
	err = p.LoadRoleGraph(se)
	require.NoError(t, err)
	require.Len(t, p.User, 0)

	mustExec(t, se, `INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_1", "%", "user2")`)
	mustExec(t, se, `INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_2", "%", "root")`)
	mustExec(t, se, `INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_3", "%", "user1")`)
	mustExec(t, se, `INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_4", "%", "root")`)

	p = privileges.MySQLPrivilege{}
	err = p.LoadRoleGraph(se)
	require.NoError(t, err)
	graph := p.RoleGraph
	require.True(t, graph["root@%"].Find("r_2", "%"))
	require.True(t, graph["root@%"].Find("r_4", "%"))
	require.True(t, graph["user2@%"].Find("r_1", "%"))
	require.True(t, graph["user1@%"].Find("r_3", "%"))
	_, ok := graph["illedal"]
	require.False(t, ok)
	require.False(t, graph["root@%"].Find("r_1", "%"))
}

func TestRoleGraphBFS(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, `CREATE ROLE r_1, r_2, r_3, r_4, r_5, r_6;`)
	mustExec(t, se, `GRANT r_2 TO r_1;`)
	mustExec(t, se, `GRANT r_3 TO r_2;`)
	mustExec(t, se, `GRANT r_4 TO r_3;`)
	mustExec(t, se, `GRANT r_1 TO r_4;`)
	mustExec(t, se, `GRANT r_5 TO r_3, r_6;`)

	var p privileges.MySQLPrivilege
	err = p.LoadRoleGraph(se)
	require.NoError(t, err)

	activeRoles := make([]*auth.RoleIdentity, 0)
	ret := p.FindAllRole(activeRoles)
	require.Len(t, ret, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_1", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	require.Len(t, ret, 5)

	activeRoles = make([]*auth.RoleIdentity, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_6", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	require.Len(t, ret, 2)

	activeRoles = make([]*auth.RoleIdentity, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_3", Hostname: "%"})
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_6", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	require.Len(t, ret, 6)
}

func TestFindAllUserEffectiveRoles(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, `CREATE USER u1`)
	mustExec(t, se, `CREATE ROLE r_1, r_2, r_3, r_4;`)
	mustExec(t, se, `GRANT r_3 to r_1`)
	mustExec(t, se, `GRANT r_4 to r_2`)
	mustExec(t, se, `GRANT r_1 to u1`)
	mustExec(t, se, `GRANT r_2 to u1`)

	var p privileges.MySQLPrivilege
	err = p.LoadAll(se)
	require.NoError(t, err)
	ret := p.FindAllUserEffectiveRoles("u1", "%", []*auth.RoleIdentity{
		{Username: "r_1", Hostname: "%"},
		{Username: "r_2", Hostname: "%"},
	})
	require.Equal(t, 4, len(ret))
	require.Equal(t, "r_1", ret[0].Username)
	require.Equal(t, "r_2", ret[1].Username)
	require.Equal(t, "r_3", ret[2].Username)
	require.Equal(t, "r_4", ret[3].Username)

	mustExec(t, se, `REVOKE r_2 from u1`)
	err = p.LoadAll(se)
	require.NoError(t, err)
	ret = p.FindAllUserEffectiveRoles("u1", "%", []*auth.RoleIdentity{
		{Username: "r_1", Hostname: "%"},
		{Username: "r_2", Hostname: "%"},
	})
	require.Equal(t, 2, len(ret))
	require.Equal(t, "r_1", ret[0].Username)
	require.Equal(t, "r_3", ret[1].Username)
}

func TestAbnormalMySQLTable(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()

	// Simulate the case mysql.user is synchronized from MySQL.
	mustExec(t, se, "DROP TABLE mysql.user;")
	mustExec(t, se, "USE mysql;")
	mustExec(t, se, `CREATE TABLE user (
  Host char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
  User char(16) COLLATE utf8_bin NOT NULL DEFAULT '',
  Password char(41) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
  Select_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Insert_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Update_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Delete_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Drop_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Reload_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Shutdown_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Process_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  File_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Config_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Grant_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  References_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Index_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Alter_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Show_db_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Super_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_tmp_table_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Lock_tables_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Execute_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Repl_slave_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Repl_client_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_view_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Show_view_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_routine_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Alter_routine_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_user_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Event_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Trigger_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_tablespace_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_role_priv ENUM('N','Y') NOT NULL DEFAULT 'N',
  Drop_role_priv ENUM('N','Y') NOT NULL DEFAULT 'N',
  Account_locked ENUM('N','Y') NOT NULL DEFAULT 'N',
  ssl_type enum('','ANY','X509','SPECIFIED') CHARACTER SET utf8 NOT NULL DEFAULT '',
  ssl_cipher blob NOT NULL,
  x509_issuer blob NOT NULL,
  x509_subject blob NOT NULL,
  max_questions int(11) unsigned NOT NULL DEFAULT '0',
  max_updates int(11) unsigned NOT NULL DEFAULT '0',
  max_connections int(11) unsigned NOT NULL DEFAULT '0',
  max_user_connections int(11) unsigned NOT NULL DEFAULT '0',
  plugin char(64) COLLATE utf8_bin DEFAULT 'mysql_native_password',
  authentication_string text COLLATE utf8_bin,
  password_expired enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  PRIMARY KEY (Host,User)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='Users and global privileges';`)
	mustExec(t, se, `INSERT INTO user VALUES ('localhost','root','','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0,0,'mysql_native_password','','N');
`)
	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	activeRoles := make([]*auth.RoleIdentity, 0)
	// MySQL mysql.user table schema is not identical to TiDB, check it doesn't break privilege.
	require.True(t, p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.SelectPriv))

	// Absent of those tables doesn't cause error.
	mustExec(t, se, "DROP TABLE mysql.db;")
	mustExec(t, se, "DROP TABLE mysql.tables_priv;")
	mustExec(t, se, "DROP TABLE mysql.columns_priv;")
	err = p.LoadAll(se)
	require.NoError(t, err)
}

func TestSortUserTable(t *testing.T) {
	var p privileges.MySQLPrivilege
	p.User = []privileges.UserRecord{
		privileges.NewUserRecord(`%`, "root"),
		privileges.NewUserRecord(`%`, "jeffrey"),
		privileges.NewUserRecord("localhost", "root"),
		privileges.NewUserRecord("localhost", ""),
	}
	p.SortUserTable()
	result := []privileges.UserRecord{
		privileges.NewUserRecord("localhost", "root"),
		privileges.NewUserRecord("localhost", ""),
		privileges.NewUserRecord(`%`, "jeffrey"),
		privileges.NewUserRecord(`%`, "root"),
	}
	checkUserRecord(t, p.User, result)

	p.User = []privileges.UserRecord{
		privileges.NewUserRecord(`%`, "jeffrey"),
		privileges.NewUserRecord("h1.example.net", ""),
	}
	p.SortUserTable()
	result = []privileges.UserRecord{
		privileges.NewUserRecord("h1.example.net", ""),
		privileges.NewUserRecord(`%`, "jeffrey"),
	}
	checkUserRecord(t, p.User, result)

	p.User = []privileges.UserRecord{
		privileges.NewUserRecord(`192.168.%`, "xxx"),
		privileges.NewUserRecord(`192.168.199.%`, "xxx"),
	}
	p.SortUserTable()
	result = []privileges.UserRecord{
		privileges.NewUserRecord(`192.168.199.%`, "xxx"),
		privileges.NewUserRecord(`192.168.%`, "xxx"),
	}
	checkUserRecord(t, p.User, result)
}

func TestGlobalPrivValueRequireStr(t *testing.T) {
	var (
		none  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeNone}
		tls   = privileges.GlobalPrivValue{SSLType: privileges.SslTypeAny}
		x509  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeX509}
		spec  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, SSLCipher: "c1", X509Subject: "s1", X509Issuer: "i1"}
		spec2 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, X509Subject: "s1", X509Issuer: "i1"}
		spec3 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, X509Issuer: "i1"}
		spec4 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified}
	)
	require.Equal(t, "NONE", none.RequireStr())
	require.Equal(t, "SSL", tls.RequireStr())
	require.Equal(t, "X509", x509.RequireStr())
	require.Equal(t, "CIPHER 'c1' ISSUER 'i1' SUBJECT 's1'", spec.RequireStr())
	require.Equal(t, "ISSUER 'i1' SUBJECT 's1'", spec2.RequireStr())
	require.Equal(t, "ISSUER 'i1'", spec3.RequireStr())
	require.Equal(t, "NONE", spec4.RequireStr())
}

func checkUserRecord(t *testing.T, x, y []privileges.UserRecord) {
	require.Equal(t, len(x), len(y))
	for i := 0; i < len(x); i++ {
		require.Equal(t, x[i].User, y[i].User)
		require.Equal(t, x[i].Host, y[i].Host)
	}
}

func TestDBIsVisible(t *testing.T) {
	store, clean := newStore(t)
	defer clean()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	mustExec(t, se, "create database visdb")
	p := privileges.MySQLPrivilege{}
	err = p.LoadAll(se)
	require.NoError(t, err)

	mustExec(t, se, `INSERT INTO mysql.user (Host, User, Create_role_priv, Super_priv) VALUES ("%", "testvisdb", "Y", "Y")`)
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	isVisible := p.DBIsVisible("testvisdb", "%", "visdb")
	require.False(t, isVisible)
	mustExec(t, se, "TRUNCATE TABLE mysql.user")

	mustExec(t, se, `INSERT INTO mysql.user (Host, User, Select_priv) VALUES ("%", "testvisdb2", "Y")`)
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	isVisible = p.DBIsVisible("testvisdb2", "%", "visdb")
	require.True(t, isVisible)
	mustExec(t, se, "TRUNCATE TABLE mysql.user")

	mustExec(t, se, `INSERT INTO mysql.user (Host, User, Create_priv) VALUES ("%", "testvisdb3", "Y")`)
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	isVisible = p.DBIsVisible("testvisdb3", "%", "visdb")
	require.True(t, isVisible)
	mustExec(t, se, "TRUNCATE TABLE mysql.user")

	mustExec(t, se, `INSERT INTO mysql.user (Host, User, Insert_priv) VALUES ("%", "testvisdb4", "Y")`)
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	isVisible = p.DBIsVisible("testvisdb4", "%", "visdb")
	require.True(t, isVisible)
	mustExec(t, se, "TRUNCATE TABLE mysql.user")

	mustExec(t, se, `INSERT INTO mysql.user (Host, User, Update_priv) VALUES ("%", "testvisdb5", "Y")`)
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	isVisible = p.DBIsVisible("testvisdb5", "%", "visdb")
	require.True(t, isVisible)
	mustExec(t, se, "TRUNCATE TABLE mysql.user")

	mustExec(t, se, `INSERT INTO mysql.user (Host, User, Create_view_priv) VALUES ("%", "testvisdb6", "Y")`)
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	isVisible = p.DBIsVisible("testvisdb6", "%", "visdb")
	require.True(t, isVisible)
	mustExec(t, se, "TRUNCATE TABLE mysql.user")

	mustExec(t, se, `INSERT INTO mysql.user (Host, User, Trigger_priv) VALUES ("%", "testvisdb7", "Y")`)
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	isVisible = p.DBIsVisible("testvisdb7", "%", "visdb")
	require.True(t, isVisible)
	mustExec(t, se, "TRUNCATE TABLE mysql.user")

	mustExec(t, se, `INSERT INTO mysql.user (Host, User, References_priv) VALUES ("%", "testvisdb8", "Y")`)
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	isVisible = p.DBIsVisible("testvisdb8", "%", "visdb")
	require.True(t, isVisible)
	mustExec(t, se, "TRUNCATE TABLE mysql.user")

	mustExec(t, se, `INSERT INTO mysql.user (Host, User, Execute_priv) VALUES ("%", "testvisdb9", "Y")`)
	err = p.LoadUserTable(se)
	require.NoError(t, err)
	isVisible = p.DBIsVisible("testvisdb9", "%", "visdb")
	require.True(t, isVisible)
	mustExec(t, se, "TRUNCATE TABLE mysql.user")
}
