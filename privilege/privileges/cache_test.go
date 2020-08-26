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
// See the License for the specific language governing permissions and
// limitations under the License.

package privileges_test

import (
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
)

var _ = Suite(&testCacheSuite{})

type testCacheSuite struct {
	store  kv.Storage
	domain *domain.Domain
}

func (s *testCacheSuite) SetUpSuite(c *C) {
	store, err := mockstore.NewMockTikvStore()
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	c.Assert(err, IsNil)
	s.domain, err = session.BootstrapSession(store)
	c.Assert(err, IsNil)
	s.store = store
}

func (s *testCacheSuite) TearDownSuit(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *testCacheSuite) TestLoadUserTable(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table user;")

	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(len(p.User), Equals, 0)

	// Host | User | authentication_string | Select_priv | Insert_priv | Update_priv | Delete_priv | Create_priv | Drop_priv | Process_priv | Grant_priv | References_priv | Alter_priv | Show_db_priv | Super_priv | Execute_priv | Index_priv | Create_user_priv | Trigger_priv
	mustExec(c, se, `INSERT INTO mysql.user (Host, User, authentication_string, Select_priv) VALUES ("%", "root", "", "Y")`)
	mustExec(c, se, `INSERT INTO mysql.user (Host, User, authentication_string, Insert_priv) VALUES ("%", "root1", "admin", "Y")`)
	mustExec(c, se, `INSERT INTO mysql.user (Host, User, authentication_string, Update_priv, Show_db_priv, References_priv) VALUES ("%", "root11", "", "Y", "Y", "Y")`)
	mustExec(c, se, `INSERT INTO mysql.user (Host, User, authentication_string, Create_user_priv, Index_priv, Execute_priv, Create_view_priv, Show_view_priv, Show_db_priv, Super_priv, Trigger_priv) VALUES ("%", "root111", "", "Y",  "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)

	p = privileges.MySQLPrivilege{}
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.User, HasLen, len(p.UserMap))

	user := p.User
	c.Assert(user[0].User, Equals, "root")
	c.Assert(user[0].Privileges, Equals, mysql.SelectPriv)
	c.Assert(user[1].Privileges, Equals, mysql.InsertPriv)
	c.Assert(user[2].Privileges, Equals, mysql.UpdatePriv|mysql.ShowDBPriv|mysql.ReferencesPriv)
	c.Assert(user[3].Privileges, Equals, mysql.CreateUserPriv|mysql.IndexPriv|mysql.ExecutePriv|mysql.CreateViewPriv|mysql.ShowViewPriv|mysql.ShowDBPriv|mysql.SuperPriv|mysql.TriggerPriv)
}

func (s *testCacheSuite) TestLoadGlobalPrivTable(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table global_priv")

	mustExec(c, se, `INSERT INTO mysql.global_priv VALUES ("%", "tu", "{\"access\":0,\"plugin\":\"mysql_native_password\",\"ssl_type\":3,
				\"ssl_cipher\":\"cipher\",\"x509_subject\":\"\C=ZH1\", \"x509_issuer\":\"\C=ZH2\", \"san\":\"\IP:127.0.0.1, IP:1.1.1.1, DNS:pingcap.com, URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me1\", \"password_last_changed\":1}")`)

	var p privileges.MySQLPrivilege
	err = p.LoadGlobalPrivTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.Global["tu"][0].Host, Equals, `%`)
	c.Assert(p.Global["tu"][0].User, Equals, `tu`)
	c.Assert(p.Global["tu"][0].Priv.SSLType, Equals, privileges.SslTypeSpecified)
	c.Assert(p.Global["tu"][0].Priv.X509Issuer, Equals, "C=ZH2")
	c.Assert(p.Global["tu"][0].Priv.X509Subject, Equals, "C=ZH1")
	c.Assert(p.Global["tu"][0].Priv.SAN, Equals, "IP:127.0.0.1, IP:1.1.1.1, DNS:pingcap.com, URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me1")
	c.Assert(len(p.Global["tu"][0].Priv.SANs[util.IP]), Equals, 2)
	c.Assert(p.Global["tu"][0].Priv.SANs[util.DNS][0], Equals, "pingcap.com")
	c.Assert(p.Global["tu"][0].Priv.SANs[util.URI][0], Equals, "spiffe://mesh.pingcap.com/ns/timesh/sa/me1")
}

func (s *testCacheSuite) TestLoadDBTable(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table db;")

	mustExec(c, se, `INSERT INTO mysql.db (Host, DB, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv) VALUES ("%", "information_schema", "root", "Y", "Y", "Y", "Y", "Y")`)
	mustExec(c, se, `INSERT INTO mysql.db (Host, DB, User, Drop_priv, Grant_priv, Index_priv, Alter_priv, Create_view_priv, Show_view_priv, Execute_priv) VALUES ("%", "mysql", "root1", "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)

	var p privileges.MySQLPrivilege
	err = p.LoadDBTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.DB, HasLen, len(p.DBMap))

	c.Assert(p.DB[0].Privileges, Equals, mysql.SelectPriv|mysql.InsertPriv|mysql.UpdatePriv|mysql.DeletePriv|mysql.CreatePriv)
	c.Assert(p.DB[1].Privileges, Equals, mysql.DropPriv|mysql.GrantPriv|mysql.IndexPriv|mysql.AlterPriv|mysql.CreateViewPriv|mysql.ShowViewPriv|mysql.ExecutePriv)
}

func (s *testCacheSuite) TestLoadTablesPrivTable(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table tables_priv")

	mustExec(c, se, `INSERT INTO mysql.tables_priv VALUES ("%", "db", "user", "table", "grantor", "2017-01-04 16:33:42.235831", "Grant,Index,Alter", "Insert,Update")`)

	var p privileges.MySQLPrivilege
	err = p.LoadTablesPrivTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.TablesPriv, HasLen, len(p.TablesPrivMap))

	c.Assert(p.TablesPriv[0].Host, Equals, `%`)
	c.Assert(p.TablesPriv[0].DB, Equals, "db")
	c.Assert(p.TablesPriv[0].User, Equals, "user")
	c.Assert(p.TablesPriv[0].TableName, Equals, "table")
	c.Assert(p.TablesPriv[0].TablePriv, Equals, mysql.GrantPriv|mysql.IndexPriv|mysql.AlterPriv)
	c.Assert(p.TablesPriv[0].ColumnPriv, Equals, mysql.InsertPriv|mysql.UpdatePriv)
}

func (s *testCacheSuite) TestLoadColumnsPrivTable(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table columns_priv")

	mustExec(c, se, `INSERT INTO mysql.columns_priv VALUES ("%", "db", "user", "table", "column", "2017-01-04 16:33:42.235831", "Insert,Update")`)
	mustExec(c, se, `INSERT INTO mysql.columns_priv VALUES ("127.0.0.1", "db", "user", "table", "column", "2017-01-04 16:33:42.235831", "Select")`)

	var p privileges.MySQLPrivilege
	err = p.LoadColumnsPrivTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.ColumnsPriv[0].Host, Equals, `%`)
	c.Assert(p.ColumnsPriv[0].DB, Equals, "db")
	c.Assert(p.ColumnsPriv[0].User, Equals, "user")
	c.Assert(p.ColumnsPriv[0].TableName, Equals, "table")
	c.Assert(p.ColumnsPriv[0].ColumnName, Equals, "column")
	c.Assert(p.ColumnsPriv[0].ColumnPriv, Equals, mysql.InsertPriv|mysql.UpdatePriv)
	c.Assert(p.ColumnsPriv[1].ColumnPriv, Equals, mysql.SelectPriv)
}

func (s *testCacheSuite) TestLoadDefaultRoleTable(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table default_roles")

	mustExec(c, se, `INSERT INTO mysql.default_roles VALUES ("%", "test_default_roles", "localhost", "r_1")`)
	mustExec(c, se, `INSERT INTO mysql.default_roles VALUES ("%", "test_default_roles", "localhost", "r_2")`)
	var p privileges.MySQLPrivilege
	err = p.LoadDefaultRoles(se)
	c.Assert(err, IsNil)
	c.Assert(p.DefaultRoles[0].Host, Equals, `%`)
	c.Assert(p.DefaultRoles[0].User, Equals, "test_default_roles")
	c.Assert(p.DefaultRoles[0].DefaultRoleHost, Equals, "localhost")
	c.Assert(p.DefaultRoles[0].DefaultRoleUser, Equals, "r_1")
	c.Assert(p.DefaultRoles[1].DefaultRoleHost, Equals, "localhost")
}

func (s *testCacheSuite) TestPatternMatch(c *C) {
	se, err := session.CreateSession4Test(s.store)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "USE MYSQL;")
	mustExec(c, se, "TRUNCATE TABLE mysql.user")
	mustExec(c, se, `INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("10.0.%", "root", "Y", "Y")`)
	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification(activeRoles, "root", "10.0.1", "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "10.0.1.118", "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "114.114.114.114", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "114.114.114.114", "test", "", "", mysql.PrivilegeType(0)), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "10.0.1.118", "test", "", "", mysql.ShutdownPriv), IsTrue)

	mustExec(c, se, "TRUNCATE TABLE mysql.user")
	mustExec(c, se, `INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("", "root", "Y", "N")`)
	p = privileges.MySQLPrivilege{}
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification(activeRoles, "root", "", "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "notnull", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "", "test", "", "", mysql.ShutdownPriv), IsFalse)

	// Pattern match for DB.
	mustExec(c, se, "TRUNCATE TABLE mysql.user")
	mustExec(c, se, "TRUNCATE TABLE mysql.db")
	mustExec(c, se, `INSERT INTO mysql.db (user,host,db,select_priv) values ('genius', '%', 'te%', 'Y')`)
	err = p.LoadDBTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification(activeRoles, "genius", "127.0.0.1", "test", "", "", mysql.SelectPriv), IsTrue)
}

func (s *testCacheSuite) TestHostMatch(c *C) {
	se, err := session.CreateSession4Test(s.store)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(err, IsNil)
	defer se.Close()

	// Host name can be IPv4 address + netmask.
	mustExec(c, se, "USE MYSQL;")
	mustExec(c, se, "TRUNCATE TABLE mysql.user")
	mustExec(c, se, `INSERT INTO mysql.user (HOST, USER, authentication_string, Select_priv, Shutdown_priv) VALUES ("172.0.0.0/255.0.0.0", "root", "", "Y", "Y")`)
	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification(activeRoles, "root", "172.0.0.1", "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "172.1.1.1", "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "198.0.0.1", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "198.0.0.1", "test", "", "", mysql.PrivilegeType(0)), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "172.0.0.1", "test", "", "", mysql.ShutdownPriv), IsTrue)
	mustExec(c, se, `TRUNCATE TABLE mysql.user`)

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
		mustExec(c, se, sql)
		p = privileges.MySQLPrivilege{}
		err = p.LoadUserTable(se)
		c.Assert(err, IsNil)
		c.Assert(p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", mysql.SelectPriv), IsFalse, Commentf("test case: %s", IPMask))
		c.Assert(p.RequestVerification(activeRoles, "root", "127.0.0.0", "test", "", "", mysql.SelectPriv), IsFalse, Commentf("test case: %s", IPMask))
		c.Assert(p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.ShutdownPriv), IsFalse, Commentf("test case: %s", IPMask))
	}

	// Netmask notation cannot be used for IPv6 addresses.
	mustExec(c, se, `INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("2001:db8::/ffff:ffff::", "root", "Y", "Y")`)
	p = privileges.MySQLPrivilege{}
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification(activeRoles, "root", "2001:db8::1234", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "2001:db8::", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.ShutdownPriv), IsFalse)
}

func (s *testCacheSuite) TestCaseInsensitive(c *C) {
	se, err := session.CreateSession4Test(s.store)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "CREATE DATABASE TCTrain;")
	mustExec(c, se, "CREATE TABLE TCTrain.TCTrainOrder (id int);")
	mustExec(c, se, "TRUNCATE TABLE mysql.user")
	mustExec(c, se, `INSERT INTO mysql.db VALUES ("127.0.0.1", "TCTrain", "genius", "Y", "Y", "Y", "Y", "Y", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N")`)
	var p privileges.MySQLPrivilege
	err = p.LoadDBTable(se)
	c.Assert(err, IsNil)
	// DB and Table names are case insensitive in MySQL.
	c.Assert(p.RequestVerification(activeRoles, "genius", "127.0.0.1", "TCTrain", "TCTrainOrder", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "genius", "127.0.0.1", "TCTRAIN", "TCTRAINORDER", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "genius", "127.0.0.1", "tctrain", "tctrainorder", "", mysql.SelectPriv), IsTrue)
}

func (s *testCacheSuite) TestLoadRoleGraph(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table user;")

	var p privileges.MySQLPrivilege
	err = p.LoadRoleGraph(se)
	c.Assert(err, IsNil)
	c.Assert(len(p.User), Equals, 0)

	mustExec(c, se, `INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_1", "%", "user2")`)
	mustExec(c, se, `INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_2", "%", "root")`)
	mustExec(c, se, `INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_3", "%", "user1")`)
	mustExec(c, se, `INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_4", "%", "root")`)

	p = privileges.MySQLPrivilege{}
	err = p.LoadRoleGraph(se)
	c.Assert(err, IsNil)
	graph := p.RoleGraph
	c.Assert(graph["root@%"].Find("r_2", "%"), Equals, true)
	c.Assert(graph["root@%"].Find("r_4", "%"), Equals, true)
	c.Assert(graph["user2@%"].Find("r_1", "%"), Equals, true)
	c.Assert(graph["user1@%"].Find("r_3", "%"), Equals, true)
	_, ok := graph["illedal"]
	c.Assert(ok, Equals, false)
	c.Assert(graph["root@%"].Find("r_1", "%"), Equals, false)
}

func (s *testCacheSuite) TestRoleGraphBFS(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, `CREATE ROLE r_1, r_2, r_3, r_4, r_5, r_6;`)
	mustExec(c, se, `GRANT r_2 TO r_1;`)
	mustExec(c, se, `GRANT r_3 TO r_2;`)
	mustExec(c, se, `GRANT r_4 TO r_3;`)
	mustExec(c, se, `GRANT r_1 TO r_4;`)
	mustExec(c, se, `GRANT r_5 TO r_3, r_6;`)

	var p privileges.MySQLPrivilege
	err = p.LoadRoleGraph(se)
	c.Assert(err, IsNil)

	activeRoles := make([]*auth.RoleIdentity, 0)
	ret := p.FindAllRole(activeRoles)
	c.Assert(len(ret), Equals, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_1", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	c.Assert(len(ret), Equals, 5)

	activeRoles = make([]*auth.RoleIdentity, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_6", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	c.Assert(len(ret), Equals, 2)

	activeRoles = make([]*auth.RoleIdentity, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_3", Hostname: "%"})
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_6", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	c.Assert(len(ret), Equals, 6)
}

func (s *testCacheSuite) TestAbnormalMySQLTable(c *C) {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	session.SetSchemaLease(0)
	session.DisableStats4Test()

	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer dom.Close()

	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	defer se.Close()

	// Simulate the case mysql.user is synchronized from MySQL.
	mustExec(c, se, "DROP TABLE mysql.user;")
	mustExec(c, se, "USE mysql;")
	mustExec(c, se, `CREATE TABLE user (
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
	mustExec(c, se, `INSERT INTO user VALUES ('localhost','root','','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0,0,'mysql_native_password','','N');
`)
	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	activeRoles := make([]*auth.RoleIdentity, 0)
	// MySQL mysql.user table schema is not identical to TiDB, check it doesn't break privilege.
	c.Assert(p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.SelectPriv), IsTrue)

	// Absent of those tables doesn't cause error.
	mustExec(c, se, "DROP TABLE mysql.db;")
	mustExec(c, se, "DROP TABLE mysql.tables_priv;")
	mustExec(c, se, "DROP TABLE mysql.columns_priv;")
	err = p.LoadAll(se)
	c.Assert(err, IsNil)
}

func (s *testCacheSuite) TestSortUserTable(c *C) {
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
	checkUserRecord(p.User, result, c)

	p.User = []privileges.UserRecord{
		privileges.NewUserRecord(`%`, "jeffrey"),
		privileges.NewUserRecord("h1.example.net", ""),
	}
	p.SortUserTable()
	result = []privileges.UserRecord{
		privileges.NewUserRecord("h1.example.net", ""),
		privileges.NewUserRecord(`%`, "jeffrey"),
	}
	checkUserRecord(p.User, result, c)

	p.User = []privileges.UserRecord{
		privileges.NewUserRecord(`192.168.%`, "xxx"),
		privileges.NewUserRecord(`192.168.199.%`, "xxx"),
	}
	p.SortUserTable()
	result = []privileges.UserRecord{
		privileges.NewUserRecord(`192.168.199.%`, "xxx"),
		privileges.NewUserRecord(`192.168.%`, "xxx"),
	}
	checkUserRecord(p.User, result, c)
}

func (s *testCacheSuite) TestGlobalPrivValueRequireStr(c *C) {
	var (
		none  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeNone}
		tls   = privileges.GlobalPrivValue{SSLType: privileges.SslTypeAny}
		x509  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeX509}
		spec  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, SSLCipher: "c1", X509Subject: "s1", X509Issuer: "i1"}
		spec2 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, X509Subject: "s1", X509Issuer: "i1"}
		spec3 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, X509Issuer: "i1"}
		spec4 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified}
	)
	c.Assert(none.RequireStr(), Equals, "NONE")
	c.Assert(tls.RequireStr(), Equals, "SSL")
	c.Assert(x509.RequireStr(), Equals, "X509")
	c.Assert(spec.RequireStr(), Equals, "CIPHER 'c1' ISSUER 'i1' SUBJECT 's1'")
	c.Assert(spec2.RequireStr(), Equals, "ISSUER 'i1' SUBJECT 's1'")
	c.Assert(spec3.RequireStr(), Equals, "ISSUER 'i1'")
	c.Assert(spec4.RequireStr(), Equals, "NONE")
}

func checkUserRecord(x, y []privileges.UserRecord, c *C) {
	c.Assert(len(x), Equals, len(y))
	for i := 0; i < len(x); i++ {
		c.Assert(x[i].User, Equals, y[i].User)
		c.Assert(x[i].Host, Equals, y[i].Host)
	}
}

func (s *testCacheSuite) TestDBIsVisible(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "create database visdb")
	p := privileges.MySQLPrivilege{}
	err = p.LoadAll(se)
	c.Assert(err, IsNil)

	mustExec(c, se, `INSERT INTO mysql.user (Host, User, Create_role_priv, Super_priv) VALUES ("%", "testvisdb", "Y", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible := p.DBIsVisible("testvisdb", "%", "visdb")
	c.Assert(isVisible, IsFalse)
	mustExec(c, se, "TRUNCATE TABLE mysql.user")

	mustExec(c, se, `INSERT INTO mysql.user (Host, User, Select_priv) VALUES ("%", "testvisdb2", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb2", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustExec(c, se, "TRUNCATE TABLE mysql.user")

	mustExec(c, se, `INSERT INTO mysql.user (Host, User, Create_priv) VALUES ("%", "testvisdb3", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb3", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustExec(c, se, "TRUNCATE TABLE mysql.user")

	mustExec(c, se, `INSERT INTO mysql.user (Host, User, Insert_priv) VALUES ("%", "testvisdb4", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb4", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustExec(c, se, "TRUNCATE TABLE mysql.user")

	mustExec(c, se, `INSERT INTO mysql.user (Host, User, Update_priv) VALUES ("%", "testvisdb5", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb5", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustExec(c, se, "TRUNCATE TABLE mysql.user")

	mustExec(c, se, `INSERT INTO mysql.user (Host, User, Create_view_priv) VALUES ("%", "testvisdb6", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb6", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustExec(c, se, "TRUNCATE TABLE mysql.user")

	mustExec(c, se, `INSERT INTO mysql.user (Host, User, Trigger_priv) VALUES ("%", "testvisdb7", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb7", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustExec(c, se, "TRUNCATE TABLE mysql.user")

	mustExec(c, se, `INSERT INTO mysql.user (Host, User, References_priv) VALUES ("%", "testvisdb8", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb8", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustExec(c, se, "TRUNCATE TABLE mysql.user")

	mustExec(c, se, `INSERT INTO mysql.user (Host, User, Execute_priv) VALUES ("%", "testvisdb9", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb9", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustExec(c, se, "TRUNCATE TABLE mysql.user")
}
