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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/privilege/privileges"
)

var _ = Suite(&testCacheSuite{})

type testCacheSuite struct {
	store  kv.Storage
	dbName string
}

func (s *testCacheSuite) SetUpSuite(c *C) {
	privileges.Enable = true
	store, err := tidb.NewStore("memory://mysql")
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	s.store = store
}

func (s *testCacheSuite) TearDown(c *C) {
	s.store.Close()
}

func (s *testCacheSuite) TestLoadUserTable(c *C) {
	se, err := tidb.CreateSession(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table user;")

	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(len(p.User), Equals, 0)

	// Host | User | Password | Select_priv | Insert_priv | Update_priv | Delete_priv | Create_priv | Drop_priv | Grant_priv | Alter_priv | Show_db_priv | Execute_priv | Index_priv | Create_user_priv
	mustExec(c, se, `INSERT INTO mysql.user VALUES ("%", "root", "", "Y", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N")`)
	mustExec(c, se, `INSERT INTO mysql.user VALUES ("%", "root1", "admin", "N", "Y", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N")`)
	mustExec(c, se, `INSERT INTO mysql.user VALUES ("%", "root11", "", "N", "N", "Y", "N", "N", "N", "N", "N", "Y", "N", "N", "N")`)
	mustExec(c, se, `INSERT INTO mysql.user VALUES ("%", "root111", "", "N", "N", "N", "N", "N", "N", "N", "N", "Y", "Y", "Y", "Y")`)

	p = privileges.MySQLPrivilege{}
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	user := p.User
	c.Assert(user[0].User, Equals, "root")
	c.Assert(user[0].Privileges, Equals, mysql.SelectPriv)
	c.Assert(user[1].Privileges, Equals, mysql.InsertPriv)
	c.Assert(user[2].Privileges, Equals, mysql.UpdatePriv|mysql.ShowDBPriv)
	c.Assert(user[3].Privileges, Equals, mysql.CreateUserPriv|mysql.IndexPriv|mysql.ExecutePriv|mysql.ShowDBPriv)
}

func (s *testCacheSuite) TestLoadDBTable(c *C) {
	se, err := tidb.CreateSession(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table db;")

	// Host | DB | User | Select_priv | Insert_priv | Update_priv | Delete_priv | Create_priv | Drop_priv | Grant_priv | Index_priv | Alter_priv | Execute_priv
	mustExec(c, se, `INSERT INTO mysql.db VALUES ("%", "information_schema", "root", "Y", "Y", "Y", "Y", "Y", "N", "N", "N", "N", "N")`)
	mustExec(c, se, `INSERT INTO mysql.db VALUES ("%", "mysql", "root1", "N", "N", "N", "N", "N", "Y", "Y", "Y", "Y", "Y")`)

	var p privileges.MySQLPrivilege
	err = p.LoadDBTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.DB[0].Privileges, Equals, mysql.SelectPriv|mysql.InsertPriv|mysql.UpdatePriv|mysql.DeletePriv|mysql.CreatePriv)
	c.Assert(p.DB[1].Privileges, Equals, mysql.DropPriv|mysql.GrantPriv|mysql.IndexPriv|mysql.AlterPriv|mysql.ExecutePriv)
}

func (s *testCacheSuite) TestLoadTablesPrivTable(c *C) {
	se, err := tidb.CreateSession(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table tables_priv")

	mustExec(c, se, `INSERT INTO mysql.tables_priv VALUES ("%", "db", "user", "table", "grantor", "2017-01-04 16:33:42.235831", "Grant,Index,Alter", "Insert,Update")`)

	var p privileges.MySQLPrivilege
	err = p.LoadTablesPrivTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.TablesPriv[0].Host, Equals, `%`)
	c.Assert(p.TablesPriv[0].DB, Equals, "db")
	c.Assert(p.TablesPriv[0].User, Equals, "user")
	c.Assert(p.TablesPriv[0].TableName, Equals, "table")
	c.Assert(p.TablesPriv[0].TablePriv, Equals, mysql.GrantPriv|mysql.IndexPriv|mysql.AlterPriv)
	c.Assert(p.TablesPriv[0].ColumnPriv, Equals, mysql.InsertPriv|mysql.UpdatePriv)
}

func (s *testCacheSuite) TestLoadColumnsPrivTable(c *C) {
	se, err := tidb.CreateSession(s.store)
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

func (s *testCacheSuite) TestPatternMatch(c *C) {
	se, err := tidb.CreateSession(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "USE MYSQL;")
	mustExec(c, se, "TRUNCATE TABLE mysql.user")
	mustExec(c, se, `INSERT INTO mysql.user VALUES ("10.0.%", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)
	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification("root", "10.0.1", "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification("root", "10.0.1.118", "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification("root", "localhost", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification("root", "127.0.0.1", "test", "", "", mysql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification("root", "114.114.114.114", "test", "", "", mysql.SelectPriv), IsFalse)

	mustExec(c, se, "TRUNCATE TABLE mysql.user")
	mustExec(c, se, `INSERT INTO mysql.user VALUES ("", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)
	p = privileges.MySQLPrivilege{}
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification("root", "", "test", "", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification("root", "notnull", "test", "", "", mysql.SelectPriv), IsFalse)
}

func (s *testCacheSuite) TestCaseInsensitive(c *C) {
	se, err := tidb.CreateSession(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "CREATE DATABASE TCTrain;")
	mustExec(c, se, "CREATE TABLE TCTrain.TCTrainOrder (id int);")
	mustExec(c, se, "TRUNCATE TABLE mysql.user")
	mustExec(c, se, `INSERT INTO mysql.db VALUES ("127.0.0.1", "TCTrain", "genius", "Y", "Y", "Y", "Y", "Y", "N", "N", "N", "N", "N")`)
	var p privileges.MySQLPrivilege
	err = p.LoadDBTable(se)
	c.Assert(err, IsNil)
	// DB and Table names are case insensitive in MySQL.
	c.Assert(p.RequestVerification("genius", "127.0.0.1", "TCTrain", "TCTrainOrder", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification("genius", "127.0.0.1", "TCTRAIN", "TCTRAINORDER", "", mysql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification("genius", "127.0.0.1", "tctrain", "tctrainorder", "", mysql.SelectPriv), IsTrue)
}

func (s *testCacheSuite) TestAbnormalMySQLTable(c *C) {
	privileges.Enable = true
	store, err := tidb.NewStore("memory://sync_mysql_user")
	c.Assert(err, IsNil)
	domain, err := tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer domain.Close()

	se, err := tidb.CreateSession(store)
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
	mustExec(c, se, `INSERT INTO user VALUES ('localhost','root','','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0,0,'mysql_native_password','','N');
`)
	var p privileges.MySQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	// MySQL mysql.user table schema is not identical to TiDB, check it doesn't break privilege.
	c.Assert(p.RequestVerification("root", "localhost", "test", "", "", mysql.SelectPriv), IsTrue)

	// Absent of those tables doesn't cause error.
	mustExec(c, se, "DROP TABLE mysql.db;")
	mustExec(c, se, "DROP TABLE mysql.tables_priv;")
	mustExec(c, se, "DROP TABLE mysql.columns_priv;")
	err = p.LoadAll(se)
	c.Assert(err, IsNil)
}
