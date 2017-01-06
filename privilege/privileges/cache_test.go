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
	store, err := tidb.NewStore("memory://mysql")
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
