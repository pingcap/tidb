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

package plans_test

import (
	"database/sql"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/mock"
)

type testShowSuit struct {
	txn kv.Transaction
	ctx context.Context
	ms  *mockStatistics

	store  kv.Storage
	dbName string

	createDBSQL              string
	dropDBSQL                string
	useDBSQL                 string
	createTableSQL           string
	createSystemDBSQL        string
	createUserTableSQL       string
	createDBPrivTableSQL     string
	createTablePrivTableSQL  string
	createColumnPrivTableSQL string
}

var _ = Suite(&testShowSuit{})

func (p *testShowSuit) SetUpSuite(c *C) {
	nc := mock.NewContext()
	p.ctx = nc
	variable.BindSessionVars(p.ctx)
	variable.BindGlobalVarAccessor(p.ctx, nc)
	variable.RegisterStatistics(p.ms)

	p.dbName = "testshowplan"
	p.store = newStore(c, p.dbName)
	p.txn, _ = p.store.Begin()
	se := newSession(c, p.store, p.dbName)
	p.createDBSQL = fmt.Sprintf("create database if not exists %s;", p.dbName)
	p.dropDBSQL = fmt.Sprintf("drop database if exists %s;", p.dbName)
	p.useDBSQL = fmt.Sprintf("use %s;", p.dbName)
	p.createTableSQL = `CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));`

	mustExecSQL(c, se, p.createDBSQL)
	mustExecSQL(c, se, p.useDBSQL)
	mustExecSQL(c, se, p.createTableSQL)

	p.createSystemDBSQL = fmt.Sprintf("create database if not exists %s;", mysql.SystemDB)
	p.createUserTableSQL = tidb.CreateUserTable
	p.createDBPrivTableSQL = tidb.CreateDBPrivTable
	p.createTablePrivTableSQL = tidb.CreateTablePrivTable
	p.createColumnPrivTableSQL = tidb.CreateColumnPrivTable

	mustExecSQL(c, se, p.createSystemDBSQL)
	mustExecSQL(c, se, p.createUserTableSQL)
	mustExecSQL(c, se, p.createDBPrivTableSQL)
	mustExecSQL(c, se, p.createTablePrivTableSQL)
	mustExecSQL(c, se, p.createColumnPrivTableSQL)

}

func (p *testShowSuit) TearDownSuite(c *C) {
	p.txn.Commit()
}

func (p *testShowSuit) TestSimple(c *C) {
	pln := &plans.ShowPlan{}
	pln.Target = stmt.ShowWarnings
	fls := pln.GetFields()
	c.Assert(fls, HasLen, 3)
	c.Assert(fls[1].Col.Tp, Equals, mysql.TypeLong)

	pln.Target = stmt.ShowCharset
	fls = pln.GetFields()
	c.Assert(fls, HasLen, 4)
	c.Assert(fls[3].Col.Tp, Equals, mysql.TypeLonglong)

	pln.Target = stmt.ShowCreateTable
	fls = pln.GetFields()
	c.Assert(fls, HasLen, 2)
}

func (p *testShowSuit) TestShowSysVariables(c *C) {
	pln := &plans.ShowPlan{
		Target:      stmt.ShowVariables,
		GlobalScope: true,
		Pattern: &expression.PatternLike{
			Pattern: &expression.Value{
				Val: "character_set_results",
			},
		},
	}
	fls := pln.GetFields()
	c.Assert(fls, HasLen, 2)
	c.Assert(fls[0].Name, Equals, "Variable_name")
	c.Assert(fls[1].Name, Equals, "Value")
	c.Assert(fls[0].Col.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fls[1].Col.Tp, Equals, mysql.TypeVarchar)

	sessionVars := variable.GetSessionVars(p.ctx)
	ret := map[string]string{}
	rset := rsets.Recordset{
		Ctx:  p.ctx,
		Plan: pln,
	}
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})
	c.Assert(ret, HasLen, 1)
	v, ok := ret["character_set_results"]
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, "latin1")
	// Set session variable to utf8
	sessionVars.Systems["character_set_results"] = "utf8"
	pln.Close()
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})
	c.Assert(ret, HasLen, 1)
	v, ok = ret["character_set_results"]
	c.Assert(ok, IsTrue)
	// Show global variable get latin1
	c.Assert(v, Equals, "latin1")

	pln.GlobalScope = false
	pln.Close()
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})
	c.Assert(ret, HasLen, 1)
	v, ok = ret["character_set_results"]
	c.Assert(ok, IsTrue)
	// Show session variable get utf8
	c.Assert(v, Equals, "utf8")
	pln.Close()
	pln.Pattern = nil
	pln.Where = &expression.BinaryOperation{
		L:  &expression.Ident{CIStr: model.NewCIStr("Variable_name")},
		R:  expression.Value{Val: "autocommit"},
		Op: opcode.EQ,
	}

	ret = map[string]string{}
	sessionVars.Systems["autocommit"] = "on"
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})

	c.Assert(ret, HasLen, 1)
	v, ok = ret["autocommit"]
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, "on")
}

// mockStatistics represents mocked statistics.
type mockStatistics struct{}

const (
	testStatusSessionScope    = "test_status_session_scope"
	testStatusBothScopes      = "test_status_both_scope"
	testStatusValSessionScope = "test_status_val_session_scope"
	testStatusValBothScope    = "test_status_val_both_scope"
)

var statusScopes map[string]variable.ScopeFlag = map[string]variable.ScopeFlag{
	testStatusSessionScope: variable.ScopeSession,
	testStatusBothScopes:   variable.ScopeGlobal,
}

func (ms *mockStatistics) GetScope(status string) variable.ScopeFlag {
	return statusScopes[status]
}

func (ms *mockStatistics) Stats() (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(statusScopes))
	m[testStatusSessionScope] = testStatusValSessionScope
	m[testStatusBothScopes] = testStatusValBothScope

	return m, nil
}

func (p *testShowSuit) TestShowStatusVariables(c *C) {
	pln := &plans.ShowPlan{
		Target:      stmt.ShowStatus,
		GlobalScope: true,
		Pattern: &expression.PatternLike{
			Pattern: &expression.Value{
				Val: testStatusBothScopes,
			},
		},
	}
	fls := pln.GetFields()
	c.Assert(fls, HasLen, 2)
	c.Assert(fls[0].Name, Equals, "Variable_name")
	c.Assert(fls[1].Name, Equals, "Value")
	c.Assert(fls[0].Col.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fls[1].Col.Tp, Equals, mysql.TypeVarchar)

	ret := map[string]string{}
	rset := rsets.Recordset{
		Ctx:  p.ctx,
		Plan: pln,
	}
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})
	c.Assert(ret, HasLen, 1)
	v, ok := ret[testStatusBothScopes]
	c.Assert(ok, IsTrue)
	c.Assert(v, DeepEquals, testStatusValBothScope)
	pln.Close()

	pln.GlobalScope = true
	pln.Pattern = &expression.PatternLike{
		Pattern: &expression.Value{
			Val: testStatusSessionScope,
		},
	}
	ret = map[string]string{}
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})
	c.Assert(ret, HasLen, 0)
	pln.Close()

	pln.Pattern = nil
	pln.GlobalScope = false
	pln.Where = &expression.BinaryOperation{
		L:  &expression.Ident{CIStr: model.NewCIStr("Variable_name")},
		R:  expression.Value{Val: testStatusBothScopes},
		Op: opcode.EQ,
	}
	ret = map[string]string{}
	rset.Do(func(data []interface{}) (bool, error) {
		ret[data[0].(string)] = data[1].(string)
		return true, nil
	})
	c.Assert(ret, HasLen, 1)
	v, ok = ret[testStatusBothScopes]
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, testStatusValBothScope)
}

func (p *testShowSuit) TestIssue540(c *C) {
	// Show variables where variable_name="time_zone"
	pln := &plans.ShowPlan{
		Target:      stmt.ShowVariables,
		GlobalScope: false,
		Pattern: &expression.PatternLike{
			Pattern: &expression.Value{
				Val: "time_zone",
			},
		},
	}
	// Make sure the session scope var is not set.
	sessionVars := variable.GetSessionVars(p.ctx)
	_, ok := sessionVars.Systems["time_zone"]
	c.Assert(ok, IsFalse)

	r, err := pln.Next(p.ctx)
	c.Assert(err, IsNil)
	c.Assert(r.Data[0], Equals, "time_zone")
	c.Assert(r.Data[1], Equals, "SYSTEM")
}

func (p *testShowSuit) TestShowCollation(c *C) {
	pln := &plans.ShowPlan{}

	pln.Target = stmt.ShowCollation
	fls := pln.GetFields()
	c.Assert(fls, HasLen, 6)
	c.Assert(fls[2].Col.Tp, Equals, mysql.TypeLonglong)

	pln.Pattern = &expression.PatternLike{
		Pattern: &expression.Value{
			Val: "utf8%",
		},
	}

	rset := rsets.Recordset{
		Ctx:  p.ctx,
		Plan: pln,
	}

	rows, err := rset.Rows(-1, 0)
	c.Assert(err, IsNil)
	c.Assert(len(rows), Greater, 0)
	pln.Close()

	pln.Pattern = nil
	tblWhere := []struct {
		Key   string
		Value interface{}
	}{
		{"Collation", "utf8_bin"},
		{"Charset", "utf8"},
		{"Id", 83},
		{"Default", "Yes"},
		{"Compiled", "Yes"},
		{"Sortlen", 1},
	}

	for _, w := range tblWhere {
		pln.Where = &expression.BinaryOperation{
			L:  &expression.Ident{CIStr: model.NewCIStr(w.Key)},
			R:  expression.Value{Val: w.Value},
			Op: opcode.EQ,
		}

		row, err := rset.FirstRow()
		c.Assert(err, IsNil)
		c.Assert(row, HasLen, 6)
		pln.Close()
	}
}

func (p *testShowSuit) TestShowTables(c *C) {
	testDB, err := sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+"/test-show-table/test")
	c.Assert(err, IsNil)
	mustExec(c, testDB, "create table tab00 (id int);")
	mustExec(c, testDB, "create table tab01 (id int);")
	mustExec(c, testDB, "create table tab10 (id int);")
	cnt := mustQuery(c, testDB, `show full tables;`)
	c.Assert(cnt, Equals, 3)
	cnt = mustQuery(c, testDB, `show full tables from test like 'tab0%';`)
	c.Assert(cnt, Equals, 2)
	cnt = mustQuery(c, testDB, `show full tables where Table_type != 'VIEW';`)
	c.Assert(cnt, Equals, 3)

	mustQuery(c, testDB, `show create table tab00;`)
	rows, _ := testDB.Query(`show create table abc;`)
	rows.Next()
	c.Assert(rows.Err(), NotNil)
}

func (p *testShowSuit) TestShowGrants(c *C) {
	se := newSession(c, p.store, p.dbName)
	ctx, _ := se.(context.Context)
	mustExecSQL(c, se, `CREATE USER 'test'@'localhost' identified by '123';`)
	variable.GetSessionVars(ctx).User = `test@localhost`
	mustExecSQL(c, se, `GRANT Index ON *.* TO  'test'@'localhost';`)

	pln := &plans.ShowPlan{
		Target: stmt.ShowGrants,
		User:   `test@localhost`,
	}
	row, err := pln.Next(ctx)
	c.Assert(err, IsNil)
	c.Assert(row.Data[0], Equals, `GRANT Index ON *.* TO 'test'@'localhost'`)

	fs := pln.GetFields()
	c.Assert(fs, HasLen, 1)
	c.Assert(fs[0].Name, Equals, `Grants for test@localhost`)
}

func mustExecSQL(c *C, se tidb.Session, sql string) {
	_, err := se.Execute(sql)
	c.Assert(err, IsNil)
}

func newStore(c *C, dbPath string) kv.Storage {
	store, err := tidb.NewStore("memory" + "://" + dbPath)
	c.Assert(err, IsNil)
	return store
}

func newSession(c *C, store kv.Storage, dbName string) tidb.Session {
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)
	mustExecSQL(c, se, "create database if not exists "+dbName)
	mustExecSQL(c, se, "use "+dbName)
	return se
}
