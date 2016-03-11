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

package server

import (
	"database/sql"
	"testing"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	tmysql "github.com/pingcap/tidb/mysql"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var regression = true

var dsn = "root@tcp(localhost:4001)/test?strict=true"

func runTests(c *C, dsn string, tests ...func(dbt *DBTest)) {
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS test")

	dbt := &DBTest{c, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}
}

type DBTest struct {
	*C
	db *sql.DB
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("Error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	dbt.Assert(err, IsNil, Commentf("Exec %s", query))
	return res
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	dbt.Assert(err, IsNil, Commentf("Query %s", query))
	return rows
}

func (dbt *DBTest) mustQueryRows(query string, args ...interface{}) {
	rows := dbt.mustQuery(query, args...)
	dbt.Assert(rows.Next(), IsTrue)
	rows.Close()
}

func runTestRegression(c *C) {
	runTests(c, dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE test (val TINYINT)")

		// Test for unexpected data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM test")
		dbt.Assert(rows.Next(), IsFalse, Commentf("unexpected data in empty table"))

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (1)")
		//		res := dbt.mustExec("INSERT INTO test VALUES (?)", 1)
		count, err := res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(1))
		id, err := res.LastInsertId()
		dbt.Assert(err, IsNil)
		dbt.Check(id, Equals, int64(0))

		// Read
		rows = dbt.mustQuery("SELECT val FROM test")
		if rows.Next() {
			rows.Scan(&out)
			dbt.Check(out, IsTrue)
			dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Update
		res = dbt.mustExec("UPDATE test SET val = 0 WHERE val = ?", 1)
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(1))

		// Check Update
		rows = dbt.mustQuery("SELECT val FROM test")
		if rows.Next() {
			rows.Scan(&out)
			dbt.Check(out, IsFalse)
			dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Delete
		res = dbt.mustExec("DELETE FROM test WHERE val = 0")
		//		res = dbt.mustExec("DELETE FROM test WHERE val = ?", 0)
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(1))

		// Check for unexpected rows
		res = dbt.mustExec("DELETE FROM test")
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(0))

		dbt.mustQueryRows("SELECT 1")

		b := []byte{}
		if err := dbt.db.QueryRow("SELECT ?", b).Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil echo from non-nil input")
		}
	})
}

func runTestPrepareResultFieldType(t *C) {
	var param int64 = 83
	runTests(t, dsn, func(dbt *DBTest) {
		stmt, err := dbt.db.Prepare(`SELECT ?`)
		if err != nil {
			dbt.Fatal(err)
		}
		defer stmt.Close()
		row := stmt.QueryRow(param)
		var result int64
		err = row.Scan(&result)
		if err != nil {
			dbt.Fatal(err)
		}
		switch {
		case result != param:
			dbt.Fatal("Unexpected result value")
		}
	})
}

func runTestSpecialType(t *C) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("create table test (a decimal(10, 5), b datetime, c time)")
		dbt.mustExec("insert test values (1.4, '2012-12-21 12:12:12', '4:23:34')")
		rows := dbt.mustQuery("select * from test where a > ?", 0)
		t.Assert(rows.Next(), IsTrue)
		var outA float64
		var outB, outC string
		err := rows.Scan(&outA, &outB, &outC)
		t.Assert(err, IsNil)
		t.Assert(outA, Equals, 1.4)
		t.Assert(outB, Equals, "2012-12-21 12:12:12")
		t.Assert(outC, Equals, "04:23:34")
	})
}

func runTestPreparedString(t *C) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("create table test (a char(10), b char(10))")
		dbt.mustExec("insert test values (?, ?)", "abcdeabcde", "abcde")
		rows := dbt.mustQuery("select * from test where 1 = ?", 1)
		t.Assert(rows.Next(), IsTrue)
		var outA, outB string
		err := rows.Scan(&outA, &outB)
		t.Assert(err, IsNil)
		t.Assert(outA, Equals, "abcdeabcde")
		t.Assert(outB, Equals, "abcde")
	})
}

func runTestConcurrentUpdate(c *C) {
	runTests(c, dsn, func(dbt *DBTest) {
		dbt.mustExec("create table test (a int, b int)")
		dbt.mustExec("insert test values (1, 1)")
		txn1, err := dbt.db.Begin()
		c.Assert(err, IsNil)

		txn2, err := dbt.db.Begin()
		c.Assert(err, IsNil)

		_, err = txn2.Exec("update test set a = a + 1 where b = 1")
		c.Assert(err, IsNil)
		err = txn2.Commit()
		c.Assert(err, IsNil)

		_, err = txn1.Exec("update test set a = a + 1 where b = 1")
		c.Assert(err, IsNil)

		err = txn1.Commit()
		c.Assert(err, IsNil)
	})
}

func runTestErrorCode(c *C) {
	runTests(c, dsn, func(dbt *DBTest) {
		dbt.mustExec("create table test (c int PRIMARY KEY);")
		dbt.mustExec("insert into test values (1);")
		txn1, err := dbt.db.Begin()
		c.Assert(err, IsNil)
		_, err = txn1.Exec("insert into test values(1)")
		c.Assert(err, IsNil)
		err = txn1.Commit()
		checkErrorCode(c, err, tmysql.ErrDupEntry)

		// Schema errors
		txn2, err := dbt.db.Begin()
		c.Assert(err, IsNil)
		_, err = txn2.Exec("use db_not_exists;")
		checkErrorCode(c, err, tmysql.ErrBadDb)
		_, err = txn2.Exec("select * from tbl_not_exists;")
		checkErrorCode(c, err, tmysql.ErrNoSuchTable)
		_, err = txn2.Exec("create database test;")
		checkErrorCode(c, err, tmysql.ErrDbCreateExists)
		_, err = txn2.Exec("create table test (c int);")
		checkErrorCode(c, err, tmysql.ErrTableExists)
		_, err = txn2.Exec("drop table unknown_table;")
		checkErrorCode(c, err, tmysql.ErrBadTable)
		_, err = txn2.Exec("drop database unknown_db;")
		checkErrorCode(c, err, tmysql.ErrDbDropExists)

		// Optimizer errors
		_, err = txn2.Exec("select *, * from test;")
		checkErrorCode(c, err, tmysql.ErrParse)
		_, err = txn2.Exec("select row(1, 2) > 1;")
		checkErrorCode(c, err, tmysql.ErrOperandColumns)
		_, err = txn2.Exec("select * from test order by row(c, c);")
		checkErrorCode(c, err, tmysql.ErrOperandColumns)

		// Variable errors
		_, err = txn2.Exec("select @@unknown_sys_var;")
		checkErrorCode(c, err, tmysql.ErrUnknownSystemVariable)
		_, err = txn2.Exec("set @@unknown_sys_var='1';")
		checkErrorCode(c, err, tmysql.ErrUnknownSystemVariable)
	})
}

func checkErrorCode(c *C, e error, code uint16) {
	me, ok := e.(*mysql.MySQLError)
	c.Assert(ok, IsTrue)
	c.Assert(me.Number, Equals, code)
}

func runTestAuth(c *C) {
	runTests(c, dsn, func(dbt *DBTest) {
		dbt.mustExec(`CREATE USER 'test'@'%' IDENTIFIED BY '123';`)
	})
	newDsn := "test:123@tcp(localhost:4001)/test?strict=true"
	runTests(c, newDsn, func(dbt *DBTest) {
		dbt.mustExec(`USE mysql;`)
	})

	db, err := sql.Open("mysql", "test:456@tcp(localhost:4001)/test?strict=true")
	_, err = db.Query("USE mysql;")
	c.Assert(err, NotNil, Commentf("Wrong password should be failed"))
	db.Close()
}

func runTestIssues(c *C) {
	// For issue #263
	unExistsSchemaDsn := "root@tcp(localhost:4001)/unexists_schema?strict=true"
	db, err := sql.Open("mysql", unExistsSchemaDsn)
	c.Assert(db, NotNil)
	c.Assert(err, IsNil)
	// Open may just validate its arguments without creating a connection to the database. To verify that the data source name is valid, call Ping.
	err = db.Ping()
	c.Assert(err, NotNil, Commentf("Connecting to an unexists schema should be error"))
	db.Close()
}

func runTestResultFieldTableIsNull(c *C) {
	runTests(c, dsn, func(dbt *DBTest) {
		dbt.mustExec("drop table if exists test;")
		dbt.mustExec("create table test (c int);")
		dbt.mustExec("explain select * from test;")
	})
}
