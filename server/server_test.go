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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	tmysql "github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/printer"
	log "github.com/sirupsen/logrus"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(&logutil.LogConfig{
		Level: logLevel,
	})
	TestingT(t)
}

var regression = true

var defaultDSNConfig = mysql.Config{
	User:   "root",
	Net:    "tcp",
	Addr:   "127.0.0.1:4001",
	DBName: "test",
	Strict: true,
}

type configOverrider func(*mysql.Config)

// getDSN generates a DSN string for MySQL connection.
func getDSN(overriders ...configOverrider) string {
	var config = defaultDSNConfig
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(&config)
		}
	}
	return config.FormatDSN()
}

// runTests runs tests using the default database `test`.
func runTests(c *C, overrider configOverrider, tests ...func(dbt *DBTest)) {
	db, err := sql.Open("mysql", getDSN(overrider))
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS test")

	dbt := &DBTest{c, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}
}

// runTestsOnNewDB runs tests using a specified database which will be created before the test and destroyed after the test.
func runTestsOnNewDB(c *C, overrider configOverrider, dbName string, tests ...func(dbt *DBTest)) {
	dsn := getDSN(overrider, func(config *mysql.Config) {
		config.DBName = ""
	})
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", dbName))
	c.Assert(err, IsNil, Commentf("Error drop database %s: %s", dbName, err))

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE `%s`;", dbName))
	c.Assert(err, IsNil, Commentf("Error create database %s: %s", dbName, err))

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", dbName))
		c.Assert(err, IsNil, Commentf("Error drop database %s: %s", dbName, err))
	}()

	_, err = db.Exec(fmt.Sprintf("USE `%s`;", dbName))
	c.Assert(err, IsNil, Commentf("Error use database %s: %s", dbName, err))

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

func runTestRegression(c *C, overrider configOverrider, dbName string) {
	runTestsOnNewDB(c, overrider, dbName, func(dbt *DBTest) {
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
	runTests(t, nil, func(dbt *DBTest) {
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
	runTestsOnNewDB(t, nil, "SpecialType", func(dbt *DBTest) {
		dbt.mustExec("create table test (a decimal(10, 5), b datetime, c time, d bit(8))")
		dbt.mustExec("insert test values (1.4, '2012-12-21 12:12:12', '4:23:34', b'1000')")
		rows := dbt.mustQuery("select * from test where a > ?", 0)
		t.Assert(rows.Next(), IsTrue)
		var outA float64
		var outB, outC string
		var outD []byte
		err := rows.Scan(&outA, &outB, &outC, &outD)
		t.Assert(err, IsNil)
		t.Assert(outA, Equals, 1.4)
		t.Assert(outB, Equals, "2012-12-21 12:12:12")
		t.Assert(outC, Equals, "04:23:34")
		t.Assert(outD, BytesEquals, []byte{8})
	})
}

func runTestClientWithCollation(t *C) {
	runTests(t, func(config *mysql.Config) {
		config.Collation = "utf8mb4_general_ci"
	}, func(dbt *DBTest) {
		var name, charset, collation string
		// check session variable collation_connection
		rows := dbt.mustQuery("show variables like 'collation_connection'")
		t.Assert(rows.Next(), IsTrue)
		err := rows.Scan(&name, &collation)
		t.Assert(err, IsNil)
		t.Assert(collation, Equals, "utf8mb4_general_ci")

		// check session variable character_set_client
		rows = dbt.mustQuery("show variables like 'character_set_client'")
		t.Assert(rows.Next(), IsTrue)
		err = rows.Scan(&name, &charset)
		t.Assert(err, IsNil)
		t.Assert(charset, Equals, "utf8mb4")

		// check session variable character_set_results
		rows = dbt.mustQuery("show variables like 'character_set_results'")
		t.Assert(rows.Next(), IsTrue)
		err = rows.Scan(&name, &charset)
		t.Assert(err, IsNil)
		t.Assert(charset, Equals, "utf8mb4")

		// check session variable character_set_connection
		rows = dbt.mustQuery("show variables like 'character_set_connection'")
		t.Assert(rows.Next(), IsTrue)
		err = rows.Scan(&name, &charset)
		t.Assert(err, IsNil)
		t.Assert(charset, Equals, "utf8mb4")
	})
}

func runTestPreparedString(t *C) {
	runTestsOnNewDB(t, nil, "PreparedString", func(dbt *DBTest) {
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

func runTestLoadData(c *C, server *Server) {
	// create a file and write data.
	path := "/tmp/load_data_test.csv"
	fp, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)
	defer func() {
		err = fp.Close()
		c.Assert(err, IsNil)
		err = os.Remove(path)
		c.Assert(err, IsNil)
	}()
	_, err = fp.WriteString("\n" +
		"xxx row1_col1	- row1_col2	1abc\n" +
		"xxx row2_col1	- row2_col2	\n" +
		"xxxy row3_col1	- row3_col2	\n" +
		"xxx row4_col1	- 		900\n" +
		"xxx row5_col1	- 	row5_col3")
	c.Assert(err, IsNil)

	// support ClientLocalFiles capability
	runTestsOnNewDB(c, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Strict = false
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("create table test (a varchar(255), b varchar(255) default 'default value', c int not null auto_increment, primary key(c))")
		rs, err1 := dbt.db.Exec("load data local infile '/tmp/load_data_test.csv' into table test")
		dbt.Assert(err1, IsNil)
		lastID, err1 := rs.LastInsertId()
		dbt.Assert(err1, IsNil)
		dbt.Assert(lastID, Equals, int64(1))
		affectedRows, err1 := rs.RowsAffected()
		dbt.Assert(err1, IsNil)
		dbt.Assert(affectedRows, Equals, int64(5))
		var (
			a  string
			b  string
			bb sql.NullString
			cc int
		)
		rows := dbt.mustQuery("select * from test")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &bb, &cc)
		dbt.Check(err, IsNil)
		dbt.Check(a, DeepEquals, "")
		dbt.Check(bb.String, DeepEquals, "")
		dbt.Check(cc, DeepEquals, 1)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "xxx row2_col1")
		dbt.Check(b, DeepEquals, "- row2_col2")
		dbt.Check(cc, DeepEquals, 2)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "xxxy row3_col1")
		dbt.Check(b, DeepEquals, "- row3_col2")
		dbt.Check(cc, DeepEquals, 3)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "xxx row4_col1")
		dbt.Check(b, DeepEquals, "- ")
		dbt.Check(cc, DeepEquals, 4)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "xxx row5_col1")
		dbt.Check(b, DeepEquals, "- ")
		dbt.Check(cc, DeepEquals, 5)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		// specify faileds and lines
		dbt.mustExec("delete from test")
		rs, err = dbt.db.Exec("load data local infile '/tmp/load_data_test.csv' into table test fields terminated by '\t- ' lines starting by 'xxx ' terminated by '\n'")
		dbt.Assert(err, IsNil)
		lastID, err = rs.LastInsertId()
		dbt.Assert(err, IsNil)
		dbt.Assert(lastID, Equals, int64(6))
		affectedRows, err = rs.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Assert(affectedRows, Equals, int64(4))
		rows = dbt.mustQuery("select * from test")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "row1_col1")
		dbt.Check(b, DeepEquals, "row1_col2\t1abc")
		dbt.Check(cc, DeepEquals, 6)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "row2_col1")
		dbt.Check(b, DeepEquals, "row2_col2\t")
		dbt.Check(cc, DeepEquals, 7)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "row4_col1")
		dbt.Check(b, DeepEquals, "\t\t900")
		dbt.Check(cc, DeepEquals, 8)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "row5_col1")
		dbt.Check(b, DeepEquals, "\trow5_col3")
		dbt.Check(cc, DeepEquals, 9)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))

		// infile size more than a packet size(16K)
		dbt.mustExec("delete from test")
		_, err = fp.WriteString("\n")
		dbt.Assert(err, IsNil)
		for i := 6; i <= 800; i++ {
			_, err = fp.WriteString(fmt.Sprintf("xxx row%d_col1	- row%d_col2\n", i, i))
			dbt.Assert(err, IsNil)
		}
		rs, err = dbt.db.Exec("load data local infile '/tmp/load_data_test.csv' into table test fields terminated by '\t- ' lines starting by 'xxx ' terminated by '\n'")
		dbt.Assert(err, IsNil)
		lastID, err = rs.LastInsertId()
		dbt.Assert(err, IsNil)
		dbt.Assert(lastID, Equals, int64(10))
		affectedRows, err = rs.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Assert(affectedRows, Equals, int64(799))
		rows = dbt.mustQuery("select * from test")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))

		// don't support lines terminated is ""
		rs, err = dbt.db.Exec("load data local infile '/tmp/load_data_test.csv' into table test lines terminated by ''")
		dbt.Assert(err, NotNil)

		// infile doesn't exist
		rs, err = dbt.db.Exec("load data local infile '/tmp/nonexistence.csv' into table test")
		dbt.Assert(err, NotNil)
	})

	// unsupport ClientLocalFiles capability
	server.capability ^= tmysql.ClientLocalFiles
	runTestsOnNewDB(c, func(config *mysql.Config) {
		config.AllowAllFiles = true
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("create table test (a varchar(255), b varchar(255) default 'default value', c int not null auto_increment, primary key(c))")
		_, err = dbt.db.Exec("load data local infile '/tmp/load_data_test.csv' into table test")
		dbt.Assert(err, NotNil)
		checkErrorCode(c, err, tmysql.ErrNotAllowedCommand)
	})
	server.capability |= tmysql.ClientLocalFiles
}

func runTestConcurrentUpdate(c *C) {
	// TODO: Should be runTestsOnNewDB. See #4205.
	runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec("drop table if exists test2")
		dbt.mustExec("create table test2 (a int, b int)")
		dbt.mustExec("insert test2 values (1, 1)")
		txn1, err := dbt.db.Begin()
		c.Assert(err, IsNil)

		txn2, err := dbt.db.Begin()
		c.Assert(err, IsNil)

		_, err = txn2.Exec("update test2 set a = a + 1 where b = 1")
		c.Assert(err, IsNil)
		err = txn2.Commit()
		c.Assert(err, IsNil)

		_, err = txn1.Exec("update test2 set a = a + 1 where b = 1")
		c.Assert(err, IsNil)

		err = txn1.Commit()
		c.Assert(err, IsNil)
	})
}

func runTestErrorCode(c *C) {
	runTestsOnNewDB(c, nil, "ErrorCode", func(dbt *DBTest) {
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
		checkErrorCode(c, err, tmysql.ErrBadDB)
		_, err = txn2.Exec("select * from tbl_not_exists;")
		checkErrorCode(c, err, tmysql.ErrNoSuchTable)
		_, err = txn2.Exec("create database test;")
		checkErrorCode(c, err, tmysql.ErrDBCreateExists)
		_, err = txn2.Exec("create database aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;")
		checkErrorCode(c, err, tmysql.ErrTooLongIdent)
		_, err = txn2.Exec("create table test (c int);")
		checkErrorCode(c, err, tmysql.ErrTableExists)
		_, err = txn2.Exec("drop table unknown_table;")
		checkErrorCode(c, err, tmysql.ErrBadTable)
		_, err = txn2.Exec("drop database unknown_db;")
		checkErrorCode(c, err, tmysql.ErrDBDropExists)
		_, err = txn2.Exec("create table aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (a int);")
		checkErrorCode(c, err, tmysql.ErrTooLongIdent)
		_, err = txn2.Exec("create table long_column_table (aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int);")
		checkErrorCode(c, err, tmysql.ErrTooLongIdent)
		_, err = txn2.Exec("alter table test add aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int;")
		checkErrorCode(c, err, tmysql.ErrTooLongIdent)

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

		// Expression errors
		_, err = txn2.Exec("select greatest(2);")
		checkErrorCode(c, err, tmysql.ErrWrongParamcountToNativeFct)
	})
}

func checkErrorCode(c *C, e error, code uint16) {
	me, ok := e.(*mysql.MySQLError)
	c.Assert(ok, IsTrue, Commentf("err: %v", e))
	c.Assert(me.Number, Equals, code)
}

func runTestAuth(c *C) {
	runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec(`CREATE USER 'authtest'@'%' IDENTIFIED BY '123';`)
		dbt.mustExec(`FLUSH PRIVILEGES;`)
	})
	runTests(c, func(config *mysql.Config) {
		config.User = "authtest"
		config.Passwd = "123"
	}, func(dbt *DBTest) {
		dbt.mustExec(`USE mysql;`)
	})

	db, err := sql.Open("mysql", getDSN(func(config *mysql.Config) {
		config.User = "authtest"
		config.Passwd = "456"
	}))
	_, err = db.Query("USE mysql;")
	c.Assert(err, NotNil, Commentf("Wrong password should be failed"))
	db.Close()

	// Test login use IP that not exists in mysql.user.
	runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec(`CREATE USER 'authtest2'@'localhost' IDENTIFIED BY '123';`)
		dbt.mustExec(`FLUSH PRIVILEGES;`)
	})
	runTests(c, func(config *mysql.Config) {
		config.User = "authtest2"
		config.Passwd = "123"
	}, func(dbt *DBTest) {
		dbt.mustExec(`USE mysql;`)
	})
}

func runTestIssue3662(c *C) {
	db, err := sql.Open("mysql", getDSN(func(config *mysql.Config) {
		config.DBName = "non_existing_schema"
	}))
	c.Assert(err, IsNil)
	defer db.Close()

	// According to documentation, "Open may just validate its arguments without
	// creating a connection to the database. To verify that the data source name
	// is valid, call Ping."
	err = db.Ping()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Error 1049: Unknown database 'non_existing_schema'")
}

func runTestIssue3680(c *C) {
	db, err := sql.Open("mysql", getDSN(func(config *mysql.Config) {
		config.User = "non_existing_user"
	}))
	c.Assert(err, IsNil)
	defer db.Close()

	// According to documentation, "Open may just validate its arguments without
	// creating a connection to the database. To verify that the data source name
	// is valid, call Ping."
	err = db.Ping()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Error 1045: Access denied for user 'non_existing_user'@'127.0.0.1' (using password: YES)")
}

func runTestIssue3682(c *C) {
	runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec(`CREATE USER 'issue3682'@'%' IDENTIFIED BY '123';`)
		dbt.mustExec(`FLUSH PRIVILEGES;`)
	})
	runTests(c, func(config *mysql.Config) {
		config.User = "issue3682"
		config.Passwd = "123"
	}, func(dbt *DBTest) {
		dbt.mustExec(`USE mysql;`)
	})
	db, err := sql.Open("mysql", getDSN(func(config *mysql.Config) {
		config.User = "issue3682"
		config.Passwd = "wrong_password"
		config.DBName = "non_existing_schema"
	}))
	c.Assert(err, IsNil)
	defer db.Close()
	err = db.Ping()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Error 1045: Access denied for user 'issue3682'@'127.0.0.1' (using password: YES)")
}

func runTestDBNameEscape(c *C) {
	runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec("CREATE DATABASE `aa-a`;")
	})
	runTests(c, func(config *mysql.Config) {
		config.DBName = "aa-a"
	}, func(dbt *DBTest) {
		dbt.mustExec(`USE mysql;`)
		dbt.mustExec("DROP DATABASE `aa-a`")
	})
}

func runTestResultFieldTableIsNull(c *C) {
	runTestsOnNewDB(c, nil, "ResultFieldTableIsNull", func(dbt *DBTest) {
		dbt.mustExec("drop table if exists test;")
		dbt.mustExec("create table test (c int);")
		dbt.mustExec("explain select * from test;")
	})
}

func runTestStatusAPI(c *C) {
	resp, err := http.Get("http://127.0.0.1:10090/status")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var data status
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Version, Equals, tmysql.ServerVersion)
	c.Assert(data.GitHash, Equals, printer.TiDBGitHash)
}

func runTestMultiStatements(c *C) {
	runTestsOnNewDB(c, nil, "MultiStatements", func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE `test` (`id` int(11) NOT NULL, `value` int(11) NOT NULL) ")

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (1, 1)")
		count, err := res.RowsAffected()
		c.Assert(err, IsNil, Commentf("res.RowsAffected() returned error"))
		c.Assert(count, Equals, int64(1))

		// Update
		res = dbt.mustExec("UPDATE test SET value = 3 WHERE id = 1; UPDATE test SET value = 4 WHERE id = 1; UPDATE test SET value = 5 WHERE id = 1;")
		count, err = res.RowsAffected()
		c.Assert(err, IsNil, Commentf("res.RowsAffected() returned error"))
		c.Assert(count, Equals, int64(1))

		// Read
		var out int
		rows := dbt.mustQuery("SELECT value FROM test WHERE id=1;")
		if rows.Next() {
			rows.Scan(&out)
			c.Assert(out, Equals, 5)

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
	})
}

func runTestStmtCount(t *C) {
	runTestsOnNewDB(t, nil, "StatementCount", func(dbt *DBTest) {
		originStmtCnt := getStmtCnt(string(getMetrics(t)))

		dbt.mustExec("create table test (a int)")

		dbt.mustExec("insert into test values(1)")
		dbt.mustExec("insert into test values(2)")
		dbt.mustExec("insert into test values(3)")
		dbt.mustExec("insert into test values(4)")
		dbt.mustExec("insert into test values(5)")

		dbt.mustExec("delete from test where a = 3")
		dbt.mustExec("update test set a = 2 where a = 1")
		dbt.mustExec("select * from test")
		dbt.mustExec("select 2")

		dbt.mustExec("prepare stmt1 from 'update test set a = 1 where a = 2'")
		dbt.mustExec("execute stmt1")
		dbt.mustExec("prepare stmt2 from 'select * from test'")
		dbt.mustExec("execute stmt2")

		currentStmtCnt := getStmtCnt(string(getMetrics(t)))
		t.Assert(currentStmtCnt[executor.CreateTable], Equals, originStmtCnt[executor.CreateTable]+1)
		t.Assert(currentStmtCnt[executor.Insert], Equals, originStmtCnt[executor.Insert]+5)
		deleteLabel := "DeleteTableFull"
		t.Assert(currentStmtCnt[deleteLabel], Equals, originStmtCnt[deleteLabel]+1)
		updateLabel := "UpdateTableFull"
		t.Assert(currentStmtCnt[updateLabel], Equals, originStmtCnt[updateLabel]+2)
		selectLabel := "SelectTableFull"
		t.Assert(currentStmtCnt[selectLabel], Equals, originStmtCnt[selectLabel]+2)
	})
}

func runTestTLSConnection(t *C, overrider configOverrider) error {
	db, err := sql.Open("mysql", getDSN(overrider))
	t.Assert(err, IsNil)
	defer db.Close()
	_, err = db.Exec("USE test")
	return err
}

func runTestSumAvg(c *C) {
	runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec("create table sumavg (a int, b decimal, c double)")
		dbt.mustExec("insert sumavg values (1, 1, 1)")
		rows := dbt.mustQuery("select sum(a), sum(b), sum(c) from sumavg")
		c.Assert(rows.Next(), IsTrue)
		var outA, outB, outC float64
		err := rows.Scan(&outA, &outB, &outC)
		c.Assert(err, IsNil)
		c.Assert(outA, Equals, 1.0)
		c.Assert(outB, Equals, 1.0)
		c.Assert(outC, Equals, 1.0)
		rows = dbt.mustQuery("select avg(a), avg(b), avg(c) from sumavg")
		c.Assert(rows.Next(), IsTrue)
		err = rows.Scan(&outA, &outB, &outC)
		c.Assert(err, IsNil)
		c.Assert(outA, Equals, 1.0)
		c.Assert(outB, Equals, 1.0)
		c.Assert(outC, Equals, 1.0)
	})
}

func getMetrics(t *C) []byte {
	resp, err := http.Get("http://127.0.0.1:10090/metrics")
	t.Assert(err, IsNil)
	content, err := ioutil.ReadAll(resp.Body)
	t.Assert(err, IsNil)
	resp.Body.Close()
	return content
}

func getStmtCnt(content string) (stmtCnt map[string]int) {
	stmtCnt = make(map[string]int)
	r, _ := regexp.Compile("tidb_executor_statement_node_total{type=\"([A-Z|a-z|-]+)\"} (\\d+)")
	matchResult := r.FindAllStringSubmatch(content, -1)
	for _, v := range matchResult {
		cnt, _ := strconv.Atoi(v[2])
		stmtCnt[v[1]] = cnt
	}
	return stmtCnt
}

const retryTime = 100

func waitUntilServerOnline(statusPort int) {
	// connect server
	retry := 0
	for ; retry < retryTime; retry++ {
		time.Sleep(time.Millisecond * 10)
		db, err := sql.Open("mysql", getDSN())
		if err == nil {
			db.Close()
			break
		}
	}
	if retry == retryTime {
		log.Fatalf("Failed to connect db for %d retries in every 10 ms", retryTime)
	}
	// connect http status
	statusURL := fmt.Sprintf("http://127.0.0.1:%d/status", statusPort)
	for retry = 0; retry < retryTime; retry++ {
		resp, err := http.Get(statusURL)
		if err == nil {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		log.Fatalf("Failed to connect http status for %d retries in every 10 ms", retryTime)
	}
}
