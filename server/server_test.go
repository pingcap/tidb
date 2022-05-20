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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/versioninfo"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	regression = true
)

type configOverrider func(*mysql.Config)

// testServerClient config server connect parameters and provider several
// method to communicate with server and run tests
type testServerClient struct {
	port         uint
	statusPort   uint
	statusScheme string
}

// newTestServerClient return a testServerClient with unique address
func newTestServerClient() *testServerClient {
	return &testServerClient{
		port:         0,
		statusPort:   0,
		statusScheme: "http",
	}
}

// statusURL return the full URL of a status path
func (cli *testServerClient) statusURL(path string) string {
	return fmt.Sprintf("%s://localhost:%d%s", cli.statusScheme, cli.statusPort, path)
}

// fetchStatus exec http.Get to server status port
func (cli *testServerClient) fetchStatus(path string) (*http.Response, error) {
	return http.Get(cli.statusURL(path))
}

// postStatus exec http.Port to server status port
func (cli *testServerClient) postStatus(path, contentType string, body io.Reader) (*http.Response, error) {
	return http.Post(cli.statusURL(path), contentType, body)
}

// formStatus post a form request to server status address
func (cli *testServerClient) formStatus(path string, data url.Values) (*http.Response, error) {
	return http.PostForm(cli.statusURL(path), data)
}

// getDSN generates a DSN string for MySQL connection.
func (cli *testServerClient) getDSN(overriders ...configOverrider) string {
	config := mysql.NewConfig()
	config.User = "root"
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("127.0.0.1:%d", cli.port)
	config.DBName = "test"
	config.Params = make(map[string]string)
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(config)
		}
	}
	return config.FormatDSN()
}

// runTests runs tests using the default database `test`.
func (cli *testServerClient) runTests(t *testing.T, overrider configOverrider, tests ...func(dbt *testkit.DBTestKit)) {
	db, err := sql.Open("mysql", cli.getDSN(overrider))
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	dbt := testkit.NewDBTestKit(t, db)
	for _, test := range tests {
		test(dbt)
	}
}

// runTestsOnNewDB runs tests using a specified database which will be created before the test and destroyed after the test.
func (cli *testServerClient) runTestsOnNewDB(t *testing.T, overrider configOverrider, dbName string, tests ...func(dbt *testkit.DBTestKit)) {
	dsn := cli.getDSN(overrider, func(config *mysql.Config) {
		config.DBName = ""
	})
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", dbName))
	if err != nil {
		fmt.Println(err)
	}
	require.NoErrorf(t, err, "Error drop database %s: %s", dbName, err)

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE `%s`;", dbName))
	require.NoErrorf(t, err, "Error create database %s: %s", dbName, err)

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", dbName))
		require.NoErrorf(t, err, "Error drop database %s: %s", dbName, err)
	}()

	_, err = db.Exec(fmt.Sprintf("USE `%s`;", dbName))
	require.NoErrorf(t, err, "Error use database %s: %s", dbName, err)

	dbt := testkit.NewDBTestKit(t, db)
	for _, test := range tests {
		test(dbt)
		// to fix : no db selected
		_, _ = dbt.GetDB().Exec("DROP TABLE IF EXISTS test")
	}
}

func (cli *testServerClient) runTestRegression(t *testing.T, overrider configOverrider, dbName string) {
	cli.runTestsOnNewDB(t, overrider, dbName, func(dbt *testkit.DBTestKit) {
		// Show the user
		dbt.MustExec("select user()")

		// Create Table
		dbt.MustExec("CREATE TABLE test (val TINYINT)")

		// Test for unexpected data
		var out bool
		rows := dbt.MustQuery("SELECT * FROM test")
		require.Falsef(t, rows.Next(), "unexpected data in empty table")
		require.NoError(t, rows.Close())
		// Create Data
		res := dbt.MustExec("INSERT INTO test VALUES (1)")
		//		res := dbt.mustExec("INSERT INTO test VALUES (?)", 1)
		count, err := res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
		id, err := res.LastInsertId()
		require.NoError(t, err)
		require.Equal(t, int64(0), id)

		// Read
		rows = dbt.MustQuery("SELECT val FROM test")
		if rows.Next() {
			err = rows.Scan(&out)
			require.NoError(t, err)
			require.True(t, out)
			require.Falsef(t, rows.Next(), "unexpected data")
		} else {
			require.Fail(t, "no data")
		}
		require.NoError(t, rows.Close())

		// Update
		res = dbt.MustExec("UPDATE test SET val = 0 WHERE val = ?", 1)
		count, err = res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), count)

		// Check Update
		rows = dbt.MustQuery("SELECT val FROM test")
		if rows.Next() {
			err = rows.Scan(&out)
			require.NoError(t, err)
			require.False(t, out)
			require.Falsef(t, rows.Next(), "unexpected data")
		} else {
			require.Fail(t, "no data")
		}
		require.NoError(t, rows.Close())

		// Delete
		res = dbt.MustExec("DELETE FROM test WHERE val = 0")
		//		res = dbt.mustExec("DELETE FROM test WHERE val = ?", 0)
		count, err = res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), count)

		// Check for unexpected rows
		res = dbt.MustExec("DELETE FROM test")
		count, err = res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(0), count)

		dbt.MustQueryRows("SELECT 1")

		var b = make([]byte, 0)
		if err := dbt.GetDB().QueryRow("SELECT ?", b).Scan(&b); err != nil {
			t.Fatal(err)
		}
		if b == nil {
			require.Fail(t, "nil echo from non-nil input")
		}
	})
}

func (cli *testServerClient) runTestPrepareResultFieldType(t *testing.T) {
	var param int64 = 83
	cli.runTests(t, nil, func(dbt *testkit.DBTestKit) {
		stmt, err := dbt.GetDB().Prepare(`SELECT ?`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err = stmt.Close()
			require.NoError(t, err)
		}()
		row := stmt.QueryRow(param)
		var result int64
		err = row.Scan(&result)
		if err != nil {
			t.Fatal(err)
		}
		if result != param {
			t.Fatal("Unexpected result value")
		}
	})
}

func (cli *testServerClient) runTestSpecialType(t *testing.T) {
	cli.runTestsOnNewDB(t, nil, "SpecialType", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (a decimal(10, 5), b datetime, c time, d bit(8))")
		dbt.MustExec("insert test values (1.4, '2012-12-21 12:12:12', '4:23:34', b'1000')")
		rows := dbt.MustQuery("select * from test where a > ?", 0)
		require.True(t, rows.Next())
		var outA float64
		var outB, outC string
		var outD []byte
		err := rows.Scan(&outA, &outB, &outC, &outD)
		require.NoError(t, err)
		require.Equal(t, 1.4, outA)
		require.Equal(t, "2012-12-21 12:12:12", outB)
		require.Equal(t, "04:23:34", outC)
		require.Equal(t, []byte{8}, outD)
		require.NoError(t, rows.Close())
	})
}

func (cli *testServerClient) runTestClientWithCollation(t *testing.T) {
	cli.runTests(t, func(config *mysql.Config) {
		config.Collation = "utf8mb4_general_ci"
	}, func(dbt *testkit.DBTestKit) {
		var name, charset, collation string
		// check session variable collation_connection
		rows := dbt.MustQuery("show variables like 'collation_connection'")
		require.True(t, rows.Next())

		err := rows.Scan(&name, &collation)
		require.NoError(t, err)
		require.Equal(t, "utf8mb4_general_ci", collation)
		require.NoError(t, rows.Close())
		// check session variable character_set_client
		rows = dbt.MustQuery("show variables like 'character_set_client'")
		require.True(t, rows.Next())
		err = rows.Scan(&name, &charset)
		require.NoError(t, err)
		require.Equal(t, "utf8mb4", charset)
		require.NoError(t, rows.Close())
		// check session variable character_set_results
		rows = dbt.MustQuery("show variables like 'character_set_results'")
		require.True(t, rows.Next())
		err = rows.Scan(&name, &charset)
		require.NoError(t, err)
		require.Equal(t, "utf8mb4", charset)
		require.NoError(t, rows.Close())

		// check session variable character_set_connection
		rows = dbt.MustQuery("show variables like 'character_set_connection'")
		require.True(t, rows.Next())
		err = rows.Scan(&name, &charset)
		require.NoError(t, err)
		require.Equal(t, "utf8mb4", charset)
		require.NoError(t, rows.Close())
	})
}

func (cli *testServerClient) runTestPreparedString(t *testing.T) {
	cli.runTestsOnNewDB(t, nil, "PreparedString", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (a char(10), b char(10))")
		dbt.MustExec("insert test values (?, ?)", "abcdeabcde", "abcde")
		rows := dbt.MustQuery("select * from test where 1 = ?", 1)
		require.True(t, rows.Next())
		var outA, outB string
		err := rows.Scan(&outA, &outB)
		require.NoError(t, err)
		require.Equal(t, "abcdeabcde", outA)
		require.Equal(t, "abcde", outB)
		require.NoError(t, rows.Close())
	})
}

// runTestPreparedTimestamp does not really cover binary timestamp format, because MySQL driver in golang
// does not use this format. MySQL driver in golang will convert the timestamp to a string.
// This case guarantees it could work.
func (cli *testServerClient) runTestPreparedTimestamp(t *testing.T) {
	cli.runTestsOnNewDB(t, nil, "prepared_timestamp", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (a timestamp, b time)")
		dbt.MustExec("set time_zone='+00:00'")
		insertStmt := dbt.MustPrepare("insert test values (?, ?)")
		vts := time.Unix(1, 1)
		vt := time.Unix(-1, 1)
		dbt.MustExecPrepared(insertStmt, vts, vt)
		require.NoError(t, insertStmt.Close())
		selectStmt := dbt.MustPrepare("select * from test where a = ? and b = ?")
		rows := dbt.MustQueryPrepared(selectStmt, vts, vt)
		require.True(t, rows.Next())
		var outA, outB string
		err := rows.Scan(&outA, &outB)
		require.NoError(t, err)
		require.Equal(t, "1970-01-01 00:00:01", outA)
		require.Equal(t, "23:59:59", outB)
		require.NoError(t, rows.Close())
		require.NoError(t, selectStmt.Close())
	})
}

func (cli *testServerClient) runTestLoadDataWithSelectIntoOutfile(t *testing.T, server *Server) {
	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "SelectIntoOutfile", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table t (i int, r real, d decimal(10, 5), s varchar(100), dt datetime, ts timestamp, j json)")
		dbt.MustExec("insert into t values (1, 1.1, 0.1, 'a', '2000-01-01', '01:01:01', '[1]')")
		dbt.MustExec("insert into t values (2, 2.2, 0.2, 'b', '2000-02-02', '02:02:02', '[1,2]')")
		dbt.MustExec("insert into t values (null, null, null, null, '2000-03-03', '03:03:03', '[1,2,3]')")
		dbt.MustExec("insert into t values (4, 4.4, 0.4, 'd', null, null, null)")
		outfile := filepath.Join(os.TempDir(), fmt.Sprintf("select_into_outfile_%v_%d.csv", time.Now().UnixNano(), rand.Int()))
		// On windows use fmt.Sprintf("%q") to escape \ for SQL,
		// outfile may be 'C:\Users\genius\AppData\Local\Temp\select_into_outfile_1582732846769492000_8074605509026837941.csv'
		// Without quote, after SQL escape it would become:
		// 'C:UsersgeniusAppDataLocalTempselect_into_outfile_1582732846769492000_8074605509026837941.csv'
		dbt.MustExec(fmt.Sprintf("select * from t into outfile %q", outfile))
		defer func() {
			require.NoError(t, os.Remove(outfile))
		}()

		dbt.MustExec("create table t1 (i int, r real, d decimal(10, 5), s varchar(100), dt datetime, ts timestamp, j json)")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t1", outfile))

		fetchResults := func(table string) [][]interface{} {
			var res [][]interface{}
			row := dbt.MustQuery("select * from " + table + " order by i")
			for row.Next() {
				r := make([]interface{}, 7)
				require.NoError(t, row.Scan(&r[0], &r[1], &r[2], &r[3], &r[4], &r[5], &r[6]))
				res = append(res, r)
			}
			require.NoError(t, row.Close())
			return res
		}

		res := fetchResults("t")
		res1 := fetchResults("t1")
		require.Equal(t, len(res1), len(res))
		for i := range res {
			for j := range res[i] {
				// using Sprintf to avoid some uncomparable types
				require.Equal(t, fmt.Sprintf("%v", res1[i][j]), fmt.Sprintf("%v", res[i][j]))
			}
		}
	})
}

func (cli *testServerClient) runTestLoadDataForSlowLog(t *testing.T, server *Server) {
	t.Skip("unstable test")
	fp, err := os.CreateTemp("", "load_data_test.csv")
	require.NoError(t, err)
	require.NotNil(t, fp)
	path := fp.Name()
	defer func() {
		err = fp.Close()
		require.NoError(t, err)
		err = os.Remove(path)
		require.NoError(t, err)
	}()
	_, err = fp.WriteString(
		"1	1\n" +
			"2	2\n" +
			"3	3\n" +
			"4	4\n" +
			"5	5\n")
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "load_data_slow_query", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table t_slow (a int key, b int)")
		defer func() {
			dbt.MustExec("set tidb_slow_log_threshold=300;")
			dbt.MustExec("set @@global.tidb_enable_stmt_summary=0")
		}()
		dbt.MustExec("set tidb_slow_log_threshold=0;")
		dbt.MustExec("set @@global.tidb_enable_stmt_summary=1")
		query := fmt.Sprintf("load data local infile %q into table t_slow", path)
		dbt.MustExec(query)
		dbt.MustExec("insert ignore into t_slow values (1,1);")

		checkPlan := func(rows *sql.Rows, expectPlan string) {
			require.Truef(t, rows.Next(), "unexpected data")
			var plan sql.NullString
			err = rows.Scan(&plan)
			require.NoError(t, err)
			planStr := strings.ReplaceAll(plan.String, "\t", " ")
			planStr = strings.ReplaceAll(planStr, "\n", " ")
			require.Regexp(t, expectPlan, planStr)
		}

		// Test for record slow log for load data statement.
		rows := dbt.MustQuery("select plan from information_schema.slow_query where query like 'load data local infile % into table t_slow;' order by time desc limit 1")
		expectedPlan := ".*LoadData.* time.* loops.* prepare.* check_insert.* mem_insert_time:.* prefetch.* rpc.* commit_txn.*"
		checkPlan(rows, expectedPlan)
		require.NoError(t, rows.Close())
		// Test for record statements_summary for load data statement.
		rows = dbt.MustQuery("select plan from information_schema.STATEMENTS_SUMMARY where QUERY_SAMPLE_TEXT like 'load data local infile %' limit 1")
		checkPlan(rows, expectedPlan)
		require.NoError(t, rows.Close())
		// Test log normal statement after executing load date.
		rows = dbt.MustQuery("select plan from information_schema.slow_query where query = 'insert ignore into t_slow values (1,1);' order by time desc limit 1")
		expectedPlan = ".*Insert.* time.* loops.* prepare.* check_insert.* mem_insert_time:.* prefetch.* rpc.*"
		checkPlan(rows, expectedPlan)
		require.NoError(t, rows.Close())
	})
}

func (cli *testServerClient) prepareLoadDataFile(t *testing.T, fp *os.File, rows ...string) {
	err := fp.Truncate(0)
	require.NoError(t, err)
	_, err = fp.Seek(0, 0)
	require.NoError(t, err)

	for _, row := range rows {
		fields := strings.Split(row, " ")
		_, err = fp.WriteString(strings.Join(fields, "\t"))
		require.NoError(t, err)
		_, err = fp.WriteString("\n")
		require.NoError(t, err)
	}
	require.NoError(t, fp.Sync())
}

func (cli *testServerClient) runTestLoadDataAutoRandom(t *testing.T) {
	fp, err := os.CreateTemp("", "load_data_txn_error.csv")
	require.NoError(t, err)
	require.NotNil(t, fp)

	path := fp.Name()

	defer func() {
		_ = os.Remove(path)
	}()

	cksum1 := 0
	cksum2 := 0
	for i := 0; i < 50000; i++ {
		n1 := rand.Intn(1000)
		n2 := rand.Intn(1000)
		str1 := strconv.Itoa(n1)
		str2 := strconv.Itoa(n2)
		row := str1 + "\t" + str2
		_, err := fp.WriteString(row)
		require.NoError(t, err)
		_, err = fp.WriteString("\n")
		require.NoError(t, err)

		if i == 0 {
			cksum1 = n1
			cksum2 = n2
		} else {
			cksum1 = cksum1 ^ n1
			cksum2 = cksum2 ^ n2
		}
	}

	err = fp.Close()
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "load_data_batch_dml", func(dbt *testkit.DBTestKit) {
		// Set batch size, and check if load data got a invalid txn error.
		dbt.MustExec("set @@session.tidb_dml_batch_size = 128")
		dbt.MustExec("drop table if exists t")
		dbt.MustExec("create table t(c1 bigint auto_random primary key, c2 bigint, c3 bigint)")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t (c2, c3)", path))
		rows := dbt.MustQuery("select count(*) from t")
		cli.checkRows(t, rows, "50000")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select bit_xor(c2), bit_xor(c3) from t")
		res := strconv.Itoa(cksum1)
		res = res + " "
		res = res + strconv.Itoa(cksum2)
		cli.checkRows(t, rows, res)
		require.NoError(t, rows.Close())
	})
}

func (cli *testServerClient) runTestLoadDataAutoRandomWithSpecialTerm(t *testing.T) {
	fp, err := os.CreateTemp("", "load_data_txn_error_term.csv")
	require.NoError(t, err)
	require.NotNil(t, fp)
	path := fp.Name()

	defer func() {
		_ = os.Remove(path)
	}()

	cksum1 := 0
	cksum2 := 0
	for i := 0; i < 50000; i++ {
		n1 := rand.Intn(1000)
		n2 := rand.Intn(1000)
		str1 := strconv.Itoa(n1)
		str2 := strconv.Itoa(n2)
		row := "'" + str1 + "','" + str2 + "'"
		_, err := fp.WriteString(row)
		require.NoError(t, err)
		if i != 49999 {
			_, err = fp.WriteString("|")
		}
		require.NoError(t, err)

		if i == 0 {
			cksum1 = n1
			cksum2 = n2
		} else {
			cksum1 = cksum1 ^ n1
			cksum2 = cksum2 ^ n2
		}
	}

	err = fp.Close()
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "load_data_batch_dml", func(dbt *testkit.DBTestKit) {
		// Set batch size, and check if load data got a invalid txn error.
		dbt.MustExec("set @@session.tidb_dml_batch_size = 128")
		dbt.MustExec("drop table if exists t1")
		dbt.MustExec("create table t1(c1 bigint auto_random primary key, c2 bigint, c3 bigint)")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t1 fields terminated by ',' enclosed by '\\'' lines terminated by '|' (c2, c3)", path))
		rows := dbt.MustQuery("select count(*) from t1")
		cli.checkRows(t, rows, "50000")
		rows = dbt.MustQuery("select bit_xor(c2), bit_xor(c3) from t1")
		res := strconv.Itoa(cksum1)
		res = res + " "
		res = res + strconv.Itoa(cksum2)
		cli.checkRows(t, rows, res)
	})
}

func (cli *testServerClient) runTestLoadDataForListPartition(t *testing.T) {
	f, err := os.CreateTemp("", "load_data_list_partition.csv")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	path := f.Name()

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "load_data_list_partition", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("set @@session.tidb_enable_list_partition = ON")
		dbt.MustExec(`create table t (id int, name varchar(10),
		unique index idx (id)) partition by list (id) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
	);`)
		// Test load data into 1 partition.
		cli.prepareLoadDataFile(t, f, "1 a", "2 b")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t", path))
		rows := dbt.MustQuery("select * from t partition(p1) order by id")
		cli.checkRows(t, rows, "1 a", "2 b")
		// Test load data into multi-partitions.
		dbt.MustExec("delete from t")
		cli.prepareLoadDataFile(t, f, "1 a", "3 c", "4 e")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t", path))
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.checkRows(t, rows, "1 a", "3 c", "4 e")
		require.NoError(t, rows.Close())
		// Test load data meet duplicate error.
		cli.prepareLoadDataFile(t, f, "1 x", "2 b", "2 x", "7 a")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t", path))
		rows = dbt.MustQuery("show warnings")
		cli.checkRows(t, rows,
			"Warning 1062 Duplicate entry '1' for key 'idx'",
			"Warning 1062 Duplicate entry '2' for key 'idx'")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.checkRows(t, rows, "1 a", "2 b", "3 c", "4 e", "7 a")
		// Test load data meet no partition warning.
		cli.prepareLoadDataFile(t, f, "5 a", "100 x")
		_, err := dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t", path))
		require.NoError(t, err)
		rows = dbt.MustQuery("show warnings")
		cli.checkRows(t, rows, "Warning 1526 Table has no partition for value 100")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.checkRows(t, rows, "1 a", "2 b", "3 c", "4 e", "5 a", "7 a")
		require.NoError(t, rows.Close())
	})
}

func (cli *testServerClient) runTestLoadDataForListPartition2(t *testing.T) {
	f, err := os.CreateTemp("", "load_data_list_partition.csv")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	path := f.Name()

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "load_data_list_partition", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("set @@session.tidb_enable_list_partition = ON")
		dbt.MustExec(`create table t (id int, name varchar(10),b int generated always as (length(name)+1) virtual,
		unique index idx (id,b)) partition by list (id*2 + b*b + b*b - b*b*2 - abs(id)) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
	);`)
		// Test load data into 1 partition.
		cli.prepareLoadDataFile(t, f, "1 a", "2 b")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t (id,name)", path))
		rows := dbt.MustQuery("select id,name from t partition(p1) order by id")
		cli.checkRows(t, rows, "1 a", "2 b")
		// Test load data into multi-partitions.
		dbt.MustExec("delete from t")
		cli.prepareLoadDataFile(t, f, "1 a", "3 c", "4 e")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t (id,name)", path))
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select id,name from t order by id")
		cli.checkRows(t, rows, "1 a", "3 c", "4 e")
		// Test load data meet duplicate error.
		cli.prepareLoadDataFile(t, f, "1 x", "2 b", "2 x", "7 a")
		require.NoError(t, rows.Close())
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t (id,name)", path))
		rows = dbt.MustQuery("show warnings")
		cli.checkRows(t, rows,
			"Warning 1062 Duplicate entry '1-2' for key 'idx'",
			"Warning 1062 Duplicate entry '2-2' for key 'idx'")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select id,name from t order by id")
		cli.checkRows(t, rows, "1 a", "2 b", "3 c", "4 e", "7 a")
		require.NoError(t, rows.Close())
		// Test load data meet no partition warning.
		cli.prepareLoadDataFile(t, f, "5 a", "100 x")
		_, err := dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t (id,name)", path))
		require.NoError(t, err)
		rows = dbt.MustQuery("show warnings")
		cli.checkRows(t, rows, "Warning 1526 Table has no partition for value 100")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select id,name from t order by id")
		cli.checkRows(t, rows, "1 a", "2 b", "3 c", "4 e", "5 a", "7 a")
		require.NoError(t, rows.Close())
	})
}

func (cli *testServerClient) runTestLoadDataForListColumnPartition(t *testing.T) {
	f, err := os.CreateTemp("", "load_data_list_partition.csv")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	path := f.Name()

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "load_data_list_partition", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("set @@session.tidb_enable_list_partition = ON")
		dbt.MustExec(`create table t (id int, name varchar(10),
		unique index idx (id)) partition by list columns (id) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
	);`)
		// Test load data into 1 partition.
		cli.prepareLoadDataFile(t, f, "1 a", "2 b")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t", path))
		rows := dbt.MustQuery("select * from t partition(p1) order by id")
		cli.checkRows(t, rows, "1 a", "2 b")
		// Test load data into multi-partitions.
		dbt.MustExec("delete from t")
		cli.prepareLoadDataFile(t, f, "1 a", "3 c", "4 e")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t", path))
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.checkRows(t, rows, "1 a", "3 c", "4 e")
		require.NoError(t, rows.Close())
		// Test load data meet duplicate error.
		cli.prepareLoadDataFile(t, f, "1 x", "2 b", "2 x", "7 a")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t", path))
		rows = dbt.MustQuery("show warnings")
		cli.checkRows(t, rows,
			"Warning 1062 Duplicate entry '1' for key 'idx'",
			"Warning 1062 Duplicate entry '2' for key 'idx'")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.checkRows(t, rows, "1 a", "2 b", "3 c", "4 e", "7 a")
		// Test load data meet no partition warning.
		cli.prepareLoadDataFile(t, f, "5 a", "100 x")
		_, err := dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t", path))
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("show warnings")
		cli.checkRows(t, rows, "Warning 1526 Table has no partition for value from column_list")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select id,name from t order by id")
		cli.checkRows(t, rows, "1 a", "2 b", "3 c", "4 e", "5 a", "7 a")
		require.NoError(t, rows.Close())
	})
}

func (cli *testServerClient) runTestLoadDataForListColumnPartition2(t *testing.T) {
	f, err := os.CreateTemp("", "load_data_list_partition.csv")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	path := f.Name()

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "load_data_list_partition", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("set @@session.tidb_enable_list_partition = ON")
		dbt.MustExec(`create table t (location varchar(10), id int, a int, unique index idx (location,id)) partition by list columns (location,id) (
    	partition p_west  values in (('w', 1),('w', 2),('w', 3),('w', 4)),
    	partition p_east  values in (('e', 5),('e', 6),('e', 7),('e', 8)),
    	partition p_north values in (('n', 9),('n',10),('n',11),('n',12)),
    	partition p_south values in (('s',13),('s',14),('s',15),('s',16))
	);`)
		// Test load data into 1 partition.
		cli.prepareLoadDataFile(t, f, "w 1 1", "w 2 2")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t", path))
		rows := dbt.MustQuery("select * from t partition(p_west) order by id")
		cli.checkRows(t, rows, "w 1 1", "w 2 2")
		// Test load data into multi-partitions.
		dbt.MustExec("delete from t")
		cli.prepareLoadDataFile(t, f, "w 1 1", "e 5 5", "n 9 9")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t", path))
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.checkRows(t, rows, "w 1 1", "e 5 5", "n 9 9")
		// Test load data meet duplicate error.
		cli.prepareLoadDataFile(t, f, "w 1 2", "w 2 2")
		_, err := dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t", path))
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("show warnings")
		cli.checkRows(t, rows, "Warning 1062 Duplicate entry 'w-1' for key 'idx'")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.checkRows(t, rows, "w 1 1", "w 2 2", "e 5 5", "n 9 9")
		// Test load data meet no partition warning.
		cli.prepareLoadDataFile(t, f, "w 3 3", "w 5 5", "e 8 8")
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t", path))
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("show warnings")
		cli.checkRows(t, rows, "Warning 1526 Table has no partition for value from column_list")
		cli.prepareLoadDataFile(t, f, "x 1 1", "w 1 1")
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t", path))
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("show warnings")
		cli.checkRows(t, rows,
			"Warning 1526 Table has no partition for value from column_list",
			"Warning 1062 Duplicate entry 'w-1' for key 'idx'")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.checkRows(t, rows, "w 1 1", "w 2 2", "w 3 3", "e 5 5", "e 8 8", "n 9 9")
		require.NoError(t, rows.Close())
	})
}

func (cli *testServerClient) Rows(t *testing.T, rows *sql.Rows) []string {
	buf := bytes.NewBuffer(nil)
	result := make([]string, 0, 2)
	for rows.Next() {
		cols, err := rows.Columns()
		require.NoError(t, err)
		rawResult := make([][]byte, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err = rows.Scan(dest...)
		require.NoError(t, err)
		buf.Reset()
		for i, raw := range rawResult {
			if i > 0 {
				buf.WriteString(" ")
			}
			if raw == nil {
				buf.WriteString("<nil>")
			} else {
				buf.WriteString(string(raw))
			}
		}
		result = append(result, buf.String())
	}
	return result
}

func (cli *testServerClient) checkRows(t *testing.T, rows *sql.Rows, expectedRows ...string) {
	result := cli.Rows(t, rows)
	require.Equal(t, strings.Join(expectedRows, "\n"), strings.Join(result, "\n"))
}

func (cli *testServerClient) runTestLoadData(t *testing.T, server *Server) {
	fp, err := os.CreateTemp("", "load_data_test.csv")
	require.NoError(t, err)
	path := fp.Name()
	require.NotNil(t, fp)
	defer func() {
		err = fp.Close()
		require.NoError(t, err)
		err = os.Remove(path)
		require.NoError(t, err)
	}()
	_, err = fp.WriteString("\n" +
		"xxx row1_col1	- row1_col2	1abc\n" +
		"xxx row2_col1	- row2_col2	\n" +
		"xxxy row3_col1	- row3_col2	\n" +
		"xxx row4_col1	- 		900\n" +
		"xxx row5_col1	- 	row5_col3")
	require.NoError(t, err)

	originalTxnTotalSizeLimit := kv.TxnTotalSizeLimit
	// If the MemBuffer can't be committed once in each batch, it will return an error like "transaction is too large".
	kv.TxnTotalSizeLimit = 10240
	defer func() { kv.TxnTotalSizeLimit = originalTxnTotalSizeLimit }()

	// support ClientLocalFiles capability
	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("set @@tidb_dml_batch_size = 3")
		dbt.MustExec("create table test (a varchar(255), b varchar(255) default 'default value', c int not null auto_increment, primary key(c))")
		dbt.MustExec("create view v1 as select 1")
		dbt.MustExec("create sequence s1")

		// can't insert into views (in TiDB) or sequences. issue #20880
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table v1", path))
		require.Error(t, err)
		require.Equal(t, "Error 1105: can only load data into base tables", err.Error())
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table s1", path))
		require.Error(t, err)
		require.Equal(t, "Error 1105: can only load data into base tables", err.Error())

		rs, err1 := dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table test", path))
		require.NoError(t, err1)
		lastID, err1 := rs.LastInsertId()
		require.NoError(t, err1)
		require.Equal(t, int64(1), lastID)
		affectedRows, err1 := rs.RowsAffected()
		require.NoError(t, err1)
		require.Equal(t, int64(5), affectedRows)
		var (
			a  string
			b  string
			bb sql.NullString
			cc int
		)
		rows := dbt.MustQuery("select * from test")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &bb, &cc)
		require.NoError(t, err)
		require.Empty(t, a)
		require.Empty(t, bb.String)
		require.Equal(t, 1, cc)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &cc)
		require.NoError(t, err)
		require.Equal(t, "xxx row2_col1", a)
		require.Equal(t, "- row2_col2", b)
		require.Equal(t, 2, cc)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &cc)
		require.NoError(t, err)
		require.Equal(t, "xxxy row3_col1", a)
		require.Equal(t, "- row3_col2", b)
		require.Equal(t, 3, cc)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &cc)
		require.NoError(t, err)
		require.Equal(t, "xxx row4_col1", a)
		require.Equal(t, "- ", b)
		require.Equal(t, 4, cc)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &cc)
		require.NoError(t, err)
		require.Equal(t, "xxx row5_col1", a)
		require.Equal(t, "- ", b)
		require.Equal(t, 5, cc)
		require.Falsef(t, rows.Next(), "unexpected data")
		require.NoError(t, rows.Close())

		// specify faileds and lines
		dbt.MustExec("delete from test")
		dbt.MustExec("set @@tidb_dml_batch_size = 3")
		rs, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table test fields terminated by '\t- ' lines starting by 'xxx ' terminated by '\n'", path))
		require.NoError(t, err)
		lastID, err = rs.LastInsertId()
		require.NoError(t, err)
		require.Equal(t, int64(6), lastID)
		affectedRows, err = rs.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(4), affectedRows)
		rows = dbt.MustQuery("select * from test")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &cc)
		require.NoError(t, err)
		require.Equal(t, "row1_col1", a)
		require.Equal(t, "row1_col2\t1abc", b)
		require.Equal(t, 6, cc)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &cc)
		require.NoError(t, err)
		require.Equal(t, "row2_col1", a)
		require.Equal(t, "row2_col2\t", b)
		require.Equal(t, 7, cc)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &cc)
		require.NoError(t, err)
		require.Equal(t, "row4_col1", a)
		require.Equal(t, "\t\t900", b)
		require.Equal(t, 8, cc)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &cc)
		require.NoError(t, err)
		require.Equal(t, "row5_col1", a)
		require.Equal(t, "\trow5_col3", b)
		require.Equal(t, 9, cc)
		require.Falsef(t, rows.Next(), "unexpected data")
		require.NoError(t, rows.Close())
		// infile size more than a packet size(16K)
		dbt.MustExec("delete from test")
		_, err = fp.WriteString("\n")
		require.NoError(t, err)
		for i := 6; i <= 800; i++ {
			_, err = fp.WriteString(fmt.Sprintf("xxx row%d_col1	- row%d_col2\n", i, i))
			require.NoError(t, err)
		}
		dbt.MustExec("set @@tidb_dml_batch_size = 3")
		rs, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table test fields terminated by '\t- ' lines starting by 'xxx ' terminated by '\n'", path))
		require.NoError(t, err)
		lastID, err = rs.LastInsertId()
		require.NoError(t, err)
		require.Equal(t, int64(10), lastID)
		affectedRows, err = rs.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(799), affectedRows)
		rows = dbt.MustQuery("select * from test")
		require.Truef(t, rows.Next(), "unexpected data")
		require.NoError(t, rows.Close())
		// don't support lines terminated is ""
		dbt.MustExec("set @@tidb_dml_batch_size = 3")
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table test lines terminated by ''", path))
		require.NotNil(t, err)

		// infile doesn't exist
		dbt.MustExec("set @@tidb_dml_batch_size = 3")
		_, err = dbt.GetDB().Exec("load data local infile '/tmp/nonexistence.csv' into table test")
		require.NotNil(t, err)
	})

	err = fp.Close()
	require.NoError(t, err)
	err = os.Remove(path)
	require.NoError(t, err)

	fp, err = os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)

	// Test mixed unenclosed and enclosed fields.
	_, err = fp.WriteString(
		"\"abc\",123\n" +
			"def,456,\n" +
			"hig,\"789\",")
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (str varchar(10) default null, i int default null)")
		dbt.MustExec("set @@tidb_dml_batch_size = 3")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table test FIELDS TERMINATED BY ',' enclosed by '"'`, path))
		require.NoError(t, err1)
		var (
			str string
			id  int
		)
		rows := dbt.MustQuery("select * from test")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&str, &id)
		require.NoError(t, err)
		require.Equal(t, "abc", str)
		require.Equal(t, 123, id)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&str, &id)
		require.NoError(t, err)
		require.Equal(t, "def", str)
		require.Equal(t, 456, id)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&str, &id)
		require.NoError(t, err)
		require.Equal(t, "hig", str)
		require.Equal(t, 789, id)
		require.Falsef(t, rows.Next(), "unexpected data")
		dbt.MustExec("delete from test")
		require.NoError(t, rows.Close())
	})

	err = fp.Close()
	require.NoError(t, err)
	err = os.Remove(path)
	require.NoError(t, err)

	fp, err = os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)

	// Test irregular csv file.
	_, err = fp.WriteString(
		`,\N,NULL,,` + "\n" +
			"00,0,000000,,\n" +
			`2003-03-03, 20030303,030303,\N` + "\n")
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (a date, b date, c date not null, d date)")
		dbt.MustExec("set @@tidb_dml_batch_size = 3")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table test FIELDS TERMINATED BY ','`, path))
		require.NoError(t, err1)
		var (
			a sql.NullString
			b sql.NullString
			d sql.NullString
			c sql.NullString
		)
		rows := dbt.MustQuery("select * from test")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &c, &d)
		require.NoError(t, err)
		require.Equal(t, "0000-00-00", a.String)
		require.Empty(t, b.String)
		require.Equal(t, "0000-00-00", c.String)
		require.Equal(t, "0000-00-00", d.String)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &c, &d)
		require.NoError(t, err)
		require.Equal(t, "0000-00-00", a.String)
		require.Equal(t, "0000-00-00", b.String)
		require.Equal(t, "0000-00-00", c.String)
		require.Equal(t, "0000-00-00", d.String)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &c, &d)
		require.NoError(t, err)
		require.Equal(t, "2003-03-03", a.String)
		require.Equal(t, "2003-03-03", b.String)
		require.Equal(t, "2003-03-03", c.String)
		require.Equal(t, "", d.String)
		require.Falsef(t, rows.Next(), "unexpected data")
		dbt.MustExec("delete from test")
		require.NoError(t, rows.Close())
	})

	err = fp.Close()
	require.NoError(t, err)
	err = os.Remove(path)
	require.NoError(t, err)

	fp, err = os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)

	// Test double enclosed.
	_, err = fp.WriteString(
		`"field1","field2"` + "\n" +
			`"a""b","cd""ef"` + "\n" +
			`"a"b",c"d"e` + "\n")
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (a varchar(20), b varchar(20))")
		dbt.MustExec("set @@tidb_dml_batch_size = 3")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table test FIELDS TERMINATED BY ',' enclosed by '"'`, path))
		require.NoError(t, err1)
		var (
			a sql.NullString
			b sql.NullString
		)
		rows := dbt.MustQuery("select * from test")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, "field1", a.String)
		require.Equal(t, "field2", b.String)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, `a"b`, a.String)
		require.Equal(t, `cd"ef`, b.String)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, `a"b`, a.String)
		require.Equal(t, `c"d"e`, b.String)
		require.Falsef(t, rows.Next(), "unexpected data")
		dbt.MustExec("delete from test")
		require.NoError(t, rows.Close())
	})

	err = fp.Close()
	require.NoError(t, err)
	err = os.Remove(path)
	require.NoError(t, err)

	fp, err = os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)

	// Test OPTIONALLY
	_, err = fp.WriteString(
		`"a,b,c` + "\n" +
			`"1",2,"3"` + "\n")
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (id INT NOT NULL PRIMARY KEY,  b INT,  c varchar(10))")
		dbt.MustExec("set @@tidb_dml_batch_size = 3")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table test FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES`, path))
		require.NoError(t, err1)
		var (
			a int
			b int
			c sql.NullString
		)
		rows := dbt.MustQuery("select * from test")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &c)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, 2, b)
		require.Equal(t, "3", c.String)
		require.Falsef(t, rows.Next(), "unexpected data")
		dbt.MustExec("delete from test")
		require.NoError(t, rows.Close())
	})

	// unsupport ClientLocalFiles capability
	server.capability ^= tmysql.ClientLocalFiles
	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (a varchar(255), b varchar(255) default 'default value', c int not null auto_increment, primary key(c))")
		dbt.MustExec("set @@tidb_dml_batch_size = 3")
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table test", path))
		require.Error(t, err)
		checkErrorCode(t, err, errno.ErrNotAllowedCommand)
	})
	server.capability |= tmysql.ClientLocalFiles

	err = fp.Close()
	require.NoError(t, err)
	err = os.Remove(path)
	require.NoError(t, err)

	fp, err = os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)

	// Test OPTIONALLY
	_, err = fp.WriteString(
		`1,2` + "\n" +
			`3,4` + "\n")
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists pn")
		dbt.MustExec("create table pn (c1 int, c2 int)")
		dbt.MustExec("set @@tidb_dml_batch_size = 1")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ','`, path))
		require.NoError(t, err1)
		var (
			a int
			b int
		)
		rows := dbt.MustQuery("select * from pn")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, 2, b)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, 3, a)
		require.Equal(t, 4, b)
		require.Falsef(t, rows.Next(), "unexpected data")
		require.NoError(t, rows.Close())
		// fail error processing test
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/commitOneTaskErr", "return"))
		_, err1 = dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ','`, path))
		mysqlErr, ok := err1.(*mysql.MySQLError)
		require.True(t, ok)
		require.Equal(t, "mock commit one task error", mysqlErr.Message)
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/commitOneTaskErr"))

		dbt.MustExec("drop table if exists pn")
	})

	err = fp.Close()
	require.NoError(t, err)
	err = os.Remove(path)
	require.NoError(t, err)

	fp, err = os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)

	// Test Column List Specification
	_, err = fp.WriteString(
		`1,2` + "\n" +
			`3,4` + "\n")
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists pn")
		dbt.MustExec("create table pn (c1 int, c2 int)")
		dbt.MustExec("set @@tidb_dml_batch_size = 1")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ',' (c1, c2)`, path))
		require.NoError(t, err1)
		var (
			a int
			b int
		)
		rows := dbt.MustQuery("select * from pn")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, 2, b)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b)
		require.NoError(t, err)
		require.Equal(t, 3, a)
		require.Equal(t, 4, b)
		require.Falsef(t, rows.Next(), "unexpected data")
		require.NoError(t, rows.Close())
		dbt.MustExec("drop table if exists pn")
	})

	err = fp.Close()
	require.NoError(t, err)
	err = os.Remove(path)
	require.NoError(t, err)

	fp, err = os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)

	// Test Column List Specification
	_, err = fp.WriteString(
		`1,2,3` + "\n" +
			`4,5,6` + "\n")
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists pn")
		dbt.MustExec("create table pn (c1 int, c2 int, c3 int)")
		dbt.MustExec("set @@tidb_dml_batch_size = 1")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ',' (c1, @dummy)`, path))
		require.NoError(t, err1)
		var (
			a int
			b sql.NullString
			c sql.NullString
		)
		rows := dbt.MustQuery("select * from pn")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &c)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Empty(t, b.String)
		require.Empty(t, c.String)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &c)
		require.NoError(t, err)
		require.Equal(t, 4, a)
		require.Empty(t, b.String)
		require.Empty(t, c.String)
		require.Falsef(t, rows.Next(), "unexpected data")
		require.NoError(t, rows.Close())
		dbt.MustExec("drop table if exists pn")
	})

	err = fp.Close()
	require.NoError(t, err)
	err = os.Remove(path)
	require.NoError(t, err)

	fp, err = os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)

	// Test Input Preprocessing
	_, err = fp.WriteString(
		`1,2,3` + "\n" +
			`4,5,6` + "\n")
	require.NoError(t, err)

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists pn")
		dbt.MustExec("create table pn (c1 int, c2 int, c3 int)")
		dbt.MustExec("set @@tidb_dml_batch_size = 1")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ',' (c1, @val1, @val2) SET c3 = @val2 * 100, c2 = CAST(@val1 AS UNSIGNED)`, path))
		require.NoError(t, err1)
		var (
			a int
			b int
			c int
		)
		rows := dbt.MustQuery("select * from pn")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &c)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, 2, b)
		require.Equal(t, 300, c)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &c)
		require.NoError(t, err)
		require.Equal(t, 4, a)
		require.Equal(t, 5, b)
		require.Equal(t, 600, c)
		require.Falsef(t, rows.Next(), "unexpected data")
		require.NoError(t, rows.Close())
		dbt.MustExec("drop table if exists pn")
	})

	// Test with upper case variables.
	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists pn")
		dbt.MustExec("create table pn (c1 int, c2 int, c3 int)")
		dbt.MustExec("set @@tidb_dml_batch_size = 1")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ',' (c1, @VAL1, @VAL2) SET c3 = @VAL2 * 100, c2 = CAST(@VAL1 AS UNSIGNED)`, path))
		require.NoError(t, err1)
		var (
			a int
			b int
			c int
		)
		rows := dbt.MustQuery("select * from pn")
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &c)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.Equal(t, 2, b)
		require.Equal(t, 300, c)
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&a, &b, &c)
		require.NoError(t, err)
		require.Equal(t, 4, a)
		require.Equal(t, 5, b)
		require.Equal(t, 600, c)
		require.Falsef(t, rows.Next(), "unexpected data")
		require.NoError(t, rows.Close())
		dbt.MustExec("drop table if exists pn")
	})
}

func (cli *testServerClient) runTestConcurrentUpdate(t *testing.T) {
	dbName := "Concurrent"
	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.Params["sql_mode"] = "''"
	}, dbName, func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists test2")
		dbt.MustExec("create table test2 (a int, b int)")
		dbt.MustExec("insert test2 values (1, 1)")
		dbt.MustExec("set @@tidb_disable_txn_auto_retry = 0")

		txn1, err := dbt.GetDB().Begin()
		require.NoError(t, err)
		_, err = txn1.Exec(fmt.Sprintf("USE `%s`;", dbName))
		require.NoError(t, err)

		txn2, err := dbt.GetDB().Begin()
		require.NoError(t, err)
		_, err = txn2.Exec(fmt.Sprintf("USE `%s`;", dbName))
		require.NoError(t, err)

		_, err = txn2.Exec("update test2 set a = a + 1 where b = 1")
		require.NoError(t, err)
		err = txn2.Commit()
		require.NoError(t, err)

		_, err = txn1.Exec("update test2 set a = a + 1 where b = 1")
		require.NoError(t, err)

		err = txn1.Commit()
		require.NoError(t, err)
	})
}

func (cli *testServerClient) runTestExplainForConn(t *testing.T) {
	cli.runTestsOnNewDB(t, nil, "explain_for_conn", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists t")
		dbt.MustExec("create table t (a int key, b int)")
		dbt.MustExec("insert t values (1, 1)")
		rows := dbt.MustQuery("select connection_id();")
		require.True(t, rows.Next())
		var connID int64
		err := rows.Scan(&connID)
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		dbt.MustQuery("select * from t where a=1")
		rows = dbt.MustQuery("explain for connection " + strconv.Itoa(int(connID)))
		require.True(t, rows.Next())
		row := make([]string, 9)
		err = rows.Scan(&row[0], &row[1], &row[2], &row[3], &row[4], &row[5], &row[6], &row[7], &row[8])
		require.NoError(t, err)
		require.Regexp(t, "^Point_Get_1,1.00,1,root,table:t,time.*loop.*handle:1", strings.Join(row, ","))
		require.NoError(t, rows.Close())
	})
}

func (cli *testServerClient) runTestErrorCode(t *testing.T) {
	cli.runTestsOnNewDB(t, nil, "ErrorCode", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (c int PRIMARY KEY);")
		dbt.MustExec("insert into test values (1);")
		txn1, err := dbt.GetDB().Begin()
		require.NoError(t, err)
		_, err = txn1.Exec("insert into test values(1)")
		require.NoError(t, err)
		err = txn1.Commit()
		checkErrorCode(t, err, errno.ErrDupEntry)

		// Schema errors
		txn2, err := dbt.GetDB().Begin()
		require.NoError(t, err)
		_, err = txn2.Exec("use db_not_exists;")
		checkErrorCode(t, err, errno.ErrBadDB)
		_, err = txn2.Exec("select * from tbl_not_exists;")
		checkErrorCode(t, err, errno.ErrNoSuchTable)
		_, err = txn2.Exec("create database test;")
		// Make tests stable. Some times the error may be the ErrInfoSchemaChanged.
		checkErrorCode(t, err, errno.ErrDBCreateExists, errno.ErrInfoSchemaChanged)
		_, err = txn2.Exec("create database aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;")
		checkErrorCode(t, err, errno.ErrTooLongIdent, errno.ErrInfoSchemaChanged)
		_, err = txn2.Exec("create table test (c int);")
		checkErrorCode(t, err, errno.ErrTableExists, errno.ErrInfoSchemaChanged)
		_, err = txn2.Exec("drop table unknown_table;")
		checkErrorCode(t, err, errno.ErrBadTable, errno.ErrInfoSchemaChanged)
		_, err = txn2.Exec("drop database unknown_db;")
		checkErrorCode(t, err, errno.ErrDBDropExists, errno.ErrInfoSchemaChanged)
		_, err = txn2.Exec("create table aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (a int);")
		checkErrorCode(t, err, errno.ErrTooLongIdent, errno.ErrInfoSchemaChanged)
		_, err = txn2.Exec("create table long_column_table (aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int);")
		checkErrorCode(t, err, errno.ErrTooLongIdent, errno.ErrInfoSchemaChanged)
		_, err = txn2.Exec("alter table test add aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int;")
		checkErrorCode(t, err, errno.ErrTooLongIdent, errno.ErrInfoSchemaChanged)

		// Optimizer errors
		_, err = txn2.Exec("select *, * from test;")
		checkErrorCode(t, err, errno.ErrInvalidWildCard)
		_, err = txn2.Exec("select row(1, 2) > 1;")
		checkErrorCode(t, err, errno.ErrOperandColumns)
		_, err = txn2.Exec("select * from test order by row(c, c);")
		checkErrorCode(t, err, errno.ErrOperandColumns)

		// Variable errors
		_, err = txn2.Exec("select @@unknown_sys_var;")
		checkErrorCode(t, err, errno.ErrUnknownSystemVariable)
		_, err = txn2.Exec("set @@unknown_sys_var='1';")
		checkErrorCode(t, err, errno.ErrUnknownSystemVariable)

		// Expression errors
		_, err = txn2.Exec("select greatest(2);")
		checkErrorCode(t, err, errno.ErrWrongParamcountToNativeFct)
	})
}

func checkErrorCode(t *testing.T, e error, codes ...uint16) {
	me, ok := e.(*mysql.MySQLError)
	require.Truef(t, ok, "err: %v", e)
	if len(codes) == 1 {
		require.Equal(t, codes[0], me.Number)
	}
	isMatchCode := false
	for _, code := range codes {
		if me.Number == code {
			isMatchCode = true
			break
		}
	}
	require.Truef(t, isMatchCode, "got err %v, expected err codes %v", me, codes)
}

func (cli *testServerClient) runTestAuth(t *testing.T) {
	cli.runTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE USER 'authtest'@'%' IDENTIFIED BY '123';`)
		dbt.MustExec(`CREATE ROLE 'authtest_r1'@'%';`)
		dbt.MustExec(`GRANT ALL on test.* to 'authtest'`)
		dbt.MustExec(`GRANT authtest_r1 to 'authtest'`)
		dbt.MustExec(`SET DEFAULT ROLE authtest_r1 TO authtest`)
	})
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "authtest"
		config.Passwd = "123"
	}, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`USE information_schema;`)
	})

	db, err := sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "authtest"
		config.Passwd = "456"
	}))
	require.NoError(t, err)
	_, err = db.Exec("USE information_schema;")
	require.NotNilf(t, err, "Wrong password should be failed")
	require.NoError(t, db.Close())

	// Test for loading active roles.
	db, err = sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "authtest"
		config.Passwd = "123"
	}))
	require.NoError(t, err)
	rows, err := db.Query("select current_role;")
	require.NoError(t, err)
	require.True(t, rows.Next())
	var outA string
	err = rows.Scan(&outA)
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	require.Equal(t, "`authtest_r1`@`%`", outA)
	err = db.Close()
	require.NoError(t, err)

	// Test login use IP that not exists in mysql.user.
	cli.runTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE USER 'authtest2'@'localhost' IDENTIFIED BY '123';`)
		dbt.MustExec(`GRANT ALL on test.* to 'authtest2'@'localhost'`)
	})
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "authtest2"
		config.Passwd = "123"
	}, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`USE information_schema;`)
	})
}

func (cli *testServerClient) runTestIssue3662(t *testing.T) {
	db, err := sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.DBName = "non_existing_schema"
	}))
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	// According to documentation, "Open may just validate its arguments without
	// creating a connection to the database. To verify that the data source name
	// is valid, call Ping."
	err = db.Ping()
	require.Error(t, err)
	require.Equal(t, "Error 1049: Unknown database 'non_existing_schema'", err.Error())
}

func (cli *testServerClient) runTestIssue3680(t *testing.T) {
	db, err := sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "non_existing_user"
	}))
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	// According to documentation, "Open may just validate its arguments without
	// creating a connection to the database. To verify that the data source name
	// is valid, call Ping."
	err = db.Ping()
	require.Error(t, err)
	require.Equal(t, "Error 1045: Access denied for user 'non_existing_user'@'127.0.0.1' (using password: NO)", err.Error())
}

func (cli *testServerClient) runTestIssue22646(t *testing.T) {
	cli.runTests(t, nil, func(dbt *testkit.DBTestKit) {
		now := time.Now()
		dbt.MustExec(``)
		if time.Since(now) > 30*time.Second {
			t.Fatal("read empty query statement timed out.")
		}
	})
}

func (cli *testServerClient) runTestIssue3682(t *testing.T) {
	cli.runTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE USER 'issue3682'@'%' IDENTIFIED BY '123';`)
		dbt.MustExec(`GRANT ALL on test.* to 'issue3682'`)
		dbt.MustExec(`GRANT ALL on mysql.* to 'issue3682'`)
	})
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "issue3682"
		config.Passwd = "123"
	}, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`USE mysql;`)
	})
	db, err := sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "issue3682"
		config.Passwd = "wrong_password"
		config.DBName = "non_existing_schema"
	}))
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	err = db.Ping()
	require.Error(t, err)
	require.Equal(t, "Error 1045: Access denied for user 'issue3682'@'127.0.0.1' (using password: YES)", err.Error())
}

func (cli *testServerClient) runTestDBNameEscape(t *testing.T) {
	cli.runTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec("CREATE DATABASE `aa-a`;")
	})
	cli.runTests(t, func(config *mysql.Config) {
		config.DBName = "aa-a"
	}, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`USE mysql;`)
		dbt.MustExec("DROP DATABASE `aa-a`")
	})
}

func (cli *testServerClient) runTestResultFieldTableIsNull(t *testing.T) {
	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.Params["sql_mode"] = "''"
	}, "ResultFieldTableIsNull", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists test;")
		dbt.MustExec("create table test (c int);")
		dbt.MustExec("explain select * from test;")
	})
}

func (cli *testServerClient) runTestStatusAPI(t *testing.T) {
	resp, err := cli.fetchStatus("/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var data status
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.Equal(t, tmysql.ServerVersion, data.Version)
	require.Equal(t, versioninfo.TiDBGitHash, data.GitHash)
}

// The golang sql driver (and most drivers) should have multi-statement
// disabled by default for security reasons. Lets ensure that the behavior
// is correct.

func (cli *testServerClient) runFailedTestMultiStatements(t *testing.T) {
	cli.runTestsOnNewDB(t, nil, "FailedMultiStatements", func(dbt *testkit.DBTestKit) {

		// Default is now OFF in new installations.
		// It is still WARN in upgrade installations (for now)
		_, err := dbt.GetDB().Exec("SELECT 1; SELECT 1; SELECT 2; SELECT 3;")
		require.Equal(t, "Error 8130: client has multi-statement capability disabled. Run SET GLOBAL tidb_multi_statement_mode='ON' after you understand the security risk", err.Error())

		// Change to WARN (legacy mode)
		dbt.MustExec("SET tidb_multi_statement_mode='WARN'")
		dbt.MustExec("CREATE TABLE `test` (`id` int(11) NOT NULL, `value` int(11) NOT NULL) ")
		res := dbt.MustExec("INSERT INTO test VALUES (1, 1)")
		count, err := res.RowsAffected()
		require.NoErrorf(t, err, "res.RowsAffected() returned error")
		require.Equal(t, int64(1), count)
		res = dbt.MustExec("UPDATE test SET value = 3 WHERE id = 1; UPDATE test SET value = 4 WHERE id = 1; UPDATE test SET value = 5 WHERE id = 1;")
		count, err = res.RowsAffected()
		require.NoErrorf(t, err, "res.RowsAffected() returned error")
		require.Equal(t, int64(1), count)
		rows := dbt.MustQuery("show warnings")
		cli.checkRows(t, rows, "Warning 8130 client has multi-statement capability disabled. Run SET GLOBAL tidb_multi_statement_mode='ON' after you understand the security risk")
		var out int
		rows = dbt.MustQuery("SELECT value FROM test WHERE id=1;")
		if rows.Next() {
			err = rows.Scan(&out)
			require.NoError(t, err)
			require.Equal(t, 5, out)

			if rows.Next() {
				require.Fail(t, "unexpected data")
			}
		} else {
			require.Fail(t, "no data")
		}

		// Change to ON = Fully supported, TiDB legacy. No warnings or Errors.
		dbt.MustExec("SET tidb_multi_statement_mode='ON';")
		dbt.MustExec("DROP TABLE IF EXISTS test")
		dbt.MustExec("CREATE TABLE `test` (`id` int(11) NOT NULL, `value` int(11) NOT NULL) ")
		res = dbt.MustExec("INSERT INTO test VALUES (1, 1)")
		count, err = res.RowsAffected()
		require.NoErrorf(t, err, "res.RowsAffected() returned error")
		require.Equal(t, int64(1), count)
		res = dbt.MustExec("update test SET value = 3 WHERE id = 1; UPDATE test SET value = 4 WHERE id = 1; UPDATE test SET value = 5 WHERE id = 1;")
		count, err = res.RowsAffected()
		require.NoErrorf(t, err, "res.RowsAffected() returned error")
		require.Equal(t, int64(1), count)
		rows = dbt.MustQuery("SELECT value FROM test WHERE id=1;")
		if rows.Next() {
			err = rows.Scan(&out)
			require.NoError(t, err)
			require.Equal(t, 5, out)

			if rows.Next() {
				require.Fail(t, "unexpected data")
			}
		} else {
			require.Fail(t, "no data")
		}
	})
}

func (cli *testServerClient) runTestMultiStatements(t *testing.T) {

	cli.runTestsOnNewDB(t, func(config *mysql.Config) {
		config.Params["multiStatements"] = "true"
	}, "MultiStatements", func(dbt *testkit.DBTestKit) {
		// Create Table
		dbt.MustExec("CREATE TABLE `test` (`id` int(11) NOT NULL, `value` int(11) NOT NULL) ")

		// Create Data
		res := dbt.MustExec("INSERT INTO test VALUES (1, 1)")
		count, err := res.RowsAffected()
		require.NoErrorf(t, err, "res.RowsAffected() returned error")
		require.Equal(t, int64(1), count)

		// Update
		res = dbt.MustExec("UPDATE test SET value = 3 WHERE id = 1; UPDATE test SET value = 4 WHERE id = 1; UPDATE test SET value = 5 WHERE id = 1;")
		count, err = res.RowsAffected()
		require.NoErrorf(t, err, "res.RowsAffected() returned error")
		require.Equal(t, int64(1), count)

		// Read
		var out int
		rows := dbt.MustQuery("SELECT value FROM test WHERE id=1;")
		if rows.Next() {
			err = rows.Scan(&out)
			require.NoError(t, err)
			require.Equal(t, 5, out)

			if rows.Next() {
				require.Fail(t, "unexpected data")
			}
		} else {
			require.Fail(t, "no data")
		}

		// Test issue #26688
		// First we "reset" the CurrentDB by using a database and then dropping it.
		dbt.MustExec("CREATE DATABASE dropme")
		dbt.MustExec("USE dropme")
		dbt.MustExec("DROP DATABASE dropme")
		var usedb string
		rows = dbt.MustQuery("SELECT IFNULL(DATABASE(),'success')")
		if rows.Next() {
			err = rows.Scan(&usedb)
			require.NoError(t, err)
			require.Equal(t, "success", usedb)
		} else {
			require.Fail(t, "no database() result")
		}
		// Because no DB is selected, if the use multistmtuse is not successful, then
		// the create table + drop table statements will return errors.
		dbt.MustExec("CREATE DATABASE multistmtuse")
		dbt.MustExec("use multistmtuse; create table if not exists t1 (id int); drop table t1;")
	})
}

func (cli *testServerClient) runTestStmtCount(t *testing.T) {
	cli.runTestsOnNewDB(t, nil, "StatementCount", func(dbt *testkit.DBTestKit) {
		originStmtCnt := getStmtCnt(string(cli.getMetrics(t)))

		dbt.MustExec("create table test (a int)")

		dbt.MustExec("insert into test values(1)")
		dbt.MustExec("insert into test values(2)")
		dbt.MustExec("insert into test values(3)")
		dbt.MustExec("insert into test values(4)")
		dbt.MustExec("insert into test values(5)")

		dbt.MustExec("delete from test where a = 3")
		dbt.MustExec("update test set a = 2 where a = 1")
		dbt.MustExec("select * from test")
		dbt.MustExec("select 2")

		dbt.MustExec("prepare stmt1 from 'update test set a = 1 where a = 2'")
		dbt.MustExec("execute stmt1")
		dbt.MustExec("prepare stmt2 from 'select * from test'")
		dbt.MustExec("execute stmt2")
		dbt.MustExec("replace into test(a) values(6);")

		currentStmtCnt := getStmtCnt(string(cli.getMetrics(t)))
		require.Equal(t, originStmtCnt["CreateTable"]+1, currentStmtCnt["CreateTable"])
		require.Equal(t, originStmtCnt["Insert"]+5, currentStmtCnt["Insert"])
		require.Equal(t, originStmtCnt["Delete"]+1, currentStmtCnt["Delete"])
		require.Equal(t, originStmtCnt["Update"]+1, currentStmtCnt["Update"])
		require.Equal(t, originStmtCnt["Select"]+2, currentStmtCnt["Select"])
		require.Equal(t, originStmtCnt["Prepare"]+2, currentStmtCnt["Prepare"])
		require.Equal(t, originStmtCnt["Execute"]+2, currentStmtCnt["Execute"])
		require.Equal(t, originStmtCnt["Replace"]+1, currentStmtCnt["Replace"])
	})
}

func (cli *testServerClient) runTestTLSConnection(t *testing.T, overrider configOverrider) error {
	dsn := cli.getDSN(overrider)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	_, err = db.Exec("USE test")
	if err != nil {
		return errors.Annotate(err, "dsn:"+dsn)
	}
	return err
}

func (cli *testServerClient) runTestEnableSecureTransport(t *testing.T, overrider configOverrider) error {
	dsn := cli.getDSN(overrider)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	_, err = db.Exec("SET GLOBAL require_secure_transport = 1")
	if err != nil {
		return errors.Annotate(err, "dsn:"+dsn)
	}
	return err
}

func (cli *testServerClient) runReloadTLS(t *testing.T, overrider configOverrider, errorNoRollback bool) error {
	db, err := sql.Open("mysql", cli.getDSN(overrider))
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	sql := "alter instance reload tls"
	if errorNoRollback {
		sql += " no rollback on error"
	}
	_, err = db.Exec(sql)
	return err
}

func (cli *testServerClient) runTestSumAvg(t *testing.T) {
	cli.runTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table sumavg (a int, b decimal, c double)")
		dbt.MustExec("insert sumavg values (1, 1, 1)")
		rows := dbt.MustQuery("select sum(a), sum(b), sum(c) from sumavg")
		require.True(t, rows.Next())
		var outA, outB, outC float64
		err := rows.Scan(&outA, &outB, &outC)
		require.NoError(t, err)
		require.Equal(t, 1.0, outA)
		require.Equal(t, 1.0, outB)
		require.Equal(t, 1.0, outC)
		rows = dbt.MustQuery("select avg(a), avg(b), avg(c) from sumavg")
		require.True(t, rows.Next())
		err = rows.Scan(&outA, &outB, &outC)
		require.NoError(t, err)
		require.Equal(t, 1.0, outA)
		require.Equal(t, 1.0, outB)
		require.Equal(t, 1.0, outC)
	})
}

func (cli *testServerClient) getMetrics(t *testing.T) []byte {
	resp, err := cli.fetchStatus("/metrics")
	require.NoError(t, err)
	content, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	return content
}

func getStmtCnt(content string) (stmtCnt map[string]int) {
	stmtCnt = make(map[string]int)
	r := regexp.MustCompile("tidb_executor_statement_total{type=\"([A-Z|a-z|-]+)\"} (\\d+)")
	matchResult := r.FindAllStringSubmatch(content, -1)
	for _, v := range matchResult {
		cnt, _ := strconv.Atoi(v[2])
		stmtCnt[v[1]] = cnt
	}
	return stmtCnt
}

const retryTime = 100

func (cli *testServerClient) waitUntilCustomServerCanConnect(overriders ...configOverrider) {
	// connect server
	retry := 0
	dsn := cli.getDSN(overriders...)
	for ; retry < retryTime; retry++ {
		time.Sleep(time.Millisecond * 10)
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			succeed := db.Ping() == nil
			if err = db.Close(); err != nil {
				log.Error("fail to connect db", zap.String("err", err.Error()), zap.String("DSN", dsn))
				continue
			}
			if succeed {
				break
			}
		}
	}
	if retry == retryTime {
		log.Fatal("failed to connect DB in every 10 ms", zap.String("DSN", dsn), zap.Int("retryTime", retryTime))
	}
}
func (cli *testServerClient) waitUntilServerCanConnect() {
	cli.waitUntilCustomServerCanConnect(nil)
}

func (cli *testServerClient) waitUntilServerOnline() {
	// connect server
	cli.waitUntilServerCanConnect()

	retry := 0
	for ; retry < retryTime; retry++ {
		// fetch http status
		resp, err := cli.fetchStatus("/status")
		if err == nil {
			_, err = io.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}
			err = resp.Body.Close()
			if err != nil {
				panic(err)
			}
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		log.Fatal("failed to connect HTTP status in every 10 ms", zap.Int("retryTime", retryTime))
	}
}

func (cli *testServerClient) runTestInitConnect(t *testing.T) {

	cli.runTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`SET GLOBAL init_connect="insert into test.ts VALUES (NOW());SET @a=1;"`)
		dbt.MustExec(`CREATE USER init_nonsuper`)
		dbt.MustExec(`CREATE USER init_super`)
		dbt.MustExec(`GRANT SELECT, INSERT, DROP ON test.* TO init_nonsuper`)
		dbt.MustExec(`GRANT SELECT, INSERT, DROP, SUPER ON *.* TO init_super`)
		dbt.MustExec(`CREATE TABLE ts (a TIMESTAMP)`)
	})

	// test init_nonsuper
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "init_nonsuper"
	}, func(dbt *testkit.DBTestKit) {
		rows := dbt.MustQuery(`SELECT @a`)
		require.True(t, rows.Next())
		var a int
		err := rows.Scan(&a)
		require.NoError(t, err)
		require.Equal(t, 1, a)
		require.NoError(t, rows.Close())
	})

	// test init_super
	cli.runTests(t, func(config *mysql.Config) {
		config.User = "init_super"
	}, func(dbt *testkit.DBTestKit) {
		rows := dbt.MustQuery(`SELECT IFNULL(@a,"")`)
		require.True(t, rows.Next())
		var a string
		err := rows.Scan(&a)
		require.NoError(t, err)
		require.Equal(t, "", a)
		require.NoError(t, rows.Close())
		// change the init-connect to invalid.
		dbt.MustExec(`SET GLOBAL init_connect="invalidstring"`)
	})
	// set global init_connect to empty to avoid fail other tests
	defer cli.runTests(t, func(config *mysql.Config) {
		config.User = "init_super"
	}, func(dbt *testkit.DBTestKit) {
		// set init_connect to empty to avoid fail other tests
		dbt.MustExec(`SET GLOBAL init_connect=""`)
	})

	db, err := sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "init_nonsuper"
	}))
	require.NoError(t, err)      // doesn't fail because of lazy loading
	defer db.Close()             // may already be closed
	_, err = db.Exec("SELECT 1") // fails because of init sql
	require.Error(t, err)
}

// Client errors are only incremented when using the TiDB Server protocol,
// and not internal SQL statements. Thus, this test is in the server-test suite.
func (cli *testServerClient) runTestInfoschemaClientErrors(t *testing.T) {
	cli.runTestsOnNewDB(t, nil, "clientErrors", func(dbt *testkit.DBTestKit) {

		clientErrors := []struct {
			stmt              string
			incrementWarnings bool
			incrementErrors   bool
			errCode           int
		}{
			{
				stmt:              "SELECT 0/0",
				incrementWarnings: true,
				errCode:           1365, // div by zero
			},
			{
				stmt:              "CREATE TABLE test_client_errors2 (a int primary key, b int primary key)",
				incrementWarnings: true,
				incrementErrors:   true,
				errCode:           1068, // multiple pkeys
			},
			{
				stmt:              "gibberish",
				incrementWarnings: true,
				incrementErrors:   true,
				errCode:           1064, // parse error
			},
		}

		sources := []string{"client_errors_summary_global", "client_errors_summary_by_user", "client_errors_summary_by_host"}

		for _, test := range clientErrors {
			for _, tbl := range sources {

				var errors, warnings int
				rows := dbt.MustQuery("SELECT SUM(error_count), SUM(warning_count) FROM information_schema."+tbl+" WHERE error_number = ? GROUP BY error_number", test.errCode)
				if rows.Next() {
					rows.Scan(&errors, &warnings)
				}
				require.NoError(t, rows.Close())

				if test.incrementErrors {
					errors++
				}
				if test.incrementWarnings {
					warnings++
				}
				var err error
				rows, err = dbt.GetDB().Query(test.stmt)
				if err == nil {
					// make sure to read the result since the error/warnings are populated in the network send code.
					if rows.Next() {
						var fake string
						rows.Scan(&fake)
					}
					require.NoError(t, rows.Close())
				}

				var newErrors, newWarnings int
				rows = dbt.MustQuery("SELECT SUM(error_count), SUM(warning_count) FROM information_schema."+tbl+" WHERE error_number = ? GROUP BY error_number", test.errCode)
				if rows.Next() {
					rows.Scan(&newErrors, &newWarnings)
				}
				require.NoError(t, rows.Close())
				require.Equal(t, errors, newErrors)
				require.Equalf(t, warnings, newWarnings, "source=information_schema.%s code=%d statement=%s", tbl, test.errCode, test.stmt)
			}
		}

	})
}
