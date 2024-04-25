// Copyright 2023 PingCAP, Inc.
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

package testserverclient

import (
	"bytes"
	"context"
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
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testenv"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/simplifiedchinese"
)

//revive:disable:exported
var (
	Regression = true
)

type configOverrider func(*mysql.Config)

// TestServerClient config server connect parameters and provider several
// method to communicate with server and run tests
type TestServerClient struct {
	StatusScheme string
	Port         uint
	StatusPort   uint
}

// NewTestServerClient return a TestServerClient with unique address
func NewTestServerClient() *TestServerClient {
	testenv.SetGOMAXPROCSForTest()
	return &TestServerClient{
		Port:         0,
		StatusPort:   0,
		StatusScheme: "http",
	}
}

// Addr returns the address of the server.
func (cli *TestServerClient) Addr() string {
	return fmt.Sprintf("%s://localhost:%d", cli.StatusScheme, cli.Port)
}

// StatusURL returns the full URL of a status path
func (cli *TestServerClient) StatusURL(path string) string {
	return fmt.Sprintf("%s://localhost:%d%s", cli.StatusScheme, cli.StatusPort, path)
}

// FetchStatus exec http.Get to server status port
func (cli *TestServerClient) FetchStatus(path string) (*http.Response, error) {
	return http.Get(cli.StatusURL(path))
}

// PostStatus exec http.Port to server status port
func (cli *TestServerClient) PostStatus(path, contentType string, body io.Reader) (*http.Response, error) {
	return http.Post(cli.StatusURL(path), contentType, body)
}

// FormStatus post a form request to server status address
func (cli *TestServerClient) FormStatus(path string, data url.Values) (*http.Response, error) {
	return http.PostForm(cli.StatusURL(path), data)
}

// GetDSN generates a DSN string for MySQL connection.
func (cli *TestServerClient) GetDSN(overriders ...configOverrider) string {
	config := mysql.NewConfig()
	config.User = "root"
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("127.0.0.1:%d", cli.Port)
	config.DBName = "test"
	config.Params = make(map[string]string)
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(config)
		}
	}
	return config.FormatDSN()
}

// RunTests runs tests using the default database `test`.
func (cli *TestServerClient) RunTests(t *testing.T, overrider configOverrider, tests ...func(dbt *testkit.DBTestKit)) {
	db, err := sql.Open("mysql", cli.GetDSN(overrider))
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

// RunTestsOnNewDB runs tests using a specified database which will be created before the test and destroyed after the test.
func (cli *TestServerClient) RunTestsOnNewDB(t *testing.T, overrider configOverrider, dbName string, tests ...func(dbt *testkit.DBTestKit)) {
	dsn := cli.GetDSN(overrider, func(config *mysql.Config) {
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

func (cli *TestServerClient) RunTestRegression(t *testing.T, overrider configOverrider, dbName string) {
	cli.RunTestsOnNewDB(t, overrider, dbName, func(dbt *testkit.DBTestKit) {
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

func (cli *TestServerClient) RunTestPrepareResultFieldType(t *testing.T) {
	var param int64 = 83
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
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

func (cli *TestServerClient) RunTestSpecialType(t *testing.T) {
	cli.RunTestsOnNewDB(t, nil, "SpecialType", func(dbt *testkit.DBTestKit) {
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

func (cli *TestServerClient) RunTestClientWithCollation(t *testing.T) {
	cli.RunTests(t, func(config *mysql.Config) {
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

func (cli *TestServerClient) RunTestPreparedString(t *testing.T) {
	cli.RunTestsOnNewDB(t, nil, "PreparedString", func(dbt *testkit.DBTestKit) {
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
func (cli *TestServerClient) RunTestPreparedTimestamp(t *testing.T) {
	cli.RunTestsOnNewDB(t, nil, "prepared_timestamp", func(dbt *testkit.DBTestKit) {
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

func (cli *TestServerClient) RunTestLoadDataWithSelectIntoOutfile(t *testing.T) {
	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
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
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t1 with thread=1", outfile))

		fetchResults := func(table string) [][]any {
			var res [][]any
			row := dbt.MustQuery("select * from " + table + " order by i")
			for row.Next() {
				r := make([]any, 7)
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

func (cli *TestServerClient) RunTestLoadDataForSlowLog(t *testing.T) {
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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
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
		query := fmt.Sprintf("load data local infile %q into table t_slow with thread=1", path)
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
		rows := dbt.MustQuery("select plan from information_schema.slow_query where query like 'load data local infile % into table t_slow with thread=1;' order by time desc limit 1")
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

func (*TestServerClient) prepareLoadDataFile(t *testing.T, fp *os.File, rows ...string) {
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

func (cli *TestServerClient) RunTestLoadDataAutoRandom(t *testing.T) {
	fp, err := os.CreateTemp("", "load_data_txn_error.csv")
	require.NoError(t, err)
	require.NotNil(t, fp)

	path := fp.Name()

	defer func() {
		_ = os.Remove(path)
	}()

	cksum1 := 0
	cksum2 := 0
	for i := 0; i < 1000; i++ {
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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "load_data_batch_dml", func(dbt *testkit.DBTestKit) {
		// Set batch size, and check if load data got a invalid txn error.
		dbt.MustExec("drop table if exists t")
		dbt.MustExec("create table t(c1 bigint auto_random primary key, c2 bigint, c3 bigint)")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t (c2, c3) with batch_size = 128, thread=1", path))
		rows := dbt.MustQuery("select count(*) from t")
		cli.CheckRows(t, rows, "1000")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select bit_xor(c2), bit_xor(c3) from t")
		res := strconv.Itoa(cksum1)
		res = res + " "
		res = res + strconv.Itoa(cksum2)
		cli.CheckRows(t, rows, res)
		require.NoError(t, rows.Close())
	})
}

func (cli *TestServerClient) RunTestLoadDataAutoRandomWithSpecialTerm(t *testing.T) {
	fp, err := os.CreateTemp("", "load_data_txn_error_term.csv")
	require.NoError(t, err)
	require.NotNil(t, fp)
	path := fp.Name()

	defer func() {
		_ = os.Remove(path)
	}()

	cksum1 := 0
	cksum2 := 0
	for i := 0; i < 5000; i++ {
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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "load_data_batch_dml", func(dbt *testkit.DBTestKit) {
		// Set batch size, and check if load data got a invalid txn error.
		dbt.MustExec("drop table if exists t1")
		dbt.MustExec("create table t1(c1 bigint auto_random primary key, c2 bigint, c3 bigint)")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t1 fields terminated by ',' enclosed by '\\'' lines terminated by '|' (c2, c3) with batch_size = 128, thread=1", path))
		rows := dbt.MustQuery("select count(*) from t1")
		cli.CheckRows(t, rows, "5000")
		rows = dbt.MustQuery("select bit_xor(c2), bit_xor(c3) from t1")
		res := strconv.Itoa(cksum1)
		res = res + " "
		res = res + strconv.Itoa(cksum2)
		cli.CheckRows(t, rows, res)
	})
}

func (cli *TestServerClient) RunTestLoadDataForListPartition(t *testing.T) {
	f, err := os.CreateTemp("", "load_data_list_partition.csv")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	path := f.Name()

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
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
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		rows := dbt.MustQuery("select * from t partition(p1) order by id")
		cli.CheckRows(t, rows, "1 a", "2 b")
		// Test load data into multi-partitions.
		dbt.MustExec("delete from t")
		cli.prepareLoadDataFile(t, f, "1 a", "3 c", "4 e")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.CheckRows(t, rows, "1 a", "3 c", "4 e")
		require.NoError(t, rows.Close())
		// Test load data meet duplicate error.
		cli.prepareLoadDataFile(t, f, "1 x", "2 b", "2 x", "7 a")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		rows = dbt.MustQuery("show warnings")
		cli.CheckRows(t, rows,
			"Warning 1062 Duplicate entry '1' for key 't.idx'",
			"Warning 1062 Duplicate entry '2' for key 't.idx'")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.CheckRows(t, rows, "1 a", "2 b", "3 c", "4 e", "7 a")
		// Test load data meet no partition warning.
		cli.prepareLoadDataFile(t, f, "5 a", "100 x")
		_, err := dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		require.NoError(t, err)
		rows = dbt.MustQuery("show warnings")
		cli.CheckRows(t, rows, "Warning 1526 Table has no partition for value 100")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.CheckRows(t, rows, "1 a", "2 b", "3 c", "4 e", "5 a", "7 a")
		require.NoError(t, rows.Close())
	})
}

func (cli *TestServerClient) RunTestLoadDataForListPartition2(t *testing.T) {
	f, err := os.CreateTemp("", "load_data_list_partition.csv")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	path := f.Name()

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
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
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t (id,name) with thread=1", path))
		rows := dbt.MustQuery("select id,name from t partition(p1) order by id")
		cli.CheckRows(t, rows, "1 a", "2 b")
		// Test load data into multi-partitions.
		dbt.MustExec("delete from t")
		cli.prepareLoadDataFile(t, f, "1 a", "3 c", "4 e")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t (id,name) with thread=1", path))
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select id,name from t order by id")
		cli.CheckRows(t, rows, "1 a", "3 c", "4 e")
		// Test load data meet duplicate error.
		cli.prepareLoadDataFile(t, f, "1 x", "2 b", "2 x", "7 a")
		require.NoError(t, rows.Close())
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t (id,name) with thread=1", path))
		rows = dbt.MustQuery("show warnings")
		cli.CheckRows(t, rows,
			"Warning 1062 Duplicate entry '1-2' for key 't.idx'",
			"Warning 1062 Duplicate entry '2-2' for key 't.idx'")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select id,name from t order by id")
		cli.CheckRows(t, rows, "1 a", "2 b", "3 c", "4 e", "7 a")
		require.NoError(t, rows.Close())
		// Test load data meet no partition warning.
		cli.prepareLoadDataFile(t, f, "5 a", "100 x")
		_, err := dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t (id,name) with thread=1", path))
		require.NoError(t, err)
		rows = dbt.MustQuery("show warnings")
		cli.CheckRows(t, rows, "Warning 1526 Table has no partition for value 100")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select id,name from t order by id")
		cli.CheckRows(t, rows, "1 a", "2 b", "3 c", "4 e", "5 a", "7 a")
		require.NoError(t, rows.Close())
	})
}

func (cli *TestServerClient) RunTestLoadDataForListColumnPartition(t *testing.T) {
	f, err := os.CreateTemp("", "load_data_list_partition.csv")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	path := f.Name()

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
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
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		rows := dbt.MustQuery("select * from t partition(p1) order by id")
		cli.CheckRows(t, rows, "1 a", "2 b")
		// Test load data into multi-partitions.
		dbt.MustExec("delete from t")
		cli.prepareLoadDataFile(t, f, "1 a", "3 c", "4 e")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.CheckRows(t, rows, "1 a", "3 c", "4 e")
		require.NoError(t, rows.Close())
		// Test load data meet duplicate error.
		cli.prepareLoadDataFile(t, f, "1 x", "2 b", "2 x", "7 a")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		rows = dbt.MustQuery("show warnings")
		cli.CheckRows(t, rows,
			"Warning 1062 Duplicate entry '1' for key 't.idx'",
			"Warning 1062 Duplicate entry '2' for key 't.idx'")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.CheckRows(t, rows, "1 a", "2 b", "3 c", "4 e", "7 a")
		// Test load data meet no partition warning.
		cli.prepareLoadDataFile(t, f, "5 a", "100 x")
		_, err := dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("show warnings")
		cli.CheckRows(t, rows, "Warning 1526 Table has no partition for value from column_list")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select id,name from t order by id")
		cli.CheckRows(t, rows, "1 a", "2 b", "3 c", "4 e", "5 a", "7 a")
		require.NoError(t, rows.Close())
	})
}

func (cli *TestServerClient) RunTestLoadDataForListColumnPartition2(t *testing.T) {
	f, err := os.CreateTemp("", "load_data_list_partition.csv")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	path := f.Name()

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
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
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		rows := dbt.MustQuery("select * from t partition(p_west) order by id")
		cli.CheckRows(t, rows, "w 1 1", "w 2 2")
		// Test load data into multi-partitions.
		dbt.MustExec("delete from t")
		cli.prepareLoadDataFile(t, f, "w 1 1", "e 5 5", "n 9 9")
		dbt.MustExec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.CheckRows(t, rows, "w 1 1", "e 5 5", "n 9 9")
		// Test load data meet duplicate error.
		cli.prepareLoadDataFile(t, f, "w 1 2", "w 2 2")
		_, err := dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("show warnings")
		cli.CheckRows(t, rows, "Warning 1062 Duplicate entry 'w-1' for key 't.idx'")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.CheckRows(t, rows, "w 1 1", "w 2 2", "e 5 5", "n 9 9")
		// Test load data meet no partition warning.
		cli.prepareLoadDataFile(t, f, "w 3 3", "w 5 5", "e 8 8")
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("show warnings")
		cli.CheckRows(t, rows, "Warning 1526 Table has no partition for value from column_list")
		cli.prepareLoadDataFile(t, f, "x 1 1", "w 1 1")
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table t with thread=1", path))
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("show warnings")
		cli.CheckRows(t, rows,
			"Warning 1526 Table has no partition for value from column_list",
			"Warning 1062 Duplicate entry 'w-1' for key 't.idx'")
		require.NoError(t, rows.Close())
		rows = dbt.MustQuery("select * from t order by id")
		cli.CheckRows(t, rows, "w 1 1", "w 2 2", "w 3 3", "e 5 5", "e 8 8", "n 9 9")
		require.NoError(t, rows.Close())
	})
}

func (*TestServerClient) Rows(t *testing.T, rows *sql.Rows) []string {
	buf := bytes.NewBuffer(nil)
	result := make([]string, 0, 2)
	for rows.Next() {
		cols, err := rows.Columns()
		require.NoError(t, err)
		rawResult := make([][]byte, len(cols))
		dest := make([]any, len(cols))
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
				buf.Write(raw)
			}
		}
		result = append(result, buf.String())
	}
	return result
}

func (cli *TestServerClient) CheckRows(t *testing.T, rows *sql.Rows, expectedRows ...string) {
	result := cli.Rows(t, rows)
	require.Equal(t, strings.Join(expectedRows, "\n"), strings.Join(result, "\n"))
}

func (cli *TestServerClient) RunTestLoadDataWithColumnList(t *testing.T, _ *server.Server) {
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

	_, err = fp.WriteString("dsadasdas\n" +
		"\"1\",\"1\",,\"2022-04-19\",\"a\",\"2022-04-19 00:00:01\"\n" +
		"\"1\",\"2\",\"a\",\"2022-04-19\",\"a\",\"2022-04-19 00:00:01\"\n" +
		"\"1\",\"3\",\"a\",\"2022-04-19\",\"a\",\"2022-04-19 00:00:01\"\n" +
		"\"1\",\"4\",\"a\",\"2022-04-19\",\"a\",\"2022-04-19 00:00:01\"")

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(db *testkit.DBTestKit) {
		db.MustExec("use test")
		db.MustExec("drop table if exists t66")
		db.MustExec("create table t66 (id int primary key,k int,c varchar(10),dt date,vv char(1),ts datetime)")
		db.MustExec(fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE t66 FIELDS TERMINATED BY ',' ENCLOSED BY '\\\"' IGNORE 1 LINES (k,id,c,dt,vv,ts) with thread=1", path))
		rows := db.MustQuery("select * from t66")
		var (
			id sql.NullString
			k  sql.NullString
			c  sql.NullString
			dt sql.NullString
			vv sql.NullString
			ts sql.NullString
		)
		columns := []*sql.NullString{&k, &id, &c, &dt, &vv, &ts}
		require.Truef(t, rows.Next(), "unexpected data")
		err := rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,1,,2022-04-19,a,2022-04-19 00:00:01", ","))
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,2,a,2022-04-19,a,2022-04-19 00:00:01", ","))
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,3,a,2022-04-19,a,2022-04-19 00:00:01", ","))
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,4,a,2022-04-19,a,2022-04-19 00:00:01", ","))
	})

	// Also test cases where column list only specifies partial columns
	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(db *testkit.DBTestKit) {
		db.MustExec("use test")
		db.MustExec("drop table if exists t66")
		db.MustExec("create table t66 (id int primary key,k int,c varchar(10),dt date,vv char(1),ts datetime)")
		db.MustExec(fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE t66 FIELDS TERMINATED BY ',' ENCLOSED BY '\\\"' IGNORE 1 LINES (k,id,c) with thread=1", path))
		rows := db.MustQuery("select * from t66")
		var (
			id sql.NullString
			k  sql.NullString
			c  sql.NullString
			dt sql.NullString
			vv sql.NullString
			ts sql.NullString
		)
		columns := []*sql.NullString{&k, &id, &c, &dt, &vv, &ts}
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,1,,,,", ","))
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,2,a,,,", ","))
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,3,a,,,", ","))
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,4,a,,,", ","))
	})

	// Also test for case-insensitivity
	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(db *testkit.DBTestKit) {
		db.MustExec("use test")
		db.MustExec("drop table if exists t66")
		db.MustExec("create table t66 (id int primary key,k int,c varchar(10),dt date,vv char(1),ts datetime)")
		// We modify the upper case and lower case in the column list to test the case-insensitivity
		db.MustExec(fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE t66 FIELDS TERMINATED BY ',' ENCLOSED BY '\\\"' IGNORE 1 LINES (K,Id,c,dT,Vv,Ts) with thread=1", path))
		rows := db.MustQuery("select * from t66")
		var (
			id sql.NullString
			k  sql.NullString
			c  sql.NullString
			dt sql.NullString
			vv sql.NullString
			ts sql.NullString
		)
		columns := []*sql.NullString{&k, &id, &c, &dt, &vv, &ts}
		require.Truef(t, rows.Next(), "unexpected data")
		err := rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,1,,2022-04-19,a,2022-04-19 00:00:01", ","))
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,2,a,2022-04-19,a,2022-04-19 00:00:01", ","))
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,3,a,2022-04-19,a,2022-04-19 00:00:01", ","))
		require.Truef(t, rows.Next(), "unexpected data")
		err = rows.Scan(&id, &k, &c, &dt, &vv, &ts)
		require.NoError(t, err)
		columnsAsExpected(t, columns, strings.Split("1,4,a,2022-04-19,a,2022-04-19 00:00:01", ","))
	})

	// Also test for name mismatches
	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(db *testkit.DBTestKit) {
		db.MustExec("use test")
		db.MustExec("drop table if exists t66")
		db.MustExec("create table t66 (id int primary key, c1 varchar(255))")
		_, err = db.GetDB().Exec(fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE t66 FIELDS TERMINATED BY ',' ENCLOSED BY '\\\"' IGNORE 1 LINES (c1, c2) with thread=1", path))
		require.EqualError(t, err, "Error 1054 (42S22): Unknown column 'c2' in 'field list'")
	})
}

func columnsAsExpected(t *testing.T, columns []*sql.NullString, expected []string) {
	require.Equal(t, len(columns), len(expected))

	for i := 0; i < len(columns); i++ {
		require.Equal(t, expected[i], columns[i].String)
	}
}

func (cli *TestServerClient) RunTestLoadDataInTransaction(t *testing.T) {
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

	_, err = fp.WriteString("1")
	require.NoError(t, err)

	// load file in transaction can be rolled back
	cli.RunTestsOnNewDB(
		t, func(config *mysql.Config) {
			config.AllowAllFiles = true
			config.Params["sql_mode"] = "''"
		}, "LoadDataInTransaction", func(dbt *testkit.DBTestKit) {
			dbt.MustExec("create table t (a int)")
			txn, err := dbt.GetDB().Begin()
			require.NoError(t, err)
			txn.Exec("insert into t values (100)") // `load data` doesn't commit current txn
			_, err = txn.Exec(fmt.Sprintf("load data local infile %q into table t", path))
			require.NoError(t, err)
			rows, err := txn.Query("select * from t")
			require.NoError(t, err)
			cli.CheckRows(t, rows, "100\n1")
			err = txn.Rollback()
			require.NoError(t, err)
			rows = dbt.MustQuery("select * from t")
			cli.CheckRows(t, rows)
		},
	)

	// load file in transaction doesn't commit until the transaction is committed
	cli.RunTestsOnNewDB(
		t, func(config *mysql.Config) {
			config.AllowAllFiles = true
			config.Params["sql_mode"] = "''"
		}, "LoadDataInTransaction", func(dbt *testkit.DBTestKit) {
			dbt.MustExec("create table t (a int)")
			txn, err := dbt.GetDB().Begin()
			require.NoError(t, err)
			_, err = txn.Exec(fmt.Sprintf("load data local infile %q into table t", path))
			require.NoError(t, err)
			rows, err := txn.Query("select * from t")
			require.NoError(t, err)
			cli.CheckRows(t, rows, "1")
			err = txn.Commit()
			require.NoError(t, err)
			rows = dbt.MustQuery("select * from t")
			cli.CheckRows(t, rows, "1")
		},
	)

	// load file in auto commit mode should succeed
	cli.RunTestsOnNewDB(
		t, func(config *mysql.Config) {
			config.AllowAllFiles = true
			config.Params["sql_mode"] = "''"
		}, "LoadDataInAutoCommit", func(dbt *testkit.DBTestKit) {
			dbt.MustExec("create table t (a int)")
			dbt.MustExec(fmt.Sprintf("load data local infile %q into table t", path))
			txn, err := dbt.GetDB().Begin()
			require.NoError(t, err)
			rows, _ := txn.Query("select * from t")
			cli.CheckRows(t, rows, "1")
		},
	)

	// load file in a pessimistic transaction,
	// should acquire locks when after its execution and before it commits.
	// The lock should be observed by another transaction that is attempting to acquire the same
	// lock.
	dbName := "LoadDataInPessimisticTransaction"
	cli.RunTestsOnNewDB(
		t, func(config *mysql.Config) {
			config.AllowAllFiles = true
			config.Params["sql_mode"] = "''"
		}, dbName, func(dbt *testkit.DBTestKit) {
			dbt.MustExec("set @@global.tidb_txn_mode = 'pessimistic'")
			dbt.MustExec("create table t (a int primary key)")
			txn, err := dbt.GetDB().Begin()
			require.NoError(t, err)
			_, err = txn.Exec(fmt.Sprintf("USE `%s`;", dbName))
			require.NoError(t, err)
			_, err = txn.Exec(fmt.Sprintf("load data local infile %q into table t", path))
			require.NoError(t, err)
			rows, err := txn.Query("select * from t")
			require.NoError(t, err)
			cli.CheckRows(t, rows, "1")

			var wg sync.WaitGroup
			wg.Add(1)
			txn2Locked := make(chan struct{}, 1)
			failed := make(chan struct{}, 1)
			go func() {
				time.Sleep(2 * time.Second)
				select {
				case <-txn2Locked:
					failed <- struct{}{}
				default:
				}

				err2 := txn.Commit()
				require.NoError(t, err2)
				wg.Done()
			}()
			txn2, err := dbt.GetDB().Begin()
			require.NoError(t, err)
			_, err = txn2.Exec(fmt.Sprintf("USE `%s`;", dbName))
			require.NoError(t, err)
			_, err = txn2.Exec("select * from t where a = 1 for update")
			require.NoError(t, err)
			txn2Locked <- struct{}{}
			wg.Wait()
			txn2.Rollback()
			select {
			case <-failed:
				require.Fail(t, "txn2 should not be able to acquire the lock")
			default:
			}

			require.NoError(t, err)
			rows = dbt.MustQuery("select * from t")
			cli.CheckRows(t, rows, "1")
		},
	)

	dbName = "LoadDataInExplicitTransaction"
	cli.RunTestsOnNewDB(
		t, func(config *mysql.Config) {
			config.AllowAllFiles = true
			config.Params["sql_mode"] = "''"
		}, dbName, func(dbt *testkit.DBTestKit) {
			// in optimistic txn, one should not block another
			dbt.MustExec("set @@global.tidb_txn_mode = 'optimistic'")
			dbt.MustExec("create table t (a int primary key)")
			txn1, err := dbt.GetDB().Begin()
			require.NoError(t, err)
			txn2, err := dbt.GetDB().Begin()
			require.NoError(t, err)
			_, err = txn1.Exec(fmt.Sprintf("USE `%s`;", dbName))
			require.NoError(t, err)
			_, err = txn2.Exec(fmt.Sprintf("USE `%s`;", dbName))
			require.NoError(t, err)
			_, err = txn1.Exec(fmt.Sprintf("load data local infile %q into table t", path))
			require.NoError(t, err)
			_, err = txn2.Exec(fmt.Sprintf("load data local infile %q into table t", path))
			require.NoError(t, err)
			err = txn1.Commit()
			require.NoError(t, err)
			err = txn2.Commit()
			require.ErrorContains(t, err, "Write conflict")
			rows := dbt.MustQuery("select * from t")
			cli.CheckRows(t, rows, "1")
		},
	)

	cli.RunTestsOnNewDB(
		t, func(config *mysql.Config) {
			config.AllowAllFiles = true
			config.Params["sql_mode"] = "''"
		}, "LoadDataFromServerFile", func(dbt *testkit.DBTestKit) {
			dbt.MustExec("create table t (a int)")
			_, err = dbt.GetDB().Exec(fmt.Sprintf("load data infile %q into table t", path))
			require.ErrorContains(t, err, "Don't support load data from tidb-server's disk.")
		},
	)

	// The test is intended to test if the load data statement correctly cleans up its
	//  resources after execution, and does not affect following statements.
	// For example, the 1st load data builds the reader and finishes.
	// The 2nd load data should not be able to access the reader, especially when it should fail
	cli.RunTestsOnNewDB(
		t, func(config *mysql.Config) {
			config.AllowAllFiles = true
			config.Params["sql_mode"] = "''"
		}, "LoadDataCleanup", func(dbt *testkit.DBTestKit) {
			dbt.MustExec("create table t (a int)")
			txn, err := dbt.GetDB().Begin()
			require.NoError(t, err)
			_, err = txn.Exec(fmt.Sprintf("load data local infile %q into table t", path))
			require.NoError(t, err)
			_, err = txn.Exec("load data local infile '/tmp/does_not_exist' into table t")
			require.ErrorContains(t, err, "no such file or directory")
			err = txn.Commit()
			require.NoError(t, err)
			rows := dbt.MustQuery("select * from t")
			cli.CheckRows(t, rows, "1")
		},
	)
}

func (cli *TestServerClient) RunTestLoadData(t *testing.T, server *server.Server) {
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

	originalTxnTotalSizeLimit := kv.TxnTotalSizeLimit.Load()
	// If the MemBuffer can't be committed once in each batch, it will return an error like "transaction is too large".
	kv.TxnTotalSizeLimit.Store(10240)
	defer func() { kv.TxnTotalSizeLimit.Store(originalTxnTotalSizeLimit) }()

	// support ClientLocalFiles capability
	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (a varchar(255), b varchar(255) default 'default value', c int not null auto_increment, primary key(c))")
		dbt.MustExec("create view v1 as select 1")
		dbt.MustExec("create sequence s1")

		// can't insert into views (in TiDB) or sequences. issue #20880
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table v1 with thread=1", path))
		require.Error(t, err)
		require.Equal(t, "Error 1288 (HY000): The target table v1 of the LOAD is not updatable", err.Error())
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table s1 with thread=1", path))
		require.Error(t, err)
		require.Equal(t, "Error 1288 (HY000): The target table s1 of the LOAD is not updatable", err.Error())

		rs, err1 := dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table test with batch_size = 3, thread=1", path))
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
		rs, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table test fields terminated by '\t- ' lines starting by 'xxx ' terminated by '\n' with batch_size = 3, thread=1", path))
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
			_, err = fmt.Fprintf(fp, "xxx row%d_col1	- row%d_col2\n", i, i)
			require.NoError(t, err)
		}
		rs, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table test fields terminated by '\t- ' lines starting by 'xxx ' terminated by '\n' with batch_size = 3, thread=1", path))
		// should be Transaction is too large
		require.ErrorContains(t, err, "Transaction is too large")
		// don't support lines terminated is ""
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table test lines terminated by '' with thread=1", path))
		require.NotNil(t, err)

		// infile doesn't exist
		_, err = dbt.GetDB().Exec("load data local infile '/tmp/nonexistence.csv' into table test with thread=1")
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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (str varchar(10) default null, i int default null)")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table test FIELDS TERMINATED BY ',' enclosed by '"' with batch_size = 3, thread=1`, path))
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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (a date, b date, c date not null, d date)")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table test FIELDS TERMINATED BY ',' with batch_size = 3, thread=1`, path))
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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (a varchar(20), b varchar(20))")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table test FIELDS TERMINATED BY ',' enclosed by '"' with batch_size = 3, thread=1`, path))
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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (id INT NOT NULL PRIMARY KEY,  b INT,  c varchar(10))")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table test FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES with batch_size = 3, thread=1`, path))
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
	server.BitwiseXorCapability(tmysql.ClientLocalFiles)
	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("create table test (a varchar(255), b varchar(255) default 'default value', c int not null auto_increment, primary key(c))")
		_, err = dbt.GetDB().Exec(fmt.Sprintf("load data local infile %q into table test with thread=1", path))
		require.Error(t, err)
		checkErrorCode(t, err, errno.ErrNotAllowedCommand)
	})
	server.BitwiseOrAssignCapability(tmysql.ClientLocalFiles)

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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists pn")
		dbt.MustExec("create table pn (c1 int, c2 int)")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ','  with batch_size = 1, thread=1`, path))
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
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/commitOneTaskErr", "return"))
		_, err1 = dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ',' with thread=1`, path))
		mysqlErr, ok := err1.(*mysql.MySQLError)
		require.True(t, ok)
		require.Equal(t, "mock commit one task error", mysqlErr.Message)
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/commitOneTaskErr"))

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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists pn")
		dbt.MustExec("create table pn (c1 int, c2 int)")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ',' (c1, c2) with batch_size = 1, thread=1`, path))
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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists pn")
		dbt.MustExec("create table pn (c1 int, c2 int, c3 int)")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ',' (c1, @dummy) with batch_size = 1, thread=1`, path))
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

	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists pn")
		dbt.MustExec("create table pn (c1 int, c2 int, c3 int)")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ',' (c1, @val1, @val2) SET c3 = @val2 * 100, c2 = CAST(@val1 AS UNSIGNED) with batch_size = 1, thread=1`, path))
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
	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}, "LoadData", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists pn")
		dbt.MustExec("create table pn (c1 int, c2 int, c3 int)")
		_, err1 := dbt.GetDB().Exec(fmt.Sprintf(`load data local infile %q into table pn FIELDS TERMINATED BY ',' (c1, @VAL1, @VAL2) SET c3 = @VAL2 * 100, c2 = CAST(@VAL1 AS UNSIGNED) with batch_size = 1, thread=1`, path))
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

func (cli *TestServerClient) RunTestExplainForConn(t *testing.T) {
	cli.RunTestsOnNewDB(t, nil, "explain_for_conn", func(dbt *testkit.DBTestKit) {
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

func (cli *TestServerClient) RunTestErrorCode(t *testing.T) {
	cli.RunTestsOnNewDB(t, nil, "ErrorCode", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("set @@tidb_txn_mode=''")
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

func (cli *TestServerClient) RunTestAuth(t *testing.T) {
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE USER 'authtest'@'%' IDENTIFIED BY '123';`)
		dbt.MustExec(`CREATE ROLE 'authtest_r1'@'%';`)
		dbt.MustExec(`GRANT ALL on test.* to 'authtest'`)
		dbt.MustExec(`GRANT authtest_r1 to 'authtest'`)
		dbt.MustExec(`SET DEFAULT ROLE authtest_r1 TO authtest`)
	})
	cli.RunTests(t, func(config *mysql.Config) {
		config.User = "authtest"
		config.Passwd = "123"
	}, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`USE information_schema;`)
	})

	db, err := sql.Open("mysql", cli.GetDSN(func(config *mysql.Config) {
		config.User = "authtest"
		config.Passwd = "456"
	}))
	require.NoError(t, err)
	_, err = db.Exec("USE information_schema;")
	require.NotNilf(t, err, "Wrong password should be failed")
	require.NoError(t, db.Close())

	// Test for loading active roles.
	db, err = sql.Open("mysql", cli.GetDSN(func(config *mysql.Config) {
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
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE USER 'authtest2'@'localhost' IDENTIFIED BY '123';`)
		dbt.MustExec(`GRANT ALL on test.* to 'authtest2'@'localhost'`)
	})
	cli.RunTests(t, func(config *mysql.Config) {
		config.User = "authtest2"
		config.Passwd = "123"
	}, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`USE information_schema;`)
	})
}

func (cli *TestServerClient) RunTestIssue3662(t *testing.T) {
	db, err := sql.Open("mysql", cli.GetDSN(func(config *mysql.Config) {
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
	require.Equal(t, "Error 1049 (42000): Unknown database 'non_existing_schema'", err.Error())
}

func (cli *TestServerClient) RunTestIssue3680(t *testing.T) {
	db, err := sql.Open("mysql", cli.GetDSN(func(config *mysql.Config) {
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
	require.Equal(t, "Error 1045 (28000): Access denied for user 'non_existing_user'@'127.0.0.1' (using password: NO)", err.Error())
}

func (cli *TestServerClient) RunTestIssue22646(t *testing.T) {
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		now := time.Now()
		dbt.MustExec(``)
		if time.Since(now) > 30*time.Second {
			t.Fatal("read empty query statement timed out.")
		}
	})
}

func (cli *TestServerClient) RunTestIssue3682(t *testing.T) {
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE USER 'issue3682'@'%' IDENTIFIED BY '123';`)
		dbt.MustExec(`GRANT ALL on test.* to 'issue3682'`)
		dbt.MustExec(`GRANT ALL on mysql.* to 'issue3682'`)
	})
	cli.RunTests(t, func(config *mysql.Config) {
		config.User = "issue3682"
		config.Passwd = "123"
	}, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`USE mysql;`)
	})
	db, err := sql.Open("mysql", cli.GetDSN(func(config *mysql.Config) {
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
	require.Equal(t, "Error 1045 (28000): Access denied for user 'issue3682'@'127.0.0.1' (using password: YES)", err.Error())
}

func (cli *TestServerClient) RunTestAccountLock(t *testing.T) {
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE USER 'test1' ACCOUNT LOCK;`)
		dbt.MustExec(`CREATE USER 'test2';`) // unlocked default
		dbt.MustExec(`GRANT ALL on test.* to 'test1', 'test2'`)
		dbt.MustExec(`GRANT ALL on mysql.* to 'test1', 'test2'`)
	})
	defer cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`DROP USER 'test1', 'test2';`)
	})

	// 1. test1 can not connect to server
	db, err := sql.Open("mysql", cli.GetDSN(func(config *mysql.Config) {
		config.User = "test1"
	}))
	require.NoError(t, err)
	err = db.Ping()
	require.Error(t, err)
	require.Equal(t, "Error 3118 (HY000): Access denied for user 'test1'@'127.0.0.1'. Account is locked.", err.Error())
	require.NoError(t, db.Close())

	// 2. test1 can connect after unlocked
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`ALTER USER 'test1' ACCOUNT UNLOCK;`)
	})
	db, err = sql.Open("mysql", cli.GetDSN(func(config *mysql.Config) {
		config.User = "test1"
	}))
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	require.NoError(t, db.Close())

	// 3. if multiple 'ACCOUNT (UN)LOCK' declared, the last declaration takes effect
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		rows := dbt.MustQuery(`SELECT user, account_locked FROM mysql.user WHERE user LIKE 'test%' ORDER BY user;`)
		cli.CheckRows(t, rows, "test1 N", "test2 N")
		dbt.MustExec(`ALTER USER test1, test2 ACCOUNT UNLOCK ACCOUNT LOCK;`)
		rows = dbt.MustQuery(`SELECT user, account_locked FROM mysql.user WHERE user LIKE 'test%' ORDER BY user;`)
		cli.CheckRows(t, rows, "test1 Y", "test2 Y")
		dbt.MustExec(`ALTER USER test1, test2 ACCOUNT LOCK ACCOUNT UNLOCK;`)
		rows = dbt.MustQuery(`SELECT user, account_locked FROM mysql.user WHERE user LIKE 'test%' ORDER BY user;`)
		cli.CheckRows(t, rows, "test1 N", "test2 N")
		dbt.MustExec(`ALTER USER test1, test2;`) // if not specified, remain the same
		rows = dbt.MustQuery(`SELECT user, account_locked FROM mysql.user WHERE user LIKE 'test%' ORDER BY user;`)
		cli.CheckRows(t, rows, "test1 N", "test2 N")
	})

	// 4. A role can be created default with account locked
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`CREATE ROLE role1;`)
		dbt.MustExec(`GRANT ALL on test.* to 'role1'`)
		rows := dbt.MustQuery(`SELECT user, account_locked, password_expired FROM mysql.user WHERE user = 'role1';`)
		cli.CheckRows(t, rows, "role1 Y Y")
	})
	// When created, the role is locked by default and cannot log in to TiDB
	db, err = sql.Open("mysql", cli.GetDSN(func(config *mysql.Config) {
		config.User = "role1"
	}))
	require.NoError(t, err)
	err = db.Ping()
	require.Error(t, err)
	require.Equal(t, "Error 3118 (HY000): Access denied for user 'role1'@'127.0.0.1'. Account is locked.", err.Error())
	require.NoError(t, db.Close())
	// After unlocked by the ALTER USER statement, the role can connect to server like a user
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`ALTER USER role1 ACCOUNT UNLOCK;`)
		dbt.MustExec(`ALTER USER role1 IDENTIFIED BY ''`)
		rows := dbt.MustQuery(`SELECT user, account_locked, password_expired FROM mysql.user WHERE user = 'role1';`)
		cli.CheckRows(t, rows, "role1 N N")
	})
	defer cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`DROP ROLE role1;`)
	})
	db, err = sql.Open("mysql", cli.GetDSN(func(config *mysql.Config) {
		config.User = "role1"
	}))
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	require.NoError(t, db.Close())

	// 5. The ability to use a view is not affected by locking the account.
	cli.RunTests(t, func(config *mysql.Config) {
		config.User = "test1"
	}, func(dbt *testkit.DBTestKit) {
		dbt.MustExec("CREATE TABLE IF NOT EXISTS t (id INT, name VARCHAR(16))")
		dbt.MustExec("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	})
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`ALTER USER test1 ACCOUNT LOCK;`)
		rows := dbt.MustQuery(`SELECT user, account_locked FROM mysql.user WHERE user = 'test1';`)
		cli.CheckRows(t, rows, "test1 Y")
		_ = dbt.MustExec("CREATE VIEW v AS SELECT name FROM t WHERE id = 2")
		rows = dbt.MustQuery("SELECT definer, security_type FROM information_schema.views WHERE table_name = 'v'")
		cli.CheckRows(t, rows, "root@% DEFINER")
		rows = dbt.MustQuery(`SELECT * FROM v;`)
		cli.CheckRows(t, rows, "b")
	})
}

func (cli *TestServerClient) RunTestDBNameEscape(t *testing.T) {
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec("CREATE DATABASE `aa-a`;")
	})
	cli.RunTests(t, func(config *mysql.Config) {
		config.DBName = "aa-a"
	}, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`USE mysql;`)
		dbt.MustExec("DROP DATABASE `aa-a`")
	})
}

func (cli *TestServerClient) RunTestResultFieldTableIsNull(t *testing.T) {
	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
		config.Params["sql_mode"] = "''"
	}, "ResultFieldTableIsNull", func(dbt *testkit.DBTestKit) {
		dbt.MustExec("drop table if exists test;")
		dbt.MustExec("create table test (c int);")
		dbt.MustExec("explain select * from test;")
	})
}

func (cli *TestServerClient) RunTestStatusAPI(t *testing.T) {
	resp, err := cli.FetchStatus("/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var data server.Status
	err = decoder.Decode(&data)
	require.NoError(t, err)
	require.Equal(t, tmysql.ServerVersion, data.Version)
	require.Equal(t, versioninfo.TiDBGitHash, data.GitHash)
}

// The golang sql driver (and most drivers) should have multi-statement
// disabled by default for security reasons. Lets ensure that the behavior
// is correct.

func (cli *TestServerClient) RunFailedTestMultiStatements(t *testing.T) {
	cli.RunTestsOnNewDB(t, nil, "FailedMultiStatements", func(dbt *testkit.DBTestKit) {
		// Default is now OFF in new installations.
		// It is still WARN in upgrade installations (for now)
		_, err := dbt.GetDB().Exec("SELECT 1; SELECT 1; SELECT 2; SELECT 3;")
		require.Equal(t, "Error 8130 (HY000): client has multi-statement capability disabled. Run SET GLOBAL tidb_multi_statement_mode='ON' after you understand the security risk", err.Error())

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
		cli.CheckRows(t, rows, "Warning 8130 client has multi-statement capability disabled. Run SET GLOBAL tidb_multi_statement_mode='ON' after you understand the security risk")
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

func (cli *TestServerClient) RunTestMultiStatements(t *testing.T) {
	cli.RunTestsOnNewDB(t, func(config *mysql.Config) {
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

		// Test issue #50012
		dbt.MustExec("create database if not exists test;")
		dbt.MustExec("use test;")
		dbt.MustExec("CREATE TABLE t (a bigint(20), b int(10), PRIMARY KEY (b, a), UNIQUE KEY uk_a (a));")
		dbt.MustExec("insert into t values (1, 1);")
		dbt.MustExec("begin;")
		rs := dbt.MustQuery("delete from t where a = 1; select 1;")
		rs.Close()
		rs = dbt.MustQuery("update t set b = 2 where a = 1; select 1;")
		rs.Close()
		dbt.MustExec("commit;")
	})
}

func (cli *TestServerClient) RunTestStmtCount(t *testing.T) {
	cli.RunTestsOnNewDB(t, nil, "StatementCount", func(dbt *testkit.DBTestKit) {
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
		require.Equal(t, originStmtCnt["Update"]+2, currentStmtCnt["Update"])
		require.Equal(t, originStmtCnt["Select"]+3, currentStmtCnt["Select"])
		require.Equal(t, originStmtCnt["Prepare"]+2, currentStmtCnt["Prepare"])
		require.Equal(t, originStmtCnt["Execute"]+0, currentStmtCnt["Execute"])
		require.Equal(t, originStmtCnt["Replace"]+1, currentStmtCnt["Replace"])
	})
}

func (cli *TestServerClient) RunTestDBStmtCount(t *testing.T) {
	// When run this test solely, the test can be stable.
	// But if other tests run during this, the result can be unstable.
	// Because the test collect some metrics before / after the test, and the metrics data are polluted.
	t.Skip("unstable test")

	cli.RunTestsOnNewDB(t, nil, "DBStatementCount", func(dbt *testkit.DBTestKit) {
		originStmtCnt := getDBStmtCnt(string(cli.getMetrics(t)), "DBStatementCount")

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
		// test for CTE
		dbt.MustExec("WITH RECURSIVE cte (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 5) SELECT * FROM cte;")

		dbt.MustExec("use DBStatementCount")
		dbt.MustExec("create table t2 (id int);")
		dbt.MustExec("truncate table t2;")
		dbt.MustExec("show tables;")
		dbt.MustExec("show create table t2;")
		dbt.MustExec("analyze table t2;")
		dbt.MustExec("analyze table test;")
		dbt.MustExec("alter table t2 add column name varchar(10);")
		dbt.MustExec("rename table t2 to t3;")
		dbt.MustExec("rename table t3 to t2;")
		dbt.MustExec("drop table t2;")

		currentStmtCnt := getStmtCnt(string(cli.getMetrics(t)))
		require.Equal(t, originStmtCnt["CreateTable"]+3, currentStmtCnt["CreateTable"])
		require.Equal(t, originStmtCnt["Insert"]+5, currentStmtCnt["Insert"])
		require.Equal(t, originStmtCnt["Delete"]+1, currentStmtCnt["Delete"])
		require.Equal(t, originStmtCnt["Update"]+2, currentStmtCnt["Update"])
		require.Equal(t, originStmtCnt["Select"]+5, currentStmtCnt["Select"])
		require.Equal(t, originStmtCnt["Prepare"]+2, currentStmtCnt["Prepare"])
		require.Equal(t, originStmtCnt["Execute"]+0, currentStmtCnt["Execute"])
		require.Equal(t, originStmtCnt["Replace"]+1, currentStmtCnt["Replace"])
		require.Equal(t, originStmtCnt["Use"]+6, currentStmtCnt["Use"])
		require.Equal(t, originStmtCnt["TruncateTable"]+1, currentStmtCnt["TruncateTable"])
		require.Equal(t, originStmtCnt["Show"]+2, currentStmtCnt["Show"])
		require.Equal(t, originStmtCnt["AnalyzeTable"]+2, currentStmtCnt["AnalyzeTable"])
		require.Equal(t, originStmtCnt["AlterTable"]+1, currentStmtCnt["AlterTable"])
		require.Equal(t, originStmtCnt["DropTable"]+1, currentStmtCnt["DropTable"])
		require.Equal(t, originStmtCnt["other"]+2, currentStmtCnt["other"])
	})
}

func (cli *TestServerClient) RunTestTLSConnection(t *testing.T, overrider configOverrider) error {
	dsn := cli.GetDSN(overrider)
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

func (cli *TestServerClient) RunTestEnableSecureTransport(t *testing.T, overrider configOverrider) error {
	dsn := cli.GetDSN(overrider)
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

func (cli *TestServerClient) RunReloadTLS(t *testing.T, overrider configOverrider, errorNoRollback bool) error {
	db, err := sql.Open("mysql", cli.GetDSN(overrider))
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

func (cli *TestServerClient) RunTestSumAvg(t *testing.T) {
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
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

func (cli *TestServerClient) getMetrics(t *testing.T) []byte {
	resp, err := cli.FetchStatus("/metrics")
	require.NoError(t, err)
	content, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	return content
}

func getStmtCnt(content string) (stmtCnt map[string]int) {
	stmtCnt = make(map[string]int)
	r := regexp.MustCompile("tidb_executor_statement_total{db=\"\",resource_group=\".*\",type=\"([A-Z|a-z|-]+)\"} (\\d+)")
	matchResult := r.FindAllStringSubmatch(content, -1)
	for _, v := range matchResult {
		cnt, _ := strconv.Atoi(v[2])
		stmtCnt[v[1]] = cnt
	}
	return stmtCnt
}

func getDBStmtCnt(content, dbName string) (stmtCnt map[string]int) {
	stmtCnt = make(map[string]int)
	r := regexp.MustCompile(fmt.Sprintf("tidb_executor_statement_total{db=\"%s\",resource_group=\".*\",type=\"([A-Z|a-z|-]+)\"} (\\d+)", dbName))
	matchResult := r.FindAllStringSubmatch(content, -1)
	for _, v := range matchResult {
		cnt, _ := strconv.Atoi(v[2])
		stmtCnt[v[1]] = cnt
	}
	return stmtCnt
}

const retryTime = 100

func (cli *TestServerClient) WaitUntilCustomServerCanConnect(overriders ...configOverrider) {
	// connect server
	retry := 0
	dsn := cli.GetDSN(overriders...)
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
func (cli *TestServerClient) WaitUntilServerCanConnect() {
	cli.WaitUntilCustomServerCanConnect(nil)
}

func (cli *TestServerClient) WaitUntilServerOnline() {
	// connect server
	cli.WaitUntilServerCanConnect()

	retry := 0
	for ; retry < retryTime; retry++ {
		// fetch http status
		resp, err := cli.FetchStatus("/status")
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

func (cli *TestServerClient) RunTestInitConnect(t *testing.T) {
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		dbt.MustExec(`SET GLOBAL init_connect="insert into test.ts VALUES (NOW());SET @a=1;"`)
		dbt.MustExec(`CREATE USER init_nonsuper`)
		dbt.MustExec(`CREATE USER init_super`)
		dbt.MustExec(`GRANT SELECT, INSERT, DROP ON test.* TO init_nonsuper`)
		dbt.MustExec(`GRANT SELECT, INSERT, DROP, SUPER ON *.* TO init_super`)
		dbt.MustExec(`CREATE TABLE ts (a TIMESTAMP)`)
	})

	// test init_nonsuper
	cli.RunTests(t, func(config *mysql.Config) {
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
	cli.RunTests(t, func(config *mysql.Config) {
		config.User = "init_super"
	}, func(dbt *testkit.DBTestKit) {
		rows := dbt.MustQuery(`SELECT IFNULL(@a,"")`)
		require.True(t, rows.Next())
		var a string
		err := rows.Scan(&a)
		require.NoError(t, err)
		require.Equal(t, "", a)
		require.NoError(t, rows.Close())
	})
	// set global init_connect to empty to avoid fail other tests
	defer cli.RunTests(t, func(config *mysql.Config) {
		config.User = "init_super"
	}, func(dbt *testkit.DBTestKit) {
		// set init_connect to empty to avoid fail other tests
		dbt.MustExec(`SET GLOBAL init_connect=""`)
	})

	db, err := sql.Open("mysql", cli.GetDSN(func(config *mysql.Config) {
		config.User = "init_nonsuper"
	}))
	require.NoError(t, err) // doesn't fail because of lazy loading
	defer db.Close()        // may already be closed
}

// Client errors are only incremented when using the TiDB Server protocol,
// and not internal SQL statements. Thus, this test is in the server-test suite.
func (cli *TestServerClient) RunTestInfoschemaClientErrors(t *testing.T) {
	cli.RunTestsOnNewDB(t, nil, "clientErrors", func(dbt *testkit.DBTestKit) {
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

func (cli *TestServerClient) RunTestSQLModeIsLoadedBeforeQuery(t *testing.T) {
	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		ctx := context.Background()

		conn, err := dbt.GetDB().Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "set global sql_mode='NO_BACKSLASH_ESCAPES';")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, `
		CREATE TABLE t1 (
			id bigint(20) NOT NULL,
			t text DEFAULT NULL,
			PRIMARY KEY (id)
		);`)
		require.NoError(t, err)

		// use another new connection
		conn1, err := dbt.GetDB().Conn(ctx)
		require.NoError(t, err)
		_, err = conn1.ExecContext(ctx, "insert into t1 values (1, 'ab\\\\c');")
		require.NoError(t, err)
		result, err := conn1.QueryContext(ctx, "select t from t1 where id = 1;")
		require.NoError(t, err)
		require.True(t, result.Next())
		var tStr string
		require.NoError(t, result.Scan(&tStr))

		require.Equal(t, "ab\\\\c", tStr)
	})
}

func (cli *TestServerClient) RunTestConnectionCount(t *testing.T) {
	readConnCount := func(resourceGroupName string) float64 {
		metric, err := metrics.ConnGauge.GetMetricWith(map[string]string{
			metrics.LblResourceGroup: resourceGroupName,
		})
		require.NoError(t, err)
		output := &dto.Metric{}
		metric.Write(output)

		return output.GetGauge().GetValue()
	}

	resourceGroupConnCountReached := func(t *testing.T, resourceGroupName string, expected float64) {
		require.Eventually(t, func() bool {
			return readConnCount(resourceGroupName) == expected
		}, 5*time.Second, 100*time.Millisecond)
	}

	cli.RunTests(t, nil, func(dbt *testkit.DBTestKit) {
		ctx := context.Background()
		dbt.GetDB().SetMaxIdleConns(0)

		// start 100 connections
		conns := make([]*sql.Conn, 100)
		for i := 0; i < 100; i++ {
			conn, err := dbt.GetDB().Conn(ctx)
			require.NoError(t, err)
			conns[i] = conn
		}
		resourceGroupConnCountReached(t, "default", 100.0)

		// close 50 connections
		for i := 0; i < 50; i++ {
			err := conns[i].Close()
			require.NoError(t, err)
		}
		resourceGroupConnCountReached(t, "default", 50.0)

		// close 25 connections
		for i := 50; i < 75; i++ {
			err := conns[i].Close()
			require.NoError(t, err)
		}
		resourceGroupConnCountReached(t, "default", 25.0)

		// change the following 25 connections from `default` resource group to `test`
		dbt.MustExec("create resource group test RU_PER_SEC = 1000;")
		for i := 75; i < 100; i++ {
			_, err := conns[i].ExecContext(ctx, "set resource group test")
			require.NoError(t, err)
		}
		resourceGroupConnCountReached(t, "default", 0.0)
		resourceGroupConnCountReached(t, "test", 25.0)

		// close 25 connections
		for i := 75; i < 100; i++ {
			err := conns[i].Close()
			require.NoError(t, err)
		}
		resourceGroupConnCountReached(t, "default", 0.0)
		resourceGroupConnCountReached(t, "test", 0.0)
	})
}

func (cli *TestServerClient) RunTestTypeAndCharsetOfSendLongData(t *testing.T) {
	cli.RunTests(t, func(config *mysql.Config) {
		config.MaxAllowedPacket = 1024
	}, func(dbt *testkit.DBTestKit) {
		ctx := context.Background()

		conn, err := dbt.GetDB().Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CREATE TABLE t (j JSON);")
		require.NoError(t, err)

		str := `"` + strings.Repeat("a", 1024) + `"`
		stmt, err := conn.PrepareContext(ctx, "INSERT INTO t VALUES (cast(? as JSON));")
		require.NoError(t, err)
		_, err = stmt.ExecContext(ctx, str)
		require.NoError(t, err)
		result, err := conn.QueryContext(ctx, "SELECT j FROM t;")
		require.NoError(t, err)

		for result.Next() {
			var j string
			require.NoError(t, result.Scan(&j))
			require.Equal(t, str, j)
		}
	})

	str := strings.Repeat("你好", 1024)
	enc := simplifiedchinese.GBK.NewEncoder()
	gbkStr, err := enc.String(str)
	require.NoError(t, err)

	cli.RunTests(t, func(config *mysql.Config) {
		config.MaxAllowedPacket = 1024
		config.Params["charset"] = "gbk"
	}, func(dbt *testkit.DBTestKit) {
		ctx := context.Background()

		conn, err := dbt.GetDB().Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "drop table t")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CREATE TABLE t (t TEXT);")
		require.NoError(t, err)

		stmt, err := conn.PrepareContext(ctx, "INSERT INTO t VALUES (?);")
		require.NoError(t, err)
		_, err = stmt.ExecContext(ctx, gbkStr)
		require.NoError(t, err)

		result, err := conn.QueryContext(ctx, "SELECT * FROM t;")
		require.NoError(t, err)

		for result.Next() {
			var txt string
			require.NoError(t, result.Scan(&txt))
			require.Equal(t, gbkStr, txt)
		}
	})
}

//revive:enable:exported
