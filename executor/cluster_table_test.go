// Copyright 2022 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func createRPCServer(t *testing.T, dom *domain.Domain) *grpc.Server {
	sm := &testkit.MockSessionManager{}
	sm.PS = append(sm.PS, &util.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: mysql.ComQuery,
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := server.NewRPCServer(config.GetGlobalConfig(), dom, sm)
	port := lis.Addr().(*net.TCPAddr).Port
	go func() {
		err = srv.Serve(lis)
		require.NoError(t, err)
	}()

	config.UpdateGlobal(func(conf *config.Config) {
		conf.Status.StatusPort = uint(port)
	})

	return srv
}

func TestClusterTableSlowQuery(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	srv := createRPCServer(t, dom)
	defer srv.Stop()

	logData0 := ""
	logData1 := `
# Time: 2020-02-15T18:00:01.000000+08:00
select 1;
# Time: 2020-02-15T19:00:05.000000+08:00
select 2;`
	logData2 := `
# Time: 2020-02-16T18:00:01.000000+08:00
select 3;
# Time: 2020-02-16T18:00:05.000000+08:00
select 4;`
	logData3 := `
# Time: 2020-02-16T19:00:00.000000+08:00
select 5;
# Time: 2020-02-17T18:00:05.000000+08:00
select 6;`
	logData4 := `
# Time: 2020-05-14T19:03:54.314615176+08:00
select 7;`
	logData := []string{logData0, logData1, logData2, logData3, logData4}

	fileName0 := "tidb-slow-2020-02-14T19-04-05.01.log"
	fileName1 := "tidb-slow-2020-02-15T19-04-05.01.log"
	fileName2 := "tidb-slow-2020-02-16T19-04-05.01.log"
	fileName3 := "tidb-slow-2020-02-17T18-00-05.01.log"
	fileName4 := "tidb-slow.log"
	fileNames := []string{fileName0, fileName1, fileName2, fileName3, fileName4}

	prepareLogs(t, logData, fileNames)
	defer func() {
		removeFiles(t, fileNames)
	}()
	tk := testkit.NewTestKit(t, store)
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	tk.Session().GetSessionVars().TimeZone = loc
	tk.MustExec("use information_schema")
	cases := []struct {
		prepareSQL string
		sql        string
		result     []string
	}{
		{
			sql:    "select count(*),min(time),max(time) from %s where time > '2019-01-26 21:51:00' and time < now()",
			result: []string{"7|2020-02-15 18:00:01.000000|2020-05-14 19:03:54.314615"},
		},
		{
			sql:    "select count(*),min(time),max(time) from %s where time > '2020-02-15 19:00:00' and time < '2020-02-16 18:00:02'",
			result: []string{"2|2020-02-15 19:00:05.000000|2020-02-16 18:00:01.000000"},
		},
		{
			sql:    "select count(*),min(time),max(time) from %s where time > '2020-02-16 18:00:02' and time < '2020-02-17 17:00:00'",
			result: []string{"2|2020-02-16 18:00:05.000000|2020-02-16 19:00:00.000000"},
		},
		{
			sql:    "select count(*),min(time),max(time) from %s where time > '2020-02-16 18:00:02' and time < '2020-02-17 20:00:00'",
			result: []string{"3|2020-02-16 18:00:05.000000|2020-02-17 18:00:05.000000"},
		},
		{
			sql:    "select count(*),min(time),max(time) from %s",
			result: []string{"1|2020-05-14 19:03:54.314615|2020-05-14 19:03:54.314615"},
		},
		{
			sql:    "select count(*),min(time) from %s where time > '2020-02-16 20:00:00'",
			result: []string{"1|2020-02-17 18:00:05.000000"},
		},
		{
			sql:    "select count(*) from %s where time > '2020-02-17 20:00:00'",
			result: []string{"0"},
		},
		{
			sql:    "select query from %s where time > '2019-01-26 21:51:00' and time < now()",
			result: []string{"select 1;", "select 2;", "select 3;", "select 4;", "select 5;", "select 6;", "select 7;"},
		},
		// Test for different timezone.
		{
			prepareSQL: "set @@time_zone = '+00:00'",
			sql:        "select time from %s where time = '2020-02-17 10:00:05.000000'",
			result:     []string{"2020-02-17 10:00:05.000000"},
		},
		{
			prepareSQL: "set @@time_zone = '+02:00'",
			sql:        "select time from %s where time = '2020-02-17 12:00:05.000000'",
			result:     []string{"2020-02-17 12:00:05.000000"},
		},
		// Test for issue 17224
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from %s where time = '2020-05-14 19:03:54.314615'",
			result:     []string{"2020-05-14 19:03:54.314615"},
		},
	}
	for _, cas := range cases {
		if len(cas.prepareSQL) > 0 {
			tk.MustExec(cas.prepareSQL)
		}
		sql := fmt.Sprintf(cas.sql, "slow_query")
		tk.MustQuery(sql).Check(testkit.RowsWithSep("|", cas.result...))
		sql = fmt.Sprintf(cas.sql, "cluster_slow_query")
		tk.MustQuery(sql).Check(testkit.RowsWithSep("|", cas.result...))
	}
}

func TestIssue20236(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	srv := createRPCServer(t, dom)
	defer srv.Stop()

	logData0 := ""
	logData1 := `
# Time: 2020-02-15T18:00:01.000000+08:00
select 1;
# Time: 2020-02-15T19:00:05.000000+08:00
select 2;
# Time: 2020-02-15T20:00:05.000000+08:00`
	logData2 := `select 3;
# Time: 2020-02-16T18:00:01.000000+08:00
select 4;
# Time: 2020-02-16T18:00:05.000000+08:00
select 5;`
	logData3 := `
# Time: 2020-02-16T19:00:00.000000+08:00
select 6;
# Time: 2020-02-17T18:00:05.000000+08:00
select 7;
# Time: 2020-02-17T19:00:00.000000+08:00`
	logData4 := `select 8;
# Time: 2020-02-17T20:00:00.000000+08:00
select 9
# Time: 2020-05-14T19:03:54.314615176+08:00
select 10;`
	logData := []string{logData0, logData1, logData2, logData3, logData4}

	fileName0 := "tidb-slow-2020-02-14T19-04-05.01.log"
	fileName1 := "tidb-slow-2020-02-15T19-04-05.01.log"
	fileName2 := "tidb-slow-2020-02-16T19-04-05.01.log"
	fileName3 := "tidb-slow-2020-02-17T18-00-05.01.log"
	fileName4 := "tidb-slow.log"
	fileNames := []string{fileName0, fileName1, fileName2, fileName3, fileName4}
	prepareLogs(t, logData, fileNames)
	defer func() {
		removeFiles(t, fileNames)
	}()
	tk := testkit.NewTestKit(t, store)
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	tk.Session().GetSessionVars().TimeZone = loc
	tk.MustExec("use information_schema")
	cases := []struct {
		prepareSQL string
		sql        string
		result     []string
	}{
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from cluster_slow_query where time > '2020-02-17 12:00:05.000000' and time < '2020-05-14 20:00:00.000000'",
			result:     []string{"2020-02-17 18:00:05.000000", "2020-02-17 19:00:00.000000", "2020-05-14 19:03:54.314615"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from cluster_slow_query where time > '2020-02-17 12:00:05.000000' and time < '2020-05-14 20:00:00.000000' order by time desc",
			result:     []string{"2020-05-14 19:03:54.314615", "2020-02-17 19:00:00.000000", "2020-02-17 18:00:05.000000"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from cluster_slow_query where (time > '2020-02-15 18:00:00' and time < '2020-02-15 20:01:00') or (time > '2020-02-17 18:00:00' and time < '2020-05-14 20:00:00') order by time",
			result:     []string{"2020-02-15 18:00:01.000000", "2020-02-15 19:00:05.000000", "2020-02-17 18:00:05.000000", "2020-02-17 19:00:00.000000", "2020-05-14 19:03:54.314615"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from cluster_slow_query where (time > '2020-02-15 18:00:00' and time < '2020-02-15 20:01:00') or (time > '2020-02-17 18:00:00' and time < '2020-05-14 20:00:00') order by time desc",
			result:     []string{"2020-05-14 19:03:54.314615", "2020-02-17 19:00:00.000000", "2020-02-17 18:00:05.000000", "2020-02-15 19:00:05.000000", "2020-02-15 18:00:01.000000"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select count(*) from cluster_slow_query where time > '2020-02-15 18:00:00.000000' and time < '2020-05-14 20:00:00.000000' order by time desc",
			result:     []string{"9"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select count(*) from cluster_slow_query where (time > '2020-02-16 18:00:00' and time < '2020-05-14 20:00:00') or (time > '2020-02-17 18:00:00' and time < '2020-05-17 20:00:00')",
			result:     []string{"6"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select count(*) from cluster_slow_query where time > '2020-02-16 18:00:00.000000' and time < '2020-02-17 20:00:00.000000' order by time desc",
			result:     []string{"5"},
		},
		{
			prepareSQL: "set @@time_zone = '+08:00'",
			sql:        "select time from cluster_slow_query where time > '2020-02-16 18:00:00.000000' and time < '2020-05-14 20:00:00.000000' order by time desc limit 3",
			result:     []string{"2020-05-14 19:03:54.314615", "2020-02-17 19:00:00.000000", "2020-02-17 18:00:05.000000"},
		},
	}
	for _, cas := range cases {
		if len(cas.prepareSQL) > 0 {
			tk.MustExec(cas.prepareSQL)
		}
		tk.MustQuery(cas.sql).Check(testkit.RowsWithSep("|", cas.result...))
	}
}

func TestSQLDigestTextRetriever(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	srv := createRPCServer(t, dom)
	defer srv.Stop()

	tkInit := testkit.NewTestKit(t, store)
	tkInit.MustExec("use test")
	tkInit.MustExec("set global tidb_enable_stmt_summary = 1")
	tkInit.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))
	tkInit.MustExec("drop table if exists test_sql_digest_text_retriever")
	tkInit.MustExec("create table test_sql_digest_text_retriever (id int primary key, v int)")

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("insert into test_sql_digest_text_retriever values (1, 1)")

	insertNormalized, insertDigest := parser.NormalizeDigest("insert into test_sql_digest_text_retriever values (1, 1)")
	_, updateDigest := parser.NormalizeDigest("update test_sql_digest_text_retriever set v = v + 1 where id = 1")
	r := &expression.SQLDigestTextRetriever{
		SQLDigestsMap: map[string]string{
			insertDigest.String(): "",
			updateDigest.String(): "",
		},
	}
	err := r.RetrieveLocal(context.Background(), tk.Session())
	require.NoError(t, err)
	require.Equal(t, insertNormalized, r.SQLDigestsMap[insertDigest.String()])
	require.Equal(t, "", r.SQLDigestsMap[updateDigest.String()])
}

func TestFunctionDecodeSQLDigests(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	srv := createRPCServer(t, dom)
	defer srv.Stop()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))
	tk.MustExec("drop table if exists test_func_decode_sql_digests")
	tk.MustExec("create table test_func_decode_sql_digests(id int primary key, v int)")

	q1 := "begin"
	norm1, digest1 := parser.NormalizeDigest(q1)
	q2 := "select @@tidb_current_ts"
	norm2, digest2 := parser.NormalizeDigest(q2)
	q3 := "select id, v from test_func_decode_sql_digests where id = 1 for update"
	norm3, digest3 := parser.NormalizeDigest(q3)

	// TIDB_DECODE_SQL_DIGESTS function doesn't actually do "decoding", instead it queries `statements_summary` and it's
	// variations for the corresponding statements.
	// Execute the statements so that the queries will be saved into statements_summary table.
	tk.MustExec(q1)
	// Save the ts to query the transaction from tidb_trx.
	ts, err := strconv.ParseUint(tk.MustQuery(q2).Rows()[0][0].(string), 10, 64)
	require.NoError(t, err)
	require.Greater(t, ts, uint64(0))
	tk.MustExec(q3)
	tk.MustExec("rollback")

	// Test statements truncating.
	decoded := fmt.Sprintf(`["%s","%s","%s"]`, norm1, norm2, norm3)
	digests := fmt.Sprintf(`["%s","%s","%s"]`, digest1, digest2, digest3)
	tk.MustQuery("select tidb_decode_sql_digests(?, 0)", digests).Check(testkit.Rows(decoded))
	// The three queries are shorter than truncate length, equal to truncate length and longer than truncate length respectively.
	tk.MustQuery("select tidb_decode_sql_digests(?, ?)", digests, len(norm2)).Check(testkit.Rows(
		"[\"begin\",\"select @@tidb_current_ts\",\"select `id` , `v` from `...\"]"))

	// Empty array.
	tk.MustQuery("select tidb_decode_sql_digests('[]')").Check(testkit.Rows("[]"))

	// NULL
	tk.MustQuery("select tidb_decode_sql_digests(null)").Check(testkit.Rows("<nil>"))

	// Array containing wrong types and not-existing digests (maps to null).
	tk.MustQuery("select tidb_decode_sql_digests(?)", fmt.Sprintf(`["%s",1,null,"%s",{"a":1},[2],"%s","","abcde"]`, digest1, digest2, digest3)).
		Check(testkit.Rows(fmt.Sprintf(`["%s",null,null,"%s",null,null,"%s",null,null]`, norm1, norm2, norm3)))

	// Not JSON array (throws warnings)
	tk.MustQuery(`select tidb_decode_sql_digests('{"a":1}')`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1210 The argument can't be unmarshalled as JSON array: '{"a":1}'`))
	tk.MustQuery(`select tidb_decode_sql_digests('aabbccdd')`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1210 The argument can't be unmarshalled as JSON array: 'aabbccdd'`))

	// Invalid argument count.
	tk.MustGetErrCode("select tidb_decode_sql_digests('a', 1, 2)", 1582)
	tk.MustGetErrCode("select tidb_decode_sql_digests()", 1582)
}

func TestFunctionDecodeSQLDigestsPrivilege(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	srv := createRPCServer(t, dom)
	defer srv.Stop()

	dropUserTk := testkit.NewTestKit(t, store)
	require.True(t, dropUserTk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))

	tk := testkit.NewTestKit(t, store)
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("create user 'testuser'@'localhost'")
	defer dropUserTk.MustExec("drop user 'testuser'@'localhost'")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "testuser", Hostname: "localhost"}, nil, nil))
	tk.MustGetErrMsg("select tidb_decode_sql_digests('[\"aa\"]')", "[expression:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	tk = testkit.NewTestKit(t, store)
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("create user 'testuser2'@'localhost'")
	defer dropUserTk.MustExec("drop user 'testuser2'@'localhost'")
	tk.MustExec("grant process on *.* to 'testuser2'@'localhost'")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "testuser2", Hostname: "localhost"}, nil, nil))
	tk.MustExec("select tidb_decode_sql_digests('[\"aa\"]')")
}

func prepareLogs(t *testing.T, logData []string, fileNames []string) {
	for i, log := range logData {
		f, err := os.OpenFile(fileNames[i], os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		require.NoError(t, err)
		_, err = f.Write([]byte(log))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
}

func removeFiles(t *testing.T, fileNames []string) {
	for _, fileName := range fileNames {
		require.NoError(t, os.Remove(fileName))
	}
}
