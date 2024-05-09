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
	"compress/gzip"
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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

	fileName0 := "tidb-slow-query-2020-02-14T19-04-05.01.log"
	fileName1 := "tidb-slow-query-2020-02-15T19-04-05.01.log"
	fileName2 := "tidb-slow-query-2020-02-16T19-04-05.01.log"
	fileName3 := "tidb-slow-query-2020-02-17T18-00-05.01.log"
	fileName4 := "tidb-slow-query.log"
	fileNames := []string{fileName0, fileName1, fileName2, fileName3, fileName4}
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowQueryFile = fileName4
	})

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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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

	fileName0 := "tidb-slow-20236-2020-02-14T19-04-05.01.log"
	fileName1 := "tidb-slow-20236-2020-02-15T19-04-05.01.log"
	fileName2 := "tidb-slow-20236-2020-02-16T19-04-05.01.log"
	fileName3 := "tidb-slow-20236-2020-02-17T18-00-05.01.log"
	fileName4 := "tidb-slow-20236.log"
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowQueryFile = fileName4
	})
	for k := 0; k < 2; k++ {
		// k = 0 for normal files
		// k = 1 for compressed files
		var fileNames []string
		if k == 0 {
			fileNames = []string{fileName0, fileName1, fileName2, fileName3, fileName4}
		} else {
			fileNames = []string{fileName0 + ".gz", fileName1 + ".gz", fileName2 + ".gz", fileName3 + ".gz", fileName4}
		}
		prepareLogs(t, logData, fileNames)
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
				sql:        "select time from cluster_slow_query where (time > '2020-02-15 18:00:00' and time < '2020-02-15 20:00:00') or (time > '2020-02-17 18:00:00' and time < '2020-05-14 20:00:00') order by time",
				result:     []string{"2020-02-15 18:00:01.000000", "2020-02-15 19:00:05.000000", "2020-02-17 18:00:05.000000", "2020-02-17 19:00:00.000000", "2020-05-14 19:03:54.314615"},
			},
			{
				prepareSQL: "set @@time_zone = '+08:00'",
				sql:        "select time from cluster_slow_query where (time > '2020-02-15 18:00:00' and time < '2020-02-15 20:00:00') or (time > '2020-02-17 18:00:00' and time < '2020-05-14 20:00:00') order by time desc",
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
		removeFiles(t, fileNames)
	}
}

func TestSQLDigestTextRetriever(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("insert into test_sql_digest_text_retriever values (1, 1)")

	insertNormalized, insertDigest := parser.NormalizeDigest("insert into test_sql_digest_text_retriever values (1, 1)")
	_, updateDigest := parser.NormalizeDigest("update test_sql_digest_text_retriever set v = v + 1 where id = 1")
	r := &expression.SQLDigestTextRetriever{
		SQLDigestsMap: map[string]string{
			insertDigest.String(): "",
			updateDigest.String(): "",
		},
	}

	err := r.RetrieveLocal(context.Background(), tk.Session().GetRestrictedSQLExecutor())
	require.NoError(t, err)
	require.Equal(t, insertNormalized, r.SQLDigestsMap[insertDigest.String()])
	require.Equal(t, "", r.SQLDigestsMap[updateDigest.String()])
}

func prepareLogs(t *testing.T, logData []string, fileNames []string) {
	writeFile := func(file string, data string) {
		if strings.HasSuffix(file, ".gz") {
			f, err := os.Create(file)
			require.NoError(t, err)
			gz := gzip.NewWriter(f)
			_, err = gz.Write([]byte(data))
			require.NoError(t, err)
			require.NoError(t, gz.Close())
			require.NoError(t, f.Close())
		} else {
			f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			require.NoError(t, err)
			_, err = f.Write([]byte(data))
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}
	}

	for i, log := range logData {
		writeFile(fileNames[i], log)
	}
}

func removeFiles(t *testing.T, fileNames []string) {
	for _, fileName := range fileNames {
		require.NoError(t, os.Remove(fileName))
	}
}
