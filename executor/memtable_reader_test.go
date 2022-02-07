// Copyright 2019 PingCAP, Inc.
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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/fn"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/sysutil"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/testkit"
	pmodel "github.com/prometheus/common/model"
	"google.golang.org/grpc"
)

type testMemTableReaderSuite struct{ *testClusterTableBase }

type testClusterTableBase struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testClusterTableBase) SetUpSuite(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *testClusterTableBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testMemTableReaderSuite) TestMetricTableData(c *C) {
	fpName := "github.com/pingcap/tidb/executor/mockMetricsPromData"
	c.Assert(failpoint.Enable(fpName, "return"), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	// mock prometheus data
	matrix := pmodel.Matrix{}
	metric := map[pmodel.LabelName]pmodel.LabelValue{
		"instance": "127.0.0.1:10080",
	}
	t, err := time.ParseInLocation("2006-01-02 15:04:05.999", "2019-12-23 20:11:35", time.Local)
	c.Assert(err, IsNil)
	v1 := pmodel.SamplePair{
		Timestamp: pmodel.Time(t.UnixNano() / int64(time.Millisecond)),
		Value:     pmodel.SampleValue(0.1),
	}
	matrix = append(matrix, &pmodel.SampleStream{Metric: metric, Values: []pmodel.SamplePair{v1}})

	ctx := context.WithValue(context.Background(), "__mockMetricsPromData", matrix)
	ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
		return fpname == fpName
	})

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use metrics_schema")

	cases := []struct {
		sql string
		exp []string
	}{
		{
			sql: "select time,instance,quantile,value from tidb_query_duration;",
			exp: []string{
				"2019-12-23 20:11:35.000000 127.0.0.1:10080 0.9 0.1",
			},
		},
		{
			sql: "select time,instance,quantile,value from tidb_query_duration where quantile in (0.85, 0.95);",
			exp: []string{
				"2019-12-23 20:11:35.000000 127.0.0.1:10080 0.85 0.1",
				"2019-12-23 20:11:35.000000 127.0.0.1:10080 0.95 0.1",
			},
		},
		{
			sql: "select time,instance,quantile,value from tidb_query_duration where quantile=0.5",
			exp: []string{
				"2019-12-23 20:11:35.000000 127.0.0.1:10080 0.5 0.1",
			},
		},
	}

	for _, cas := range cases {
		rs, err := tk.Se.Execute(ctx, cas.sql)
		c.Assert(err, IsNil)
		result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("sql: %s", cas.sql))
		result.Check(testkit.Rows(cas.exp...))
	}
}

func (s *testMemTableReaderSuite) TestTiDBClusterConfig(c *C) {
	// mock PD http server
	router := mux.NewRouter()

	type mockServer struct {
		address string
		server  *httptest.Server
	}
	const testServerCount = 3
	var testServers []*mockServer
	for i := 0; i < testServerCount; i++ {
		server := httptest.NewServer(router)
		address := strings.TrimPrefix(server.URL, "http://")
		testServers = append(testServers, &mockServer{
			address: address,
			server:  server,
		})
	}
	defer func() {
		for _, server := range testServers {
			server.server.Close()
		}
	}()

	// We check the counter to valid how many times request has been sent
	var requestCounter int32
	var mockConfig = func() (map[string]interface{}, error) {
		atomic.AddInt32(&requestCounter, 1)
		configuration := map[string]interface{}{
			"key1": "value1",
			"key2": map[string]string{
				"nest1": "n-value1",
				"nest2": "n-value2",
			},
			// We need hide the follow config
			// TODO: we need remove it when index usage is GA.
			"performance": map[string]string{
				"index-usage-sync-lease": "0s",
				"INDEX-USAGE-SYNC-LEASE": "0s",
			},
		}
		return configuration, nil
	}

	// pd config
	router.Handle(pdapi.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config
	router.Handle("/config", fn.Wrap(mockConfig))

	// mock servers
	servers := []string{}
	for _, typ := range []string{"tidb", "tikv", "tiflash", "pd"} {
		for _, server := range testServers {
			servers = append(servers, strings.Join([]string{typ, server.address, server.address}, ","))
		}
	}

	fpName := "github.com/pingcap/tidb/executor/mockClusterConfigServerInfo"
	fpExpr := strings.Join(servers, ";")
	c.Assert(failpoint.Enable(fpName, fmt.Sprintf(`return("%s")`, fpExpr)), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select type, `key`, value from information_schema.cluster_config").Check(testkit.Rows(
		"tidb key1 value1",
		"tidb key2.nest1 n-value1",
		"tidb key2.nest2 n-value2",
		"tidb key1 value1",
		"tidb key2.nest1 n-value1",
		"tidb key2.nest2 n-value2",
		"tidb key1 value1",
		"tidb key2.nest1 n-value1",
		"tidb key2.nest2 n-value2",
		"tikv key1 value1",
		"tikv key2.nest1 n-value1",
		"tikv key2.nest2 n-value2",
		"tikv key1 value1",
		"tikv key2.nest1 n-value1",
		"tikv key2.nest2 n-value2",
		"tikv key1 value1",
		"tikv key2.nest1 n-value1",
		"tikv key2.nest2 n-value2",
		"tiflash key1 value1",
		"tiflash key2.nest1 n-value1",
		"tiflash key2.nest2 n-value2",
		"tiflash key1 value1",
		"tiflash key2.nest1 n-value1",
		"tiflash key2.nest2 n-value2",
		"tiflash key1 value1",
		"tiflash key2.nest1 n-value1",
		"tiflash key2.nest2 n-value2",
		"pd key1 value1",
		"pd key2.nest1 n-value1",
		"pd key2.nest2 n-value2",
		"pd key1 value1",
		"pd key2.nest1 n-value1",
		"pd key2.nest2 n-value2",
		"pd key1 value1",
		"pd key2.nest1 n-value1",
		"pd key2.nest2 n-value2",
	))
	warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("unexpected warnigns: %+v", warnings))
	c.Assert(requestCounter, Equals, int32(12))

	// TODO: we need remove it when index usage is GA.
	rs := tk.MustQuery("show config").Rows()
	for _, r := range rs {
		s, ok := r[2].(string)
		c.Assert(ok, IsTrue)
		c.Assert(strings.Contains(s, "index-usage-sync-lease"), IsFalse)
		c.Assert(strings.Contains(s, "INDEX-USAGE-SYNC-LEASE"), IsFalse)
	}

	// type => server index => row
	rows := map[string][][]string{}
	for _, typ := range []string{"tidb", "tikv", "tiflash", "pd"} {
		for _, server := range testServers {
			rows[typ] = append(rows[typ], []string{
				fmt.Sprintf("%s %s key1 value1", typ, server.address),
				fmt.Sprintf("%s %s key2.nest1 n-value1", typ, server.address),
				fmt.Sprintf("%s %s key2.nest2 n-value2", typ, server.address),
			})
		}
	}
	var flatten = func(ss ...[]string) []string {
		var result []string
		for _, xs := range ss {
			result = append(result, xs...)
		}
		return result
	}
	var cases = []struct {
		sql      string
		reqCount int32
		rows     []string
	}{
		{
			sql:      "select * from information_schema.cluster_config",
			reqCount: 12,
			rows: flatten(
				rows["tidb"][0],
				rows["tidb"][1],
				rows["tidb"][2],
				rows["tikv"][0],
				rows["tikv"][1],
				rows["tikv"][2],
				rows["tiflash"][0],
				rows["tiflash"][1],
				rows["tiflash"][2],
				rows["pd"][0],
				rows["pd"][1],
				rows["pd"][2],
			),
		},
		{
			sql:      "select * from information_schema.cluster_config where type='pd' or type='tikv'",
			reqCount: 6,
			rows: flatten(
				rows["tikv"][0],
				rows["tikv"][1],
				rows["tikv"][2],
				rows["pd"][0],
				rows["pd"][1],
				rows["pd"][2],
			),
		},
		{
			sql:      "select * from information_schema.cluster_config where type='pd' or instance='" + testServers[0].address + "'",
			reqCount: 12,
			rows: flatten(
				rows["tidb"][0],
				rows["tikv"][0],
				rows["tiflash"][0],
				rows["pd"][0],
				rows["pd"][1],
				rows["pd"][2],
			),
		},
		{
			sql:      "select * from information_schema.cluster_config where type='pd' and type='tikv'",
			reqCount: 0,
		},
		{
			sql:      "select * from information_schema.cluster_config where type='tikv'",
			reqCount: 3,
			rows: flatten(
				rows["tikv"][0],
				rows["tikv"][1],
				rows["tikv"][2],
			),
		},
		{
			sql:      "select * from information_schema.cluster_config where type='pd'",
			reqCount: 3,
			rows: flatten(
				rows["pd"][0],
				rows["pd"][1],
				rows["pd"][2],
			),
		},
		{
			sql:      "select * from information_schema.cluster_config where type='tidb'",
			reqCount: 3,
			rows: flatten(
				rows["tidb"][0],
				rows["tidb"][1],
				rows["tidb"][2],
			),
		},
		{
			sql:      "select * from information_schema.cluster_config where 'tidb'=type",
			reqCount: 3,
			rows: flatten(
				rows["tidb"][0],
				rows["tidb"][1],
				rows["tidb"][2],
			),
		},
		{
			sql:      "select * from information_schema.cluster_config where type in ('tidb', 'tikv')",
			reqCount: 6,
			rows: flatten(
				rows["tidb"][0],
				rows["tidb"][1],
				rows["tidb"][2],
				rows["tikv"][0],
				rows["tikv"][1],
				rows["tikv"][2],
			),
		},
		{
			sql:      "select * from information_schema.cluster_config where type in ('tidb', 'tikv', 'pd')",
			reqCount: 9,
			rows: flatten(
				rows["tidb"][0],
				rows["tidb"][1],
				rows["tidb"][2],
				rows["tikv"][0],
				rows["tikv"][1],
				rows["tikv"][2],
				rows["pd"][0],
				rows["pd"][1],
				rows["pd"][2],
			),
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where instance='%s'`,
				testServers[0].address),
			reqCount: 4,
			rows: flatten(
				rows["tidb"][0],
				rows["tikv"][0],
				rows["tiflash"][0],
				rows["pd"][0],
			),
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type='tidb' and instance='%s'`,
				testServers[0].address),
			reqCount: 1,
			rows: flatten(
				rows["tidb"][0],
			),
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and instance='%s'`,
				testServers[0].address),
			reqCount: 2,
			rows: flatten(
				rows["tidb"][0],
				rows["tikv"][0],
			),
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and instance in ('%s', '%s')`,
				testServers[0].address, testServers[0].address),
			reqCount: 2,
			rows: flatten(
				rows["tidb"][0],
				rows["tikv"][0],
			),
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and instance in ('%s', '%s')`,
				testServers[0].address, testServers[1].address),
			reqCount: 4,
			rows: flatten(
				rows["tidb"][0],
				rows["tidb"][1],
				rows["tikv"][0],
				rows["tikv"][1],
			),
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and type='pd' and instance in ('%s', '%s')`,
				testServers[0].address, testServers[1].address),
			reqCount: 0,
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and instance in ('%s', '%s') and instance='%s'`,
				testServers[0].address, testServers[1].address, testServers[2].address),
			reqCount: 0,
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and instance in ('%s', '%s') and instance='%s'`,
				testServers[0].address, testServers[1].address, testServers[0].address),
			reqCount: 2,
			rows: flatten(
				rows["tidb"][0],
				rows["tikv"][0],
			),
		},
	}

	for _, ca := range cases {
		// reset the request counter
		requestCounter = 0
		tk.MustQuery(ca.sql).Check(testkit.Rows(ca.rows...))
		warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, 0, Commentf("unexpected warnigns: %+v", warnings))
		c.Assert(requestCounter, Equals, ca.reqCount, Commentf("SQL: %s", ca.sql))
	}
}

func (s *testClusterTableBase) writeTmpFile(c *C, dir, filename string, lines []string) {
	err := os.WriteFile(filepath.Join(dir, filename), []byte(strings.Join(lines, "\n")), os.ModePerm)
	c.Assert(err, IsNil, Commentf("write tmp file %s failed", filename))
}

type testServer struct {
	typ     string
	server  *grpc.Server
	address string
	tmpDir  string
	logFile string
}

func (s *testClusterTableBase) setupClusterGRPCServer(c *C) map[string]*testServer {
	// tp => testServer
	testServers := map[string]*testServer{}

	// create gRPC servers
	for _, typ := range []string{"tidb", "tikv", "pd"} {
		tmpDir, err := os.MkdirTemp("", typ)
		c.Assert(err, IsNil)

		server := grpc.NewServer()
		logFile := filepath.Join(tmpDir, fmt.Sprintf("%s.log", typ))
		diagnosticspb.RegisterDiagnosticsServer(server, sysutil.NewDiagnosticsServer(logFile))

		// Find a available port
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		c.Assert(err, IsNil, Commentf("cannot find available port"))

		testServers[typ] = &testServer{
			typ:     typ,
			server:  server,
			address: fmt.Sprintf("127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port),
			tmpDir:  tmpDir,
			logFile: logFile,
		}
		go func() {
			if err := server.Serve(listener); err != nil {
				log.Fatalf("failed to serve: %v", err)
			}
		}()
	}
	return testServers
}

func (s *testMemTableReaderSuite) TestTiDBClusterLog(c *C) {
	testServers := s.setupClusterGRPCServer(c)
	defer func() {
		for _, s := range testServers {
			s.server.Stop()
			c.Assert(os.RemoveAll(s.tmpDir), IsNil, Commentf("remove tmpDir %v failed", s.tmpDir))
		}
	}()

	// time format of log file
	var logtime = func(s string) string {
		t, err := time.ParseInLocation("2006/01/02 15:04:05.000", s, time.Local)
		c.Assert(err, IsNil)
		return t.Format("[2006/01/02 15:04:05.000 -07:00]")
	}

	// time format of query output
	var restime = func(s string) string {
		t, err := time.ParseInLocation("2006/01/02 15:04:05.000", s, time.Local)
		c.Assert(err, IsNil)
		return t.Format("2006/01/02 15:04:05.000")
	}

	// prepare log files
	// TiDB
	s.writeTmpFile(c, testServers["tidb"].tmpDir, "tidb.log", []string{
		logtime(`2019/08/26 06:19:13.011`) + ` [INFO] [test log message tidb 1, foo]`,
		logtime(`2019/08/26 06:19:14.011`) + ` [DEBUG] [test log message tidb 2, foo]`,
		logtime(`2019/08/26 06:19:15.011`) + ` [error] [test log message tidb 3, foo]`,
		logtime(`2019/08/26 06:19:16.011`) + ` [trace] [test log message tidb 4, foo]`,
		logtime(`2019/08/26 06:19:17.011`) + ` [CRITICAL] [test log message tidb 5, foo]`,
	})
	s.writeTmpFile(c, testServers["tidb"].tmpDir, "tidb-1.log", []string{
		logtime(`2019/08/26 06:25:13.011`) + ` [info] [test log message tidb 10, bar]`,
		logtime(`2019/08/26 06:25:14.011`) + ` [debug] [test log message tidb 11, bar]`,
		logtime(`2019/08/26 06:25:15.011`) + ` [ERROR] [test log message tidb 12, bar]`,
		logtime(`2019/08/26 06:25:16.011`) + ` [TRACE] [test log message tidb 13, bar]`,
		logtime(`2019/08/26 06:25:17.011`) + ` [critical] [test log message tidb 14, bar]`,
	})

	// TiKV
	s.writeTmpFile(c, testServers["tikv"].tmpDir, "tikv.log", []string{
		logtime(`2019/08/26 06:19:13.011`) + ` [INFO] [test log message tikv 1, foo]`,
		logtime(`2019/08/26 06:20:14.011`) + ` [DEBUG] [test log message tikv 2, foo]`,
		logtime(`2019/08/26 06:21:15.011`) + ` [error] [test log message tikv 3, foo]`,
		logtime(`2019/08/26 06:22:16.011`) + ` [trace] [test log message tikv 4, foo]`,
		logtime(`2019/08/26 06:23:17.011`) + ` [CRITICAL] [test log message tikv 5, foo]`,
	})
	s.writeTmpFile(c, testServers["tikv"].tmpDir, "tikv-1.log", []string{
		logtime(`2019/08/26 06:24:15.011`) + ` [info] [test log message tikv 10, bar]`,
		logtime(`2019/08/26 06:25:16.011`) + ` [debug] [test log message tikv 11, bar]`,
		logtime(`2019/08/26 06:26:17.011`) + ` [ERROR] [test log message tikv 12, bar]`,
		logtime(`2019/08/26 06:27:18.011`) + ` [TRACE] [test log message tikv 13, bar]`,
		logtime(`2019/08/26 06:28:19.011`) + ` [critical] [test log message tikv 14, bar]`,
	})

	// PD
	s.writeTmpFile(c, testServers["pd"].tmpDir, "pd.log", []string{
		logtime(`2019/08/26 06:18:13.011`) + ` [INFO] [test log message pd 1, foo]`,
		logtime(`2019/08/26 06:19:14.011`) + ` [DEBUG] [test log message pd 2, foo]`,
		logtime(`2019/08/26 06:20:15.011`) + ` [error] [test log message pd 3, foo]`,
		logtime(`2019/08/26 06:21:16.011`) + ` [trace] [test log message pd 4, foo]`,
		logtime(`2019/08/26 06:22:17.011`) + ` [CRITICAL] [test log message pd 5, foo]`,
	})
	s.writeTmpFile(c, testServers["pd"].tmpDir, "pd-1.log", []string{
		logtime(`2019/08/26 06:23:13.011`) + ` [info] [test log message pd 10, bar]`,
		logtime(`2019/08/26 06:24:14.011`) + ` [debug] [test log message pd 11, bar]`,
		logtime(`2019/08/26 06:25:15.011`) + ` [ERROR] [test log message pd 12, bar]`,
		logtime(`2019/08/26 06:26:16.011`) + ` [TRACE] [test log message pd 13, bar]`,
		logtime(`2019/08/26 06:27:17.011`) + ` [critical] [test log message pd 14, bar]`,
	})

	fullLogs := [][]string{
		{"2019/08/26 06:18:13.011", "pd", "INFO", "[test log message pd 1, foo]"},
		{"2019/08/26 06:19:13.011", "tidb", "INFO", "[test log message tidb 1, foo]"},
		{"2019/08/26 06:19:13.011", "tikv", "INFO", "[test log message tikv 1, foo]"},
		{"2019/08/26 06:19:14.011", "pd", "DEBUG", "[test log message pd 2, foo]"},
		{"2019/08/26 06:19:14.011", "tidb", "DEBUG", "[test log message tidb 2, foo]"},
		{"2019/08/26 06:19:15.011", "tidb", "error", "[test log message tidb 3, foo]"},
		{"2019/08/26 06:19:16.011", "tidb", "trace", "[test log message tidb 4, foo]"},
		{"2019/08/26 06:19:17.011", "tidb", "CRITICAL", "[test log message tidb 5, foo]"},
		{"2019/08/26 06:20:14.011", "tikv", "DEBUG", "[test log message tikv 2, foo]"},
		{"2019/08/26 06:20:15.011", "pd", "error", "[test log message pd 3, foo]"},
		{"2019/08/26 06:21:15.011", "tikv", "error", "[test log message tikv 3, foo]"},
		{"2019/08/26 06:21:16.011", "pd", "trace", "[test log message pd 4, foo]"},
		{"2019/08/26 06:22:16.011", "tikv", "trace", "[test log message tikv 4, foo]"},
		{"2019/08/26 06:22:17.011", "pd", "CRITICAL", "[test log message pd 5, foo]"},
		{"2019/08/26 06:23:13.011", "pd", "info", "[test log message pd 10, bar]"},
		{"2019/08/26 06:23:17.011", "tikv", "CRITICAL", "[test log message tikv 5, foo]"},
		{"2019/08/26 06:24:14.011", "pd", "debug", "[test log message pd 11, bar]"},
		{"2019/08/26 06:24:15.011", "tikv", "info", "[test log message tikv 10, bar]"},
		{"2019/08/26 06:25:13.011", "tidb", "info", "[test log message tidb 10, bar]"},
		{"2019/08/26 06:25:14.011", "tidb", "debug", "[test log message tidb 11, bar]"},
		{"2019/08/26 06:25:15.011", "pd", "ERROR", "[test log message pd 12, bar]"},
		{"2019/08/26 06:25:15.011", "tidb", "ERROR", "[test log message tidb 12, bar]"},
		{"2019/08/26 06:25:16.011", "tidb", "TRACE", "[test log message tidb 13, bar]"},
		{"2019/08/26 06:25:16.011", "tikv", "debug", "[test log message tikv 11, bar]"},
		{"2019/08/26 06:25:17.011", "tidb", "critical", "[test log message tidb 14, bar]"},
		{"2019/08/26 06:26:16.011", "pd", "TRACE", "[test log message pd 13, bar]"},
		{"2019/08/26 06:26:17.011", "tikv", "ERROR", "[test log message tikv 12, bar]"},
		{"2019/08/26 06:27:17.011", "pd", "critical", "[test log message pd 14, bar]"},
		{"2019/08/26 06:27:18.011", "tikv", "TRACE", "[test log message tikv 13, bar]"},
		{"2019/08/26 06:28:19.011", "tikv", "critical", "[test log message tikv 14, bar]"},
	}

	var cases = []struct {
		conditions []string
		expected   [][]string
	}{
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2099/08/26 06:28:19.011'",
				"message like '%'",
			},
			expected: fullLogs,
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:19:13.011'",
				"time<='2019/08/26 06:21:15.011'",
				"message like '%'",
			},
			expected: [][]string{
				{"2019/08/26 06:19:13.011", "tidb", "INFO", "[test log message tidb 1, foo]"},
				{"2019/08/26 06:19:13.011", "tikv", "INFO", "[test log message tikv 1, foo]"},
				{"2019/08/26 06:19:14.011", "pd", "DEBUG", "[test log message pd 2, foo]"},
				{"2019/08/26 06:19:14.011", "tidb", "DEBUG", "[test log message tidb 2, foo]"},
				{"2019/08/26 06:19:15.011", "tidb", "error", "[test log message tidb 3, foo]"},
				{"2019/08/26 06:19:16.011", "tidb", "trace", "[test log message tidb 4, foo]"},
				{"2019/08/26 06:19:17.011", "tidb", "CRITICAL", "[test log message tidb 5, foo]"},
				{"2019/08/26 06:20:14.011", "tikv", "DEBUG", "[test log message tikv 2, foo]"},
				{"2019/08/26 06:20:15.011", "pd", "error", "[test log message pd 3, foo]"},
				{"2019/08/26 06:21:15.011", "tikv", "error", "[test log message tikv 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:19:13.011'",
				"time<='2019/08/26 06:21:15.011'",
				"message like '%'",
				"type='pd'",
			},
			expected: [][]string{
				{"2019/08/26 06:19:14.011", "pd", "DEBUG", "[test log message pd 2, foo]"},
				{"2019/08/26 06:20:15.011", "pd", "error", "[test log message pd 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time>='2019/08/26 06:19:13.011'",
				"time>='2019/08/26 06:19:14.011'",
				"time<='2019/08/26 06:21:15.011'",
				"type='pd'",
			},
			expected: [][]string{
				{"2019/08/26 06:19:14.011", "pd", "DEBUG", "[test log message pd 2, foo]"},
				{"2019/08/26 06:20:15.011", "pd", "error", "[test log message pd 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time>='2019/08/26 06:19:13.011'",
				"time='2019/08/26 06:19:14.011'",
				"message like '%'",
				"type='pd'",
			},
			expected: [][]string{
				{"2019/08/26 06:19:14.011", "pd", "DEBUG", "[test log message pd 2, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:19:13.011'",
				"time<='2019/08/26 06:21:15.011'",
				"message like '%'",
				"type='tidb'",
			},
			expected: [][]string{
				{"2019/08/26 06:19:13.011", "tidb", "INFO", "[test log message tidb 1, foo]"},
				{"2019/08/26 06:19:14.011", "tidb", "DEBUG", "[test log message tidb 2, foo]"},
				{"2019/08/26 06:19:15.011", "tidb", "error", "[test log message tidb 3, foo]"},
				{"2019/08/26 06:19:16.011", "tidb", "trace", "[test log message tidb 4, foo]"},
				{"2019/08/26 06:19:17.011", "tidb", "CRITICAL", "[test log message tidb 5, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:19:13.011'",
				"time<='2019/08/26 06:21:15.011'",
				"message like '%'",
				"type='tikv'",
			},
			expected: [][]string{
				{"2019/08/26 06:19:13.011", "tikv", "INFO", "[test log message tikv 1, foo]"},
				{"2019/08/26 06:20:14.011", "tikv", "DEBUG", "[test log message tikv 2, foo]"},
				{"2019/08/26 06:21:15.011", "tikv", "error", "[test log message tikv 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:19:13.011'",
				"time<='2019/08/26 06:21:15.011'",
				"message like '%'",
				fmt.Sprintf("instance='%s'", testServers["pd"].address),
			},
			expected: [][]string{
				{"2019/08/26 06:19:14.011", "pd", "DEBUG", "[test log message pd 2, foo]"},
				{"2019/08/26 06:20:15.011", "pd", "error", "[test log message pd 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:19:13.011'",
				"time<='2019/08/26 06:21:15.011'",
				"message like '%'",
				fmt.Sprintf("instance='%s'", testServers["tidb"].address),
			},
			expected: [][]string{
				{"2019/08/26 06:19:13.011", "tidb", "INFO", "[test log message tidb 1, foo]"},
				{"2019/08/26 06:19:14.011", "tidb", "DEBUG", "[test log message tidb 2, foo]"},
				{"2019/08/26 06:19:15.011", "tidb", "error", "[test log message tidb 3, foo]"},
				{"2019/08/26 06:19:16.011", "tidb", "trace", "[test log message tidb 4, foo]"},
				{"2019/08/26 06:19:17.011", "tidb", "CRITICAL", "[test log message tidb 5, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:19:13.011'",
				"time<='2019/08/26 06:21:15.011'",
				"message like '%'",
				fmt.Sprintf("instance='%s'", testServers["tikv"].address),
			},
			expected: [][]string{
				{"2019/08/26 06:19:13.011", "tikv", "INFO", "[test log message tikv 1, foo]"},
				{"2019/08/26 06:20:14.011", "tikv", "DEBUG", "[test log message tikv 2, foo]"},
				{"2019/08/26 06:21:15.011", "tikv", "error", "[test log message tikv 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:19:13.011'",
				"time<='2019/08/26 06:21:15.011'",
				"message like '%'",
				fmt.Sprintf("instance in ('%s', '%s')", testServers["pd"].address, testServers["tidb"].address),
			},
			expected: [][]string{
				{"2019/08/26 06:19:13.011", "tidb", "INFO", "[test log message tidb 1, foo]"},
				{"2019/08/26 06:19:14.011", "pd", "DEBUG", "[test log message pd 2, foo]"},
				{"2019/08/26 06:19:14.011", "tidb", "DEBUG", "[test log message tidb 2, foo]"},
				{"2019/08/26 06:19:15.011", "tidb", "error", "[test log message tidb 3, foo]"},
				{"2019/08/26 06:19:16.011", "tidb", "trace", "[test log message tidb 4, foo]"},
				{"2019/08/26 06:19:17.011", "tidb", "CRITICAL", "[test log message tidb 5, foo]"},
				{"2019/08/26 06:20:15.011", "pd", "error", "[test log message pd 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
				"message like '%'",
				"level='critical'",
			},
			expected: [][]string{
				{"2019/08/26 06:19:17.011", "tidb", "CRITICAL", "[test log message tidb 5, foo]"},
				{"2019/08/26 06:22:17.011", "pd", "CRITICAL", "[test log message pd 5, foo]"},
				{"2019/08/26 06:23:17.011", "tikv", "CRITICAL", "[test log message tikv 5, foo]"},
				{"2019/08/26 06:25:17.011", "tidb", "critical", "[test log message tidb 14, bar]"},
				{"2019/08/26 06:27:17.011", "pd", "critical", "[test log message pd 14, bar]"},
				{"2019/08/26 06:28:19.011", "tikv", "critical", "[test log message tikv 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
				"message like '%'",
				"level='critical'",
				"type in ('pd', 'tikv')",
			},
			expected: [][]string{
				{"2019/08/26 06:22:17.011", "pd", "CRITICAL", "[test log message pd 5, foo]"},
				{"2019/08/26 06:23:17.011", "tikv", "CRITICAL", "[test log message tikv 5, foo]"},
				{"2019/08/26 06:27:17.011", "pd", "critical", "[test log message pd 14, bar]"},
				{"2019/08/26 06:28:19.011", "tikv", "critical", "[test log message tikv 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
				"message like '%'",
				"level='critical'",
				"(type='pd' or type='tikv')",
			},
			expected: [][]string{
				{"2019/08/26 06:22:17.011", "pd", "CRITICAL", "[test log message pd 5, foo]"},
				{"2019/08/26 06:23:17.011", "tikv", "CRITICAL", "[test log message tikv 5, foo]"},
				{"2019/08/26 06:27:17.011", "pd", "critical", "[test log message pd 14, bar]"},
				{"2019/08/26 06:28:19.011", "tikv", "critical", "[test log message tikv 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
				"level='critical'",
				"message like '%pd%'",
			},
			expected: [][]string{
				{"2019/08/26 06:22:17.011", "pd", "CRITICAL", "[test log message pd 5, foo]"},
				{"2019/08/26 06:27:17.011", "pd", "critical", "[test log message pd 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
				"level='critical'",
				"message like '%pd%'",
				"message like '%5%'",
			},
			expected: [][]string{
				{"2019/08/26 06:22:17.011", "pd", "CRITICAL", "[test log message pd 5, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
				"level='critical'",
				"message like '%pd%'",
				"message like '%5%'",
				"message like '%x%'",
			},
			expected: [][]string{},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
				"level='critical'",
				"message regexp '.*pd.*'",
			},
			expected: [][]string{
				{"2019/08/26 06:22:17.011", "pd", "CRITICAL", "[test log message pd 5, foo]"},
				{"2019/08/26 06:27:17.011", "pd", "critical", "[test log message pd 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
				"level='critical'",
				"message regexp '.*pd.*'",
				"message regexp '.*foo]$'",
			},
			expected: [][]string{
				{"2019/08/26 06:22:17.011", "pd", "CRITICAL", "[test log message pd 5, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
				"level='critical'",
				"message regexp '.*pd.*'",
				"message regexp '.*5.*'",
				"message regexp '.*x.*'",
			},
			expected: [][]string{},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
				"level='critical'",
				"(message regexp '.*pd.*' or message regexp '.*tidb.*')",
			},
			expected: [][]string{
				{"2019/08/26 06:19:17.011", "tidb", "CRITICAL", "[test log message tidb 5, foo]"},
				{"2019/08/26 06:22:17.011", "pd", "CRITICAL", "[test log message pd 5, foo]"},
				{"2019/08/26 06:25:17.011", "tidb", "critical", "[test log message tidb 14, bar]"},
				{"2019/08/26 06:27:17.011", "pd", "critical", "[test log message pd 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2099/08/26 06:28:19.011'",
				// this pattern verifies that there is no optimization breaking
				// length of multiple wildcards, for example, %% may be
				// converted to %, but %_ cannot be converted to %.
				"message like '%tidb_%_4%'",
			},
			expected: [][]string{
				{"2019/08/26 06:25:17.011", "tidb", "critical", "[test log message tidb 14, bar]"},
			},
		},
	}

	var servers = make([]string, 0, len(testServers))
	for _, s := range testServers {
		servers = append(servers, strings.Join([]string{s.typ, s.address, s.address}, ","))
	}
	fpName := "github.com/pingcap/tidb/executor/mockClusterLogServerInfo"
	fpExpr := strings.Join(servers, ";")
	c.Assert(failpoint.Enable(fpName, fmt.Sprintf(`return("%s")`, fpExpr)), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk := testkit.NewTestKit(c, s.store)
	for _, cas := range cases {
		sql := "select * from information_schema.cluster_log"
		if len(cas.conditions) > 0 {
			sql = fmt.Sprintf("%s where %s", sql, strings.Join(cas.conditions, " and "))
		}
		result := tk.MustQuery(sql)
		warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, 0, Commentf("unexpected warnigns: %+v", warnings))
		var expected []string
		for _, row := range cas.expected {
			expectedRow := []string{
				restime(row[0]),             // time column
				row[1],                      // type column
				testServers[row[1]].address, // instance column
				strings.ToUpper(sysutil.ParseLogLevel(row[2]).String()), // level column
				row[3], // message column
			}
			expected = append(expected, strings.Join(expectedRow, " "))
		}
		result.Check(testkit.Rows(expected...))
	}
}

func (s *testMemTableReaderSuite) TestTiDBClusterLogError(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	fpName := "github.com/pingcap/tidb/executor/mockClusterLogServerInfo"
	c.Assert(failpoint.Enable(fpName, `return("")`), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	// Test without start time error.
	rs, err := tk.Exec("select * from information_schema.cluster_log")
	c.Assert(err, IsNil)
	_, err = session.ResultSetToStringSlice(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "denied to scan logs, please specified the start time, such as `time > '2020-01-01 00:00:00'`")
	c.Assert(rs.Close(), IsNil)

	// Test without end time error.
	rs, err = tk.Exec("select * from information_schema.cluster_log where time>='2019/08/26 06:18:13.011'")
	c.Assert(err, IsNil)
	_, err = session.ResultSetToStringSlice(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "denied to scan logs, please specified the end time, such as `time < '2020-01-01 00:00:00'`")
	c.Assert(rs.Close(), IsNil)

	// Test without specified message error.
	rs, err = tk.Exec("select * from information_schema.cluster_log where time>='2019/08/26 06:18:13.011' and time<'2019/08/26 16:18:13.011'")
	c.Assert(err, IsNil)
	_, err = session.ResultSetToStringSlice(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "denied to scan full logs (use `SELECT * FROM cluster_log WHERE message LIKE '%'` explicitly if intentionally)")
	c.Assert(rs.Close(), IsNil)
}

type mockStoreWithMultiPD struct {
	helper.Storage
	hosts []string
}

var hotRegionsResponses = make(map[string]*executor.HistoryHotRegions, 3)

func (s *mockStoreWithMultiPD) EtcdAddrs() ([]string, error) { return s.hosts, nil }
func (s *mockStoreWithMultiPD) TLSConfig() *tls.Config       { panic("not implemented") }
func (s *mockStoreWithMultiPD) StartGCWorker() error         { panic("not implemented") }
func (s *mockStoreWithMultiPD) Name() string                 { return "mockStore" }
func (s *mockStoreWithMultiPD) Describe() string             { return "" }

var _ = SerialSuites(&testHotRegionsHistoryTableSuite{testInfoschemaTableSuiteBase: &testInfoschemaTableSuiteBase{}})

type testHotRegionsHistoryTableSuite struct {
	*testInfoschemaTableSuiteBase
	httpServers []*httptest.Server
	startTime   time.Time
}

func (s *testHotRegionsHistoryTableSuite) SetUpSuite(c *C) {
	s.testInfoschemaTableSuiteBase.SetUpSuite(c)
	store := &mockStoreWithMultiPD{
		s.store.(helper.Storage),
		make([]string, 3),
	}
	// start 3 PD server with hotRegionsServer and store them in s.store
	for i := 0; i < 3; i++ {
		httpServer, mockAddr := s.setUpMockPDHTTPServer(c)
		c.Assert(httpServer, NotNil)
		s.httpServers = append(s.httpServers, httpServer)
		store.hosts[i] = mockAddr
	}
	s.store = store
	s.startTime = time.Now()
}

func writeResp(w http.ResponseWriter, resp interface{}) {
	w.WriteHeader(http.StatusOK)
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to marshal resp", err)
		return
	}
	w.Write(jsonResp)
}

func writeJSONError(w http.ResponseWriter, code int, prefix string, err error) {
	type errorResponse struct {
		Error string `json:"error"`
	}
	w.WriteHeader(code)
	if err != nil {
		prefix += ": " + err.Error()
	}
	_ = json.NewEncoder(w).Encode(errorResponse{Error: prefix})
}

func hisHotRegionsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to read req", err)
		return
	}
	r.Body.Close()
	req := &executor.HistoryHotRegionsRequest{}
	err = json.Unmarshal(data, req)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to serialize req", err)
		return
	}
	resp := &executor.HistoryHotRegions{}
	for _, typ := range req.HotRegionTypes {
		resp.HistoryHotRegion = append(resp.HistoryHotRegion, hotRegionsResponses[typ+r.Host].HistoryHotRegion...)
	}
	w.WriteHeader(http.StatusOK)
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "unable to marshal resp", err)
		return
	}
	w.Write(jsonResp)
}

func (s *testHotRegionsHistoryTableSuite) setUpMockPDHTTPServer(c *C) (*httptest.Server, string) {
	// mock PD http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	mockAddr := strings.TrimPrefix(server.URL, "http://")
	// mock PD API
	router.Handle(pdapi.Status, fn.Wrap(func() (interface{}, error) {
		return struct {
			Version        string `json:"version"`
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
			Version:        "4.0.0-alpha",
			GitHash:        "mock-pd-githash",
			StartTimestamp: s.startTime.Unix(),
		}, nil
	}))
	// mock hisory hot regions response
	router.HandleFunc(pdapi.HotHistory, hisHotRegionsHandler)
	return server, mockAddr
}

func (s *testHotRegionsHistoryTableSuite) TearDownSuite(c *C) {
	for _, server := range s.httpServers {
		server.Close()
	}
	s.testInfoschemaTableSuiteBase.TearDownSuite(c)
}

func (s *testHotRegionsHistoryTableSuite) TestTiDBHotRegionsHistory(c *C) {
	var unixTimeMs = func(s string) int64 {
		t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.Local)
		c.Assert(err, IsNil)
		return t.UnixNano() / int64(time.Millisecond)
	}
	fullHotRegions := [][]string{
		// mysql table_id = 11, table_name = TABLES_PRIV
		{"2019-10-10 10:10:11", "MYSQL", "TABLES_PRIV", "11", "<nil>", "<nil>", "1", "1", "11111", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:12", "MYSQL", "TABLES_PRIV", "11", "<nil>", "<nil>", "2", "2", "22222", "0", "0", "WRITE", "99", "99", "99", "99"},
		// mysql table_id = 21, table_name = STATS_META
		{"2019-10-10 10:10:13", "MYSQL", "STATS_META", "21", "<nil>", "<nil>", "3", "3", "33333", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:14", "MYSQL", "STATS_META", "21", "<nil>", "<nil>", "4", "4", "44444", "0", "0", "WRITE", "99", "99", "99", "99"},
		// table_id = 1313, deleted schema
		{"2019-10-10 10:10:15", "UNKNOWN", "UNKNOWN", "1313", "UNKNOWN", "<nil>", "5", "5", "55555", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:16", "UNKNOWN", "UNKNOWN", "1313", "UNKNOWN", "<nil>", "6", "6", "66666", "0", "0", "WRITE", "99", "99", "99", "99"},
		// mysql table_id = 11, index_id = 1, table_name = TABLES_PRIV, index_name = PRIMARY
		{"2019-10-10 10:10:17", "MYSQL", "TABLES_PRIV", "11", "PRIMARY", "1", "1", "1", "11111", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:18", "MYSQL", "TABLES_PRIV", "11", "PRIMARY", "1", "2", "2", "22222", "0", "0", "WRITE", "99", "99", "99", "99"},
		// mysql table_id = 21 ,index_id = 1, table_name = STATS_META, index_name = IDX_VER
		{"2019-10-10 10:10:19", "MYSQL", "STATS_META", "21", "IDX_VER", "1", "3", "3", "33333", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:20", "MYSQL", "STATS_META", "21", "IDX_VER", "1", "4", "4", "44444", "0", "0", "WRITE", "99", "99", "99", "99"},
		// mysql table_id = 21 ,index_id = 2, table_name = STATS_META, index_name = TBL
		{"2019-10-10 10:10:21", "MYSQL", "STATS_META", "21", "TBL", "2", "5", "5", "55555", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:22", "MYSQL", "STATS_META", "21", "TBL", "2", "6", "6", "66666", "0", "0", "WRITE", "99", "99", "99", "99"},
		// table_id = 1313, index_id = 1, deleted schema
		{"2019-10-10 10:10:23", "UNKNOWN", "UNKNOWN", "1313", "UNKNOWN", "1", "7", "7", "77777", "0", "1", "READ", "99", "99", "99", "99"},
		{"2019-10-10 10:10:24", "UNKNOWN", "UNKNOWN", "1313", "UNKNOWN", "1", "8", "8", "88888", "0", "0", "WRITE", "99", "99", "99", "99"},
	}

	mockDB := &model.DBInfo{}
	pdResps := []map[string]*executor.HistoryHotRegions{
		{
			core.HotRegionTypeRead: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// mysql table_id = 11, table_name = TABLES_PRIV
					{UpdateTime: unixTimeMs("2019-10-10 10:10:11"), RegionID: 1, StoreID: 1, PeerID: 11111, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 11}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 11}).EndKey,
					},
					// mysql table_id = 21, table_name = STATS_META
					{UpdateTime: unixTimeMs("2019-10-10 10:10:13"), RegionID: 3, StoreID: 3, PeerID: 33333, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 21}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 21}).EndKey,
					},
				},
			},
			core.HotRegionTypeWrite: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// mysql table_id = 11, table_name = TABLES_PRIV
					{UpdateTime: unixTimeMs("2019-10-10 10:10:12"), RegionID: 2, StoreID: 2, PeerID: 22222, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 11}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 11}).EndKey,
					},
					// mysql table_id = 21, table_name = STATS_META
					{UpdateTime: unixTimeMs("2019-10-10 10:10:14"), RegionID: 4, StoreID: 4, PeerID: 44444, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 21}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 21}).EndKey,
					},
				},
			},
		},
		{
			core.HotRegionTypeRead: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// table_id = 1313, deleted schema
					{UpdateTime: unixTimeMs("2019-10-10 10:10:15"), RegionID: 5, StoreID: 5, PeerID: 55555, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 1313}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 1313}).EndKey,
					},
					// mysql table_id = 11, index_id = 1, table_name = TABLES_PRIV, index_name = PRIMARY
					{UpdateTime: unixTimeMs("2019-10-10 10:10:17"), RegionID: 1, StoreID: 1, PeerID: 11111, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 11}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 11}, &model.IndexInfo{ID: 1}).EndKey,
					},
				},
			},
			core.HotRegionTypeWrite: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// table_id = 1313, deleted schema
					{UpdateTime: unixTimeMs("2019-10-10 10:10:16"), RegionID: 6, StoreID: 6, PeerID: 66666, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 1313}).StartKey,
						EndKey:   helper.NewTableWithKeyRange(mockDB, &model.TableInfo{ID: 1313}).EndKey,
					},
					// mysql table_id = 11, index_id = 1, table_name = TABLES_PRIV, index_name = PRIMARY
					{UpdateTime: unixTimeMs("2019-10-10 10:10:18"), RegionID: 2, StoreID: 2, PeerID: 22222, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 11}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 11}, &model.IndexInfo{ID: 1}).EndKey,
					},
				},
			},
		},
		{
			core.HotRegionTypeRead: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// mysql table_id = 21 ,index_id = 1, table_name = STATS_META, index_name = IDX_VER
					{UpdateTime: unixTimeMs("2019-10-10 10:10:19"), RegionID: 3, StoreID: 3, PeerID: 33333, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 21}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 21}, &model.IndexInfo{ID: 1}).EndKey,
					},
					// mysql table_id = 21 ,index_id = 2, table_name = STATS_META, index_name = TBL
					{UpdateTime: unixTimeMs("2019-10-10 10:10:21"), RegionID: 5, StoreID: 5, PeerID: 55555, IsLearner: false,
						IsLeader: true, HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 21}, &model.IndexInfo{ID: 2}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 21}, &model.IndexInfo{ID: 2}).EndKey,
					},
					//      table_id = 1313, index_id = 1, deleted schema
					{UpdateTime: unixTimeMs("2019-10-10 10:10:23"), RegionID: 7, StoreID: 7, PeerID: 77777, IsLeader: true,
						HotRegionType: "READ", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 1313}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 1313}, &model.IndexInfo{ID: 1}).EndKey,
					},
				},
			},
			core.HotRegionTypeWrite: {
				HistoryHotRegion: []*executor.HistoryHotRegion{
					// mysql table_id = 21 ,index_id = 1, table_name = STATS_META, index_name = IDX_VER
					{UpdateTime: unixTimeMs("2019-10-10 10:10:20"), RegionID: 4, StoreID: 4, PeerID: 44444, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 21}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 21}, &model.IndexInfo{ID: 1}).EndKey,
					},
					// mysql table_id = 21 ,index_id = 2, table_name = STATS_META, index_name = TBL
					{UpdateTime: unixTimeMs("2019-10-10 10:10:22"), RegionID: 6, StoreID: 6, PeerID: 66666, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 21}, &model.IndexInfo{ID: 2}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 21}, &model.IndexInfo{ID: 2}).EndKey,
					},
					//      table_id = 1313, index_id = 1, deleted schema
					{UpdateTime: unixTimeMs("2019-10-10 10:10:24"), RegionID: 8, StoreID: 8, PeerID: 88888, IsLearner: false,
						IsLeader: false, HotRegionType: "WRITE", HotDegree: 99, FlowBytes: 99, KeyRate: 99, QueryRate: 99,
						StartKey: helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 1313}, &model.IndexInfo{ID: 1}).StartKey,
						EndKey:   helper.NewIndexWithKeyRange(mockDB, &model.TableInfo{ID: 1313}, &model.IndexInfo{ID: 1}).EndKey,
					},
				},
			},
		},
	}

	var cases = []struct {
		conditions []string
		reqCount   int32
		expected   [][]string
	}{
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
			}, // time filtered by PD, assume response suit time range, and ignore deleted schemas
			expected: [][]string{
				fullHotRegions[0], fullHotRegions[1], fullHotRegions[2],
				fullHotRegions[3],
				fullHotRegions[6], fullHotRegions[7], fullHotRegions[8],
				fullHotRegions[9], fullHotRegions[10], fullHotRegions[11],
			},
		},
		{
			conditions: []string{
				"update_time>=TIMESTAMP('2019-10-10 10:10:10')",
				"update_time<=TIMESTAMP('2019-10-11 10:10:10')",
			}, // test support of timestamp
			expected: [][]string{
				fullHotRegions[0], fullHotRegions[1], fullHotRegions[2],
				fullHotRegions[3],
				fullHotRegions[6], fullHotRegions[7], fullHotRegions[8],
				fullHotRegions[9], fullHotRegions[10], fullHotRegions[11],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=11",
			},
			expected: [][]string{
				fullHotRegions[0], fullHotRegions[1], fullHotRegions[6], fullHotRegions[7],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_name='TABLES_PRIV'",
			},
			expected: [][]string{
				fullHotRegions[0], fullHotRegions[1], fullHotRegions[6], fullHotRegions[7],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=21",
				"index_id=1",
			},
			expected: [][]string{
				fullHotRegions[8], fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=21",
				"index_id=1",
				"table_name='TABLES_PRIV'",
			}, // table_id != table_name -> nil
			expected: [][]string{},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=21",
				"index_id=1",
				"table_name='STATS_META'",
			}, // table_id = table_name
			expected: [][]string{
				fullHotRegions[8], fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=21",
				"index_id=1",
				"index_name='UNKNOWN'",
			}, // index_id != index_name -> nil
			expected: [][]string{},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"table_id=21",
				"index_id=1",
				"index_name='IDX_VER'",
			}, // index_id = index_name
			expected: [][]string{
				fullHotRegions[8], fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"index_id=1",
				"index_name='IDX_VER'",
				"table_id>=21", // unpushed down predicates 21>=21
			},
			expected: [][]string{
				fullHotRegions[8], fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"index_id=1",
				"index_name='IDX_VER'",
				"table_id>21", // unpushed down predicates
			}, // 21!>21 -> nil
			expected: [][]string{},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"index_id=1",
				"index_name='IDX_VER'",
				"table_id>=21", // unpushed down predicates
				"db_name='MYSQL'",
			},
			expected: [][]string{
				fullHotRegions[8], fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"index_id=1",
				"index_name='IDX_VER'",
				"table_id>=21", // unpushed down predicates
				"db_name='MYSQL'",
				"peer_id>=33334",
			},
			expected: [][]string{
				fullHotRegions[9],
			},
		},
		{
			conditions: []string{
				"update_time>='2019-10-10 10:10:10'",
				"update_time<='2019-10-11 10:10:10'",
				"index_id=1",
				"index_name='IDX_VER'",
				"table_id>=21", // unpushed down predicates
				"db_name='UNKNOWN'",
			},
			expected: [][]string{},
		},
	}

	// mock http resp
	store := s.store.(*mockStoreWithMultiPD)
	for i, resp := range pdResps {
		for k, v := range resp {
			hotRegionsResponses[k+store.hosts[i]] = v
		}
	}
	tk := testkit.NewTestKit(c, s.store)
	for _, cas := range cases {
		sql := "select * from information_schema.tidb_hot_regions_history"
		if len(cas.conditions) > 0 {
			sql = fmt.Sprintf("%s where %s", sql, strings.Join(cas.conditions, " and "))
		}
		result := tk.MustQuery(sql)
		warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, 0, Commentf("unexpected warnigns: %+v, sql: %s", warnings))
		var expected []string
		for _, row := range cas.expected {
			expectedRow := row
			expected = append(expected, strings.Join(expectedRow, " "))
		}
		result.Check(testkit.Rows(expected...))
	}
}

func (s *testHotRegionsHistoryTableSuite) TestTiDBHotRegionsHistoryError(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// Test without start time error
	rs, err := tk.Exec("select * from information_schema.tidb_hot_regions_history")
	c.Assert(err, IsNil)
	_, err = session.ResultSetToStringSlice(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "denied to scan hot regions, please specified the start time, such as `update_time > '2020-01-01 00:00:00'`")
	c.Assert(rs.Close(), IsNil)

	// Test without end time error.
	rs, err = tk.Exec("select * from information_schema.tidb_hot_regions_history where update_time>='2019/08/26 06:18:13.011'")
	c.Assert(err, IsNil)
	_, err = session.ResultSetToStringSlice(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "denied to scan hot regions, please specified the end time, such as `update_time < '2020-01-01 00:00:00'`")
	c.Assert(rs.Close(), IsNil)
}

var regionsInfo = map[uint64]helper.RegionInfo{
	1: {
		ID:     1,
		Peers:  []helper.RegionPeer{{ID: 11, StoreID: 1, IsLearner: false}, {ID: 12, StoreID: 2, IsLearner: false}, {ID: 13, StoreID: 3, IsLearner: false}},
		Leader: helper.RegionPeer{ID: 11, StoreID: 1, IsLearner: false},
	},
	2: {
		ID:     2,
		Peers:  []helper.RegionPeer{{ID: 21, StoreID: 1, IsLearner: false}, {ID: 22, StoreID: 2, IsLearner: false}, {ID: 23, StoreID: 3, IsLearner: false}},
		Leader: helper.RegionPeer{ID: 22, StoreID: 2, IsLearner: false},
	},
	3: {
		ID:     3,
		Peers:  []helper.RegionPeer{{ID: 31, StoreID: 1, IsLearner: false}, {ID: 32, StoreID: 2, IsLearner: false}, {ID: 33, StoreID: 3, IsLearner: false}},
		Leader: helper.RegionPeer{ID: 33, StoreID: 3, IsLearner: false},
	},
}

var storeRegionsInfo = &helper.RegionsInfo{
	Count: 3,
	Regions: []helper.RegionInfo{
		regionsInfo[1],
		regionsInfo[2],
		regionsInfo[3],
	},
}

var storesRegionsInfo = map[uint64]*helper.RegionsInfo{
	1: storeRegionsInfo,
	2: storeRegionsInfo,
	3: storeRegionsInfo,
}

var _ = SerialSuites(&testTikvRegionPeersTableSuite{testInfoschemaTableSuiteBase: &testInfoschemaTableSuiteBase{}})

type testTikvRegionPeersTableSuite struct {
	*testInfoschemaTableSuiteBase
	httpServer *httptest.Server
	mockAddr   string
	startTime  time.Time
}

func (s *testTikvRegionPeersTableSuite) SetUpSuite(c *C) {
	s.testInfoschemaTableSuiteBase.SetUpSuite(c)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
}

func storesRegionsInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "unable to parse id", err)
		return
	}
	writeResp(w, storesRegionsInfo[uint64(id)])
}

func regionsInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "unable to parse id", err)
		return
	}
	writeResp(w, regionsInfo[uint64(id)])
}
func (s *testTikvRegionPeersTableSuite) TearDownSuite(c *C) {
	s.httpServer.Close()
	s.testInfoschemaTableSuiteBase.TearDownSuite(c)
}

func (s *testTikvRegionPeersTableSuite) setUpMockPDHTTPServer() (*httptest.Server, string) {
	// mock PD http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	// mock store stats stat
	mockAddr := strings.TrimPrefix(server.URL, "http://")
	// mock PD API
	router.Handle(pdapi.Status, fn.Wrap(func() (interface{}, error) {
		return struct {
			Version        string `json:"version"`
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
			Version:        "4.0.0-alpha",
			GitHash:        "mock-pd-githash",
			StartTimestamp: s.startTime.Unix(),
		}, nil
	}))
	// mock get regionsInfo by store id
	router.HandleFunc(pdapi.StoreRegions+"/"+"{id}", storesRegionsInfoHandler)
	// mock get regionInfo by region id
	router.HandleFunc(pdapi.RegionByID+"/"+"{id}", regionsInfoHandler)
	return server, mockAddr
}

func (s *testTikvRegionPeersTableSuite) TestTikvRegionPeers(c *C) {
	mockAddr := s.mockAddr
	store := &mockStore{
		s.store.(helper.Storage),
		mockAddr,
	}

	fullRegionPeers := [][]string{
		{"1", "11", "1", "0", "1", "NORMAL", "<nil>"},
		{"1", "12", "2", "0", "0", "NORMAL", "<nil>"},
		{"1", "13", "3", "0", "0", "NORMAL", "<nil>"},

		{"2", "21", "1", "0", "0", "NORMAL", "<nil>"},
		{"2", "22", "2", "0", "1", "NORMAL", "<nil>"},
		{"2", "23", "3", "0", "0", "NORMAL", "<nil>"},

		{"3", "31", "1", "0", "0", "NORMAL", "<nil>"},
		{"3", "32", "2", "0", "0", "NORMAL", "<nil>"},
		{"3", "33", "3", "0", "1", "NORMAL", "<nil>"},
	}

	var cases = []struct {
		conditions []string
		reqCount   int32
		expected   [][]string
	}{
		{
			conditions: []string{
				"store_id in (1,2,3)",
				"region_id in (1,2,3)",
			},
			expected: fullRegionPeers,
		},
		{
			conditions: []string{
				"store_id in (1,2)",
				"region_id=1",
			},
			expected: [][]string{
				fullRegionPeers[0], fullRegionPeers[1],
			},
		},
		{
			conditions: []string{
				"store_id in (1,2)",
				"region_id=1",
				"is_leader=1",
			},
			expected: [][]string{
				fullRegionPeers[0],
			},
		},
		{
			conditions: []string{
				"store_id in (1,2)",
				"region_id=1",
				"is_leader=0",
			},
			expected: [][]string{
				fullRegionPeers[1],
			},
		},
		{
			conditions: []string{
				"store_id =1",
				"region_id =1",
				"is_leader =0",
			},
			expected: [][]string{},
		},
	}

	tk := testkit.NewTestKit(c, store)
	for _, cas := range cases {
		sql := "select * from information_schema.tikv_region_peers"
		if len(cas.conditions) > 0 {
			sql = fmt.Sprintf("%s where %s", sql, strings.Join(cas.conditions, " and "))
		}
		result := tk.MustQuery(sql)
		warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, 0, Commentf("unexpected warnigns: %+v", warnings))
		var expected []string
		for _, row := range cas.expected {
			expectedRow := row
			expected = append(expected, strings.Join(expectedRow, " "))
		}
		result.Check(testkit.Rows(expected...))
	}
}
