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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
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
		}
		return configuration, nil
	}

	// pd config
	router.Handle(pdapi.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config
	router.Handle("/config", fn.Wrap(mockConfig))

	// mock servers
	servers := []string{}
	for _, typ := range []string{"tidb", "tikv", "pd"} {
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
	c.Assert(requestCounter, Equals, int32(9))

	// type => server index => row
	rows := map[string][][]string{}
	for _, typ := range []string{"tidb", "tikv", "pd"} {
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
			reqCount: 9,
			rows: flatten(
				rows["tidb"][0],
				rows["tikv"][0],
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
			reqCount: 3,
			rows: flatten(
				rows["tidb"][0],
				rows["tikv"][0],
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
	err := ioutil.WriteFile(filepath.Join(dir, filename), []byte(strings.Join(lines, "\n")), os.ModePerm)
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
		tmpDir, err := ioutil.TempDir("", typ)
		c.Assert(err, IsNil)

		server := grpc.NewServer()
		logFile := filepath.Join(tmpDir, fmt.Sprintf("%s.log", typ))
		diagnosticspb.RegisterDiagnosticsServer(server, sysutil.NewDiagnosticsServer(logFile))

		// Find a available port
		listener, err := net.Listen("tcp", ":0")
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
		// all log items
		{
			conditions: []string{},
			expected:   fullLogs,
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
			},
			expected: fullLogs,
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2019/08/26 06:28:19.011'",
			},
			expected: fullLogs,
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:18:13.011'",
				"time<='2099/08/26 06:28:19.011'",
			},
			expected: fullLogs,
		},
		{
			conditions: []string{
				"time>='2019/08/26 06:19:13.011'",
				"time<='2019/08/26 06:21:15.011'",
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
				"level='critical'",
				"message like '%pd%'",
				"message like '%5%'",
				"message like '%x%'",
			},
			expected: [][]string{},
		},
		{
			conditions: []string{
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
				"level='critical'",
				"message regexp '.*pd.*'",
				"message regexp '.*5.*'",
				"message regexp '.*x.*'",
			},
			expected: [][]string{},
		},
		{
			conditions: []string{
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
	}

	var servers []string
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

	rs, err := tk.Exec("select * from information_schema.cluster_log")
	c.Assert(err, IsNil)
	_, err = session.ResultSetToStringSlice(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "denied to scan full logs (use `SELECT * FROM cluster_log WHERE message LIKE '%'` explicitly if intentionally)")
}
