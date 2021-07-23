// Copyright 2018 PingCAP, Inc.
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

package infoschema_test

import (
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"time"
	"unsafe"

	"github.com/gorilla/mux"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/fn"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mockstorage"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/resourcegrouptag"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"google.golang.org/grpc"
)

var _ = Suite(&testTableSuite{&testTableSuiteBase{}})
var _ = Suite(&testDataLockWaitSuite{&testTableSuiteBase{}})
var _ = SerialSuites(&testClusterTableSuite{testTableSuiteBase: &testTableSuiteBase{}})

type testTableSuite struct {
	*testTableSuiteBase
}

type testDataLockWaitSuite struct {
	*testTableSuiteBase
}

type testTableSuiteBase struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testTableSuiteBase) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.store, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testTableSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testTableSuiteBase) newTestKitWithRoot(c *C) *testkit.TestKit {
	tk := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	return tk
}

func (s *testTableSuiteBase) newTestKitWithPlanCache(c *C) *testkit.TestKit {
	tk := testkit.NewTestKit(c, s.store)
	var err error
	tk.Se, err = session.CreateSession4TestWithOpt(s.store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)
	tk.GetConnectionID()
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	return tk
}

type testClusterTableSuite struct {
	*testTableSuiteBase
	rpcserver  *grpc.Server
	httpServer *httptest.Server
	mockAddr   string
	listenAddr string
	startTime  time.Time
}

func (s *testClusterTableSuite) SetUpSuite(c *C) {
	s.testTableSuiteBase.SetUpSuite(c)
	s.rpcserver, s.listenAddr = s.setUpRPCService(c, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
}

func (s *testClusterTableSuite) setUpRPCService(c *C, addr string) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil)
	// Fix issue 9836
	sm := &mockSessionManager{make(map[uint64]*util.ProcessInfo, 1), nil}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: mysql.ComQuery,
	}
	srv := server.NewRPCServer(config.GetGlobalConfig(), s.dom, sm)
	port := lis.Addr().(*net.TCPAddr).Port
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	go func() {
		err = srv.Serve(lis)
		c.Assert(err, IsNil)
	}()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Status.StatusPort = uint(port)
	})
	return srv, addr
}

func (s *testClusterTableSuite) setUpMockPDHTTPServer() (*httptest.Server, string) {
	// mock PD http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	// mock store stats stat
	mockAddr := strings.TrimPrefix(server.URL, "http://")
	router.Handle(pdapi.Stores, fn.Wrap(func() (*helper.StoresStat, error) {
		return &helper.StoresStat{
			Count: 1,
			Stores: []helper.StoreStat{
				{
					Store: helper.StoreBaseStat{
						ID:             1,
						Address:        "127.0.0.1:20160",
						State:          0,
						StateName:      "Up",
						Version:        "4.0.0-alpha",
						StatusAddress:  mockAddr,
						GitHash:        "mock-tikv-githash",
						StartTimestamp: s.startTime.Unix(),
					},
				},
			},
		}, nil
	}))
	// mock PD API
	router.Handle(pdapi.ClusterVersion, fn.Wrap(func() (string, error) { return "4.0.0-alpha", nil }))
	router.Handle(pdapi.Status, fn.Wrap(func() (interface{}, error) {
		return struct {
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
			GitHash:        "mock-pd-githash",
			StartTimestamp: s.startTime.Unix(),
		}, nil
	}))
	var mockConfig = func() (map[string]interface{}, error) {
		configuration := map[string]interface{}{
			"key1": "value1",
			"key2": map[string]string{
				"nest1": "n-value1",
				"nest2": "n-value2",
			},
			"key3": map[string]interface{}{
				"nest1": "n-value1",
				"nest2": "n-value2",
				"key4": map[string]string{
					"nest3": "n-value4",
					"nest4": "n-value5",
				},
			},
		}
		return configuration, nil
	}
	// pd config
	router.Handle(pdapi.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config
	router.Handle("/config", fn.Wrap(mockConfig))
	return server, mockAddr
}

func (s *testClusterTableSuite) TearDownSuite(c *C) {
	if s.rpcserver != nil {
		s.rpcserver.Stop()
		s.rpcserver = nil
	}
	if s.httpServer != nil {
		s.httpServer.Close()
	}
	s.testTableSuiteBase.TearDownSuite(c)
}

func (s *testTableSuite) TestInfoschemaFieldValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists numschema, timeschema")
	tk.MustExec("create table numschema(i int(2), f float(4,2), d decimal(4,3))")
	tk.MustExec("create table timeschema(d date, dt datetime(3), ts timestamp(3), t time(4), y year(4))")
	tk.MustExec("create table strschema(c char(3), c2 varchar(3), b blob(3), t text(3))")
	tk.MustExec("create table floatschema(a float, b double(7, 3))")

	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where table_name='numschema'").
		Check(testkit.Rows("<nil> <nil> 2 0 <nil>", "<nil> <nil> 4 2 <nil>", "<nil> <nil> 4 3 <nil>")) // FIXME: for mysql first one will be "<nil> <nil> 10 0 <nil>"
	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where table_name='timeschema'").
		Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>", "<nil> <nil> <nil> <nil> 3", "<nil> <nil> <nil> <nil> 3", "<nil> <nil> <nil> <nil> 4", "<nil> <nil> <nil> <nil> <nil>"))
	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where table_name='strschema'").
		Check(testkit.Rows("3 3 <nil> <nil> <nil>", "3 3 <nil> <nil> <nil>", "255 255 <nil> <nil> <nil>", "255 255 <nil> <nil> <nil>"))
	tk.MustQuery("select NUMERIC_SCALE from information_schema.COLUMNS where table_name='floatschema'").
		Check(testkit.Rows("<nil>", "3"))

	// Test for auto increment ID.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int auto_increment primary key, d int)")
	tk.MustQuery("select auto_increment from information_schema.tables where table_name='t'").Check(
		testkit.Rows("1"))
	tk.MustExec("insert into t(c, d) values(1, 1)")
	tk.MustQuery("select auto_increment from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2"))

	tk.MustQuery("show create table t").Check(
		testkit.Rows("" +
			"t CREATE TABLE `t` (\n" +
			"  `c` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `d` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`c`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=30002"))

	// Test auto_increment for table without auto_increment column
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (d int)")
	tk.MustQuery("select auto_increment from information_schema.tables where table_name='t'").Check(
		testkit.Rows("<nil>"))

	tk.MustExec("create user xxx")

	// Test for length of enum and set
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t ( s set('a','bc','def','ghij') default NULL, e1 enum('a', 'ab', 'cdef'), s2 SET('1','2','3','4','1585','ONE','TWO','Y','N','THREE'))")
	tk.MustQuery("select column_name, character_maximum_length from information_schema.columns where table_schema=Database() and table_name = 't' and column_name = 's'").Check(
		testkit.Rows("s 13"))
	tk.MustQuery("select column_name, character_maximum_length from information_schema.columns where table_schema=Database() and table_name = 't' and column_name = 's2'").Check(
		testkit.Rows("s2 30"))
	tk.MustQuery("select column_name, character_maximum_length from information_schema.columns where table_schema=Database() and table_name = 't' and column_name = 'e1'").Check(
		testkit.Rows("e1 4"))

	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{
		Username: "xxx",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)

	tk1.MustQuery("select distinct(table_schema) from information_schema.tables").Check(testkit.Rows("INFORMATION_SCHEMA"))

	// Fix issue 9836
	sm := &mockSessionManager{make(map[uint64]*util.ProcessInfo, 1), nil}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: mysql.ComQuery,
		StmtCtx: tk.Se.GetSessionVars().StmtCtx,
	}
	tk.Se.SetSessionManager(sm)
	tk.MustQuery("SELECT user,host,command FROM information_schema.processlist;").Check(testkit.Rows("root 127.0.0.1 Query"))

	// Test for all system tables `TABLE_TYPE` is `SYSTEM VIEW`.
	rows1 := tk.MustQuery("select count(*) from information_schema.tables where table_schema in ('INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','METRICS_SCHEMA');").Rows()
	rows2 := tk.MustQuery("select count(*) from information_schema.tables where table_schema in ('INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','METRICS_SCHEMA') and  table_type = 'SYSTEM VIEW';").Rows()
	c.Assert(rows1, DeepEquals, rows2)
	// Test for system table default value
	tk.MustQuery("show create table information_schema.PROCESSLIST").Check(
		testkit.Rows("" +
			"PROCESSLIST CREATE TABLE `PROCESSLIST` (\n" +
			"  `ID` bigint(21) unsigned NOT NULL DEFAULT '0',\n" +
			"  `USER` varchar(16) NOT NULL DEFAULT '',\n" +
			"  `HOST` varchar(64) NOT NULL DEFAULT '',\n" +
			"  `DB` varchar(64) DEFAULT NULL,\n" +
			"  `COMMAND` varchar(16) NOT NULL DEFAULT '',\n" +
			"  `TIME` int(7) NOT NULL DEFAULT '0',\n" +
			"  `STATE` varchar(7) DEFAULT NULL,\n" +
			"  `INFO` longtext DEFAULT NULL,\n" +
			"  `DIGEST` varchar(64) DEFAULT '',\n" +
			"  `MEM` bigint(21) unsigned DEFAULT NULL,\n" +
			"  `DISK` bigint(21) unsigned DEFAULT NULL,\n" +
			"  `TxnStart` varchar(64) NOT NULL DEFAULT ''\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table information_schema.cluster_log").Check(
		testkit.Rows("" +
			"CLUSTER_LOG CREATE TABLE `CLUSTER_LOG` (\n" +
			"  `TIME` varchar(32) DEFAULT NULL,\n" +
			"  `TYPE` varchar(64) DEFAULT NULL,\n" +
			"  `INSTANCE` varchar(64) DEFAULT NULL,\n" +
			"  `LEVEL` varchar(8) DEFAULT NULL,\n" +
			"  `MESSAGE` longtext DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func (s *testTableSuite) TestCharacterSetCollations(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// Test charset/collation in information_schema.COLUMNS table.
	tk.MustExec("DROP DATABASE IF EXISTS charset_collate_test")
	tk.MustExec("CREATE DATABASE charset_collate_test; USE charset_collate_test")

	// TODO: Specifying the charset for national char/varchar should not be supported.
	tk.MustExec(`CREATE TABLE charset_collate_col_test(
		c_int int,
		c_float float,
		c_bit bit,
		c_bool bool,
		c_char char(1) charset ascii collate ascii_bin,
		c_nchar national char(1) charset ascii collate ascii_bin,
		c_binary binary,
		c_varchar varchar(1) charset ascii collate ascii_bin,
		c_nvarchar national varchar(1) charset ascii collate ascii_bin,
		c_varbinary varbinary(1),
		c_year year,
		c_date date,
		c_time time,
		c_datetime datetime,
		c_timestamp timestamp,
		c_blob blob,
		c_tinyblob tinyblob,
		c_mediumblob mediumblob,
		c_longblob longblob,
		c_text text charset ascii collate ascii_bin,
		c_tinytext tinytext charset ascii collate ascii_bin,
		c_mediumtext mediumtext charset ascii collate ascii_bin,
		c_longtext longtext charset ascii collate ascii_bin,
		c_json json,
		c_enum enum('1') charset ascii collate ascii_bin,
		c_set set('1') charset ascii collate ascii_bin
	)`)

	tk.MustQuery(`SELECT column_name, character_set_name, collation_name
					FROM information_schema.COLUMNS
					WHERE table_schema = "charset_collate_test" AND table_name = "charset_collate_col_test"
					ORDER BY column_name`,
	).Check(testkit.Rows(
		"c_binary <nil> <nil>",
		"c_bit <nil> <nil>",
		"c_blob <nil> <nil>",
		"c_bool <nil> <nil>",
		"c_char ascii ascii_bin",
		"c_date <nil> <nil>",
		"c_datetime <nil> <nil>",
		"c_enum ascii ascii_bin",
		"c_float <nil> <nil>",
		"c_int <nil> <nil>",
		"c_json <nil> <nil>",
		"c_longblob <nil> <nil>",
		"c_longtext ascii ascii_bin",
		"c_mediumblob <nil> <nil>",
		"c_mediumtext ascii ascii_bin",
		"c_nchar ascii ascii_bin",
		"c_nvarchar ascii ascii_bin",
		"c_set ascii ascii_bin",
		"c_text ascii ascii_bin",
		"c_time <nil> <nil>",
		"c_timestamp <nil> <nil>",
		"c_tinyblob <nil> <nil>",
		"c_tinytext ascii ascii_bin",
		"c_varbinary <nil> <nil>",
		"c_varchar ascii ascii_bin",
		"c_year <nil> <nil>",
	))
	tk.MustExec("DROP DATABASE charset_collate_test")
}

func (s *testTableSuite) TestCurrentTimestampAsDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("DROP DATABASE IF EXISTS default_time_test")
	tk.MustExec("CREATE DATABASE default_time_test; USE default_time_test")

	tk.MustExec(`CREATE TABLE default_time_table(
					c_datetime datetime,
					c_datetime_default datetime default current_timestamp,
					c_datetime_default_2 datetime(2) default current_timestamp(2),
					c_timestamp timestamp,
					c_timestamp_default timestamp default current_timestamp,
					c_timestamp_default_3 timestamp(3) default current_timestamp(3),
					c_varchar_default varchar(20) default "current_timestamp",
					c_varchar_default_3 varchar(20) default "current_timestamp(3)",
					c_varchar_default_on_update datetime default current_timestamp on update current_timestamp,
					c_varchar_default_on_update_fsp datetime(3) default current_timestamp(3) on update current_timestamp(3),
					c_varchar_default_with_case varchar(20) default "cUrrent_tImestamp"
				);`)

	tk.MustQuery(`SELECT column_name, column_default, extra
					FROM information_schema.COLUMNS
					WHERE table_schema = "default_time_test" AND table_name = "default_time_table"
					ORDER BY column_name`,
	).Check(testkit.Rows(
		"c_datetime <nil> ",
		"c_datetime_default CURRENT_TIMESTAMP ",
		"c_datetime_default_2 CURRENT_TIMESTAMP(2) ",
		"c_timestamp <nil> ",
		"c_timestamp_default CURRENT_TIMESTAMP ",
		"c_timestamp_default_3 CURRENT_TIMESTAMP(3) ",
		"c_varchar_default current_timestamp ",
		"c_varchar_default_3 current_timestamp(3) ",
		"c_varchar_default_on_update CURRENT_TIMESTAMP DEFAULT_GENERATED on update CURRENT_TIMESTAMP",
		"c_varchar_default_on_update_fsp CURRENT_TIMESTAMP(3) DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)",
		"c_varchar_default_with_case cUrrent_tImestamp ",
	))
	tk.MustExec("DROP DATABASE default_time_test")
}

type mockSessionManager struct {
	processInfoMap map[uint64]*util.ProcessInfo
	txnInfo        []*txninfo.TxnInfo
}

func (sm *mockSessionManager) ShowTxnList() []*txninfo.TxnInfo {
	return sm.txnInfo
}

func (sm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	return sm.processInfoMap
}

func (sm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	rs, ok := sm.processInfoMap[id]
	return rs, ok
}

func (sm *mockSessionManager) Kill(connectionID uint64, query bool) {}

func (sm *mockSessionManager) KillAllConnections() {}

func (sm *mockSessionManager) UpdateTLSConfig(cfg *tls.Config) {}

func (sm *mockSessionManager) ServerID() uint64 {
	return 1
}

func (s *testTableSuite) TestSomeTables(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, s.store)
	tk.Se = se
	sm := &mockSessionManager{make(map[uint64]*util.ProcessInfo, 2), nil}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "user-1",
		Host:    "localhost",
		Port:    "",
		DB:      "information_schema",
		Command: byte(1),
		Digest:  "abc1",
		State:   1,
		Info:    "do something",
		StmtCtx: tk.Se.GetSessionVars().StmtCtx,
	}
	sm.processInfoMap[2] = &util.ProcessInfo{
		ID:      2,
		User:    "user-2",
		Host:    "localhost",
		Port:    "",
		DB:      "test",
		Command: byte(2),
		Digest:  "abc2",
		State:   2,
		Info:    strings.Repeat("x", 101),
		StmtCtx: tk.Se.GetSessionVars().StmtCtx,
	}
	sm.processInfoMap[3] = &util.ProcessInfo{
		ID:      3,
		User:    "user-3",
		Host:    "127.0.0.1",
		Port:    "12345",
		DB:      "test",
		Command: byte(2),
		Digest:  "abc3",
		State:   1,
		Info:    "check port",
		StmtCtx: tk.Se.GetSessionVars().StmtCtx,
	}
	tk.Se.SetSessionManager(sm)
	tk.MustQuery("select * from information_schema.PROCESSLIST order by ID;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 0 ", "in transaction", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init DB 9223372036 %s %s abc2 0 0 ", "autocommit", strings.Repeat("x", 101)),
			fmt.Sprintf("3 user-3 127.0.0.1:12345 test Init DB 9223372036 %s %s abc3 0 0 ", "in transaction", "check port"),
		))
	tk.MustQuery("SHOW PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init DB 9223372036 %s %s", "autocommit", strings.Repeat("x", 100)),
			fmt.Sprintf("3 user-3 127.0.0.1:12345 test Init DB 9223372036 %s %s", "in transaction", "check port"),
		))
	tk.MustQuery("SHOW FULL PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init DB 9223372036 %s %s", "autocommit", strings.Repeat("x", 101)),
			fmt.Sprintf("3 user-3 127.0.0.1:12345 test Init DB 9223372036 %s %s", "in transaction", "check port"),
		))

	sm = &mockSessionManager{make(map[uint64]*util.ProcessInfo, 2), nil}
	sm.processInfoMap[1] = &util.ProcessInfo{
		ID:      1,
		User:    "user-1",
		Host:    "localhost",
		DB:      "information_schema",
		Command: byte(1),
		Digest:  "abc1",
		State:   1,
		StmtCtx: tk.Se.GetSessionVars().StmtCtx,
	}
	sm.processInfoMap[2] = &util.ProcessInfo{
		ID:            2,
		User:          "user-2",
		Host:          "localhost",
		Command:       byte(2),
		Digest:        "abc2",
		State:         2,
		Info:          strings.Repeat("x", 101),
		StmtCtx:       tk.Se.GetSessionVars().StmtCtx,
		CurTxnStartTS: 410090409861578752,
	}
	tk.Se.SetSessionManager(sm)
	tk.Se.GetSessionVars().TimeZone = time.UTC
	tk.MustQuery("select * from information_schema.PROCESSLIST order by ID;").Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 0 ", "in transaction", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 %s %s abc2 0 0 07-29 03:26:05.158(410090409861578752)", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("SHOW PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 %s %s", "autocommit", strings.Repeat("x", 100)),
		))
	tk.MustQuery("SHOW FULL PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 %s %s", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("select * from information_schema.PROCESSLIST where db is null;").Check(
		testkit.Rows(
			fmt.Sprintf("2 user-2 localhost <nil> Init DB 9223372036 %s %s abc2 0 0 07-29 03:26:05.158(410090409861578752)", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("select * from information_schema.PROCESSLIST where Info is null;").Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 0 ", "in transaction", "<nil>"),
		))
}

func prepareSlowLogfile(c *C, slowLogFileName string) {
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	c.Assert(err, IsNil)
	_, err = f.Write([]byte(`# Time: 2019-02-12T19:33:56.571953+08:00
# Txn_start_ts: 406315658548871171
# User@Host: root[root] @ localhost [127.0.0.1]
# Conn_ID: 6
# Exec_retry_time: 0.12 Exec_retry_count: 57
# Query_time: 4.895492
# Parse_time: 0.4
# Compile_time: 0.2
# Rewrite_time: 0.000000003 Preproc_subqueries: 2 Preproc_subqueries_time: 0.000000002
# Optimize_time: 0.00000001
# Wait_TS: 0.000000003
# LockKeys_time: 1.71 Request_count: 1 Prewrite_time: 0.19 Wait_prewrite_binlog_time: 0.21 Commit_time: 0.01 Commit_backoff_time: 0.18 Backoff_types: [txnLock] Resolve_lock_time: 0.03 Write_keys: 15 Write_size: 480 Prewrite_region: 1 Txn_retry: 8
# Cop_time: 0.3824278 Process_time: 0.161 Request_count: 1 Total_keys: 100001 Process_keys: 100000
# Rocksdb_delete_skipped_count: 100 Rocksdb_key_skipped_count: 10 Rocksdb_block_cache_hit_count: 10 Rocksdb_block_read_count: 10 Rocksdb_block_read_byte: 100
# Wait_time: 0.101
# Backoff_time: 0.092
# DB: test
# Is_internal: false
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03 Cop_proc_addr: 127.0.0.1:20160
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8 Cop_wait_addr: 0.0.0.0:20160
# Mem_max: 70724
# Disk_max: 65536
# Plan_from_cache: true
# Succ: true
# Plan: abcd
# Plan_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
# Prev_stmt: update t set i = 2;
select * from t_slim;`))
	c.Assert(f.Close(), IsNil)
	c.Assert(err, IsNil)
}

func (s *testTableSuite) TestTableRowIDShardingInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("DROP DATABASE IF EXISTS `sharding_info_test_db`")
	tk.MustExec("CREATE DATABASE `sharding_info_test_db`")

	assertShardingInfo := func(tableName string, expectInfo interface{}) {
		querySQL := fmt.Sprintf("select tidb_row_id_sharding_info from information_schema.tables where table_schema = 'sharding_info_test_db' and table_name = '%s'", tableName)
		info := tk.MustQuery(querySQL).Rows()[0][0]
		if expectInfo == nil {
			c.Assert(info, Equals, "<nil>")
		} else {
			c.Assert(info, Equals, expectInfo)
		}
	}
	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t1` (a int)")
	assertShardingInfo("t1", "NOT_SHARDED")

	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t2` (a int key)")
	assertShardingInfo("t2", "NOT_SHARDED(PK_IS_HANDLE)")

	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t3` (a int) SHARD_ROW_ID_BITS=4")
	assertShardingInfo("t3", "SHARD_BITS=4")

	tk.MustExec("CREATE VIEW `sharding_info_test_db`.`tv` AS select 1")
	assertShardingInfo("tv", nil)

	testFunc := func(dbName string, expectInfo interface{}) {
		dbInfo := model.DBInfo{Name: model.NewCIStr(dbName)}
		tableInfo := model.TableInfo{}

		info := infoschema.GetShardingInfo(&dbInfo, &tableInfo)
		c.Assert(info, Equals, expectInfo)
	}

	testFunc("information_schema", nil)
	testFunc("mysql", nil)
	testFunc("performance_schema", nil)
	testFunc("uucc", "NOT_SHARDED")

	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t4` (a bigint key clustered auto_random)")
	assertShardingInfo("t4", "PK_AUTO_RANDOM_BITS=5")

	tk.MustExec("CREATE TABLE `sharding_info_test_db`.`t5` (a bigint key clustered auto_random(1))")
	assertShardingInfo("t5", "PK_AUTO_RANDOM_BITS=1")

	tk.MustExec("DROP DATABASE `sharding_info_test_db`")
}

func (s *testTableSuite) TestSlowQuery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Prepare slow log file.
	slowLogFileName := "tidb_slow.log"
	prepareSlowLogfile(c, slowLogFileName)
	defer os.Remove(slowLogFileName)

	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", slowLogFileName))
	tk.MustExec("set time_zone = '+08:00';")
	re := tk.MustQuery("select * from information_schema.slow_query")
	re.Check(testutil.RowsWithSep("|",
		"2019-02-12 19:33:56.571953|406315658548871171|root|localhost|6|57|0.12|4.895492|0.4|0.2|0.000000003|2|0.000000002|0.00000001|0.000000003|0.19|0.21|0.01|0|0.18|[txnLock]|0.03|0|15|480|1|8|0.3824278|0.161|0.101|0.092|1.71|1|100001|100000|100|10|10|10|100|test||0|42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772|t1:1,t2:2|0.1|0.2|0.03|127.0.0.1:20160|0.05|0.6|0.8|0.0.0.0:20160|70724|65536|0|0|0|0||0|1|1|0|abcd|60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4|update t set i = 2;|select * from t_slim;"))
	tk.MustExec("set time_zone = '+00:00';")
	re = tk.MustQuery("select * from information_schema.slow_query")
	re.Check(testutil.RowsWithSep("|", "2019-02-12 11:33:56.571953|406315658548871171|root|localhost|6|57|0.12|4.895492|0.4|0.2|0.000000003|2|0.000000002|0.00000001|0.000000003|0.19|0.21|0.01|0|0.18|[txnLock]|0.03|0|15|480|1|8|0.3824278|0.161|0.101|0.092|1.71|1|100001|100000|100|10|10|10|100|test||0|42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772|t1:1,t2:2|0.1|0.2|0.03|127.0.0.1:20160|0.05|0.6|0.8|0.0.0.0:20160|70724|65536|0|0|0|0||0|1|1|0|abcd|60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4|update t set i = 2;|select * from t_slim;"))

	// Test for long query.
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	c.Assert(err, IsNil)
	defer f.Close()
	_, err = f.Write([]byte(`
# Time: 2019-02-13T19:33:56.571953+08:00
`))
	c.Assert(err, IsNil)
	sql := "select * from "
	for len(sql) < 5000 {
		sql += "abcdefghijklmnopqrstuvwxyz_1234567890_qwertyuiopasdfghjklzxcvbnm"
	}
	sql += ";"
	_, err = f.Write([]byte(sql))
	c.Assert(err, IsNil)
	c.Assert(f.Close(), IsNil)
	re = tk.MustQuery("select query from information_schema.slow_query order by time desc limit 1")
	rows := re.Rows()
	c.Assert(rows[0][0], Equals, sql)
}

func (s *testTableSuite) TestColumnStatistics(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select * from information_schema.column_statistics").Check(testkit.Rows())
}

func (s *testTableSuite) TestReloadDropDatabase(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database test_dbs")
	tk.MustExec("use test_dbs")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("create table t3 (a int)")
	is := domain.GetDomain(tk.Se).InfoSchema()
	t2, err := is.TableByName(model.NewCIStr("test_dbs"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tk.MustExec("drop database test_dbs")
	is = domain.GetDomain(tk.Se).InfoSchema()
	_, err = is.TableByName(model.NewCIStr("test_dbs"), model.NewCIStr("t2"))
	c.Assert(terror.ErrorEqual(infoschema.ErrTableNotExists, err), IsTrue)
	_, ok := is.TableByID(t2.Meta().ID)
	c.Assert(ok, IsFalse)
}

func (s *testClusterTableSuite) TestForClusterServerInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	instances := []string{
		strings.Join([]string{"tidb", s.listenAddr, s.listenAddr, "mock-version,mock-githash,1001"}, ","),
		strings.Join([]string{"pd", s.listenAddr, s.listenAddr, "mock-version,mock-githash,0"}, ","),
		strings.Join([]string{"tikv", s.listenAddr, s.listenAddr, "mock-version,mock-githash,0"}, ","),
	}

	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	fpName := "github.com/pingcap/tidb/infoschema/mockClusterInfo"
	c.Assert(failpoint.Enable(fpName, fpExpr), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	cases := []struct {
		sql      string
		types    set.StringSet
		addrs    set.StringSet
		names    set.StringSet
		skipOnOS string
	}{
		{
			sql:   "select * from information_schema.CLUSTER_LOAD;",
			types: set.NewStringSet("tidb", "tikv", "pd"),
			addrs: set.NewStringSet(s.listenAddr),
			names: set.NewStringSet("cpu", "memory", "net"),
		},
		{
			sql:   "select * from information_schema.CLUSTER_HARDWARE;",
			types: set.NewStringSet("tidb", "tikv", "pd"),
			addrs: set.NewStringSet(s.listenAddr),
			names: set.NewStringSet("cpu", "memory", "net", "disk"),
			// The sysutil package will filter out all disk don't have /dev prefix.
			skipOnOS: "windows",
		},
		{
			sql:   "select * from information_schema.CLUSTER_SYSTEMINFO;",
			types: set.NewStringSet("tidb", "tikv", "pd"),
			addrs: set.NewStringSet(s.listenAddr),
			names: set.NewStringSet("system"),
			// This test get empty result and fails on the windows platform.
			// Because the underlying implementation use `sysctl` command to get the result
			// and there is no such command on windows.
			// https://github.com/pingcap/sysutil/blob/2bfa6dc40bcd4c103bf684fba528ae4279c7ec9f/system_info.go#L50
			skipOnOS: "windows",
		},
	}

	for _, cas := range cases {
		if cas.skipOnOS == runtime.GOOS {
			continue
		}

		result := tk.MustQuery(cas.sql)
		rows := result.Rows()
		c.Assert(len(rows), Greater, 0)

		gotTypes := set.StringSet{}
		gotAddrs := set.StringSet{}
		gotNames := set.StringSet{}

		for _, row := range rows {
			gotTypes.Insert(row[0].(string))
			gotAddrs.Insert(row[1].(string))
			gotNames.Insert(row[2].(string))
		}

		c.Assert(gotTypes, DeepEquals, cas.types, Commentf("sql: %s", cas.sql))
		c.Assert(gotAddrs, DeepEquals, cas.addrs, Commentf("sql: %s", cas.sql))
		c.Assert(gotNames, DeepEquals, cas.names, Commentf("sql: %s", cas.sql))
	}
}

func (s *testTableSuite) TestSystemSchemaID(c *C) {
	uniqueIDMap := make(map[int64]string)
	s.checkSystemSchemaTableID(c, "information_schema", autoid.InformationSchemaDBID, 1, 10000, uniqueIDMap)
	s.checkSystemSchemaTableID(c, "performance_schema", autoid.PerformanceSchemaDBID, 10000, 20000, uniqueIDMap)
	s.checkSystemSchemaTableID(c, "metrics_schema", autoid.MetricSchemaDBID, 20000, 30000, uniqueIDMap)
}

func (s *testTableSuite) checkSystemSchemaTableID(c *C, dbName string, dbID, start, end int64, uniqueIDMap map[int64]string) {
	is := s.dom.InfoSchema()
	c.Assert(is, NotNil)
	db, ok := is.SchemaByName(model.NewCIStr(dbName))
	c.Assert(ok, IsTrue)
	c.Assert(db.ID, Equals, dbID)
	// Test for information_schema table id.
	tables := is.SchemaTables(model.NewCIStr(dbName))
	c.Assert(len(tables), Greater, 0)
	for _, tbl := range tables {
		tid := tbl.Meta().ID
		comment := Commentf("table name is %v", tbl.Meta().Name)
		c.Assert(tid&autoid.SystemSchemaIDFlag, Greater, int64(0), comment)
		c.Assert(tid&^autoid.SystemSchemaIDFlag, Greater, start, comment)
		c.Assert(tid&^autoid.SystemSchemaIDFlag, Less, end, comment)
		name, ok := uniqueIDMap[tid]
		c.Assert(ok, IsFalse, Commentf("schema id of %v is duplicate with %v, both is %v", name, tbl.Meta().Name, tid))
		uniqueIDMap[tid] = tbl.Meta().Name.O
	}
}

func (s *testClusterTableSuite) TestSelectClusterTable(c *C) {
	tk := s.newTestKitWithRoot(c)
	slowLogFileName := "tidb-slow.log"
	prepareSlowLogfile(c, slowLogFileName)
	defer os.Remove(slowLogFileName)
	for i := 0; i < 2; i++ {
		tk.MustExec("use information_schema")
		tk.MustExec(fmt.Sprintf("set @@tidb_enable_streaming=%d", i))
		tk.MustExec("set @@global.tidb_enable_stmt_summary=1")
		tk.MustExec("set time_zone = '+08:00';")
		tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("1"))
		tk.MustQuery("select time from `CLUSTER_SLOW_QUERY` where time='2019-02-12 19:33:56.571953'").Check(testutil.RowsWithSep("|", "2019-02-12 19:33:56.571953"))
		tk.MustQuery("select count(*) from `CLUSTER_PROCESSLIST`").Check(testkit.Rows("1"))
		tk.MustQuery("select * from `CLUSTER_PROCESSLIST`").Check(testkit.Rows(fmt.Sprintf(":10080 1 root 127.0.0.1 <nil> Query 9223372036 %s <nil>  0 0 ", "")))
		tk.MustQuery("select query_time, conn_id from `CLUSTER_SLOW_QUERY` order by time limit 1").Check(testkit.Rows("4.895492 6"))
		tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY` group by digest").Check(testkit.Rows("1"))
		tk.MustQuery("select digest, count(*) from `CLUSTER_SLOW_QUERY` group by digest").Check(testkit.Rows("42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772 1"))
		tk.MustQuery(`select length(query) as l,time from information_schema.cluster_slow_query where time > "2019-02-12 19:33:56" order by abs(l) desc limit 10;`).Check(testkit.Rows("21 2019-02-12 19:33:56.571953"))
		tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY` where time > now() group by digest").Check(testkit.Rows())
		re := tk.MustQuery("select * from `CLUSTER_statements_summary`")
		c.Assert(re, NotNil)
		c.Assert(len(re.Rows()) > 0, IsTrue)
		// Test for TiDB issue 14915.
		re = tk.MustQuery("select sum(exec_count*avg_mem) from cluster_statements_summary_history group by schema_name,digest,digest_text;")
		c.Assert(re, NotNil)
		c.Assert(len(re.Rows()) > 0, IsTrue)
		tk.MustQuery("select * from `CLUSTER_statements_summary_history`")
		c.Assert(re, NotNil)
		c.Assert(len(re.Rows()) > 0, IsTrue)
		tk.MustExec("set @@global.tidb_enable_stmt_summary=0")
		re = tk.MustQuery("select * from `CLUSTER_statements_summary`")
		c.Assert(re, NotNil)
		c.Assert(len(re.Rows()) == 0, IsTrue)
		tk.MustQuery("select * from `CLUSTER_statements_summary_history`")
		c.Assert(re, NotNil)
		c.Assert(len(re.Rows()) == 0, IsTrue)
	}
}

func (s *testClusterTableSuite) TestSelectClusterTablePrivelege(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	slowLogFileName := "tidb-slow.log"
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	c.Assert(err, IsNil)
	_, err = f.Write([]byte(
		`# Time: 2019-02-12T19:33:57.571953+08:00
# User@Host: user2 [user2] @ 127.0.0.1 [127.0.0.1]
select * from t2;
# Time: 2019-02-12T19:33:56.571953+08:00
# User@Host: user1 [user1] @ 127.0.0.1 [127.0.0.1]
select * from t1;
# Time: 2019-02-12T19:33:58.571953+08:00
# User@Host: user2 [user2] @ 127.0.0.1 [127.0.0.1]
select * from t3;
# Time: 2019-02-12T19:33:59.571953+08:00
select * from t3;
`))
	c.Assert(f.Close(), IsNil)
	c.Assert(err, IsNil)
	defer os.Remove(slowLogFileName)
	tk.MustExec("use information_schema")
	tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from `SLOW_QUERY`").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from `CLUSTER_PROCESSLIST`").Check(testkit.Rows("1"))
	tk.MustQuery("select * from `CLUSTER_PROCESSLIST`").Check(testkit.Rows(fmt.Sprintf(":10080 1 root 127.0.0.1 <nil> Query 9223372036 %s <nil>  0 0 ", "")))
	tk.MustExec("create user user1")
	tk.MustExec("create user user2")
	user1 := testkit.NewTestKit(c, s.store)
	user1.MustExec("use information_schema")
	c.Assert(user1.Se.Auth(&auth.UserIdentity{
		Username: "user1",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	user1.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("1"))
	user1.MustQuery("select count(*) from `SLOW_QUERY`").Check(testkit.Rows("1"))
	user1.MustQuery("select user,query from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("user1 select * from t1;"))

	user2 := testkit.NewTestKit(c, s.store)
	user2.MustExec("use information_schema")
	c.Assert(user2.Se.Auth(&auth.UserIdentity{
		Username: "user2",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	user2.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("2"))
	user2.MustQuery("select user,query from `CLUSTER_SLOW_QUERY` order by query").Check(testkit.Rows("user2 select * from t2;", "user2 select * from t3;"))
}

func (s *testTableSuite) TestSelectHiddenColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("DROP DATABASE IF EXISTS `test_hidden`;")
	tk.MustExec("CREATE DATABASE `test_hidden`;")
	tk.MustExec("USE test_hidden;")
	tk.MustExec("CREATE TABLE hidden (a int , b int, c int);")
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden'").Check(testkit.Rows("3"))
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test_hidden"), model.NewCIStr("hidden"))
	c.Assert(err, IsNil)
	colInfo := tb.Meta().Columns
	// Set column b to hidden
	colInfo[1].Hidden = true
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden'").Check(testkit.Rows("2"))
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden' and column_name = 'b'").Check(testkit.Rows("0"))
	// Set column b to visible
	colInfo[1].Hidden = false
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden' and column_name = 'b'").Check(testkit.Rows("1"))
	// Set a, b ,c to hidden
	colInfo[0].Hidden = true
	colInfo[1].Hidden = true
	colInfo[2].Hidden = true
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where table_name = 'hidden'").Check(testkit.Rows("0"))
}

func (s *testTableSuite) TestFormatVersion(c *C) {
	// Test for defaultVersions.
	defaultVersions := []string{"5.7.25-TiDB-None", "5.7.25-TiDB-8.0.18", "5.7.25-TiDB-8.0.18-beta.1", "5.7.25-TiDB-v4.0.0-beta-446-g5268094af"}
	defaultRes := []string{"None", "8.0.18", "8.0.18-beta.1", "4.0.0-beta"}
	for i, v := range defaultVersions {
		version := infoschema.FormatVersion(v, true)
		c.Assert(version, Equals, defaultRes[i])
	}

	// Test for versions user set.
	versions := []string{"8.0.18", "5.7.25-TiDB", "8.0.18-TiDB-4.0.0-beta.1"}
	res := []string{"8.0.18", "5.7.25-TiDB", "8.0.18-TiDB-4.0.0-beta.1"}
	for i, v := range versions {
		version := infoschema.FormatVersion(v, false)
		c.Assert(version, Equals, res[i])
	}
}

// Test statements_summary.
func (s *testTableSuite) TestStmtSummaryTable(c *C) {
	tk := s.newTestKitWithRoot(c)

	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustQuery("select column_comment from information_schema.columns " +
		"where table_name='STATEMENTS_SUMMARY' and column_name='STMT_TYPE'",
	).Check(testkit.Rows("Statement type"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")

	// Clear all statements.
	tk.MustExec("set session tidb_enable_stmt_summary = 0")
	tk.MustExec("set session tidb_enable_stmt_summary = ''")

	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))

	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Create a new session to test.
	tk = s.newTestKitWithRoot(c)

	// Test INSERT
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustExec("insert into t    values(2, 'b')")
	tk.MustExec("insert into t VALUES(3, 'c')")
	tk.MustExec("/**/insert into t values(4, 'd')")

	sql := "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text " +
		"from information_schema.statements_summary " +
		"where digest_text like 'insert into `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Insert test test.t <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into t values(1, 'a')"))

	// Test point get.
	tk.MustExec("drop table if exists p")
	tk.MustExec("create table p(a int primary key, b int)")
	for i := 1; i < 3; i++ {
		tk.MustQuery("select b from p where a=1")
		expectedResult := fmt.Sprintf("%d \tid         \ttask\testRows\toperator info\n\tPoint_Get_1\troot\t1      \ttable:p, handle:1 %s", i, "test.p")
		// Also make sure that the plan digest is not empty
		sql = "select exec_count, plan, table_names from information_schema.statements_summary " +
			"where digest_text like 'select `b` from `p`%' and plan_digest != ''"
		tk.MustQuery(sql).Check(testkit.Rows(expectedResult))
	}

	// Point get another database.
	tk.MustQuery("select variable_value from mysql.tidb where variable_name = 'system_tz'")
	// Test for Encode plan cache.
	p1 := tk.Se.GetSessionVars().StmtCtx.GetEncodedPlan()
	c.Assert(len(p1) > 0, IsTrue)
	rows := tk.MustQuery("select tidb_decode_plan('" + p1 + "');").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(len(rows[0]), Equals, 1)
	c.Assert(rows[0][0], Matches, ".*\n.*Point_Get.*table.tidb, index.PRIMARY.VARIABLE_NAME.*")

	sql = "select table_names from information_schema.statements_summary " +
		"where digest_text like 'select `variable_value`%' and `schema_name`='test'"
	tk.MustQuery(sql).Check(testkit.Rows("mysql.tidb"))

	// Test `create database`.
	tk.MustExec("create database if not exists test")
	// Test for Encode plan cache.
	p2 := tk.Se.GetSessionVars().StmtCtx.GetEncodedPlan()
	c.Assert(p2, Equals, "")
	tk.MustQuery(`select table_names
			from information_schema.statements_summary
			where digest_text like 'create database%' and schema_name='test'`,
	).Check(testkit.Rows("<nil>"))

	// Test SELECT.
	const failpointName = "github.com/pingcap/tidb/planner/core/mockPlanRowCount"
	c.Assert(failpoint.Enable(failpointName, "return(100)"), IsNil)
	defer func() { c.Assert(failpoint.Disable(failpointName), IsNil) }()
	tk.MustQuery("select * from t where a=2")

	sql = "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text, plan " +
		"from information_schema.statements_summary " +
		"where digest_text like 'select * from `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Select test test.t t:k 1 2 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid                \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10    \troot     \t100    \t\n" +
		"\t├─IndexRangeScan_8\tcop[tikv]\t100    \ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableRowIDScan_9\tcop[tikv]\t100    \ttable:t, keep order:false, stats:pseudo"))

	// select ... order by
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text
		from information_schema.statements_summary
		order by exec_count desc limit 1`,
	).Check(testkit.Rows("Insert test test.t <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into t values(1, 'a')"))

	// Test different plans with same digest.
	c.Assert(failpoint.Enable(failpointName, "return(1000)"), IsNil)
	tk.MustQuery("select * from t where a=3")
	sql = "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text, plan " +
		"from information_schema.statements_summary " +
		"where digest_text like 'select * from `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows(
		"Select test test.t t:k 2 4 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid                \ttask     \testRows\toperator info\n" +
			"\tIndexLookUp_10    \troot     \t100    \t\n" +
			"\t├─IndexRangeScan_8\tcop[tikv]\t100    \ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
			"\t└─TableRowIDScan_9\tcop[tikv]\t100    \ttable:t, keep order:false, stats:pseudo"))

	// Disable it again.
	tk.MustExec("set global tidb_enable_stmt_summary = false")
	tk.MustExec("set session tidb_enable_stmt_summary = false")
	defer tk.MustExec("set global tidb_enable_stmt_summary = 1")
	defer tk.MustExec("set session tidb_enable_stmt_summary = ''")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("0"))

	// Create a new session to test
	tk = s.newTestKitWithRoot(c)

	// This statement shouldn't be summarized.
	tk.MustQuery("select * from t where a=2")

	// The table should be cleared.
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.statements_summary`,
	).Check(testkit.Rows())

	// Enable it in session scope.
	tk.MustExec("set session tidb_enable_stmt_summary = on")
	// It should work immediately.
	tk.MustExec("begin")
	tk.MustExec("insert into t values(1, 'a')")
	tk.MustExec("commit")
	sql = "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text, prev_sample_text " +
		"from information_schema.statements_summary " +
		"where digest_text like 'insert into `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Insert test test.t <nil> 1 0 0 0 0 0 0 0 0 0 1 insert into t values(1, 'a') "))
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, prev_sample_text
		from information_schema.statements_summary
		where digest_text='commit'`,
	).Check(testkit.Rows("Commit test <nil> <nil> 1 0 0 0 0 0 2 2 1 1 0 commit insert into t values(1, 'a')"))

	tk.MustQuery("select * from t where a=2")
	sql = "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text, plan " +
		"from information_schema.statements_summary " +
		"where digest_text like 'select * from `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Select test test.t t:k 1 2 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid                \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10    \troot     \t1000   \t\n" +
		"\t├─IndexRangeScan_8\tcop[tikv]\t1000   \ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableRowIDScan_9\tcop[tikv]\t1000   \ttable:t, keep order:false, stats:pseudo"))

	// Disable it in global scope.
	tk.MustExec("set global tidb_enable_stmt_summary = false")

	// Create a new session to test.
	tk = s.newTestKitWithRoot(c)

	tk.MustQuery("select * from t where a=2")

	// Statement summary is still enabled.
	sql = "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys, " +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions, " +
		"max_prewrite_regions, avg_affected_rows, query_sample_text, plan " +
		"from information_schema.statements_summary " +
		"where digest_text like 'select * from `t`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Select test test.t t:k 2 4 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid                \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10    \troot     \t1000   \t\n" +
		"\t├─IndexRangeScan_8\tcop[tikv]\t1000   \ttable:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─TableRowIDScan_9\tcop[tikv]\t1000   \ttable:t, keep order:false, stats:pseudo"))

	// Unset session variable.
	tk.MustExec("set session tidb_enable_stmt_summary = ''")
	tk.MustQuery("select * from t where a=2")

	// Statement summary is disabled.
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.statements_summary`,
	).Check(testkit.Rows())

	// Create a new session to test
	tk = s.newTestKitWithRoot(c)

	tk.MustExec("set global tidb_enable_stmt_summary = on")
	tk.MustExec("set global tidb_stmt_summary_history_size = 24")

	// Create a new user to test statements summary table privilege
	tk.MustExec("create user 'test_user'@'localhost'")
	tk.MustExec("grant select on *.* to 'test_user'@'localhost'")
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "root",
		Hostname:     "%",
		AuthUsername: "root",
		AuthHostname: "%",
	}, nil, nil)
	tk.MustExec("select * from t where a=1")
	result := tk.MustQuery("select * from information_schema.statements_summary where digest_text like 'select * from `t`%'")
	// Super user can query all records.
	c.Assert(len(result.Rows()), Equals, 1)
	result = tk.MustQuery("select *	from information_schema.statements_summary_history	where digest_text like 'select * from `t`%'")
	c.Assert(len(result.Rows()), Equals, 1)
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "test_user",
		Hostname:     "localhost",
		AuthUsername: "test_user",
		AuthHostname: "localhost",
	}, nil, nil)
	result = tk.MustQuery("select * from information_schema.statements_summary where digest_text like 'select * from `t`%'")
	// Ordinary users can not see others' records
	c.Assert(len(result.Rows()), Equals, 0)
	result = tk.MustQuery("select *	from information_schema.statements_summary_history where digest_text like 'select * from `t`%'")
	c.Assert(len(result.Rows()), Equals, 0)
	tk.MustExec("select * from t where a=1")
	result = tk.MustQuery("select *	from information_schema.statements_summary	where digest_text like 'select * from `t`%'")
	c.Assert(len(result.Rows()), Equals, 1)
	tk.MustExec("select * from t where a=1")
	result = tk.MustQuery("select *	from information_schema.statements_summary_history	where digest_text like 'select * from `t`%'")
	c.Assert(len(result.Rows()), Equals, 1)
	// use root user to set variables back
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "root",
		Hostname:     "%",
		AuthUsername: "root",
		AuthHostname: "%",
	}, nil, nil)
}

func (s *testTableSuite) TestIssue18845(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`CREATE USER 'user18845'@'localhost';`)
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "user18845",
		Hostname:     "localhost",
		AuthUsername: "user18845",
		AuthHostname: "localhost",
	}, nil, nil)
	tk.MustQuery(`select count(*) from information_schema.columns;`)
}

// Test statements_summary_history.
func (s *testClusterTableSuite) TestStmtSummaryHistoryTable(c *C) {
	tk := s.newTestKitWithRoot(c)
	tk.MustExec("drop table if exists test_summary")
	tk.MustExec("create table test_summary(a int, b varchar(10), key k(a))")

	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))

	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Create a new session to test.
	tk = s.newTestKitWithRoot(c)

	// Test INSERT
	tk.MustExec("insert into test_summary values(1, 'a')")
	tk.MustExec("insert into test_summary    values(2, 'b')")
	tk.MustExec("insert into TEST_SUMMARY VALUES(3, 'c')")
	tk.MustExec("/**/insert into test_summary values(4, 'd')")

	sql := "select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys," +
		"max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions," +
		"max_prewrite_regions, avg_affected_rows, query_sample_text " +
		"from information_schema.statements_summary_history " +
		"where digest_text like 'insert into `test_summary`%'"
	tk.MustQuery(sql).Check(testkit.Rows("Insert test test.test_summary <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into test_summary values(1, 'a')"))

	tk.MustExec("set global tidb_stmt_summary_history_size = 0")
	tk.MustQuery(`select stmt_type, schema_name, table_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.statements_summary_history`,
	).Check(testkit.Rows())

	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("drop table if exists `table`")
	tk.MustExec("set global tidb_stmt_summary_history_size = 1")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustExec("create table `table`(`insert` int)")
	tk.MustExec("select `insert` from `table`")

	sql = "select digest_text from information_schema.statements_summary_history;"
	tk.MustQuery(sql).Check(testkit.Rows(
		"select `insert` from `table`",
		"create table `table` ( `insert` int )",
		"set global `tidb_enable_stmt_summary` = ?",
	))
}

// Test statements_summary_history.
func (s *testTableSuite) TestStmtSummaryInternalQuery(c *C) {
	tk := s.newTestKitWithRoot(c)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), key k(a))")

	// We use the sql binding evolve to check the internal query summary.
	tk.MustExec("set @@tidb_use_plan_baselines = 1")
	tk.MustExec("set @@tidb_evolve_plan_baselines = 1")
	tk.MustExec("create global binding for select * from t where t.a = 1 using select * from t ignore index(k) where t.a = 1")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))
	// Disable refreshing summary.
	tk.MustExec("set global tidb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.tidb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Test Internal

	// Create a new session to test.
	tk = s.newTestKitWithRoot(c)

	tk.MustExec("select * from t where t.a = 1")
	tk.MustQuery(`select exec_count, digest_text
		from information_schema.statements_summary
		where digest_text like "select original_sql , bind_sql , default_db , status%"`).Check(testkit.Rows())

	// Enable internal query and evolve baseline.
	tk.MustExec("set global tidb_stmt_summary_internal_query = 1")
	defer tk.MustExec("set global tidb_stmt_summary_internal_query = false")

	// Create a new session to test.
	tk = s.newTestKitWithRoot(c)

	tk.MustExec("admin flush bindings")
	tk.MustExec("admin evolve bindings")

	// `exec_count` may be bigger than 1 because other cases are also running.
	sql := "select digest_text " +
		"from information_schema.statements_summary " +
		"where digest_text like \"select `original_sql` , `bind_sql` , `default_db` , status%\""
	tk.MustQuery(sql).Check(testkit.Rows(
		"select `original_sql` , `bind_sql` , `default_db` , status , `create_time` , `update_time` , charset , " +
			"collation , source from `mysql` . `bind_info` where `update_time` > ? order by `update_time`"))

	// Test for issue #21642.
	tk.MustQuery(`select tidb_version()`)
	rows := tk.MustQuery("select plan from information_schema.statements_summary where digest_text like \"select `tidb_version`%\"").Rows()
	c.Assert(strings.Contains(rows[0][0].(string), "Projection"), IsTrue)
}

// Test error count and warning count.
func (s *testTableSuite) TestStmtSummaryErrorCount(c *C) {
	tk := s.newTestKitWithRoot(c)

	// Clear summaries.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists stmt_summary_test")
	tk.MustExec("create table stmt_summary_test(id int primary key)")
	tk.MustExec("insert into stmt_summary_test values(1)")
	_, err := tk.Exec("insert into stmt_summary_test values(1)")
	c.Assert(err, NotNil)

	sql := "select exec_count, sum_errors, sum_warnings from information_schema.statements_summary where digest_text like \"insert into `stmt_summary_test`%\""
	tk.MustQuery(sql).Check(testkit.Rows("2 1 0"))

	tk.MustExec("insert ignore into stmt_summary_test values(1)")
	sql = "select exec_count, sum_errors, sum_warnings from information_schema.statements_summary where digest_text like \"insert ignore into `stmt_summary_test`%\""
	tk.MustQuery(sql).Check(testkit.Rows("1 0 1"))
}

func (s *testTableSuite) TestStmtSummaryPreparedStatements(c *C) {
	tk := s.newTestKitWithRoot(c)

	// Clear summaries.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")

	tk.MustExec("use test")
	tk.MustExec("prepare stmt from 'select ?'")
	tk.MustExec("set @number=1")
	tk.MustExec("execute stmt using @number")

	tk.MustQuery(`select exec_count
		from information_schema.statements_summary
		where digest_text like "prepare%"`).Check(testkit.Rows())
	tk.MustQuery(`select exec_count
		from information_schema.statements_summary
		where digest_text like "select ?"`).Check(testkit.Rows("1"))
}

func (s *testTableSuite) TestStmtSummarySensitiveQuery(c *C) {
	tk := s.newTestKitWithRoot(c)
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustExec("drop user if exists user_sensitive;")
	tk.MustExec("create user user_sensitive identified by '123456789';")
	tk.MustExec("alter user 'user_sensitive'@'%' identified by 'abcdefg';")
	tk.MustExec("set password for 'user_sensitive'@'%' = 'xyzuvw';")
	tk.MustQuery("select query_sample_text from `information_schema`.`STATEMENTS_SUMMARY` " +
		"where query_sample_text like '%user_sensitive%' and " +
		"(query_sample_text like 'set password%' or query_sample_text like 'create user%' or query_sample_text like 'alter user%') " +
		"order by query_sample_text;").
		Check(testkit.Rows(
			"alter user {user_sensitive@% password = ***}",
			"create user {user_sensitive@% password = ***}",
			"set password for user user_sensitive@%",
		))
}

// test stmtSummaryEvictedCount
func (s *testTableSuite) TestSimpleStmtSummaryEvictedCount(c *C) {
	now := time.Now().Unix()
	interval := int64(1800)
	beginTimeForCurInterval := now - now%interval
	tk := s.newTestKitWithPlanCache(c)
	tk.MustExec(fmt.Sprintf("set global tidb_stmt_summary_refresh_interval = %v", interval))

	// clean up side effects
	defer tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 100")
	defer tk.MustExec("set global tidb_stmt_summary_refresh_interval = 1800")

	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	// first sql
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 1")
	// second sql
	tk.MustQuery("show databases;")
	// query `evicted table` is also a SQL, passing it leads to the eviction of the previous SQLs.
	tk.MustQuery("select * from `information_schema`.`STATEMENTS_SUMMARY_EVICTED`;").
		Check(testkit.Rows(
			fmt.Sprintf("%s %s %v",
				time.Unix(beginTimeForCurInterval, 0).Format("2006-01-02 15:04:05"),
				time.Unix(beginTimeForCurInterval+interval, 0).Format("2006-01-02 15:04:05"),
				int64(2)),
		))
	// TODO: Add more tests.
}

// test stmtSummaryEvictedCount cluster table
func (s *testClusterTableSuite) TestStmtSummaryEvictedCountTable(c *C) {
	tk := s.newTestKitWithRoot(c)
	// disable refreshing
	tk.MustExec("set global tidb_stmt_summary_refresh_interval=9999")
	// set information_schema.statements_summary's size to 1
	tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 1")
	// clean up side effects
	defer tk.MustExec("set global tidb_stmt_summary_max_stmt_count = 100")
	defer tk.MustExec("set global tidb_stmt_summary_refresh_interval = 1800")
	// clear information_schema.statements_summary
	tk.MustExec("set global tidb_enable_stmt_summary=0")
	tk.MustExec("set global tidb_enable_stmt_summary=1")

	// make a new session for test...
	tk = s.newTestKitWithRoot(c)
	// first sql
	tk.MustExec("show databases;")
	// second sql, evict former sql from stmt_summary
	tk.MustQuery("select evicted_count from information_schema.cluster_statements_summary_evicted;").
		Check(testkit.Rows("1"))
	// after executed the sql above
	tk.MustQuery("select evicted_count from information_schema.cluster_statements_summary_evicted;").
		Check(testkit.Rows("2"))
	// TODO: Add more tests.

}

func (s *testTableSuite) TestPerformanceSchemaforPlanCache(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)

	tk := s.newTestKitWithPlanCache(c)

	// Clear summaries.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("prepare stmt from 'select * from t'")
	tk.MustExec("execute stmt")
	tk.MustQuery("select plan_cache_hits, plan_in_cache from information_schema.statements_summary where digest_text='select * from `t`'").Check(
		testkit.Rows("0 0"))
	tk.MustExec("execute stmt")
	tk.MustExec("execute stmt")
	tk.MustExec("execute stmt")
	tk.MustQuery("select plan_cache_hits, plan_in_cache from information_schema.statements_summary where digest_text='select * from `t`'").Check(
		testkit.Rows("3 1"))
}

func (s *testTableSuite) TestServerInfoResolveLoopBackAddr(c *C) {
	nodes := []infoschema.ServerInfo{
		{Address: "127.0.0.1:4000", StatusAddr: "192.168.130.22:10080"},
		{Address: "0.0.0.0:4000", StatusAddr: "192.168.130.22:10080"},
		{Address: "localhost:4000", StatusAddr: "192.168.130.22:10080"},
		{Address: "192.168.130.22:4000", StatusAddr: "0.0.0.0:10080"},
		{Address: "192.168.130.22:4000", StatusAddr: "127.0.0.1:10080"},
		{Address: "192.168.130.22:4000", StatusAddr: "localhost:10080"},
	}
	for i := range nodes {
		nodes[i].ResolveLoopBackAddr()
	}
	for _, n := range nodes {
		c.Assert(n.Address, Equals, "192.168.130.22:4000")
		c.Assert(n.StatusAddr, Equals, "192.168.130.22:10080")
	}
}

func (s *testTableSuite) TestPlacementPolicy(c *C) {
	tk := s.newTestKitWithRoot(c)
	tk.MustExec("use test")
	tk.MustExec("create table test_placement(id int primary key) partition by hash(id) partitions 2")

	is := s.dom.InfoSchema()
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_placement"))
	c.Assert(err, IsNil)
	partDefs := tb.Meta().GetPartitionInfo().Definitions

	tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows())

	bundleID := "pd"
	is.SetBundle(&placement.Bundle{
		ID: bundleID,
		Rules: []*placement.Rule{
			{
				GroupID: bundleID,
				ID:      "default",
				Role:    "voter",
				Count:   3,
			},
		},
	})
	tk.MustQuery("select * from information_schema.placement_policy").Check(testkit.Rows())

	bundleID = fmt.Sprintf("%s%d", placement.BundleIDPrefix, partDefs[0].ID)
	bundle := &placement.Bundle{
		ID:       bundleID,
		Index:    3,
		Override: true,
		Rules: []*placement.Rule{
			{
				GroupID: bundleID,
				ID:      "0",
				Role:    "voter",
				Count:   3,
				Constraints: []placement.Constraint{
					{
						Key:    "zone",
						Op:     "in",
						Values: []string{"bj"},
					},
				},
			},
		},
	}
	is.SetBundle(bundle)
	expected := fmt.Sprintf(`%s 3 0 test test_placement p0 <nil> voter 3 "+zone=bj"`, bundleID)
	tk.MustQuery(`select group_id, group_index, rule_id, schema_name, table_name, partition_name, index_name,
		role, replicas, constraints from information_schema.placement_policy`).Check(testkit.Rows(expected))

	rule1 := bundle.Rules[0].Clone()
	rule1.ID = "1"
	bundle.Rules = append(bundle.Rules, rule1)
	tk.MustQuery("select rule_id, schema_name, table_name, partition_name from information_schema.placement_policy order by rule_id").Check(testkit.Rows(
		"0 test test_placement p0", "1 test test_placement p0"))

	bundleID = fmt.Sprintf("%s%d", placement.BundleIDPrefix, partDefs[1].ID)
	bundle1 := bundle.Clone()
	bundle1.ID = bundleID
	bundle1.Rules[0].GroupID = bundleID
	bundle1.Rules[1].GroupID = bundleID
	is.SetBundle(bundle1)
	tk.MustQuery("select rule_id, schema_name, table_name, partition_name from information_schema.placement_policy order by partition_name, rule_id").Check(testkit.Rows(
		"0 test test_placement p0", "1 test test_placement p0", "0 test test_placement p1", "1 test test_placement p1"))

	// do not report error for invalid ObjectID
	// check pingcap/tidb/issues/22950
	bundle1.ID = placement.GroupID(1)
	tk.MustQuery("select rule_id from information_schema.placement_policy order by rule_id").Check(testkit.Rows(
		"0", "1"))

	// test the failpoint for testing
	fpName := "github.com/pingcap/tidb/executor/outputInvalidPlacementRules"
	c.Assert(failpoint.Enable(fpName, "return(true)"), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()
	tk.MustQuery("select rule_id from information_schema.placement_policy order by rule_id").Check(testkit.Rows(
		"0", "0", "1", "1"))
}

func (s *testTableSuite) TestInfoschemaClientErrors(c *C) {
	tk := s.newTestKitWithRoot(c)

	tk.MustExec("FLUSH CLIENT_ERRORS_SUMMARY")

	errno.IncrementError(1365, "root", "localhost")
	errno.IncrementError(1365, "infoschematest", "localhost")
	errno.IncrementError(1365, "root", "localhost")

	tk.MustExec("CREATE USER 'infoschematest'@'localhost'")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "infoschematest", Hostname: "localhost"}, nil, nil), IsTrue)

	err := tk.QueryToErr("SELECT * FROM information_schema.client_errors_summary_global")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	err = tk.QueryToErr("SELECT * FROM information_schema.client_errors_summary_by_host")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	tk.MustQuery("SELECT error_number, error_count, warning_count FROM information_schema.client_errors_summary_by_user ORDER BY error_number").Check(testkit.Rows("1365 1 0"))

	err = tk.ExecToErr("FLUSH CLIENT_ERRORS_SUMMARY")
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the RELOAD privilege(s) for this operation")
}

func (s *testTableSuite) TestTrx(c *C) {
	tk := s.newTestKitWithRoot(c)
	_, digest := parser.NormalizeDigest("select * from trx for update;")
	sm := &mockSessionManager{nil, make([]*txninfo.TxnInfo, 2)}
	sm.txnInfo[0] = &txninfo.TxnInfo{
		StartTS:          424768545227014155,
		CurrentSQLDigest: digest.String(),
		State:            txninfo.TxnRunningNormal,
		BlockStartTime:   nil,
		EntriesCount:     1,
		EntriesSize:      19,
		ConnectionID:     2,
		Username:         "root",
		CurrentDB:        "test",
	}
	blockTime2 := time.Date(2021, 05, 20, 13, 18, 30, 123456000, time.Local)
	sm.txnInfo[1] = &txninfo.TxnInfo{
		StartTS:          425070846483628033,
		CurrentSQLDigest: "",
		AllSQLDigests:    []string{"sql1", "sql2"},
		State:            txninfo.TxnLockWaiting,
		BlockStartTime:   unsafe.Pointer(&blockTime2),
		ConnectionID:     10,
		Username:         "user1",
		CurrentDB:        "db1",
	}
	tk.Se.SetSessionManager(sm)
	tk.MustQuery("select * from information_schema.TIDB_TRX;").Check(testkit.Rows(
		"424768545227014155 2021-05-07 12:56:48.001000 "+digest.String()+" Normal <nil> 1 19 2 root test []",
		"425070846483628033 2021-05-20 21:16:35.778000 <nil> LockWaiting 2021-05-20 13:18:30.123456 0 0 10 user1 db1 [sql1, sql2]"))
}

func (s *testTableSuite) TestInfoschemaDeadlockPrivilege(c *C) {
	tk := s.newTestKitWithRoot(c)
	tk.MustExec("create user 'testuser'@'localhost'")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil), IsTrue)
	err := tk.QueryToErr("select * from information_schema.deadlocks")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	tk = s.newTestKitWithRoot(c)
	tk.MustExec("create user 'testuser2'@'localhost'")
	tk.MustExec("grant process on *.* to 'testuser2'@'localhost'")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil), IsTrue)
	_ = tk.MustQuery("select * from information_schema.deadlocks")
}

func (s *testDataLockWaitSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	kvstore, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
	_, digest1 := parser.NormalizeDigest("select * from t1 for update;")
	_, digest2 := parser.NormalizeDigest("update t1 set f1=1 where id=2;")
	s.store, err = mockstorage.NewMockStorageWithLockWaits(kvstore, []*deadlock.WaitForEntry{
		{Txn: 1, WaitForTxn: 2, KeyHash: 3, Key: []byte("a"), ResourceGroupTag: resourcegrouptag.EncodeResourceGroupTag(digest1, nil)},
		{Txn: 4, WaitForTxn: 5, KeyHash: 6, Key: []byte("b"), ResourceGroupTag: resourcegrouptag.EncodeResourceGroupTag(digest2, nil)},
	})
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testDataLockWaitSuite) TestDataLockWait(c *C) {
	_, digest1 := parser.NormalizeDigest("select * from t1 for update;")
	_, digest2 := parser.NormalizeDigest("update t1 set f1=1 where id=2;")
	keyHex1 := hex.EncodeToString([]byte("a"))
	keyHex2 := hex.EncodeToString([]byte("b"))
	tk := s.newTestKitWithRoot(c)
	tk.MustQuery("select * from information_schema.DATA_LOCK_WAITS;").
		Check(testkit.Rows(keyHex1+" 1 2 "+digest1.String(), keyHex2+" 4 5 "+digest2.String()))
}

func (s *testDataLockWaitSuite) TestDataLockPrivilege(c *C) {
	tk := s.newTestKitWithRoot(c)
	tk.MustExec("create user 'testuser'@'localhost'")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{
		Username: "testuser",
		Hostname: "localhost",
	}, nil, nil), IsTrue)
	err := tk.QueryToErr("select * from information_schema.DATA_LOCK_WAITS")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	tk = s.newTestKitWithRoot(c)
	tk.MustExec("create user 'testuser2'@'localhost'")
	tk.MustExec("grant process on *.* to 'testuser2'@'localhost'")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{
		Username: "testuser2",
		Hostname: "localhost",
	}, nil, nil), IsTrue)
	_ = tk.MustQuery("select * from information_schema.DATA_LOCK_WAITS")
}
