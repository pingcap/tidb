package expression_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/server/client"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testServerSuite{})

type testServerSuite struct {
	testIntegrationSuiteBase

	port       uint
	statusPort uint

	tidbdrv *server.TiDBDriver
	tidbsrv *server.Server
	tk      *testkit.TestKit
	client  client.Client
}

// SetUpSuite create a mocked server mocked store
func (s *testServerSuite) SetUpSuite(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
	s.tidbdrv = server.NewTiDBDriver(s.store)
	globalConfig := config.GetGlobalConfig()
	s.port = globalConfig.Port
	s.statusPort = globalConfig.Status.StatusPort

	cfg := config.NewConfig()
	cfg.Port = s.port
	cfg.Store = "tikv"
	cfg.Status.StatusPort = s.statusPort
	cfg.Status.ReportStatus = true

	srv, err := server.NewServer(cfg, s.tidbdrv)
	c.Assert(err, IsNil)
	go srv.Run()

	s.client = client.NewFromGlobalConfig()
	err = s.client.PollServerOnline()
	c.Assert(err, IsNil)

	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("create database tidb;")
}

// TearDownSuite will close mocked store and server
func (s *testServerSuite) TearDownSuite(c *C) {
	if s.dom != nil {
		s.dom.Close()
	}
	if s.store != nil {
		_ = s.store.Close()
	}
	if s.tidbsrv != nil {
		s.tidbsrv.Close()
	}
}

func (s *testServerSuite) prepare(c *C) {
	s.tk.MustExec("use tidb;")
}

// TearDownTest will drop all table created by testing
func (s *testServerSuite) clean(c *C) {
	tk := s.tk
	tk.MustExec("use tidb")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testServerSuite) TestMVCCGetInfo(c *C) {
	s.prepare(c)
	defer s.clean(c)

	tk := s.tk

	// prepare test table
	tk.MustExec("create table tidb.test (a int auto_increment primary key, b varchar(20));")
	tk.MustExec("insert tidb.test values (1, 1);")

	// prepare test table with txn
	tk.MustExec(`
begin;
update tidb.test set b = b + 1 where a = 1;
insert tidb.test values (2, 2);
insert tidb.test (a) values (3);
insert tidb.test values (4, '');
commit;
`)

	// prepare test table with index
	tk.MustExec("alter table tidb.test add index idx1 (a, b);")
	tk.MustExec("alter table tidb.test add unique index idx2 (a, b);")

	// load fixture to initialize test cases
	fixture, err := s.getMVCCInfo("/mvcc/key/tidb/test/1")
	c.Check(err, IsNil)
	var startTs uint64
	if c.Check(len(fixture.Value.Info.Values), Greater, 0) {
		startTs = fixture.Value.Info.Values[0].StartTs
	}

	tests := []struct {
		Query   string
		Builtin string
	}{
		{fmt.Sprintf("/mvcc/hex/%s", fixture.Key), fmt.Sprintf("get_mvcc_info('hex', '%s')", fixture.Key)},
		{"/mvcc/key/tidb/test/1", "get_mvcc_info('handle', 'tidb', 'test', 1)"},
		{fmt.Sprintf("/mvcc/txn/%d/tidb/test", startTs), fmt.Sprintf("get_mvcc_info('startTs', 'tidb', 'test', %d)", startTs)},
		{"/mvcc/index/tidb/test/idx1/1", "get_mvcc_info('index', 'tidb', 'test', 1, 'idx1')"},
		{"/mvcc/index/tidb/test/idx1/1?a=1&b=2", "get_mvcc_info('index', 'tidb', 'test', 1, 'idx1', 'a=1', 'b=2')"},
		{"/mvcc/index/tidb/test/idx2/1?a=1&b=2", "get_mvcc_info('index', 'tidb', 'test', 1, 'idx2', 'a=1', 'b=2')"},
		{"/mvcc/index/tidb/test/idx1/3?a=3&b", "get_mvcc_info('index', 'tidb', 'test', 3, 'idx1', 'a=3', 'b')"},
		{"/mvcc/index/tidb/test/idx2/3?a=3&b", "get_mvcc_info('index', 'tidb', 'test', 3, 'idx2', 'a=3', 'b')"},
		{"/mvcc/index/tidb/test/idx1/4?a=4&b=", "get_mvcc_info('index', 'tidb', 'test', 4, 'idx1', 'a=4', 'b=')"},
		{"/mvcc/index/tidb/test/idx2/3?a=4&b=", "get_mvcc_info('index', 'tidb', 'test', 3, 'idx2', 'a=4', 'b=')"},
		{"/mvcc/index/tidb/test/idx1/5?a=5&b=1", "get_mvcc_info('index', 'tidb', 'test', 5, 'idx1', 'a=5', 'b=1')"},
		{"/mvcc/index/tidb/test/idx2/5?a=5&b=1", "get_mvcc_info('index', 'tidb', 'test', 5, 'idx2', 'a=5', 'b=1')"},
		{"/mvcc/index/tidb/test/idx1/1?a=1", "get_mvcc_info('index', 'tidb', 'test', 1, 'idx1', 'a=1')"},
		{"/mvcc/index/tidb/test/idx2/1?a=1", "get_mvcc_info('index', 'tidb', 'test', 1, 'idx2', 'a=1')"},
	}

	for _, tt := range tests {
		resp, err := s.client.Get(tt.Query)
		c.Check(err, IsNil)
		respBody, err := ioutil.ReadAll(resp.Body)
		c.Check(err, IsNil)
		s.tk.MustQuery(fmt.Sprintf("select %s;", tt.Builtin)).Check(testkit.Rows(string(respBody)))
	}
}

func (s *testServerSuite) TestMVCCGetInfoPartitionTable(c *C) {
	s.prepare(c)
	defer s.clean(c)

	tk := s.tk

	// prepare test table with partition
	tk.MustExec(`create table tidb.pt (a int primary key, b varchar(20), key idx(a, b))
partition by range (a)
(partition p0 values less than (256),
partition p1 values less than (512),
partition p2 values less than (1024))`)
	tk.MustExec("insert tidb.pt(p2) values (1000, 'a');")

	// prepare test table with partition and txn
	tk.MustExec(`
begin;
insert into tidb.pt values (42, '123');
insert into tidb.pt values (256, 'b');
insert into tidb.pt values (666, 'def');
commit;
`)

	// load fixture to initialize test cases
	fixture, err := s.getMVCCInfo("/mvcc/key/tidb/pt(p2)/1")
	c.Check(err, IsNil)
	var startTs uint64
	if c.Check(len(fixture.Value.Info.Values), Greater, 0) {
		startTs = fixture.Value.Info.Values[0].StartTs
	}

	tests := []struct {
		Query   string
		Builtin string
	}{
		{fmt.Sprintf("/mvcc/hex/%s", fixture.Key), fmt.Sprintf("get_mvcc_info('hex', '%s')", fixture.Key)},
		{"/mvcc/key/tidb/pt(p2)/1", "get_mvcc_info('handle', 'tidb', 'pt(p2)', 1)"},
		{fmt.Sprintf("/mvcc/txn/%d/tidb/pt(p2)", startTs), fmt.Sprintf("get_mvcc_info('startTs', 'tidb', 'pt(p2)', %d)", startTs)},
		{"/mvcc/index/tidb/pt(p2)/idx/1", "get_mvcc_info('index', 'tidb', 'pt(p2)', 1, 'idx')"},
		{"/mvcc/index/tidb/pt(p2)/idx/1?a=666&b=def", "get_mvcc_info('index', 'tidb', 'pt(p2)', 1, 'idx', 'a=666', 'b=def')"},
		{"/mvcc/index/tidb/pt(p2)/idx/1?a=666&b", "get_mvcc_info('index', 'tidb', 'pt(p2)', 1, 'idx', 'a=666', 'b')"},
		{"/mvcc/index/tidb/pt(p2)/idx/1?a=666&b=", "get_mvcc_info('index', 'tidb', 'pt(p2)', 1, 'idx', 'a=666', 'b=')"},
		{"/mvcc/index/tidb/pt(p2)/idx/1?a=1", "get_mvcc_info('index', 'tidb', 'pt(p2)', 1, 'idx', 'a=666')"},
	}

	for _, tt := range tests {
		resp, err := s.client.Get(tt.Query)
		c.Check(err, IsNil)
		respBody, err := ioutil.ReadAll(resp.Body)
		c.Check(err, IsNil)
		s.tk.MustQuery(fmt.Sprintf("select %s;", tt.Builtin)).Check(testkit.Rows(string(respBody)))
	}
}

type mvccInfo struct {
	Key      string                        `json:"key"`
	RegionID uint64                        `json:"region_id"`
	Value    *kvrpcpb.MvccGetByKeyResponse `json:"value"`
}

func (s *testServerSuite) getMVCCInfo(path string) (*mvccInfo, error) {
	// retrieveMVCC information from http api
	resp, err := s.client.Get(path)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	mvcc := &mvccInfo{}
	if err := json.Unmarshal(body, &mvcc); err != nil {
		return nil, err
	}

	// check rpc error
	if mvcc.Value.Error != "" {
		return nil, fmt.Errorf(mvcc.Value.Error)
	}
	if mvcc.Value.RegionError != nil {
		return nil, fmt.Errorf(mvcc.Value.RegionError.Message)
	}
	return mvcc, nil
}
