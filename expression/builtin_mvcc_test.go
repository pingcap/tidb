package expression_test

import (
	"fmt"
	"io/ioutil"

	"github.com/pingcap/check"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testServerSuite) TestMVCCGetInfo(c *C) {
	s.prepare(c)
	defer s.clean(c)

	tk := s.tk

	// prepare test table
	tk.MustExec("create table tidb.test (a int auto_increment primary key, b varchar(20));")
	tk.MustExec("insert tidb.test values (1, 1);")

	// with txn
	tk.MustExec(`
begin;
update tidb.test set b = b + 1 where a = 1;
insert tidb.test values (2, 2);
insert tidb.test (a) values (3);
insert tidb.test values (4, '');
commit;
`)

	// with index
	tk.MustExec("alter table tidb.test add index idx1 (a, b);")
	tk.MustExec("alter table tidb.test add unique index idx2 (a, b);")

	// load fixture to initialize test cases
	fixture, err := s.getMVCCInfo("/mvcc/key/tidb/test/1")
	c.Check(err, check.IsNil)
	var startTs uint64
	if c.Check(len(fixture.Value.Info.Values), check.Greater, 0) {
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
		resp, err := s.fetchStatus(tt.Query)
		c.Check(err, check.IsNil)
		respBody, err := ioutil.ReadAll(resp.Body)
		c.Check(err, check.IsNil)
		s.tk.MustQuery(fmt.Sprintf("select %s;", tt.Builtin)).Check(testkit.Rows(string(respBody)))
	}
}

func (s *testServerSuite) TestMVCCGetInfoPartitionTable(c *C) {
	s.prepare(c)
	defer s.clean(c)

	tk := s.tk

	// with partition
	tk.MustExec(`create table tidb.pt (a int primary key, b varchar(20), key idx(a, b))
partition by range (a)
(partition p0 values less than (256),
partition p1 values less than (512),
partition p2 values less than (1024))`)
	tk.MustExec("insert tidb.pt(p2) values (1000, 'a');")

	// with partition and txn
	tk.MustExec(`
begin;
insert into tidb.pt values (42, '123');
insert into tidb.pt values (256, 'b');
insert into tidb.pt values (666, 'def');
commit;
`)

	// load fixture to initialize test cases
	fixture, err := s.getMVCCInfo("/mvcc/key/tidb/pt(p2)/1")
	c.Check(err, check.IsNil)
	var startTs uint64
	if c.Check(len(fixture.Value.Info.Values), check.Greater, 0) {
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
		resp, err := s.fetchStatus(tt.Query)
		c.Check(err, check.IsNil)
		respBody, err := ioutil.ReadAll(resp.Body)
		c.Check(err, check.IsNil)
		s.tk.MustQuery(fmt.Sprintf("select %s;", tt.Builtin)).Check(testkit.Rows(string(respBody)))
	}
}
