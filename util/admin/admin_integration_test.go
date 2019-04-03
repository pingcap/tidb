package admin_test

import (
	"context"
	"fmt"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testAdminSuite{})

type testAdminSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
}

func (s *testAdminSuite) SetUpSuite(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)
	s.store = store
	session.SetSchemaLease(0)
	session.SetStatsLease(0)
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *testAdminSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *testAdminSuite) TestAdmin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("insert admin_test (c1) values (1),(2),(NULL)")

	ctx := context.Background()
	// cancel DDL jobs test
	r, err := tk.Exec("admin cancel ddl jobs 1")
	c.Assert(err, IsNil, Commentf("err %v", err))
	req := r.NewRecordBatch()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row := req.GetRow(0)
	c.Assert(row.Len(), Equals, 2)
	c.Assert(row.GetString(0), Equals, "1")
	c.Assert(row.GetString(1), Equals, "error: [admin:4]DDL Job:1 not found")

	// show ddl test;
	r, err = tk.Exec("admin show ddl")
	c.Assert(err, IsNil)
	req = r.NewRecordBatch()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 6)
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	ddlInfo, err := admin.GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(row.GetInt64(0), Equals, ddlInfo.SchemaVer)
	// TODO: Pass this test.
	// rowOwnerInfos := strings.Split(row.Data[1].GetString(), ",")
	// ownerInfos := strings.Split(ddlInfo.Owner.String(), ",")
	// c.Assert(rowOwnerInfos[0], Equals, ownerInfos[0])
	do := domain.GetDomain(tk.Se.(sessionctx.Context))
	serverInfo, err := do.InfoSyncer().GetServerInfoByID(ctx, row.GetString(1))
	c.Assert(err, IsNil)
	c.Assert(row.GetString(2), Equals, serverInfo.IP+":"+
		strconv.FormatUint(uint64(serverInfo.Port), 10))
	c.Assert(row.GetString(3), Equals, "")
	req = r.NewRecordBatch()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsTrue)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	// show DDL jobs test
	r, err = tk.Exec("admin show ddl jobs")
	c.Assert(err, IsNil)
	req = r.NewRecordBatch()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 10)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	historyJobs, err := admin.GetHistoryDDLJobs(txn, admin.DefNumHistoryJobs)
	c.Assert(len(historyJobs), Greater, 1)
	c.Assert(len(row.GetString(1)), Greater, 0)
	c.Assert(err, IsNil)
	c.Assert(row.GetInt64(0), Equals, historyJobs[0].ID)
	c.Assert(err, IsNil)

	r, err = tk.Exec("admin show ddl jobs 20")
	c.Assert(err, IsNil)
	req = r.NewRecordBatch()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 10)
	c.Assert(row.GetInt64(0), Equals, historyJobs[0].ID)
	c.Assert(err, IsNil)

	// show DDL job queries test
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test2")
	tk.MustExec("create table admin_test2 (c1 int, c2 int, c3 int default 1, index (c1))")
	result := tk.MustQuery(`admin show ddl job queries 1, 1, 1`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`admin show ddl job queries 1, 2, 3, 4`)
	result.Check(testkit.Rows())
	historyJob, err := admin.GetHistoryDDLJobs(txn, admin.DefNumHistoryJobs)
	result = tk.MustQuery(fmt.Sprintf("admin show ddl job queries %d", historyJob[0].ID))
	result.Check(testkit.Rows(historyJob[0].Query))
	c.Assert(err, IsNil)

	// check table test
	tk.MustExec("create table admin_test1 (c1 int, c2 int default 1, index (c1))")
	tk.MustExec("insert admin_test1 (c1) values (21),(22)")
	r, err = tk.Exec("admin check table admin_test, admin_test1")
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
	// error table name
	err = tk.ExecToErr("admin check table admin_test_error")
	c.Assert(err, NotNil)
	// different index values
	sctx := tk.Se.(sessionctx.Context)
	dom := domain.GetDomain(sctx)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("admin_test"))
	c.Assert(err, IsNil)
	c.Assert(tb.Indices(), HasLen, 1)
	_, err = tb.Indices()[0].Create(mock.NewContext(), txn, types.MakeDatums(int64(10)), 1)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	errAdmin := tk.ExecToErr("admin check table admin_test")
	c.Assert(errAdmin, NotNil)

	if config.CheckTableBeforeDrop {
		err = tk.ExecToErr("drop table admin_test")
		c.Assert(err.Error(), Equals, errAdmin.Error())

		// Drop inconsistency index.
		tk.MustExec("alter table admin_test drop index c1")
		tk.MustExec("admin check table admin_test")
	}
	// checksum table test
	tk.MustExec("create table checksum_with_index (id int, count int, PRIMARY KEY(id), KEY(count))")
	tk.MustExec("create table checksum_without_index (id int, count int, PRIMARY KEY(id))")
	r, err = tk.Exec("admin checksum table checksum_with_index, checksum_without_index")
	c.Assert(err, IsNil)
	res := tk.ResultSetToResult(r, Commentf("admin checksum table"))
	// Mocktikv returns 1 for every table/index scan, then we will xor the checksums of a table.
	// For "checksum_with_index", we have two checksums, so the result will be 1^1 = 0.
	// For "checksum_without_index", we only have one checksum, so the result will be 1.
	res.Sort().Check(testkit.Rows("test checksum_with_index 0 2 2", "test checksum_without_index 1 1 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned primary key, b int, c int, index idx(a, b));")
	tk.MustExec("insert into t values(1, 1, 1)")
	tk.MustExec("admin check table t")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (c2 BOOL, PRIMARY KEY (c2));")
	tk.MustExec("INSERT INTO t1 SET c2 = '0';")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c3 DATETIME NULL DEFAULT '2668-02-03 17:19:31';")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx2 (c3);")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c4 bit(10) default 127;")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx3 (c4);")
	tk.MustExec("admin check table t1;")

	// For add index on virtual column
	tk.MustExec("drop table if exists t1;")
	tk.MustExec(`create table t1 (
		a int             as (JSON_EXTRACT(k,'$.a')),
		c double          as (JSON_EXTRACT(k,'$.c')),
		d decimal(20,10)  as (JSON_EXTRACT(k,'$.d')),
		e char(10)        as (JSON_EXTRACT(k,'$.e')),
		f date            as (JSON_EXTRACT(k,'$.f')),
		g time            as (JSON_EXTRACT(k,'$.g')),
		h datetime        as (JSON_EXTRACT(k,'$.h')),
		i timestamp       as (JSON_EXTRACT(k,'$.i')),
		j year            as (JSON_EXTRACT(k,'$.j')),
		k json);`)

	tk.MustExec("insert into t1 set k='{\"a\": 100,\"c\":1.234,\"d\":1.2340000000,\"e\":\"abcdefg\",\"f\":\"2018-09-28\",\"g\":\"12:59:59\",\"h\":\"2018-09-28 12:59:59\",\"i\":\"2018-09-28 16:40:33\",\"j\":\"2018\"}';")
	tk.MustExec("alter table t1 add index idx_a(a);")
	tk.MustExec("alter table t1 add index idx_c(c);")
	tk.MustExec("alter table t1 add index idx_d(d);")
	tk.MustExec("alter table t1 add index idx_e(e);")
	tk.MustExec("alter table t1 add index idx_f(f);")
	tk.MustExec("alter table t1 add index idx_g(g);")
	tk.MustExec("alter table t1 add index idx_h(h);")
	tk.MustExec("alter table t1 add index idx_j(j);")
	tk.MustExec("alter table t1 add index idx_i(i);")
	tk.MustExec("alter table t1 add index idx_m(a,c,d,e,f,g,h,i,j);")
	tk.MustExec("admin check table t1;")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (c1 int);")
	tk.MustExec("INSERT INTO t1 SET c1 = 1;")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN cc1 CHAR(36)    NULL DEFAULT '';")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN cc2 VARCHAR(36) NULL DEFAULT ''")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx1 (cc1);")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx2 (cc2);")
	tk.MustExec("admin check table t1;")
}
