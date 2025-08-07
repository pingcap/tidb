// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package checksum_test

import (
	"context"
	"math"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func getTableInfo(t *testing.T, mock *mock.Cluster, db, table string) *model.TableInfo {
	info, err := mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	cDBName := pmodel.NewCIStr(db)
	cTableName := pmodel.NewCIStr(table)
	tableInfo, err := info.TableByName(context.Background(), cDBName, cTableName)
	require.NoError(t, err)
	return tableInfo.Meta()
}

func TestChecksumContextDone(t *testing.T) {
	mock, err := mock.NewCluster()
	require.NoError(t, err)
	require.NoError(t, mock.Start())
	defer mock.Stop()

	tk := testkit.NewTestKit(t, mock.Storage)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int, key i1(a, b), primary key (a));")
	tk.MustExec("insert into t1 values (10, 10);")
	tableInfo1 := getTableInfo(t, mock, "test", "t1")
	exe, err := checksum.NewExecutorBuilder(tableInfo1, math.MaxUint64).
		SetConcurrency(variable.DefChecksumTableConcurrency).
		Build()
	require.NoError(t, err)

	ctx := context.Background()
	cctx, cancel := context.WithCancel(ctx)

	cancel()

	resp, err := exe.Execute(cctx, mock.Storage.GetClient(), func() { t.Log("request done") })
	t.Log(err)
	t.Log(resp)
	require.Error(t, err)
}

func TestChecksum(t *testing.T) {
	mock, err := mock.NewCluster()
	require.NoError(t, err)
	require.NoError(t, mock.Start())
	defer mock.Stop()

	tk := testkit.NewTestKit(t, mock.Storage)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("insert into t1 values (10);")
	tableInfo1 := getTableInfo(t, mock, "test", "t1")
	exe1, err := checksum.NewExecutorBuilder(tableInfo1, math.MaxUint64).
		SetConcurrency(variable.DefChecksumTableConcurrency).
		Build()
	require.NoError(t, err)
	require.NoError(t, exe1.Each(func(r *kv.Request) error {
		require.True(t, r.NotFillCache)
		require.Equal(t, variable.DefChecksumTableConcurrency, r.Concurrency)
		return nil
	}))
	require.Equal(t, 1, exe1.Len())
	resp, err := exe1.Execute(context.TODO(), mock.Storage.GetClient(), func() {})
	require.NoError(t, err)
	// Cluster returns a dummy checksum (all fields are 1).
	require.Equalf(t, uint64(1), resp.Checksum, "%v", resp)
	require.Equalf(t, uint64(1), resp.TotalKvs, "%v", resp)
	require.Equalf(t, uint64(1), resp.TotalBytes, "%v", resp)

	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int);")
	tk.MustExec("alter table t2 add index i2(a);")
	tk.MustExec("insert into t2 values (10);")
	tableInfo2 := getTableInfo(t, mock, "test", "t2")
	exe2, err := checksum.NewExecutorBuilder(tableInfo2, math.MaxUint64).Build()
	require.NoError(t, err)
	require.Equalf(t, 2, exe2.Len(), "%v", tableInfo2)
	resp2, err := exe2.Execute(context.TODO(), mock.Storage.GetClient(), func() {})
	require.NoError(t, err)
	require.Equalf(t, uint64(0), resp2.Checksum, "%v", resp2)
	require.Equalf(t, uint64(2), resp2.TotalKvs, "%v", resp2)
	require.Equalf(t, uint64(2), resp2.TotalBytes, "%v", resp2)

	// Test rewrite rules
	tk.MustExec("alter table t1 add index i2(a);")
	tableInfo1 = getTableInfo(t, mock, "test", "t1")
	oldTable := metautil.Table{Info: tableInfo1}
	exe2, err = checksum.NewExecutorBuilder(tableInfo2, math.MaxUint64).
		SetOldTable(&oldTable).Build()
	require.NoError(t, err)
	require.Equal(t, 2, exe2.Len())
	rawReqs, err := exe2.RawRequests()
	require.NoError(t, err)
	require.Len(t, rawReqs, 2)
	for _, rawReq := range rawReqs {
		require.NotNil(t, rawReq.Rule)
	}
	resp2, err = exe2.Execute(context.TODO(), mock.Storage.GetClient(), func() {})
	require.NoError(t, err)
	require.NotNil(t, resp2)

	// Test commonHandle ranges

	tk.MustExec("drop table if exists t3;")
	tk.MustExec("create table t3 (a char(255), b int, primary key(a) CLUSTERED);")
	tk.MustExec("insert into t3 values ('fffffffff', 1), ('010101010', 2), ('394393fj39efefe', 3);")
	tableInfo3 := getTableInfo(t, mock, "test", "t3")
	exe3, err := checksum.NewExecutorBuilder(tableInfo3, math.MaxUint64).Build()
	require.NoError(t, err)
	first := true
	require.NoError(t, exe3.Each(func(req *kv.Request) error {
		if first {
			first = false
			ranges, err := distsql.BuildTableRanges(tableInfo3)
			require.NoError(t, err)
			require.Equalf(t, ranges[:1], req.KeyRanges.FirstPartitionRange(), "%v", req.KeyRanges.FirstPartitionRange())
		}
		return nil
	}))

	exe4, err := checksum.NewExecutorBuilder(tableInfo3, math.MaxUint64).Build()
	require.NoError(t, err)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/checksum/checksumRetryErr", `1*return(true)`))
	failpoint.Disable("github.com/pingcap/tidb/br/pkg/checksum/checksumRetryErr")
	resp4, err := exe4.Execute(context.TODO(), mock.Storage.GetClient(), func() {})
	require.NoError(t, err)
	require.NotNil(t, resp4)
}

func TestAsd(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("create table s(a int, c int);")
	// dml
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("insert into s values (1, 1);")
	tk.MustExec("update t set a = 2;")
	tk.MustExec("update t join s on t.a = s.a set t.b = 2, s.c = 2;")
	tk.MustExec("update t join s on t.a = s.a set t.a = 2;")
	tk.MustExec("delete t, s from t join s on t.a = s.a where t.a = 1;")
	tk.MustExec("delete t from t join s on t.a = s.a where t.a = 1;")
	tk.MustExec("delete from t where t.a = 1;")
	tk.MustExec("replace into t values (1, 1)")
	// fk
	tk.MustExec("truncate table t; truncate table s")
	tk.MustExec("alter table t add index(a)")
	tk.MustExec("alter table s add index(a)")
	//tk.MustExec("alter table s add foreign key fk_1(a) references t(a) on update cascade;") // referredFKCascades
	//tk.MustExec("alter table s add foreign key fk_1(a) references t(a) on update set null;") // referredFKCascades
	//tk.MustExec("alter table s add foreign key fk_1(a) references t(a) on update RESTRICT;") // referredFKChecks
	//tk.MustExec("alter table s add foreign key fk_1(a) references t(a) on delete cascade;") //
	//tk.MustExec("alter table s add foreign key fk_1(a) references t(a) on delete set null;") //
	//tk.MustExec("alter table s add foreign key fk_1(a) references t(a) on delete RESTRICT;") //
	tk.MustExec("insert into t values (1, 1);insert into s values (1, 1);")
	//tk.MustExec("update t set a = 2;")
	tk.MustExec("delete from t where a = 2;")
	// select for update
	tk.MustExec("select * from t join s for update;")
	tk.MustExec("select t.a from t join s for update;")
	tk.MustExec("select t.a+1 from t join s for update;")
	tk.MustExec("table t for update;")
	// alter/drop db
	tk.MustExec("alter database test collate utf8mb4_bin;")
	tk.MustExec("create database ttt; drop database if exists ttt;")
	// create/alter/rename/truncate/drop table
	tk.MustExec("create table ttt(a int);")
	tk.MustExec("alter table ttt add column b int;")
	tk.MustExec("alter table ttt add index(a);")
	tk.MustExec("alter table ttt rename to ttt2;")
	tk.MustExec("rename table ttt2 to ttt;")
	tk.MustExec("truncate table ttt;")
	tk.MustExec("drop table if exists ttt;")
	// create/drop index
	tk.MustExec("create index idx_a on t(a);")
	tk.MustExec("drop index idx_a on t;")
	// load data import into
	err := tk.ExecToErr("LOAD DATA LOCAL INFILE 'x.csv' INTO TABLE t")
	require.Error(t, err)
	tk.MustExec("IMPORT INTO test.t FROM 's3://bucket'")
	// create/drop view
	tk.MustExec("create view v1 as select * from t;")
	tk.MustExec("drop view if exists v1;")
	// temporary table
	tk.MustExec("create temporary table tt(a int);")
	tk.MustExec("insert into tt values (1),(2);")
	tk.MustExec("delete from tt where a = 2;")
	tk.MustExec("update tt set a = 2;")
	tk.MustExec("replace into tt values (3);")
	tk.MustExec("select * from tt for update;")
	tk.MustExec("truncate table tt;")
	tk.MustExec("drop temporary table tt;")
	// alter database
	tk.MustExec("alter database test character set gbk;")
}
