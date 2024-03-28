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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func getTableInfo(t *testing.T, mock *mock.Cluster, db, table string) *model.TableInfo {
	info, err := mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	cDBName := model.NewCIStr(db)
	cTableName := model.NewCIStr(table)
	tableInfo, err := info.TableByName(cDBName, cTableName)
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
