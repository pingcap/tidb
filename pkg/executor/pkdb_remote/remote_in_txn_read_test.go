// Copyright 2025 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0

//go:build intest

package pkdbremote_test

import (
	"context"
	"sync"
	"testing"

	pkdb_remote "github.com/pingcap/tidb/pkg/executor/pkdb_remote"
	"github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlmock "github.com/pingcap/tidb/pkg/util/topsql/collector/mock"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/stretchr/testify/require"
)

type mockLocationResolver struct {
	local  string
	remote string
}

func (m *mockLocationResolver) ResolveTableLocation(int64) string { return m.remote }
func (m *mockLocationResolver) ResolvePartitionLocation(int64, int64) string {
	return m.remote
}
func (m *mockLocationResolver) ResolveKeyLocation(int64, int64, []byte) string { return m.remote }
func (m *mockLocationResolver) IsLocalStore(storeAddr string) bool {
	return storeAddr == m.local || storeAddr == ""
}
func (m *mockLocationResolver) GetLocalStoreAddr() string { return m.local }

type mockClient struct {
	mu         sync.Mutex
	calls      int
	lastInTxn  bool
	lastStartT uint64
}

func (c *mockClient) Execute(_ context.Context, req *pb.RemoteRequest, _ sessionctx.Context) (sqlexec.RecordSet, error) {
	c.mu.Lock()
	c.calls++
	if sess := req.GetSession(); sess != nil && sess.Txn != nil {
		c.lastInTxn = sess.Txn.InTxn
		c.lastStartT = sess.Txn.StartTs
	}
	c.mu.Unlock()

	col := &model.ColumnInfo{
		Name:      pmodel.NewCIStr("sum(a)"),
		State:     model.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}
	return &singleInt64RecordSet{
		field: &resolve.ResultField{Column: col},
		val:   1,
	}, nil
}

func (c *mockClient) Close() error { return nil }

type singleInt64RecordSet struct {
	field *resolve.ResultField
	val   int64
	done  bool
}

func (r *singleInt64RecordSet) Fields() []*resolve.ResultField {
	return []*resolve.ResultField{r.field}
}

func (r *singleInt64RecordSet) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if r.done {
		return nil
	}
	req.AppendInt64(0, r.val)
	r.done = true
	return nil
}

func (r *singleInt64RecordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	fields := []*types.FieldType{&r.field.Column.FieldType}
	if alloc != nil {
		return alloc.Alloc(fields, 0, 1024)
	}
	return chunk.New(fields, 1024, 1024)
}

func (r *singleInt64RecordSet) Close() error { return nil }

func TestRemotePlanInTxnReadSwitchOff(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetCommandValue(mysql.ComQuery)

	prevResolver := plannercore.GetLocationResolver()
	plannercore.SetLocationResolver(&mockLocationResolver{local: "local", remote: "remote"})
	t.Cleanup(func() { plannercore.SetLocationResolver(prevResolver) })

	mock := &mockClient{}
	t.Cleanup(pkdb_remote.SetDefaultClientForTest(mock))

	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("analyze table t")

	tk.MustExec("set tidbx_remote_plan_enable = ON")
	tk.MustExec("set tidbx_remote_plan_enable_in_txn_read = OFF")

	tk.MustExec("begin")
	tk.MustExec("prepare stmt from 'select sum(a) from t where b = ?'")
	tk.MustExec("set @b = 1")
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("1"))
	tk.MustExec("commit")

	mock.mu.Lock()
	calls := mock.calls
	mock.mu.Unlock()
	require.Equal(t, 0, calls)
}

func TestRemotePlanInTxnReadCleanTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetCommandValue(mysql.ComQuery)

	prevResolver := plannercore.GetLocationResolver()
	plannercore.SetLocationResolver(&mockLocationResolver{local: "local", remote: "remote"})
	t.Cleanup(func() { plannercore.SetLocationResolver(prevResolver) })

	mock := &mockClient{}
	t.Cleanup(pkdb_remote.SetDefaultClientForTest(mock))

	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("analyze table t")

	tk.MustExec("set tidbx_remote_plan_enable = ON")
	tk.MustExec("set tidbx_remote_plan_enable_in_txn_read = ON")

	tk.MustExec("begin")
	tk.MustExec("prepare stmt from 'select sum(a) from t where b = ?'")
	tk.MustExec("set @b = 1")
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("1"))
	tk.MustExec("commit")

	mock.mu.Lock()
	calls := mock.calls
	inTxn := mock.lastInTxn
	startTS := mock.lastStartT
	mock.mu.Unlock()
	require.Equal(t, 1, calls)
	require.True(t, inTxn)
	require.Greater(t, startTS, uint64(0))
}

func TestRemotePlanForwardedWithTopSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetCommandValue(mysql.ComQuery)

	prevResolver := plannercore.GetLocationResolver()
	plannercore.SetLocationResolver(&mockLocationResolver{local: "local", remote: "remote"})
	t.Cleanup(func() { plannercore.SetLocationResolver(prevResolver) })

	mock := &mockClient{}
	t.Cleanup(pkdb_remote.SetDefaultClientForTest(mock))

	topsqlstate.EnableTopSQL()
	collector := topsqlmock.NewTopSQLCollector()
	topsql.SetupTopSQLForTest(collector)
	t.Cleanup(topsqlstate.DisableTopSQL)

	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 1)")
	tk.MustExec("analyze table t")

	tk.MustExec("set tidbx_remote_plan_enable = ON")
	tk.MustExec("prepare stmt from 'select sum(a) from t where b = ?'")
	tk.MustExec("set @b = 1")
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("1"))

	mock.mu.Lock()
	calls := mock.calls
	mock.mu.Unlock()
	require.Equal(t, 1, calls)
}

func TestRemotePlanInTxnReadDirtyTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetCommandValue(mysql.ComQuery)

	prevResolver := plannercore.GetLocationResolver()
	plannercore.SetLocationResolver(&mockLocationResolver{local: "local", remote: "remote"})
	t.Cleanup(func() { plannercore.SetLocationResolver(prevResolver) })

	mock := &mockClient{}
	t.Cleanup(pkdb_remote.SetDefaultClientForTest(mock))

	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("analyze table t")

	tk.MustExec("set tidbx_remote_plan_enable = ON")
	tk.MustExec("set tidbx_remote_plan_enable_in_txn_read = ON")

	tk.MustExec("begin")
	tk.MustExec("prepare stmt from 'select sum(a) from t where b = ?'")
	tk.MustExec("insert into t values (10, 1)")
	tk.MustExec("set @b = 1")
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("11"))
	tk.MustExec("rollback")

	mock.mu.Lock()
	calls := mock.calls
	mock.mu.Unlock()
	require.Equal(t, 0, calls)
}

func TestRemotePlanSelectForUpdateNotForwarded(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetCommandValue(mysql.ComQuery)

	prevResolver := plannercore.GetLocationResolver()
	plannercore.SetLocationResolver(&mockLocationResolver{local: "local", remote: "remote"})
	t.Cleanup(func() { plannercore.SetLocationResolver(prevResolver) })

	mock := &mockClient{}
	t.Cleanup(pkdb_remote.SetDefaultClientForTest(mock))

	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("analyze table t")

	tk.MustExec("set tidbx_remote_plan_enable = ON")
	tk.MustExec("set @@tidb_txn_mode = 'pessimistic'")

	tk.MustExec("prepare stmt from 'select a from t where b = ? for update'")
	tk.MustExec("set @b = 1")
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("1"))

	mock.mu.Lock()
	calls := mock.calls
	mock.mu.Unlock()
	require.Equal(t, 0, calls)
}
