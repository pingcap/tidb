// Copyright 2026 PingCAP, Inc.
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

package domain_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	core "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

func TestMaskingPolicyPersistAfterRestart(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	session.DisableStats4Test()
	originalDomain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	currentDomain := originalDomain
	defer func() {
		if currentDomain != nil {
			currentDomain.Close()
		}
	}()
	tk := testkit.NewTestKit(t, store)

	// Create table and masking policy
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key auto_increment, c char(120))")
	tk.MustExec("create masking policy simple_mask on t(c) as c enable")

	// Verify policy shows in show create table
	tk.MustQuery("show create table t").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `c` char(120) DEFAULT NULL /* MASKING POLICY `simple_mask` ENABLED */,\n" +
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Verify policy is in system table
	tk.MustQuery("select policy_name, db_name, table_name, column_name, status from mysql.tidb_masking_policy where policy_name = 'simple_mask'").
		Check(testkit.Rows("simple_mask test t c ENABLED"))

	// Close the original Domain and its cache, then bootstrap a new Domain on
	// the same store so persisted policy rows must be loaded again.
	tk.Session().Close()
	originalDomain.Close()
	currentDomain = nil
	restartedDomain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	currentDomain = restartedDomain
	require.NotSame(t, originalDomain, restartedDomain)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk2.MustQuery("show create table t").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `c` char(120) DEFAULT NULL /* MASKING POLICY `simple_mask` ENABLED */,\n" +
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Verify policy is still in system table
	tk2.MustQuery("select policy_name, db_name, table_name, column_name, status from mysql.tidb_masking_policy where policy_name = 'simple_mask'").
		Check(testkit.Rows("simple_mask test t c ENABLED"))
}

func TestMaskingPolicyColdSnapshot(t *testing.T) {
	for _, change := range []string{
		"alter table t_cold disable masking policy p",
		"alter table t_cold drop masking policy p",
		"truncate table t_cold",
	} {
		t.Run(change, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_cold(c varchar(20))")
			tk.MustExec("insert into t_cold values ('secret')")
			tk.MustExec("create masking policy p on t_cold(c) as mask_full(c)")
			ctx := context.Background()
			tbl, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr("t_cold"))
			require.NoError(t, err)
			snapshot, err := store.CurrentVersion(kv.GlobalTxnScope)
			require.NoError(t, err)
			tk.MustExec(change)
			latest := dom.InfoSchema()
			current, err := store.CurrentVersion(kv.GlobalTxnScope)
			require.NoError(t, err)
			// Evict historical schemas so the next read cannot reuse a warmed policy map.
			dom.InfoCache().Reset(16)
			dom.InfoCache().Insert(latest, current.Ver)
			historical, err := dom.GetSnapshotInfoSchema(snapshot.Ver)
			require.NoError(t, err)
			var intercepted atomic.Int32
			loadCtx := interceptor.WithRPCInterceptor(ctx, interceptor.NewRPCInterceptor("masking-policy-user-context", func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
				return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
					intercepted.Add(1)
					return next(target, req)
				}
			}))
			policy, ok, err := historical.MaskingPolicyByTableColumn(loadCtx, tbl.Meta().ID, tbl.Meta().Columns[0].ID)
			require.NoError(t, err)
			require.Zero(t, intercepted.Load(), "policy SQL must not inherit the user's RPC interceptor")
			require.True(t, ok)
			require.Equal(t, model.MaskingPolicyStatusEnable, policy.Status)
			require.Equal(t, tbl.Meta().ID, policy.TableID)
		})
	}
}

func TestMaskingPolicyReloadWarmsCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t_warm_policy(c varchar(20))")
	for _, tc := range []struct {
		sql         string
		hasPolicies bool
	}{
		{"", false},
		{"create masking policy p on test.t_warm_policy(c) as mask_full(c)", true},
		{"truncate table test.t_warm_policy", true},
		{"alter table test.t_warm_policy drop masking policy p", false},
	} {
		if tc.sql != "" {
			tk.MustExec(tc.sql)
		}
		require.NoError(t, dom.Reload())
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError", "return(true)")
		hasPolicies, err := dom.InfoSchema().HasMaskingPolicies(context.Background())
		require.NoError(t, err, "Reload must warm the cache before the first user query")
		require.Equal(t, tc.hasPolicies, hasPolicies)
		testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError")
	}
}

func TestMaskingPolicyPreloadBeforePublish(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t_preload_policy(c varchar(20))")
	tk.MustExec("create masking policy p on test.t_preload_policy(c) as mask_full(c)")
	ctx := context.Background()
	latest := dom.InfoSchema()
	tbl, err := latest.TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr("t_preload_policy"))
	require.NoError(t, err)
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	dbInfos := latest.AllSchemas()
	for i, db := range dbInfos {
		dbInfos[i] = db.Copy()
		dbInfos[i].TableName2ID = nil
		dbInfos[i].Deprecated.Tables, err = latest.SchemaTableInfos(ctx, db.Name)
		require.NoError(t, err)
	}

	for _, useV2 := range []bool{false, true} {
		name := "v1"
		if useV2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) {
			// A starting Domain has no published schema yet. Internal SQL must
			// use the candidate schema even while deriving and recording stats.
			startingDomain := domain.NewMockDomain()
			startingDomain.InfoCache().Reset(16)
			factory := func() (pools.Resource, error) {
				se, err := session.CreateSessionWithDomain(store, startingDomain)
				if err != nil {
					return nil, err
				}
				se.GetSessionVars().CommonGlobalLoaded = true
				se.GetSessionVars().InRestrictedSQL = true
				return se, nil
			}
			builder := infoschema.NewBuilder(dom, factory, infoschema.NewData(), useV2)
			require.NoError(t, builder.InitWithDBInfos(dbInfos, latest.AllPlacementPolicies(), latest.AllResourceGroups(), latest.SchemaMetaVersion()))
			candidate := builder.Build(version.Ver)
			require.Nil(t, startingDomain.InfoSchema())
			require.NoError(t, infoschema.PreloadMaskingPolicies(ctx, candidate))
			require.Nil(t, startingDomain.InfoSchema())
			// A successful preload must serve subsequent reads from the cache.
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError", "return(true)")
			policy, ok, err := candidate.MaskingPolicyByTableColumn(ctx, tbl.Meta().ID, tbl.Meta().Columns[0].ID)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, "p", policy.Name.O)
		})
	}
}

func TestMaskingPolicyPreloadFailureRetry(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/skipLoadMaskingPolicies", "return(true)")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t_retry_policy(c varchar(20))")
	tk.MustExec("create masking policy p on test.t_retry_policy(c) as mask_full(c)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError", "return(true)")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/domain/skipLoadMaskingPolicies")

	// Publishing the schema must still renew validation and synchronize versions.
	require.NoError(t, dom.Reload())
	_, err := dom.InfoSchema().HasMaskingPolicies(context.Background())
	require.ErrorContains(t, err, "mock masking policy load error")
	require.ErrorContains(t, tk.ExecToErr("select c from test.t_retry_policy"), "mock masking policy load error")

	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError")
	require.NoError(t, dom.Reload())
	hasPolicies, err := dom.InfoSchema().HasMaskingPolicies(context.Background())
	require.NoError(t, err)
	require.True(t, hasPolicies)
}

func TestMaskingPolicyLatestSnapshot(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t_latest_policy(c varchar(20))")
	tk.MustExec("create masking policy p on test.t_latest_policy(c) as mask_full(c)")
	ctx := context.Background()
	is, err := dom.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	tbl, err := is.TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr("t_latest_policy"))
	require.NoError(t, err)
	policy, ok, err := is.MaskingPolicyByTableColumn(ctx, tbl.Meta().ID, tbl.Meta().Columns[0].ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, model.MaskingPolicyStatusEnable, policy.Status)
}

func TestMaskingPolicyPreloadTimeoutDoesNotExpireSchema(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/skipLoadMaskingPolicies", "return(true)")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t_preload_timeout(c varchar(20))")
	tk.MustExec("create table test.t_unrelated(id int)")
	tk.MustExec("create masking policy p on test.t_preload_timeout(c) as mask_full(c)")
	tk.MustExec("begin")
	tk.MustExec("insert into test.t_unrelated values (1)")
	var calls atomic.Int32
	release := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/infoschema/beforeLoadMaskingPolicies", func(ctx context.Context) {
		calls.Add(1)
		select {
		case <-ctx.Done():
		case <-release:
		}
	})
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/domain/skipLoadMaskingPolicies")
	// Force a new full schema, which used to preload twice before renewal.
	dom.InfoCache().Reset(16)
	done := make(chan error, 1)
	go func() { done <- dom.Reload() }()
	select {
	case err := <-done:
		close(release)
		require.NoError(t, err)
	case <-time.After(2 * dom.GetSchemaLease()):
		close(release)
		<-done
		t.Fatal("policy preloading blocked schema renewal for more than two leases")
	}
	require.GreaterOrEqual(t, calls.Load(), int32(1), "the timeout path must be exercised")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/infoschema/beforeLoadMaskingPolicies")
	require.False(t, dom.IsLeaseExpired())
	tk.MustExec("commit")
	tk.MustQuery("select count(*) from test.t_unrelated").Check(testkit.Rows("1"))
}

func newColdMaskingInfoSchema(t *testing.T, store kv.Storage, dom *domain.Domain, factory func() (pools.Resource, error)) infoschema.InfoSchema {
	t.Helper()
	ctx := context.Background()
	latest := dom.InfoSchema()
	dbs := latest.AllSchemas()
	for i, db := range dbs {
		dbs[i] = db.Copy()
		dbs[i].TableName2ID = nil
		var err error
		dbs[i].Deprecated.Tables, err = latest.SchemaTableInfos(ctx, db.Name)
		require.NoError(t, err)
	}
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	b := infoschema.NewBuilder(dom, factory, infoschema.NewData(), false)
	require.NoError(t, b.InitWithDBInfos(dbs, latest.AllPlacementPolicies(), latest.AllResourceGroups(), latest.SchemaMetaVersion()))
	cold := b.Build(version.Ver)
	require.False(t, infoschema.MaskingPoliciesLoaded(cold))
	return cold
}

func TestMaskingPolicyColdPointGetCancellation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_cold_point(id int primary key,c varchar(20))")
	tk.MustExec("create masking policy p on t_cold_point(c) as mask_full(c)")
	var loads atomic.Int32
	cold := newColdMaskingInfoSchema(t, store, dom, func() (pools.Resource, error) {
		loads.Add(1)
		return nil, errors.New("unexpected fast-plan policy SQL")
	})
	vars := tk.Session().GetSessionVars()
	vars.SnapshotInfoschema = cold
	defer func() { vars.SnapshotInfoschema = nil }()
	for _, sql := range []string{"select c from t_cold_point where id=1", "select c from t_cold_point where id in (1,2)"} {
		stmts, err := session.Parse(tk.Session(), sql)
		require.NoError(t, err)
		node := resolve.NewNodeW(stmts[0])
		ret := &core.PreprocessorReturn{}
		require.NoError(t, core.Preprocess(context.Background(), tk.Session(), node, core.WithPreprocessorReturn(ret)))
		require.Nil(t, core.TryFastPlan(tk.Session().GetPlanCtx(), node), sql)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = cold.HasMaskingPolicies(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Zero(t, loads.Load(), "neither the fast path nor a cancelled planner may start policy SQL")
}

func TestMaskingPolicyTemporaryShowColdCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp_mask_show(c varchar(20))")
	tbl, err := tk.Session().GetInfoSchema().(infoschema.InfoSchema).TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("tmp_mask_show"))
	require.NoError(t, err)
	var loads atomic.Int32
	cold := newColdMaskingInfoSchema(t, store, dom, func() (pools.Resource, error) { loads.Add(1); return nil, errors.New("policy table unavailable") })
	vars := tk.Session().GetSessionVars()
	vars.SnapshotInfoschema = cold
	defer func() { vars.SnapshotInfoschema = nil }()
	var buf bytes.Buffer
	require.NoError(t, executor.ConstructResultOfShowCreateTable(tk.Session(), tbl.Meta(), tbl.Allocators(tk.Session().GetTableCtx()), &buf))
	require.Contains(t, buf.String(), "CREATE TEMPORARY TABLE")
	require.Zero(t, loads.Load())
}

func TestMaskingPolicyUnverifiedSnapshotKeepsPolicyTS(t *testing.T) {
	// Keep the newest cached schema stale, as on a node that has not yet seen
	// a policy DDL committed by another node.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/disableOnTickReload", "return(true)")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`insert into mysql.tidb values ('tikv_gc_safe_point', '20160102-15:04:05 -0700', '')
		on duplicate key update variable_value = '20160102-15:04:05 -0700'`)
	tk.MustExec("use test")
	tk.MustExec("create table t_unverified(c varchar(20))")
	tk.MustExec("insert into t_unverified values ('secret')")
	tk.MustExec("create masking policy p on t_unverified(c) as mask_full(c)")
	ctx := context.Background()
	latest := dom.InfoSchema()
	dbs := latest.AllSchemas()
	for i, db := range dbs {
		dbs[i] = db.Copy()
		dbs[i].TableName2ID = nil
		var err error
		dbs[i].Deprecated.Tables, err = latest.SchemaTableInfos(ctx, db.Name)
		require.NoError(t, err)
	}
	t0, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)

	// Another node drops the policy. This node still has the schema of t0 as
	// its newest cached schema, with a cold policy cache.
	tk.MustExec("alter table t_unverified drop masking policy p")
	factory := func() (pools.Resource, error) {
		se, err := session.CreateSessionWithDomain(store, dom)
		if err != nil {
			return nil, err
		}
		se.GetSessionVars().CommonGlobalLoaded = true
		se.GetSessionVars().InRestrictedSQL = true
		return se, nil
	}
	b := infoschema.NewBuilder(dom, factory, infoschema.NewData(), false)
	require.NoError(t, b.InitWithDBInfos(dbs, latest.AllPlacementPolicies(), latest.AllResourceGroups(), latest.SchemaMetaVersion()))
	stale := b.Build(t0.Ver)
	dom.InfoCache().Reset(16)
	dom.InfoCache().Insert(stale, t0.Ver)

	// A historical session holds the stale schema.
	historical := testkit.NewTestKit(t, store)
	historical.MustExec("use test")
	historical.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%d'", t0.Ver))
	require.False(t, infoschema.MaskingPoliciesLoaded(stale))

	// A later snapshot lookup hits the stale schema, which is not known to be
	// valid at that time.
	t1, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	is, err := dom.GetSnapshotInfoSchema(t1.Ver)
	require.NoError(t, err)
	require.Same(t, stale, is)
	require.False(t, infoschema.MaskingPoliciesLoaded(stale))

	// The policy was enabled at t0, so the historical read stays masked.
	historical.MustQuery("select c from t_unverified").Check(testkit.Rows("XXXXXX"))
}

func TestHistoricalReadLaggingSchemaCache(t *testing.T) {
	// Keep the newest cached schema stale, as on a node that has not yet
	// loaded the schema versions committed by another node.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/disableOnTickReload", "return(true)")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`insert into mysql.tidb values ('tikv_gc_safe_point', '20160102-15:04:05 -0700', '')
		on duplicate key update variable_value = '20160102-15:04:05 -0700'`)
	tk.MustExec("use test")
	tk.MustExec("create table t_old(c varchar(20))")
	ctx := context.Background()
	latest := dom.InfoSchema()
	dbs := latest.AllSchemas()
	for i, db := range dbs {
		dbs[i] = db.Copy()
		dbs[i].TableName2ID = nil
		var err error
		dbs[i].Deprecated.Tables, err = latest.SchemaTableInfos(ctx, db.Name)
		require.NoError(t, err)
	}
	t0, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)

	// Another node creates the tables and the policy.
	tk.MustExec("create table t_new(c varchar(20))")
	tk.MustExec("create table t_mask(c varchar(20))")
	tk.MustExec("insert into t_mask values ('secret')")
	tk.MustExec("create masking policy p on t_mask(c) as mask_full(c)")
	b := infoschema.NewBuilder(dom, nil, infoschema.NewData(), false)
	require.NoError(t, b.InitWithDBInfos(dbs, latest.AllPlacementPolicies(), latest.AllResourceGroups(), latest.SchemaMetaVersion()))
	stale := b.Build(t0.Ver)
	dom.InfoCache().Reset(16)
	dom.InfoCache().Insert(stale, t0.Ver)
	// Let tidb_read_staleness pick a time after the tables were created.
	time.Sleep(1500 * time.Millisecond)
	t1, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)

	// Explicit historical reads at t1 see the tables and the policy at t1.
	historical := testkit.NewTestKit(t, store)
	historical.MustExec("use test")
	tablesSQL := "select count(*) from information_schema.tables%s where table_schema = 'test' and table_name = 't_new'"
	check := func(asOf string) {
		historical.MustQuery("select count(*) from t_new" + asOf).Check(testkit.Rows("0"))
		historical.MustQuery("select c from t_mask" + asOf).Check(testkit.Rows("XXXXXX"))
		// MustQuery runs a query on information_schema twice. Run it once: in
		// a historical transaction, the first read of a memory table ends the
		// transaction (see MemTableReaderExec.Open).
		historical.MustQueryWithContext(ctx, fmt.Sprintf(tablesSQL, asOf)).Check(testkit.Rows("1"))
	}
	historical.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%d'", t1.Ver))
	check("")
	historical.MustExec("set @@tidb_snapshot = ''")
	// tidb_snapshot set inside a transaction.
	historical.MustExec("begin")
	historical.MustQuery("select count(*) from t_old").Check(testkit.Rows("0"))
	historical.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%d'", t1.Ver))
	check("")
	historical.MustExec("set @@tidb_snapshot = ''")
	historical.MustExec("rollback")
	check(fmt.Sprintf(" as of timestamp tidb_parse_tso(%d)", t1.Ver))
	historical.MustExec(fmt.Sprintf("start transaction read only as of timestamp tidb_parse_tso(%d)", t1.Ver))
	check("")
	historical.MustExec("commit")
	historical.MustExec("set @@tidb_read_staleness = -1")
	check("")
	historical.MustExec("set @@tidb_read_staleness = 0")
	// A current read keeps the schema this node has published.
	historical.MustQuery(fmt.Sprintf(tablesSQL, "")).Check(testkit.Rows("0"))

	// A historical read at t0 still gets the cached schema of t0.
	is, err := dom.GetHistoricalInfoSchema(t0.Ver)
	require.NoError(t, err)
	require.Same(t, stale, is)

	// The reads did not publish the schema of t1 on this node.
	require.Same(t, stale, dom.InfoSchema())
	require.Same(t, stale, dom.InfoCache().GetLatest())
	require.Equal(t, stale.SchemaMetaVersion(), dom.InfoSchema().SchemaMetaVersion())
	// Internal lookups keep using the newest cached schema for a later time.
	is, err = dom.GetSnapshotInfoSchema(t1.Ver)
	require.NoError(t, err)
	require.Same(t, stale, is)
}
