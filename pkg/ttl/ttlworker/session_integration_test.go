// Copyright 2024 PingCAP, Inc.
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

package ttlworker_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type fault interface {
	// shouldFault returns whether the session should fault this time.
	shouldFault() bool
}

var _ fault = &faultAfterCount{}

type faultAfterCount struct {
	faultCount int

	currentCount int
}

func (f *faultAfterCount) shouldFault() bool {
	if f.currentCount >= f.faultCount {
		return true
	}

	f.currentCount++
	return false
}

// sessionWithFault is a session which will fail to execute SQL after successfully executing several SQLs. It's designed
// to trigger every possible branch of returning error from `Execute`
type sessionWithFault struct {
	sessionctx.Context

	fault *atomic.Pointer[fault]
}

// Close implements pools.Resource
func (s *sessionWithFault) Close() {
	s.Context.(pools.Resource).Close()
}

// GetSQLExecutor implements sessionctx.Context.
func (s *sessionWithFault) GetSQLExecutor() sqlexec.SQLExecutor {
	return s
}

// Execute implements sqlexec.SQLExecutor.
func (s *sessionWithFault) Execute(ctx context.Context, sql string) ([]sqlexec.RecordSet, error) {
	if s.shouldFault(sql) {
		return nil, errors.New("fault in test")
	}
	return s.Context.GetSQLExecutor().Execute(ctx, sql)
}

// ExecuteStmt implements sqlexec.SQLExecutor.
func (s *sessionWithFault) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	if s.shouldFault(stmtNode.Text()) {
		return nil, errors.New("fault in test")
	}
	return s.Context.GetSQLExecutor().ExecuteStmt(ctx, stmtNode)
}

func (s *sessionWithFault) ExecuteInternal(ctx context.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	if s.shouldFault(sql) {
		return nil, errors.New("fault in test")
	}
	return s.Context.GetSQLExecutor().ExecuteInternal(ctx, sql, args...)
}

func (s *sessionWithFault) shouldFault(sql string) bool {
	if s.fault.Load() == nil {
		return false
	}

	// as a fault implementation may have side-effect, we should always call it before checking the SQL.
	shouldFault := (*s.fault.Load()).shouldFault()

	// skip some local only sql, ref `getSession()` in `session.go`
	if strings.HasPrefix(sql, "set tidb_") || strings.HasPrefix(sql, "set @@") {
		return false
	}

	return shouldFault
}

type faultSessionPool struct {
	util.SessionPool

	fault *atomic.Pointer[fault]
}

func newFaultSessionPool(sp util.SessionPool) *faultSessionPool {
	return &faultSessionPool{
		SessionPool: sp,
		fault:       &atomic.Pointer[fault]{},
	}
}

// Get implements util.SessionPool.
func (f *faultSessionPool) Get() (pools.Resource, error) {
	resource, err := f.SessionPool.Get()
	if err != nil {
		return nil, err
	}

	return &sessionWithFault{
		Context: resource.(sessionctx.Context),
		fault:   f.fault,
	}, nil
}

// Put implements util.SessionPool.
func (f *faultSessionPool) Put(se pools.Resource) {
	f.SessionPool.Put(se.(*sessionWithFault).Context.(pools.Resource))
}

func (f *faultSessionPool) setFault(ft fault) {
	f.fault.Store(&ft)
}

func TestGetSessionWithFault(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)

	pool := newFaultSessionPool(dom.SysSessionPool())

	for i := 0; i < 50; i++ {
		pool.setFault(&faultAfterCount{faultCount: i})
		se, err := ttlworker.GetSessionForTest(pool)
		logutil.BgLogger().Info("get session", zap.Int("error after count", i), zap.Bool("session is nil", se == nil), zap.Bool("error is nil", err == nil))
		require.True(t, se != nil || err != nil)
	}
}
<<<<<<< HEAD
=======

func TestNewScanSession(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)
	pool := newFaultSessionPool(t, dom.AdvancedSysSessionPool())
	pool.setFault(newFaultWithFilter(func(s string) bool { return false }, newFaultAfterCount(0)))
	var sysSe *syssession.Session
	pool.onSysSession = func(se *syssession.Session) {
		require.Nil(t, sysSe)
		sysSe = se
		se.InternalSctxForTest().GetSessionVars().SetDistSQLScanConcurrency(123)
		se.InternalSctxForTest().GetSessionVars().EnablePaging = true
	}

	for _, errSQL := range []string{
		"",
		"set @@tidb_distsql_scan_concurrency=1",
		"set @@tidb_enable_paging=OFF",
	} {
		t.Run("test err in SQL: "+errSQL, func(t *testing.T) {
			sysSe = nil
			pool.setFault(newFaultWithFilter(func(s string) bool {
				return s != "" && s == errSQL
			}, newFaultAfterCount(0)))

			called := false
			require.NoError(t, ttlworker.WithSessionForTest(pool, func(se session.Session) error {
				require.False(t, called)
				tblSe, restore, err := ttlworker.NewScanSession(context.Background(), se, &cache.PhysicalTable{}, time.Now())
				called = true
				if errSQL == "" {
					// success case
					require.NoError(t, err)
					require.NotNil(t, tblSe)
					require.NotNil(t, restore)
					require.Same(t, se, tblSe.Session)
					// NewScanSession should override @@dist_sql_scan_concurrency and @@tidb_enable_paging
					require.Equal(t, 1, se.GetSessionVars().DistSQLScanConcurrency())
					require.False(t, se.GetSessionVars().EnablePaging)
					require.True(t, se.GetSessionVars().InternalSQLScanUserTable)
					// restore should restore the session variables
					restore()
				} else {
					// fault case
					require.EqualError(t, err, "fault in test")
					require.Nil(t, tblSe)
					require.Nil(t, restore)
				}
				// Not matter returns an error or not, the session should be closed
				require.Equal(t, 123, se.GetSessionVars().DistSQLScanConcurrency())
				require.True(t, se.GetSessionVars().EnablePaging)
				require.False(t, se.GetSessionVars().InternalSQLScanUserTable)
				return nil
			}))
			require.True(t, called)
			if errSQL == "" {
				// A successfully restored session remains reusable.
				require.False(t, sysSe.IsInternalClosed())
			} else {
				// A setup statement can take effect before returning its error.
				// Discard the session even when best-effort restoration succeeded.
				require.True(t, sysSe.IsInternalClosed())
			}
		})
	}

	// error in restore
	for _, prefixSQL := range []string{
		"set @@tidb_distsql_scan_concurrency=",
		"set @@tidb_enable_paging=",
	} {
		sysSe = nil
		called := false
		pool.setFault(newFaultWithFilter(func(s string) bool {
			return called && strings.HasPrefix(s, prefixSQL)
		}, newFaultAfterCount(0)))
		require.NoError(t, ttlworker.WithSessionForTest(pool, func(se session.Session) error {
			require.False(t, called)
			tblSe, restore, err := ttlworker.NewScanSession(context.Background(), se, &cache.PhysicalTable{}, time.Now())
			called = true
			require.NoError(t, err)
			require.NotNil(t, tblSe)
			require.Equal(t, 1, se.GetSessionVars().DistSQLScanConcurrency())
			require.False(t, se.GetSessionVars().EnablePaging)
			// restore should return error
			require.EqualError(t, restore(), "fault in test")
			// other session variables should be restored
			if !strings.Contains(prefixSQL, "tidb_distsql_scan_concurrency") {
				require.Equal(t, 123, se.GetSessionVars().DistSQLScanConcurrency())
			}
			if !strings.Contains(prefixSQL, "tidb_enable_paging") {
				require.True(t, se.GetSessionVars().EnablePaging)
			}
			return nil
		}))
		require.True(t, called)
		// internal should be closed because restore failed
		require.True(t, sysSe.IsInternalClosed())
	}
}

func TestTTLJobRUAttribution(t *testing.T) {
	original := config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load()
	config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Store(true)
	t.Cleanup(func() { config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Store(original) })
	store, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.ttl_ru_data(id int primary key, ts datetime) ttl=ts+interval 1 day")
	tblInfo, err := dom.InfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("ttl_ru_data"))
	require.NoError(t, err)
	tbl, err := cache.NewPhysicalTable(ast.NewCIStr("test"), tblInfo, ast.CIStr{})
	require.NoError(t, err)
	se, closeSe := sessionFactory(t, dom)()
	defer closeSe()
	vars := se.GetSessionVars()
	// SQL observation must work even when internal session logs are redacted.
	vars.EnableRedactLog = vardef.On
	ctx := context.Background()
	manager := ttlworker.NewJobManager("ttl-ru-owner", nil, store, nil, nil)
	manager.InfoSchemaCache().Tables[tbl.ID] = tbl

	const jobID = "ttl-ru-job"
	globalCounts, globalRefreshes, jobCommits := 0, 0, 0
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/observeStatementRUOwnerInstallForTest", func(stmt *executor.ExecStmt) {
		if stmt.Ctx.GetSessionVars() != vars {
			return
		}
		// Classify the original SQL independently of log redaction.
		sql := stmt.Text()
		if strings.HasPrefix(sql, "SELECT LOW_PRIORITY ") && strings.HasSuffix(sql, " FROM mysql.tidb_ttl_table_status") {
			require.Empty(t, vars.TTLJobID, sql)
			globalRefreshes++
			return
		}
		if strings.EqualFold(sql, "SELECT count(1) FROM mysql.tidb_ttl_task WHERE status = 'running'") {
			require.Empty(t, vars.TTLJobID, sql)
			globalCounts++
			return
		}
		require.Empty(t, vars.TTLJobID, sql)
		if strings.EqualFold(sql, "COMMIT") {
			jobCommits++
		}
	})

	before := testutil.ToFloat64(rumetrics.RUV2TTLTotal)
	// These maintenance queries run without a user job and remain uncharged.
	for range 2 {
		_, err = se.ExecuteSQL(ctx, "select * from mysql.tidb_ttl_table_status")
		require.NoError(t, err)
		_, err = se.ExecuteSQL(ctx, "select count(*) from mysql.tidb_ttl_task")
		require.NoError(t, err)
	}
	require.Equal(t, before, testutil.ToFloat64(rumetrics.RUV2TTLTotal))

	now := se.Now()
	job, err := manager.LockJob(ctx, se, tbl, now, jobID, false)
	require.NoError(t, err)
	require.Equal(t, 1, globalRefreshes, "job creation refreshes the global table-status cache")
	require.NoError(t, manager.UpdateHeartBeatForJob(ctx, se, now.Add(time.Minute), job))
	taskManager := ttlworker.NewTaskManager(ctx, nil, manager.InfoSchemaCache(), "ttl-ru-owner", store)
	task, err := taskManager.LockScanTask(se, &cache.TTLTask{JobID: jobID, ScanID: 0, TableID: tbl.ID}, se.Now())
	require.NoError(t, err)
	require.NoError(t, taskManager.UpdateHeartBeatForTask(ctx, se, now.Add(time.Minute), task))
	task.SetResult(nil)
	require.NoError(t, taskManager.ReportTaskFinished(se, se.Now(), task))
	// Taking over an expired owner is also metadata work.
	takeoverManager := ttlworker.NewJobManager("ttl-ru-takeover", nil, store, nil, nil)
	job, err = takeoverManager.LockJob(ctx, se, tbl, now.Add(time.Hour), "", false)
	require.NoError(t, err)
	require.Equal(t, 2, globalRefreshes, "takeover also refreshes the global table-status cache")
	require.NoError(t, job.Finish(se, se.Now(), &ttlworker.TTLSummary{}))
	require.Equal(t, 1, globalCounts)
	require.Equal(t, 4, jobCommits, "creation, task claim, takeover and job completion")
	require.Equal(t, before, testutil.ToFloat64(rumetrics.RUV2TTLTotal))
	require.Empty(t, vars.TTLJobID)

	before = testutil.ToFloat64(rumetrics.RUV2TTLTotal)
	_, err = se.ExecuteSQL(ctx, "select * from mysql.tidb_ttl_job_history")
	require.NoError(t, err)
	require.Equal(t, before, testutil.ToFloat64(rumetrics.RUV2TTLTotal))
}

func TestWithSessionUnderSEMv2(t *testing.T) {
	require.NoError(t, semv2.EnableBy(&semv2.Config{
		Version:     "1.0",
		TiDBVersion: "v0.0.0",
		RestrictedVariables: []semv2.VariableRestriction{
			{Name: vardef.TiDBEnable1PC, Readonly: true},
			{Name: vardef.TiDBEnableAsyncCommit, Readonly: true},
		},
	}))
	t.Cleanup(semv2.Disable)

	_, dom := testkit.CreateMockStoreAndDomain(t)
	waitAndStopTTLManager(t, dom)

	// The TTL worker takes its session from an internal session pool, so the
	// session has no privilege manager bound. Preparing the session sets some
	// variables which may be read-only under SEM, and that used to panic with a
	// nil pointer dereference, which left the TTL job manager unable to run.
	require.NoError(t, ttlworker.WithSessionForTest(dom.AdvancedSysSessionPool(), func(se session.Session) error {
		require.NotNil(t, se)
		return nil
	}))
}
>>>>>>> 47dc1214658 (session, lightning: enable log redaction by default (#72666))
