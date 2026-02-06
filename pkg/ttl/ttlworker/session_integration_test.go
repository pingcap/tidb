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

<<<<<<< HEAD
	for i := 0; i < 50; i++ {
		pool.setFault(&faultAfterCount{faultCount: i})
		se, err := ttlworker.GetSessionForTest(pool)
		logutil.BgLogger().Info("get session", zap.Int("error after count", i), zap.Bool("session is nil", se == nil), zap.Bool("error is nil", err == nil))
		require.True(t, se != nil || err != nil)
=======
	var sysSe *syssession.Session
	pool.onSysSession = func(se *syssession.Session) {
		require.Nil(t, sysSe)
		sysSe = se
		// set some session variables to make sure to test all variables setting/restore
		delete(se.InternalSctxForTest().GetSessionVars().IsolationReadEngines, kv.TiFlash)
		se.InternalSctxForTest().GetSessionVars().Enable1PC = false
		se.InternalSctxForTest().GetSessionVars().EnableAsyncCommit = false
	}

	type mockAttached struct{ sqlexec.SQLExecutor }
	var attached *mockAttached
	var detached sqlexec.SQLExecutor
	statshandle.AttachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
		require.Nil(t, attached)
		require.Nil(t, detached)
		attached = &mockAttached{SQLExecutor: s}
		return attached
	}
	statshandle.DetachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
		require.NotNil(t, attached)
		require.Same(t, attached, s)
		require.Nil(t, detached)
		detached = attached.SQLExecutor
		return detached
	}

	prepareFaults := []struct {
		sql   string
		panic bool
	}{
		{sql: "set tidb_retry_limit=0"},
		{sql: "set tidb_retry_limit=0", panic: true},
		{sql: "set tidb_enable_1pc=ON"},
		{sql: "set tidb_enable_async_commit=ON"},
		{sql: "ROLLBACK"},
		{sql: "set @@time_zone='UTC'"},
		{sql: "select @@tidb_isolation_read_engines"},
		{sql: "set tidb_isolation_read_engines='tikv,tiflash,tidb'"},
	}

	for _, f := range prepareFaults {
		t.Run(f.sql, func(t *testing.T) {
			sysSe, attached, detached = nil, nil, nil
			pool.setFault(newFaultWithFilter(func(sql string) bool {
				if f.panic && sql == f.sql {
					panic(sql)
				}
				return sql == f.sql
			}, newFaultAfterCount(0)))
			if f.panic {
				require.PanicsWithValue(t, f.sql, func() {
					_ = ttlworker.WithSessionForTest(pool, func(se session.Session) error {
						require.FailNow(t, f.sql, "should not reach here")
						return nil
					})
				})
			} else {
				err := ttlworker.WithSessionForTest(pool, func(se session.Session) error {
					require.FailNow(t, f.sql, "should not reach here")
					return nil
				})
				require.Error(t, err)
			}
			require.NotNil(t, sysSe)
			// check the session should have been detached
			exec := sysSe.InternalSctxForTest().GetSQLExecutor()
			require.Same(t, detached.(*sessionWithFault).SessionContext.GetSQLExecutor(), exec)
			// check the session should be closed instead of put back due to the fault
			require.True(t, sysSe.IsInternalClosed())
		})
	}

	t.Run("use error", func(t *testing.T) {
		sysSe, attached, detached = nil, nil, nil
		pool.setFault(newFaultWithFilter(func(sql string) bool { return false }, newFaultAfterCount(0)))
		err := ttlworker.WithSessionForTest(pool, func(session.Session) error {
			require.NotNil(t, attached)
			require.Nil(t, detached)
			return errors.New("mockErr1")
		})
		require.EqualError(t, err, "mockErr1")
		require.NotNil(t, sysSe)
		// check the session should have been attached and detached
		exec := sysSe.InternalSctxForTest().GetSQLExecutor()
		require.Same(t, attached.SQLExecutor.(*sessionWithFault).SessionContext.GetSQLExecutor(), exec)
		require.Same(t, detached.(*sessionWithFault).SessionContext.GetSQLExecutor(), exec)
		// check the session should be closed instead of put back due to the fault
		require.True(t, sysSe.IsInternalClosed())
	})

	t.Run("use panic", func(t *testing.T) {
		sysSe, attached, detached = nil, nil, nil
		pool.setFault(newFaultWithFilter(func(sql string) bool { return false }, newFaultAfterCount(0)))
		require.PanicsWithValue(t, "mockPanic1", func() {
			_ = ttlworker.WithSessionForTest(pool, func(session.Session) error {
				require.NotNil(t, attached)
				require.Nil(t, detached)
				panic("mockPanic1")
			})
		})
		require.NotNil(t, sysSe)
		// check the session should have been attached and detached
		exec := sysSe.InternalSctxForTest().GetSQLExecutor()
		require.Same(t, attached.SQLExecutor.(*sessionWithFault).SessionContext.GetSQLExecutor(), exec)
		require.Same(t, detached.(*sessionWithFault).SessionContext.GetSQLExecutor(), exec)
		// check the session should be closed instead of put back due to the fault
		require.True(t, sysSe.IsInternalClosed())
	})

	restoreFaults := []struct {
		prefix string
		panic  bool
	}{
		{prefix: "set tidb_retry_limit="},
		{prefix: "set tidb_retry_limit=", panic: true},
		{prefix: "set tidb_enable_1pc="},
		{prefix: "set tidb_enable_async_commit="},
		{prefix: "set @@time_zone="},
		{prefix: "set tidb_isolation_read_engines="},
	}

	for _, f := range restoreFaults {
		t.Run(f.prefix, func(t *testing.T) {
			sysSe, attached, detached = nil, nil, nil
			afterPrepare := false
			pool.setFault(newFaultWithFilter(func(sql string) bool {
				if !afterPrepare {
					return false
				}
				if f.panic && strings.HasPrefix(sql, f.prefix) {
					panic(f.prefix)
				}
				return strings.HasPrefix(sql, f.prefix)
			}, newFaultAfterCount(0)))
			if f.panic {
				require.PanicsWithValue(t, f.prefix, func() {
					_ = ttlworker.WithSessionForTest(pool, func(se session.Session) error {
						require.NotNil(t, attached)
						require.Nil(t, detached)
						require.False(t, afterPrepare)
						afterPrepare = true
						return nil
					})
				})
			} else {
				require.NoError(t, ttlworker.WithSessionForTest(pool, func(se session.Session) error {
					require.NotNil(t, attached)
					require.Nil(t, detached)
					require.False(t, afterPrepare)
					afterPrepare = true
					return nil
				}))
			}
			require.NotNil(t, sysSe)
			// check With function has been called
			require.True(t, afterPrepare)
			// check the session should have been attached and detached
			exec := sysSe.InternalSctxForTest().GetSQLExecutor()
			require.Same(t, attached.SQLExecutor.(*sessionWithFault).SessionContext.GetSQLExecutor(), exec)
			require.Same(t, detached.(*sessionWithFault).SessionContext.GetSQLExecutor(), exec)
			// check the session should be closed instead of put back due to the fault
			require.True(t, sysSe.IsInternalClosed())
		})
	}
}

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
				tblSe, restore, err := ttlworker.NewScanSession(se, &cache.PhysicalTable{}, time.Now(), session.TTLJobTypeTTL)
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
			// internal should not close
			require.False(t, sysSe.IsInternalClosed())
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
			tblSe, restore, err := ttlworker.NewScanSession(se, &cache.PhysicalTable{}, time.Now(), session.TTLJobTypeTTL)
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
>>>>>>> 6e50f2744f (Squashed commit of the active-active)
	}
}
