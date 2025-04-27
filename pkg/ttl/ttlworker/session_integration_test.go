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
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

type fault interface {
	// shouldFault returns whether the session should fault this time.
	shouldFault(sql string) bool
}

var _ fault = &faultAfterCount{}

type faultAfterCount struct {
	faultCount int

	currentCount int
}

func newFaultAfterCount(faultCount int) *faultAfterCount {
	return &faultAfterCount{faultCount: faultCount}
}

func (f *faultAfterCount) shouldFault(sql string) bool {
	if f.currentCount >= f.faultCount {
		return true
	}

	f.currentCount++
	return false
}

type faultWithFilter struct {
	filter func(string) bool
	f      fault
}

func (f *faultWithFilter) shouldFault(sql string) bool {
	if f.filter == nil || f.filter(sql) {
		return f.f.shouldFault(sql)
	}

	return false
}

func newFaultWithFilter(filter func(string) bool, f fault) *faultWithFilter {
	return &faultWithFilter{filter: filter, f: f}
}

// sessionWithFault is a session which will fail to execute SQL after successfully executing several SQLs. It's designed
// to trigger every possible branch of returning error from `Execute`
type sessionWithFault struct {
	syssession.SessionContext
	closed bool
	fault  *atomic.Pointer[fault]
}

// Close implements pools.Resource
func (s *sessionWithFault) Close() {
	s.closed = true
	s.SessionContext.Close()
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
	return s.SessionContext.GetSQLExecutor().Execute(ctx, sql)
}

// ExecuteStmt implements sqlexec.SQLExecutor.
func (s *sessionWithFault) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	if s.shouldFault(stmtNode.Text()) {
		return nil, errors.New("fault in test")
	}
	return s.SessionContext.GetSQLExecutor().ExecuteStmt(ctx, stmtNode)
}

func (s *sessionWithFault) ExecuteInternal(ctx context.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	if s.shouldFault(sql) {
		return nil, errors.New("fault in test")
	}
	return s.SessionContext.GetSQLExecutor().ExecuteInternal(ctx, sql, args...)
}

func (s *sessionWithFault) shouldFault(sql string) bool {
	fault := s.fault.Load()
	if fault == nil {
		return false
	}

	return (*fault).shouldFault(sql)
}

type faultSessionPool struct {
	t *testing.T
	syssession.Pool
	sp           syssession.Pool
	fault        *atomic.Pointer[fault]
	onSysSession func(*syssession.Session)
}

func newFaultSessionPool(t *testing.T, sp syssession.Pool) *faultSessionPool {
	return &faultSessionPool{
		t:     t,
		sp:    sp,
		fault: &atomic.Pointer[fault]{},
	}
}

func (f *faultSessionPool) WithSession(fn func(*syssession.Session) error) error {
	return f.sp.WithSession(func(se *syssession.Session) error {
		require.NoError(f.t, se.ResetSctxForTest(func(sctx syssession.SessionContext) syssession.SessionContext {
			return &sessionWithFault{
				SessionContext: sctx,
				fault:          f.fault,
			}
		}))
		defer func() {
			require.NoError(f.t, se.ResetSctxForTest(func(sctx syssession.SessionContext) syssession.SessionContext {
				return sctx.(*sessionWithFault).SessionContext
			}))
		}()
		if f.onSysSession != nil {
			f.onSysSession(se)
		}
		return fn(se)
	})
}

func (f *faultSessionPool) setFault(ft fault) {
	if ft == nil {
		f.fault.Store(nil)
		return
	}

	f.fault.Store(&ft)
}

func TestGetSessionWithFault(t *testing.T) {
	origAttachStats, origDetachStats := ttlworker.AttachStatsCollector, ttlworker.DetachStatsCollector
	defer func() {
		ttlworker.AttachStatsCollector = origAttachStats
		ttlworker.DetachStatsCollector = origDetachStats
	}()

	_, dom := testkit.CreateMockStoreAndDomain(t)
	pool := newFaultSessionPool(t, dom.AdvancedSysSessionPool())

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
	ttlworker.AttachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
		require.Nil(t, attached)
		require.Nil(t, detached)
		attached = &mockAttached{SQLExecutor: s}
		return attached
	}
	ttlworker.DetachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
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
				tblSe, restore, err := ttlworker.NewScanSession(se, &cache.PhysicalTable{}, time.Now())
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
			tblSe, restore, err := ttlworker.NewScanSession(se, &cache.PhysicalTable{}, time.Now())
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
