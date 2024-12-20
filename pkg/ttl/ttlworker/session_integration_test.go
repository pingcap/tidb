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
	fault := s.fault.Load()
	if fault == nil {
		return false
	}

	return (*fault).shouldFault(sql)
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
	if ft == nil {
		f.fault.Store(nil)
		return
	}

	f.fault.Store(&ft)
}

func TestGetSessionWithFault(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)

	pool := newFaultSessionPool(dom.SysSessionPool())

	for i := 0; i < 50; i++ {
		pool.setFault(newFaultWithFilter(func(sql string) bool {
			// skip some local only sql, ref `getSession()` in `session.go`
			if strings.HasPrefix(sql, "set tidb_") || strings.HasPrefix(sql, "set @@") {
				return false
			}
			return true
		}, newFaultAfterCount(i)))

		se, err := ttlworker.GetSessionForTest(pool)
		logutil.BgLogger().Info("get session", zap.Int("error after count", i), zap.Bool("session is nil", se == nil), zap.Bool("error is nil", err == nil))
		require.True(t, se != nil || err != nil)
	}
}
