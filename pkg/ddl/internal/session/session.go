// Copyright 2023 PingCAP, Inc.
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

package session

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// Session wraps sessionctx.Context for transaction usage.
type Session struct {
	sessionctx.Context
}

// NewSession creates a new Session.
func NewSession(s sessionctx.Context) *Session {
	return &Session{s}
}

// TODO(lance6716): provide a NewSessionWithCtx

// Begin starts a transaction.
func (s *Session) Begin() error {
	err := sessiontxn.NewTxn(context.Background(), s.Context)
	if err != nil {
		return err
	}
	s.GetSessionVars().SetInTxn(true)
	return nil
}

// Commit commits the transaction.
func (s *Session) Commit() error {
	s.StmtCommit(context.Background())
	return s.CommitTxn(context.Background())
}

// Txn activate and returns the current transaction.
func (s *Session) Txn() (kv.Transaction, error) {
	return s.Context.Txn(true)
}

// Rollback aborts the transaction.
func (s *Session) Rollback() {
	s.StmtRollback(context.Background(), false)
	s.RollbackTxn(context.Background())
}

// Reset resets the session.
func (s *Session) Reset() {
	s.StmtRollback(context.Background(), false)
}

// Execute executes a query.
func (s *Session) Execute(ctx context.Context, query string, label string) ([]chunk.Row, error) {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DDLJobTableDuration.WithLabelValues(label + "-" + metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	if ctx.Value(kv.RequestSourceKey) == nil {
		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	}
	rs, err := s.Context.GetSQLExecutor().ExecuteInternal(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

// Session returns the sessionctx.Context.
func (s *Session) Session() sessionctx.Context {
	return s.Context
}

// RunInTxn runs a function in a transaction.
func (s *Session) RunInTxn(f func(*Session) error) (err error) {
	err = s.Begin()
	if err != nil {
		return err
	}
	failpoint.Inject("NotifyBeginTxnCh", func(val failpoint.Value) {
		//nolint:forcetypeassert
		v := val.(int)
		if v == 1 {
			MockDDLOnce = 1
			TestNotifyBeginTxnCh <- struct{}{}
		} else if v == 2 && MockDDLOnce == 1 {
			<-TestNotifyBeginTxnCh
			MockDDLOnce = 0
		}
	})

	err = f(s)
	if err != nil {
		s.Rollback()
		return
	}
	return errors.Trace(s.Commit())
}

var (
	// MockDDLOnce is only used for test.
	MockDDLOnce = int64(0)
	// TestNotifyBeginTxnCh is used for if the txn is beginning in RunInTxn.
	TestNotifyBeginTxnCh = make(chan struct{})
)
