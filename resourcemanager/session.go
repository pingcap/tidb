// Copyright 2022 PingCAP, Inc.
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

package resourcemanager

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// Session wraps sessionctx.Context for transaction usage.
type Session struct {
	sessionctx.Context
}

// NewSession is return new point of session to be used.
func NewSession(s sessionctx.Context) *Session {
	return &Session{s}
}

// Begin is to begin a new transaction.
func (s *Session) Begin() error {
	err := sessiontxn.NewTxn(context.Background(), s)
	if err != nil {
		return err
	}
	s.GetSessionVars().SetInTxn(true)
	return nil
}

// Commit is commit one transaction.
func (s *Session) Commit() error {
	s.StmtCommit(context.Background())
	return s.CommitTxn(context.Background())
}

// GetTxn return active transaction.
func (s *Session) GetTxn() (kv.Transaction, error) {
	return s.Txn(true)
}

// Rollback is rollback a transaction.
func (s *Session) Rollback() {
	s.StmtRollback(context.Background(), false)
	s.RollbackTxn(context.Background())
}

// Reset is reset statement.
func (s *Session) Reset() {
	s.StmtRollback(context.Background(), false)
}

// Execute is used to run one query internal.
func (s *Session) Execute(ctx context.Context, query string, label string) ([]chunk.Row, error) {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DDLJobTableDuration.WithLabelValues(label + "-" + metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	rs, err := s.Context.(sqlexec.SQLExecutor).ExecuteInternal(kv.WithInternalSourceType(ctx, kv.InternalTxnDDL), query)
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

// Session return session context.
func (s *Session) Session() sessionctx.Context {
	return s.Context
}

// RunInTxn used to pack transaction execution for function f.
func RunInTxn(se *Session, f func(*Session) error) (err error) {
	err = se.Begin()
	if err != nil {
		return err
	}
	err = f(se)
	if err != nil {
		se.Rollback()
		return
	}
	return errors.Trace(se.Commit())
}
