// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttl

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// Session is used to execute queries for TTL case
type Session struct {
	Sctx    sessionctx.Context
	SQLExec sqlexec.SQLExecutor
	CloseFn func()
}

// GetSessionVars returns the sessionVars
func (s *Session) GetSessionVars() *variable.SessionVars {
	if s.Sctx != nil {
		return s.Sctx.GetSessionVars()
	}
	return nil
}

// ExecuteSQL executes the sql
func (s *Session) ExecuteSQL(ctx context.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	if s.SQLExec == nil {
		return nil, errors.New("session is closed")
	}

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnTTL)
	rs, err := s.SQLExec.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	if rs == nil {
		return nil, nil
	}

	defer func() {
		terror.Log(rs.Close())
	}()

	return sqlexec.DrainRecordSet(ctx, rs, 8)
}

// RunInTxn executes the specified function in a txn
func (s *Session) RunInTxn(ctx context.Context, fn func() error) (err error) {
	if _, err = s.ExecuteSQL(ctx, "BEGIN"); err != nil {
		return err
	}

	success := false
	defer func() {
		if !success {
			_, err = s.ExecuteSQL(ctx, "ROLLBACK")
			terror.Log(err)
		}
	}()

	err = fn()
	success = err == nil
	return err
}

// Close closed the session
func (s *Session) Close() {
	if s.CloseFn != nil {
		s.CloseFn()
		s.Sctx = nil
		s.SQLExec = nil
		s.CloseFn = nil
	}
}
