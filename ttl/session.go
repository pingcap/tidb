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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// Session is used to execute queries for TTL case
type Session interface {
	sessionctx.Context
	// SessionInfoSchema returns information schema of current session
	SessionInfoSchema() infoschema.InfoSchema
	// ExecuteSQL executes the sql
	ExecuteSQL(ctx context.Context, sql string, args ...interface{}) ([]chunk.Row, error)
	// RunInTxn executes the specified function in a txn
	RunInTxn(ctx context.Context, fn func() error) (err error)
	// Close closes the session
	Close()
}

type session struct {
	sessionctx.Context
	sqlExec sqlexec.SQLExecutor
	closeFn func()
}

// NewSession creates a new Session
func NewSession(sctx sessionctx.Context, sqlExec sqlexec.SQLExecutor, closeFn func()) Session {
	return &session{
		Context: sctx,
		sqlExec: sqlExec,
		closeFn: closeFn,
	}
}

// SessionInfoSchema returns information schema of current session
func (s *session) SessionInfoSchema() infoschema.InfoSchema {
	if s.Context == nil {
		return nil
	}
	return sessiontxn.GetTxnManager(s.Context).GetTxnInfoSchema()
}

// ExecuteSQL executes the sql
func (s *session) ExecuteSQL(ctx context.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	if s.sqlExec == nil {
		return nil, errors.New("session is closed")
	}

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnTTL)
	rs, err := s.sqlExec.ExecuteInternal(ctx, sql, args...)
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
func (s *session) RunInTxn(ctx context.Context, fn func() error) (err error) {
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

	if err = fn(); err != nil {
		return err
	}

	if _, err = s.ExecuteSQL(ctx, "COMMIT"); err != nil {
		return err
	}

	success = true
	return err
}

// Close closes the session
func (s *session) Close() {
	if s.closeFn != nil {
		s.closeFn()
		s.Context = nil
		s.sqlExec = nil
		s.closeFn = nil
	}
}
