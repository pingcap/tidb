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

package session

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

// TxnMode represents using optimistic or pessimistic mode in the transaction
type TxnMode int

const (
	// TxnModeOptimistic means using the optimistic transaction with "BEGIN OPTIMISTIC"
	TxnModeOptimistic TxnMode = iota
	// TxnModePessimistic means using the pessimistic transaction with "BEGIN PESSIMISTIC"
	TxnModePessimistic
)

// Session is used to execute queries for TTL case
type Session interface {
	sessionctx.Context
	// SessionInfoSchema returns information schema of current session
	SessionInfoSchema() infoschema.InfoSchema
	// ExecuteSQL executes the sql
	ExecuteSQL(ctx context.Context, sql string, args ...any) ([]chunk.Row, error)
	// RunInTxn executes the specified function in a txn
	RunInTxn(ctx context.Context, fn func() error, mode TxnMode) (err error)
	// ResetWithGlobalTimeZone resets the session time zone to global time zone
	ResetWithGlobalTimeZone(ctx context.Context) error
	// GlobalTimeZone returns the global timezone. It is used to compute expire time for TTL
	GlobalTimeZone(ctx context.Context) (*time.Location, error)
	// Close closes the session
	Close()
	// Now returns the current time in location specified by session var
	Now() time.Time
}

type session struct {
	sessionctx.Context
	sqlExec sqlexec.SQLExecutor
	closeFn func(Session)
}

// NewSession creates a new Session
func NewSession(sctx sessionctx.Context, sqlExec sqlexec.SQLExecutor, closeFn func(Session)) Session {
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
func (s *session) ExecuteSQL(ctx context.Context, sql string, args ...any) ([]chunk.Row, error) {
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
func (s *session) RunInTxn(ctx context.Context, fn func() error, txnMode TxnMode) (err error) {
	tracer := metrics.PhaseTracerFromCtx(ctx)
	defer tracer.EnterPhase(tracer.Phase())

	tracer.EnterPhase(metrics.PhaseBeginTxn)
	sql := "BEGIN "
	switch txnMode {
	case TxnModeOptimistic:
		sql += "OPTIMISTIC"
	case TxnModePessimistic:
		sql += "PESSIMISTIC"
	default:
		return errors.New("unknown transaction mode")
	}
	if _, err = s.ExecuteSQL(ctx, sql); err != nil {
		return err
	}
	tracer.EnterPhase(metrics.PhaseOther)

	success := false
	defer func() {
		if !success {
			_, rollbackErr := s.ExecuteSQL(ctx, "ROLLBACK")
			terror.Log(rollbackErr)
		}
	}()

	if err = fn(); err != nil {
		return err
	}

	tracer.EnterPhase(metrics.PhaseCommitTxn)
	if _, err = s.ExecuteSQL(ctx, "COMMIT"); err != nil {
		return err
	}
	tracer.EnterPhase(metrics.PhaseOther)

	success = true
	return err
}

// ResetWithGlobalTimeZone resets the session time zone to global time zone
func (s *session) ResetWithGlobalTimeZone(ctx context.Context) error {
	sessVar := s.GetSessionVars()
	if sessVar.TimeZone != nil {
		globalTZ, err := sessVar.GetGlobalSystemVar(ctx, variable.TimeZone)
		if err != nil {
			return err
		}

		tz, err := sessVar.GetSessionOrGlobalSystemVar(ctx, variable.TimeZone)
		if err != nil {
			return err
		}

		if globalTZ == tz {
			return nil
		}
	}

	_, err := s.ExecuteSQL(ctx, "SET @@time_zone=@@global.time_zone")
	return err
}

// GlobalTimeZone returns the global timezone
func (s *session) GlobalTimeZone(ctx context.Context) (*time.Location, error) {
	str, err := s.GetSessionVars().GetGlobalSystemVar(ctx, "time_zone")
	if err != nil {
		return nil, err
	}
	return timeutil.ParseTimeZone(str)
}

// Close closes the session
func (s *session) Close() {
	if s.closeFn != nil {
		s.closeFn(s)
		s.Context = nil
		s.sqlExec = nil
		s.closeFn = nil
	}
}

// Now returns the current time in the location of time_zone session var
func (s *session) Now() time.Time {
	return time.Now().In(s.Context.GetSessionVars().Location())
}
