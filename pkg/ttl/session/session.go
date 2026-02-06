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
	"fmt"
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
<<<<<<< HEAD
=======
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
>>>>>>> 6e50f2744f (Squashed commit of the active-active)
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

const minActiveActiveCheckpointTSRefreshInterval = time.Minute

const tiCDCProgressDB = "tidb_cdc"
const tiCDCProgressTable = "ticdc_progress_table"

// TTLJobType represents the type of TTL job
type TTLJobType = string

const (
	// TTLJobTypeTTL represents a normal TTL job
	TTLJobTypeTTL TTLJobType = "ttl"
	// TTLJobTypeSoftDelete represents a softdelete cleanup job
	TTLJobTypeSoftDelete TTLJobType = "softdelete"
	// TTLJobTypeRunawayGC represents a GC job for runaway system table.
	// It is used by the SQL builder to pick the correct time column.
	TTLJobTypeRunawayGC TTLJobType = "runaway_gc"
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
	// KillStmt kills the current statement execution
	KillStmt()
	// Close closes the session
	Close()
	// Now returns the current time in location specified by session var
	Now() time.Time
<<<<<<< HEAD
=======
	// GetMinActiveActiveCheckpointTS returns cached min checkpoint ts from TiCDC progress table.
	// It refreshes the cache periodically to avoid querying for every SQL.
	GetMinActiveActiveCheckpointTS(ctx context.Context, dbName, tableName string) (uint64, error)
	// AvoidReuse is used to avoid reuse the session
	AvoidReuse()
>>>>>>> 6e50f2744f (Squashed commit of the active-active)
}
type session struct {
<<<<<<< HEAD
	sessionctx.Context
	sqlExec sqlexec.SQLExecutor
	closeFn func(Session)
=======
	sctx       sessionctx.Context
	sqlExec    sqlexec.SQLExecutor
	avoidReuse func()

	minActiveActiveCheckpointTS            uint64
	minActiveActiveCheckpointTSLastRefresh time.Time
>>>>>>> 6e50f2744f (Squashed commit of the active-active)
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
	success := false
	defer func() {
		// Always try to `ROLLBACK` the transaction even if only the `BEGIN` fails. If the `BEGIN` is killed
		// after it runs the first `Next`, the transaction is already active and needs to be `ROLLBACK`ed.
		if !success {
			// For now, the "ROLLBACK" can execute successfully even when the context has already been cancelled.
			// Using another timeout context to avoid that this behavior will be changed in the future.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, rollbackErr := s.ExecuteSQL(ctx, "ROLLBACK")
			terror.Log(rollbackErr)
			cancel()
		}
	}()

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

// KillStmt kills the current statement execution
func (s *session) KillStmt() {
	s.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
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

// GetMinActiveActiveCheckpointTS get min safe checkpoint ts on demand.
func (s *session) GetMinActiveActiveCheckpointTS(ctx context.Context, dbName, tableName string) (uint64, error) {
	// Refresh periodically to avoid querying TiCDC progress table for every SQL.
	if !s.minActiveActiveCheckpointTSLastRefresh.IsZero() &&
		time.Since(s.minActiveActiveCheckpointTSLastRefresh) < minActiveActiveCheckpointTSRefreshInterval {
		return s.minActiveActiveCheckpointTS, nil
	}

	sql := fmt.Sprintf(
		"SELECT IFNULL(MIN(checkpoint_ts), 0) FROM %s.%s WHERE database_name=%%? AND table_name=%%?",
		tiCDCProgressDB,
		tiCDCProgressTable,
	)

	rows, err := s.ExecuteSQL(ctx,
		sql,
		sqlescape.EscapeString(dbName),
		sqlescape.EscapeString(tableName),
	)
	if err != nil {
		return 0, err
	}

	var ts uint64
	if len(rows) == 1 && rows[0].Len() == 1 {
		ts = rows[0].GetUint64(0)
	}

	s.minActiveActiveCheckpointTS = ts
	s.minActiveActiveCheckpointTSLastRefresh = time.Now()
	return ts, nil
}
