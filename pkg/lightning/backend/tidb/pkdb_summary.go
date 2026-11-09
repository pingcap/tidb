// Copyright 2026 PingCAP, Inc.
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

package tidb

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"

	gmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/pkdbsummary"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
)

func splitSummaryRows(ctx context.Context, rows tidbRows, maxSize uint64, maxRows int) []tidbRows {
	chunks := rows.splitIntoChunks(maxSize, maxRows)
	p := pkdbsummary.FromContext(ctx)
	if p == nil {
		return chunks
	}
	result := make([]tidbRows, 0, len(chunks)+1)
	for _, chunk := range chunks {
		// A batch must not mix acknowledged replay with new input: statement-level
		// telemetry cannot apportion replacements between those two groups.
		split := 0
		for split < len(chunk) && !p.Ledger.Eligible(p.Key, chunk[split].offset) {
			split++
		}
		if split > 0 && split < len(chunk) {
			result = append(result, chunk[:split], chunk[split:])
		} else {
			result = append(result, chunk)
		}
	}
	return result
}

// stmtSetupError distinguishes a statement that was never executed from a row
// rejected by DML. Row-error tolerance must not consume connection/prepare errors.
type stmtSetupError struct{ error }

func (e *stmtSetupError) Cause() error  { return e.error }
func (e *stmtSetupError) Unwrap() error { return e.error }

func isStmtSetupError(err error) bool {
	var setup *stmtSetupError
	return stderrors.As(err, &setup)
}

func (be *tidbBackend) execSummaryStmt(ctx context.Context, task stmtTask) (sql.Result, error) {
	p := pkdbsummary.FromContext(ctx)
	var conn *sql.Conn
	if p != nil && len(task.rows) > 0 {
		session, _ := ctx.Value(summarySessionKey{}).(*summarySession)
		if session == nil {
			return nil, &stmtSetupError{errors.New("missing source writer session")}
		}
		if session.conn == nil {
			var err error
			session.conn, err = be.db.Conn(ctx)
			if err != nil {
				return nil, &stmtSetupError{err}
			}
		}
		conn = session.conn
		p.Ledger.BeginWrite(ctx, p.Key, task.rows[len(task.rows)-1].offset)
	}

	result, err := be.execOnConnection(ctx, task, conn)
	if p == nil || len(task.rows) == 0 {
		return result, err
	}
	end := task.rows[len(task.rows)-1].offset
	if err != nil {
		// A transport/commit error can be followed by a successful retry which sees
		// our own earlier write. Its REPLACE outcome cannot classify the original.
		serverError, sqlError := errors.Cause(err).(*gmysql.MySQLError)
		if !sqlError {
			ctx.Value(summarySessionKey{}).(*summarySession).close()
		}
		if isStmtSetupError(err) {
			// No DML was sent, even if preparing the statement lost its connection.
			p.Ledger.RollbackWrite(ctx, p.Key)
			return result, err
		}
		// A reported deadlock proves rollback. Its successful retry can still be
		// classified; network and other ambiguous retry results retain uncertainty.
		rolledBack := sqlError && (!common.IsRetryableError(err) || serverError.Number == errno.ErrLockDeadlock)
		if !rolledBack {
			p.Ledger.MarkAmbiguous(ctx, p.Key, end)
		} else {
			p.Ledger.RollbackWrite(ctx, p.Key)
		}
		return result, err
	}
	if !p.Ledger.Eligible(p.Key, task.rows[0].offset) {
		return result, nil
	}
	outcomes := pkdbsummary.Outcomes{}
	var raw string
	var info struct {
		Result *stmtctx.DMLResult `json:"dml_result"`
	}
	// A failed diagnostic read never causes a successful DML to be retried.
	diagnosticErr := conn.QueryRowContext(ctx, "SELECT @@tidb_last_query_info").Scan(&raw)
	if diagnosticErr != nil {
		if _, sqlError := errors.Cause(diagnosticErr).(*gmysql.MySQLError); !sqlError {
			ctx.Value(summarySessionKey{}).(*summarySession).close()
		}
	}
	if diagnosticErr == nil && json.Unmarshal([]byte(raw), &info) == nil && info.Result.Valid() && info.Result.Records == uint64(len(task.rows)) {
		outcomes = pkdbsummary.Outcomes{Imported: info.Result.Imported, Replaced: info.Result.Replaced, Skipped: info.Result.Skipped, Error: info.Result.Error}
	}
	p.Ledger.FinishWrite(ctx, p.Key, end, outcomes, 0, 0)
	return result, nil
}

func (be *tidbBackend) execOnConnection(ctx context.Context, task stmtTask, conn *sql.Conn) (sql.Result, error) {
	var result sql.Result
	var err error
	query := task.stmt
	if conn != nil {
		if be.stmtCache != nil {
			session := ctx.Value(summarySessionKey{}).(*summarySession)
			stmt, e := session.prepare(ctx, query)
			if e != nil {
				return nil, &stmtSetupError{e}
			}
			return stmt.ExecContext(ctx, task.values...)
		}
		return conn.ExecContext(ctx, query)
	}
	if be.stmtCache != nil {
		var prepStmt *sql.Stmt
		key := &stmtKey{query: query}
		be.stmtCacheMutex.RLock()
		stmt, ok := be.stmtCache.Get(key)
		be.stmtCacheMutex.RUnlock()
		if ok {
			prepStmt = stmt.(*sql.Stmt)
		} else if stmt, err := be.db.PrepareContext(ctx, query); err == nil {
			be.stmtCacheMutex.Lock()
			// check again if the key is already in the cache
			// to avoid override existing stmt without closing it
			if cachedStmt, ok := be.stmtCache.Get(key); !ok {
				prepStmt = stmt
				be.stmtCache.Put(key, stmt)
			} else {
				prepStmt = cachedStmt.(*sql.Stmt)
				stmt.Close()
			}
			be.stmtCacheMutex.Unlock()
		} else {
			return nil, &stmtSetupError{errors.Trace(err)}
		}
		result, err = prepStmt.ExecContext(ctx, task.values...)
	} else {
		if conn != nil {
			result, err = conn.ExecContext(ctx, query)
		} else {
			result, err = be.db.ExecContext(ctx, query)
		}
	}
	return result, err
}

type summarySessionKey struct{}

type summarySession struct {
	conn       *sql.Conn
	statements map[string]*sql.Stmt
}

func (s *summarySession) prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	if stmt := s.statements[query]; stmt != nil {
		return stmt, nil
	}
	stmt, err := s.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	// Bound statement shapes per writer, independently of other connections.
	if len(s.statements) >= int(prepStmtCacheSize) {
		for key, old := range s.statements {
			_ = old.Close()
			delete(s.statements, key)
			break
		}
	}
	s.statements[query] = stmt
	return stmt, nil
}

func (s *summarySession) close() {
	for _, stmt := range s.statements {
		_ = stmt.Close()
	}
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.statements = make(map[string]*sql.Stmt)
}

func rejectSummaryRows(ctx context.Context, rows tidbRows, err error) {
	p := pkdbsummary.FromContext(ctx)
	if p == nil {
		return
	}
	if _, ok := errors.Cause(err).(*gmysql.MySQLError); !ok || common.IsRetryableError(err) {
		return
	}
	offsets := make([]int64, 0, len(rows))
	for _, row := range rows {
		offsets = append(offsets, row.offset)
	}
	p.Ledger.Reject(ctx, p.Key, offsets)
}

func recordConsumedSummaryRow(ctx context.Context, task stmtTask, writeErr error) {
	p := pkdbsummary.FromContext(ctx)
	if p == nil {
		return
	}
	// The error manager has accepted this row's omission. Duplicate policy skips
	// are distinct from consumed conversion/write errors even when INSERT is used
	// row-by-row to collect detailed conflict diagnostics.
	outcome := pkdbsummary.Outcomes{Error: 1}
	if isDupEntryError(writeErr) {
		outcome = pkdbsummary.Outcomes{Skipped: 1}
	}
	p.Ledger.FinishWrite(ctx, p.Key, task.rows[len(task.rows)-1].offset, outcome, 0, 0)
}
