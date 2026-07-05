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

package session

import (
	"bytes"
	"context"

	stderrors "errors"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	tikverr "github.com/tikv/client-go/v2/error"
)

const nonTransactionalDMLCheckpointTableName = "tidb_nontransactional_dml_checkpoint"

const createNonTransactionalDMLCheckpointTableSQL = `CREATE TABLE IF NOT EXISTS mysql.tidb_nontransactional_dml_checkpoint (
	job_id VARCHAR(128) NOT NULL,
	range_id BIGINT(64) NOT NULL,
	mode VARCHAR(16) NOT NULL,
	dml_type VARCHAR(16) NOT NULL,
	db_name VARCHAR(64) NOT NULL,
	table_id BIGINT(64) NOT NULL,
	physical_table_id BIGINT(64) NOT NULL,
	handle_kind VARCHAR(32) NOT NULL,
	range_start BLOB DEFAULT NULL,
	range_start_inclusive TINYINT(1) NOT NULL DEFAULT 0,
	range_end BLOB DEFAULT NULL,
	range_end_inclusive TINYINT(1) NOT NULL DEFAULT 0,
	checkpoint BLOB DEFAULT NULL,
	status VARCHAR(16) NOT NULL,
	retry_count BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
	error_class VARCHAR(64) DEFAULT NULL,
	error_text TEXT DEFAULT NULL,
	scanned BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
	affected BIGINT(64) UNSIGNED NOT NULL DEFAULT 0,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	finished_at TIMESTAMP NULL DEFAULT NULL,
	PRIMARY KEY(job_id, range_id),
	KEY idx_table_status(table_id, status),
	KEY idx_updated_at(updated_at)
);`

type nonTransactionalDMLCheckpointStatus string

const (
	nonTransactionalDMLCheckpointDone   nonTransactionalDMLCheckpointStatus = "done"
	nonTransactionalDMLCheckpointFailed nonTransactionalDMLCheckpointStatus = "failed"
)

type nonTransactionalDMLCheckpoint struct {
	JobID           string
	RangeID         int64
	Mode            string
	DMLType         string
	DBName          string
	TableID         int64
	PhysicalTableID int64
	HandleKind      nonTransactionalDMLHandleKind
	Lower           *nonTransactionalDMLBoundary
	Upper           *nonTransactionalDMLBoundary
	Checkpoint      *nonTransactionalDMLBoundary
	Status          nonTransactionalDMLCheckpointStatus
	RetryCount      uint64
	ErrorClass      string
	ErrorText       string
	Scanned         uint64
	Affected        uint64
}

type nonTransactionalDMLCheckpointSummary struct {
	TotalRanges  uint64
	DoneRanges   uint64
	FailedRanges uint64
	RetryCount   uint64
	Scanned      uint64
	Affected     uint64
}

func writeNonTransactionalDMLCheckpoint(ctx context.Context, se sessiontypes.Session, checkpoint nonTransactionalDMLCheckpoint) error {
	rangeStart, rangeStartInclusive := nonTransactionalDMLBoundarySQLValues(checkpoint.Lower)
	rangeEnd, rangeEndInclusive := nonTransactionalDMLBoundarySQLValues(checkpoint.Upper)
	progress, _ := nonTransactionalDMLBoundarySQLValues(checkpoint.Checkpoint)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	_, err := sqlexec.ExecSQL(ctx, se, `
		INSERT INTO mysql.tidb_nontransactional_dml_checkpoint (
			job_id, range_id, mode, dml_type, db_name, table_id, physical_table_id,
			handle_kind, range_start, range_start_inclusive, range_end, range_end_inclusive,
			checkpoint, status, retry_count, error_class, error_text, scanned, affected, finished_at
		)
		VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?,
			IF(%? IN ('done', 'failed'), CURRENT_TIMESTAMP, NULL))
		ON DUPLICATE KEY UPDATE
			mode=VALUES(mode),
			dml_type=VALUES(dml_type),
			db_name=VALUES(db_name),
			table_id=VALUES(table_id),
			physical_table_id=VALUES(physical_table_id),
			handle_kind=VALUES(handle_kind),
			range_start=VALUES(range_start),
			range_start_inclusive=VALUES(range_start_inclusive),
			range_end=VALUES(range_end),
			range_end_inclusive=VALUES(range_end_inclusive),
			checkpoint=VALUES(checkpoint),
			status=VALUES(status),
			retry_count=VALUES(retry_count),
			error_class=VALUES(error_class),
			error_text=VALUES(error_text),
			scanned=VALUES(scanned),
			affected=VALUES(affected),
			finished_at=VALUES(finished_at)`,
		checkpoint.JobID,
		checkpoint.RangeID,
		checkpoint.Mode,
		checkpoint.DMLType,
		checkpoint.DBName,
		checkpoint.TableID,
		checkpoint.PhysicalTableID,
		string(checkpoint.HandleKind),
		rangeStart,
		rangeStartInclusive,
		rangeEnd,
		rangeEndInclusive,
		progress,
		string(checkpoint.Status),
		checkpoint.RetryCount,
		nullStringForNonTransactionalDMLCheckpoint(checkpoint.ErrorClass),
		nullStringForNonTransactionalDMLCheckpoint(checkpoint.ErrorText),
		checkpoint.Scanned,
		checkpoint.Affected,
		string(checkpoint.Status),
	)
	return perrors.Trace(err)
}

func loadNonTransactionalDMLCheckpoint(ctx context.Context, se sessiontypes.Session, jobID string, rangeID int64, desc *nonTransactionalDMLHandleDescriptor) (*nonTransactionalDMLCheckpoint, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	rows, err := sqlexec.ExecSQL(ctx, se, `
		SELECT job_id, range_id, mode, dml_type, db_name, table_id, physical_table_id,
			handle_kind, range_start, range_start_inclusive, range_end, range_end_inclusive,
			checkpoint, status, retry_count, error_class, error_text, scanned, affected
		FROM mysql.tidb_nontransactional_dml_checkpoint
		WHERE job_id=%? AND range_id=%?`,
		jobID,
		rangeID,
	)
	if err != nil {
		return nil, perrors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	checkpoint, err := nonTransactionalDMLCheckpointFromRow(se.GetSessionVars().StmtCtx, desc, rows[0])
	if err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func summarizeNonTransactionalDMLCheckpoints(ctx context.Context, se sessiontypes.Session, jobID string) (nonTransactionalDMLCheckpointSummary, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	rows, err := sqlexec.ExecSQL(ctx, se, `
		SELECT status, retry_count, scanned, affected
		FROM mysql.tidb_nontransactional_dml_checkpoint
		WHERE job_id=%?`,
		jobID,
	)
	if err != nil {
		return nonTransactionalDMLCheckpointSummary{}, perrors.Trace(err)
	}
	var summary nonTransactionalDMLCheckpointSummary
	for _, row := range rows {
		summary.TotalRanges++
		switch nonTransactionalDMLCheckpointStatus(row.GetString(0)) {
		case nonTransactionalDMLCheckpointDone:
			summary.DoneRanges++
		case nonTransactionalDMLCheckpointFailed:
			summary.FailedRanges++
		}
		summary.RetryCount += row.GetUint64(1)
		summary.Scanned += row.GetUint64(2)
		summary.Affected += row.GetUint64(3)
	}
	return summary, nil
}

func deleteNonTransactionalDMLCheckpoints(ctx context.Context, se sessiontypes.Session, jobID string) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	_, err := sqlexec.ExecSQL(ctx, se, `
		DELETE FROM mysql.tidb_nontransactional_dml_checkpoint
		WHERE job_id=%?`,
		jobID,
	)
	return perrors.Trace(err)
}

func deleteNonTransactionalDMLCheckpoint(ctx context.Context, se sessiontypes.Session, jobID string, rangeID int64) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	_, err := sqlexec.ExecSQL(ctx, se, `
		DELETE FROM mysql.tidb_nontransactional_dml_checkpoint
		WHERE job_id=%? AND range_id=%?`,
		jobID,
		rangeID,
	)
	return perrors.Trace(err)
}

func nonTransactionalDMLCheckpointCoversBoundary(ctx context.Context, se sessiontypes.Session, jobID string, rangeID int64,
	desc *nonTransactionalDMLHandleDescriptor, boundary nonTransactionalDMLBoundary) (bool, error) {
	checkpoint, err := loadNonTransactionalDMLCheckpoint(ctx, se, jobID, rangeID, desc)
	if err != nil {
		return false, err
	}
	if checkpoint == nil || checkpoint.Status != nonTransactionalDMLCheckpointDone || checkpoint.Checkpoint == nil || !checkpoint.Checkpoint.hasValue {
		return false, nil
	}
	return bytes.Compare(checkpoint.Checkpoint.encoded, boundary.encoded) >= 0, nil
}

func nonTransactionalDMLCheckpointFromRow(sc *stmtctx.StatementContext, desc *nonTransactionalDMLHandleDescriptor, row chunk.Row) (nonTransactionalDMLCheckpoint, error) {
	lower, err := nonTransactionalDMLBoundaryFromCheckpointRow(sc, desc, row, 8, 9)
	if err != nil {
		return nonTransactionalDMLCheckpoint{}, err
	}
	upper, err := nonTransactionalDMLBoundaryFromCheckpointRow(sc, desc, row, 10, 11)
	if err != nil {
		return nonTransactionalDMLCheckpoint{}, err
	}
	progress, err := nonTransactionalDMLBoundaryFromCheckpointRow(sc, desc, row, 12, -1)
	if err != nil {
		return nonTransactionalDMLCheckpoint{}, err
	}
	checkpoint := nonTransactionalDMLCheckpoint{
		JobID:           row.GetString(0),
		RangeID:         row.GetInt64(1),
		Mode:            row.GetString(2),
		DMLType:         row.GetString(3),
		DBName:          row.GetString(4),
		TableID:         row.GetInt64(5),
		PhysicalTableID: row.GetInt64(6),
		HandleKind:      nonTransactionalDMLHandleKind(row.GetString(7)),
		Lower:           lower,
		Upper:           upper,
		Checkpoint:      progress,
		Status:          nonTransactionalDMLCheckpointStatus(row.GetString(13)),
		RetryCount:      row.GetUint64(14),
		Scanned:         row.GetUint64(17),
		Affected:        row.GetUint64(18),
	}
	if !row.IsNull(15) {
		checkpoint.ErrorClass = row.GetString(15)
	}
	if !row.IsNull(16) {
		checkpoint.ErrorText = row.GetString(16)
	}
	return checkpoint, nil
}

func nonTransactionalDMLBoundaryFromCheckpointRow(sc *stmtctx.StatementContext, desc *nonTransactionalDMLHandleDescriptor, row chunk.Row, valueIdx int, inclusiveIdx int) (*nonTransactionalDMLBoundary, error) {
	if row.IsNull(valueIdx) {
		return nil, nil
	}
	inclusive := false
	if inclusiveIdx >= 0 {
		inclusive = row.GetInt64(inclusiveIdx) != 0
	}
	encoded := append([]byte(nil), row.GetBytes(valueIdx)...)
	boundary, err := decodeNonTransactionalDMLBoundary(sc, desc, encoded, inclusive)
	if err != nil {
		return nil, err
	}
	return &boundary, nil
}

func nonTransactionalDMLBoundarySQLValues(boundary *nonTransactionalDMLBoundary) (any, int64) {
	if boundary == nil || !boundary.hasValue {
		return nil, 0
	}
	return append([]byte(nil), boundary.encoded...), boolToInt64(boundary.inclusive)
}

func nullStringForNonTransactionalDMLCheckpoint(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func boolToInt64(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func isNonTransactionalDMLRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if kv.IsTxnRetryableError(err) {
		return true
	}
	if storeerr.ErrLockWaitTimeout.Equal(err) || storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err) || storeerr.ErrResolveLockTimeout.Equal(err) {
		return true
	}
	var deadlock *tikverr.ErrDeadlock
	if stderrors.As(err, &deadlock) {
		return deadlock.IsRetryable
	}
	if cause := perrors.Cause(err); cause != err && stderrors.As(cause, &deadlock) {
		return deadlock.IsRetryable
	}
	return false
}
