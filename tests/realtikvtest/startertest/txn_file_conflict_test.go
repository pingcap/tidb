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

package startertest

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

const txnFileConflictTable = txnFileSchema + ".conflict_case"

func TestExternalStarterTxnFileWriteConflictRollsBack(t *testing.T) {
	statusURL := requireStarterStatusURL(t)
	db := openStarterDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	require.NoError(t, dropTxnFileSchema(ctx, db))
	require.NoError(t, execSQL(ctx, db, "CREATE DATABASE "+txnFileSchema))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		require.NoError(t, dropTxnFileSchema(cleanupCtx, db))
	})
	require.NoError(t, execSQL(ctx, db, "CREATE TABLE "+txnFileConflictTable+" (id BIGINT PRIMARY KEY, v LONGBLOB)"))
	require.NoError(t, execSQL(ctx, db, "SPLIT TABLE "+txnFileConflictTable+" BY (1000), (2000)"))

	baseline := makeTxnFileRows('d')
	losing := makeTxnFileRows('g')
	winner := bytes.Repeat([]byte{'w'}, txnFileValueSize)
	seedConn := openTxnFileConn(ctx, t, db)
	require.NoError(t, disableTxnFileSession(ctx, seedConn))
	seedTx, err := seedConn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer seedTx.Rollback()
	require.NoError(t, insertTxnFileConflictRows(ctx, seedTx, baseline))
	require.NoError(t, seedTx.Commit())

	loserConn := openTxnFileConn(ctx, t, db)
	require.NoError(t, enableTxnFileSession(ctx, loserConn))
	loserTx, err := loserConn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer loserTx.Rollback()
	for _, row := range losing {
		_, err := loserTx.ExecContext(ctx, "UPDATE "+txnFileConflictTable+" SET v=? WHERE id=?", row.value, row.id)
		require.NoError(t, err)
	}

	winnerConn := openTxnFileConn(ctx, t, db)
	require.NoError(t, disableTxnFileSession(ctx, winnerConn))
	var winnerMode string
	require.NoError(t, winnerConn.QueryRowContext(ctx, "SELECT @@tidb_txn_mode").Scan(&winnerMode))
	require.Equal(t, "pessimistic", winnerMode)
	winnerTx, err := winnerConn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer winnerTx.Rollback()
	_, err = winnerTx.ExecContext(ctx, "UPDATE "+txnFileConflictTable+" SET v=? WHERE id=1001", winner)
	require.NoError(t, err)
	require.NoError(t, winnerTx.Commit())

	before, err := readTxnFileCounters(ctx, statusURL)
	require.NoError(t, err)
	err = loserTx.Commit()
	var mysqlErr *mysql.MySQLError
	require.Error(t, err)
	require.True(t, errors.As(err, &mysqlErr), "expected MySQL write-conflict error, got %T: %v", err, err)
	require.Equal(t, uint16(9007), mysqlErr.Number)
	after, err := readTxnFileCounters(ctx, statusURL)
	require.NoError(t, err)
	require.Equal(t, float64(0), after.ok-before.ok)
	require.Equal(t, float64(1), after.err-before.err)
	t.Logf("txn-file conflict mysql_error=%d metric_delta ok=%.0f err=%.0f", mysqlErr.Number, after.ok-before.ok, after.err-before.err)

	requireTxnFileConflictState(ctx, t, db, baseline, losing, winner)
	probeTxnFileConflictLocks(ctx, t, db)
}

func openTxnFileConn(ctx context.Context, t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func disableTxnFileSession(ctx context.Context, conn *sql.Conn) error {
	statement := "SET SESSION tidb_txn_mode='pessimistic', SESSION tidb_txn_assertion_level='OFF', SESSION tidb_disable_txn_file=ON, SESSION tidb_txn_file_min_mutation_size=1048576"
	if _, err := conn.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("configure non-txn-file session: %w", err)
	}
	return nil
}

func insertTxnFileConflictRows(ctx context.Context, tx *sql.Tx, rows []txnFileRow) error {
	for _, row := range rows {
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+txnFileConflictTable+" (id, v) VALUES (?, ?)", row.id, row.value); err != nil {
			return fmt.Errorf("insert txn-file conflict row %d: %w", row.id, err)
		}
	}
	return nil
}

func requireTxnFileConflictState(ctx context.Context, t *testing.T, db *sql.DB, baseline, losing []txnFileRow, winner []byte) {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT id, v FROM "+txnFileConflictTable+" ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()
	rowIndex := 0
	totalLength := 0
	losingRows := 0
	for rows.Next() {
		var id int64
		var value []byte
		require.NoError(t, rows.Scan(&id, &value))
		require.Less(t, rowIndex, len(baseline))
		require.Equal(t, baseline[rowIndex].id, id)
		expected := baseline[rowIndex].value
		if id == 1001 {
			expected = winner
		}
		require.Equal(t, expected, value)
		if bytes.Equal(value, losing[rowIndex].value) {
			losingRows++
		}
		totalLength += len(value)
		rowIndex++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 24, rowIndex)
	require.Equal(t, 24*txnFileValueSize, totalLength)
	require.Zero(t, losingRows)
	t.Logf("txn-file conflict state rows=%d baseline_rows=23 winner_rows=1 losing_rows=%d total_length=%d", rowIndex, losingRows, totalLength)
}

func probeTxnFileConflictLocks(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn := openTxnFileConn(probeCtx, t, db)
	require.NoError(t, disableTxnFileSession(probeCtx, conn))
	_, err := conn.ExecContext(probeCtx, "BEGIN PESSIMISTIC")
	require.NoError(t, err)
	defer func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rollbackCancel()
		_, _ = conn.ExecContext(rollbackCtx, "ROLLBACK")
	}()
	rows, err := conn.QueryContext(probeCtx, "SELECT id FROM "+txnFileConflictTable+" WHERE id IN (1,1001,2001) ORDER BY id FOR UPDATE NOWAIT")
	require.NoError(t, err)
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int64{1, 1001, 2001}, ids)
	require.NoError(t, rows.Close())
	_, err = conn.ExecContext(probeCtx, "ROLLBACK")
	require.NoError(t, err)
	t.Logf("txn-file conflict lock_probe mode=pessimistic nowait_ids=%v result=success", ids)
}
