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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	txnFileSchema          = "starter_txn_file"
	txnFileTable           = txnFileSchema + ".commit_case"
	txnFileValueSize       = 48 * 1024
	txnFileChunkSize       = 256 * 1024
	txnFileMutationLimit   = 1024 * 1024
	preparedInsertOverhead = 31
)

type txnFileRow struct {
	id    int64
	value []byte
}

type txnFileSummary struct {
	count, totalLength, exactRows int
}

func TestExternalStarterTxnFileCommitAcrossChunksAndRegions(t *testing.T) {
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
	require.NoError(t, execSQL(ctx, db, "CREATE TABLE "+txnFileTable+" (id BIGINT PRIMARY KEY, v LONGBLOB)"))
	require.NoError(t, execSQL(ctx, db, "SPLIT TABLE "+txnFileTable+" BY (1000), (2000)"))

	regionIDs, err := waitForTxnFileRegions(ctx, db)
	require.NoError(t, err)
	require.Len(t, map[uint64]struct{}{regionIDs[0]: {}, regionIDs[1]: {}, regionIDs[2]: {}}, 3)
	t.Logf("txn-file representative_ids=[1 1001 2001] region_ids=%v", regionIDs)

	expected := makeTxnFileRows('a')
	payload := 0
	for _, row := range expected {
		require.Less(t, len(row.value)+preparedInsertOverhead, 65536)
		payload += len(row.value)
	}
	minimumChunks := (payload + txnFileChunkSize - 1) / txnFileChunkSize
	require.Greater(t, payload, txnFileMutationLimit)
	require.GreaterOrEqual(t, minimumChunks, 4)
	t.Logf("txn-file rows=%d value_bytes=%d payload_bytes=%d chunk_size=%d minimum_chunks=%d packet_overhead=%d", len(expected), txnFileValueSize, payload, txnFileChunkSize, minimumChunks, preparedInsertOverhead)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, enableTxnFileSession(ctx, conn))
	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, insertTxnFileRows(ctx, tx, expected))

	before, err := readTxnFileCounters(ctx, statusURL)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	after, err := readTxnFileCounters(ctx, statusURL)
	require.NoError(t, err)
	require.Equal(t, float64(1), after.ok-before.ok)
	require.Equal(t, float64(0), after.err-before.err)
	t.Logf("txn-file metric_delta ok=%.0f err=%.0f", after.ok-before.ok, after.err-before.err)

	actual, err := readTxnFileSummary(ctx, db)
	require.NoError(t, err)
	require.Equal(t, txnFileSummary{count: 24, totalLength: payload, exactRows: 24}, actual)
	for _, row := range []txnFileRow{expected[0], expected[8], expected[16]} {
		var value []byte
		require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM "+txnFileTable+" WHERE id=?", row.id).Scan(&value))
		require.Equal(t, row.value, value)
		t.Logf("txn-file representative id=%d first_byte=%02x last_byte=%02x length=%d", row.id, value[0], value[len(value)-1], len(value))
	}
	t.Logf("txn-file data rows=%d total_length=%d exact_rows=%d", actual.count, actual.totalLength, actual.exactRows)
}

func waitForTxnFileRegions(ctx context.Context, db *sql.DB) ([3]uint64, error) {
	regionCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		ids, complete, err := queryTxnFileRegionIDs(regionCtx, db)
		if err != nil {
			return ids, err
		}
		if complete {
			return ids, nil
		}
		select {
		case <-regionCtx.Done():
			return ids, fmt.Errorf("wait for three txn-file regions: %w", regionCtx.Err())
		case <-ticker.C:
		}
	}
}

func queryTxnFileRegionIDs(ctx context.Context, db *sql.DB) ([3]uint64, bool, error) {
	rows, err := db.QueryContext(ctx, "SHOW TABLE "+txnFileTable+" REGIONS")
	if err != nil {
		return [3]uint64{}, false, fmt.Errorf("show txn-file regions: %w", err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return [3]uint64{}, false, fmt.Errorf("read txn-file region columns: %w", err)
	}
	regionIDIndex, startKeyIndex, endKeyIndex := -1, -1, -1
	for i, column := range columns {
		switch {
		case strings.EqualFold(column, "region_id"):
			regionIDIndex = i
		case strings.EqualFold(column, "start_key"):
			startKeyIndex = i
		case strings.EqualFold(column, "end_key"):
			endKeyIndex = i
		}
	}
	if regionIDIndex < 0 || startKeyIndex < 0 || endKeyIndex < 0 {
		return [3]uint64{}, false, fmt.Errorf("show txn-file regions returned columns %v without region_id, start_key, and end_key", columns)
	}
	var ids [3]uint64
	for rows.Next() {
		var regionID uint64
		var startKey, endKey string
		destinations := make([]any, len(columns))
		for i := range destinations {
			destinations[i] = new(any)
		}
		destinations[regionIDIndex] = &regionID
		destinations[startKeyIndex] = &startKey
		destinations[endKeyIndex] = &endKey
		if err := rows.Scan(destinations...); err != nil {
			return ids, false, fmt.Errorf("scan txn-file region: %w", err)
		}
		switch {
		case strings.HasSuffix(endKey, "_r_1000"):
			ids[0] = regionID
		case strings.HasSuffix(startKey, "_r_1000"):
			ids[1] = regionID
		case strings.HasSuffix(startKey, "_r_2000"):
			ids[2] = regionID
		}
	}
	if err := rows.Err(); err != nil {
		return ids, false, fmt.Errorf("iterate txn-file regions: %w", err)
	}
	return ids, ids[0] != 0 && ids[1] != 0 && ids[2] != 0 && ids[0] != ids[1] && ids[0] != ids[2] && ids[1] != ids[2], nil
}

func makeTxnFileRows(base byte) []txnFileRow {
	rows := make([]txnFileRow, 0, 24)
	for rangeIndex, firstID := range []int64{1, 1001, 2001} {
		value := bytes.Repeat([]byte{base + byte(rangeIndex)}, txnFileValueSize)
		for offset := range int64(8) {
			rows = append(rows, txnFileRow{id: firstID + offset, value: value})
		}
	}
	return rows
}

func enableTxnFileSession(ctx context.Context, conn *sql.Conn) error {
	statement := "SET SESSION tidb_txn_mode='optimistic', SESSION tidb_txn_assertion_level='OFF', SESSION tidb_disable_txn_file=OFF, SESSION tidb_txn_file_min_mutation_size=1048576"
	if _, err := conn.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("configure txn-file session: %w", err)
	}
	return nil
}

func insertTxnFileRows(ctx context.Context, tx *sql.Tx, rows []txnFileRow) error {
	for _, row := range rows {
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+txnFileTable+" (id, v) VALUES (?, ?)", row.id, row.value); err != nil {
			return fmt.Errorf("insert txn-file row %d: %w", row.id, err)
		}
	}
	return nil
}

func readTxnFileSummary(ctx context.Context, db *sql.DB) (txnFileSummary, error) {
	query := "SELECT COUNT(*), COALESCE(SUM(OCTET_LENGTH(v)),0), COALESCE(SUM(CASE WHEN " +
		"(id BETWEEN 1 AND 8 AND v=REPEAT(?,?)) OR " +
		"(id BETWEEN 1001 AND 1008 AND v=REPEAT(?,?)) OR " +
		"(id BETWEEN 2001 AND 2008 AND v=REPEAT(?,?)) THEN 1 ELSE 0 END),0) FROM " + txnFileTable
	var summary txnFileSummary
	if err := db.QueryRowContext(ctx, query, []byte{'a'}, txnFileValueSize, []byte{'b'}, txnFileValueSize, []byte{'c'}, txnFileValueSize).
		Scan(&summary.count, &summary.totalLength, &summary.exactRows); err != nil {
		return summary, fmt.Errorf("read txn-file summary: %w", err)
	}
	return summary, nil
}

func dropTxnFileSchema(ctx context.Context, db *sql.DB) error {
	return execSQL(ctx, db, "DROP DATABASE IF EXISTS "+txnFileSchema)
}
