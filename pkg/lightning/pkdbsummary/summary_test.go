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

package pkdbsummary

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

type memoryStore struct {
	data []byte
	fail bool
}

func (s *memoryStore) Load(context.Context) ([]byte, error) {
	return append([]byte(nil), s.data...), nil
}
func (s *memoryStore) Save(_ context.Context, data []byte) error {
	if s.fail {
		return errors.New("statistics storage unavailable")
	}
	s.data = append([]byte(nil), data...)
	return nil
}

func assertConservation(t *testing.T, result Result, total uint64) {
	t.Helper()
	require.NotNil(t, result.Total)
	require.Equal(t, total, *result.Total)
	require.NotNil(t, result.Unresolved)
	require.Equal(t, total, result.Outcomes.Count()+*result.Unresolved)
}

// TestLedgerSourceOutcomes reads five sources, acknowledges insert/replace,
// skips a duplicate, rejects a conversion, and leaves one pending source.
func TestLedgerSourceOutcomes(t *testing.T) {
	ctx := context.Background()
	ledger := Open(ctx, nil, false)
	ledger.Ensure("chunk", "table", 0, 0)
	for offset := int64(0); offset < 5; offset++ {
		require.True(t, ledger.Observe("chunk", offset, Outcomes{}))
	}
	ledger.BeginWrite(ctx, "chunk", 0)
	ledger.FinishWrite(ctx, "chunk", 0, Outcomes{Imported: 1}, 0, 0)
	ledger.BeginWrite(ctx, "chunk", 1)
	ledger.FinishWrite(ctx, "chunk", 1, Outcomes{Replaced: 1}, 0, 0)
	ledger.ClassifyRead("chunk", 2, true, Outcomes{Skipped: 1})
	ledger.ClassifyRead("chunk", 3, true, Outcomes{Error: 1})
	tables, result := ledger.Snapshot()
	assertConservation(t, result, 5)
	require.Equal(t, Outcomes{Imported: 1, Replaced: 1, Skipped: 1, Error: 1}, result.Outcomes)
	require.Equal(t, uint64(1), *result.Unresolved)
	require.False(t, result.Complete)
	require.Equal(t, result, tables["table"])
}

// TestLedgerResume preserves acknowledged outcomes when a control checkpoint
// lags, then counts a new source once and keeps an earlier insertion cumulative.
func TestLedgerResume(t *testing.T) {
	ctx := context.Background()
	store := &memoryStore{}
	ledger := Open(ctx, store, false)
	ledger.Ensure("chunk", "table", 0, 0)
	ledger.Observe("chunk", 0, Outcomes{})
	ledger.BeginWrite(ctx, "chunk", 0)
	ledger.FinishWrite(ctx, "chunk", 0, Outcomes{Imported: 1}, 0, 0)
	require.NoError(t, ledger.Close(ctx))
	ledger = Open(ctx, store, true)
	ledger.Ensure("chunk", "table", 0, 0)
	require.False(t, ledger.Observe("chunk", 0, Outcomes{}))
	ledger.FinishWrite(ctx, "chunk", 0, Outcomes{Replaced: 1}, 0, 0)
	ledger.Observe("chunk", 1, Outcomes{})
	ledger.BeginWrite(ctx, "chunk", 1)
	ledger.FinishWrite(ctx, "chunk", 1, Outcomes{Replaced: 1}, 0, 0)
	_, result := ledger.Snapshot()
	assertConservation(t, result, 2)
	require.Equal(t, Outcomes{Imported: 1, Replaced: 1}, result.Outcomes)
	require.True(t, result.Complete)
}

// TestLedgerUnconfirmedCommit resumes a fenced, unacknowledged input. Replaying
// it as REPLACE must not classify an insertion whose original commit is unknown.
func TestLedgerUnconfirmedCommit(t *testing.T) {
	ctx := context.Background()
	store := &memoryStore{}
	ledger := Open(ctx, store, false)
	ledger.Ensure("chunk", "table", 0, 0)
	ledger.Observe("chunk", 0, Outcomes{})
	ledger.BeginWrite(ctx, "chunk", 0)
	require.NoError(t, ledger.Close(ctx))
	ledger = Open(ctx, store, true)
	ledger.Ensure("chunk", "table", 0, 0)
	ledger.Observe("chunk", 0, Outcomes{})
	ledger.FinishWrite(ctx, "chunk", 0, Outcomes{Replaced: 1}, 0, 0)
	_, result := ledger.Snapshot()
	assertConservation(t, result, 1)
	require.Zero(t, result.Outcomes.Count())
	require.Equal(t, uint64(1), *result.Unresolved)
}

// TestLedgerFailedRecordRetry repairs a known conversion failure on resume;
// its old Error classification is removed before the confirmed insertion.
func TestLedgerFailedRecordRetry(t *testing.T) {
	ctx := context.Background()
	store := &memoryStore{}
	ledger := Open(ctx, store, false)
	ledger.Ensure("chunk", "table", 0, 0)
	fresh := ledger.Observe("chunk", 0, Outcomes{})
	ledger.ClassifyRead("chunk", 0, fresh, Outcomes{Error: 1})
	require.NoError(t, ledger.Close(ctx))
	ledger = Open(ctx, store, true)
	ledger.Ensure("chunk", "table", 0, 0)
	require.True(t, ledger.Observe("chunk", 0, Outcomes{}))
	ledger.BeginWrite(ctx, "chunk", 0)
	ledger.FinishWrite(ctx, "chunk", 0, Outcomes{Imported: 1}, 0, 0)
	_, result := ledger.Snapshot()
	assertConservation(t, result, 1)
	require.Equal(t, Outcomes{Imported: 1}, result.Outcomes)
}

// TestLedgerLocalAcknowledgement stages both local outcomes, restores them,
// and confirms them only after target data/index ingestion is acknowledged.
func TestLedgerLocalAcknowledgement(t *testing.T) {
	ctx := context.Background()
	store := &memoryStore{}
	ledger := Open(ctx, store, false)
	ledger.Ensure("chunk", "table", 0, 0)
	ledger.Observe("chunk", 0, Outcomes{})
	ledger.Observe("chunk", 1, Outcomes{})
	ledger.FinishWrite(ctx, "chunk", 1, Outcomes{}, 1, 1)
	_, result := ledger.Snapshot()
	assertConservation(t, result, 2)
	require.Zero(t, result.Outcomes.Count())
	require.NoError(t, ledger.Close(ctx))
	ledger = Open(ctx, store, true)
	ledger.ConfirmTable(ctx, "table")
	ledger.ConfirmTable(ctx, "table")
	_, result = ledger.Snapshot()
	assertConservation(t, result, 2)
	require.Equal(t, Outcomes{Imported: 1, Replaced: 1}, result.Outcomes)
}

// TestLedgerMissingHistory covers old checkpoints and a process killed after a
// durable write fence; both retain confirmed values without inventing a total.
func TestLedgerMissingHistory(t *testing.T) {
	ctx := context.Background()
	old := Open(ctx, nil, true)
	old.Ensure("chunk", "table", 0, 10)
	_, result := old.Snapshot()
	require.Nil(t, result.Total)
	require.Nil(t, result.Unresolved)
	store := &memoryStore{}
	ledger := Open(ctx, store, false)
	ledger.Ensure("chunk", "table", 0, 0)
	ledger.Observe("chunk", 0, Outcomes{})
	ledger.BeginWrite(ctx, "chunk", 0)
	ledger.FinishWrite(ctx, "chunk", 0, Outcomes{Imported: 1}, 0, 0)
	// Do not close: simulate a crash with a potentially unpersisted read-ahead tail.
	recovered := Open(ctx, store, true)
	_, result = recovered.Snapshot()
	require.Nil(t, result.Total)
	require.Equal(t, uint64(1), result.Imported)
	require.False(t, result.Complete)
}

// TestAbsoluteJobSnapshots polls the same job twice, replaces a retry generation,
// and restores a finished table after other tables resume.
func TestAbsoluteJobSnapshots(t *testing.T) {
	ctx := context.Background()
	store := &memoryStore{}
	ledger := OpenSnapshots(ctx, store, "group")
	first := NewResult(3, true, Outcomes{Imported: 2, Skipped: 1})
	ledger.ReplaceResult(ctx, "a", "a", first)
	ledger.ReplaceResult(ctx, "a", "a", first)
	ledger.ReplaceResult(ctx, "b", "b", NewResult(2, true, Outcomes{}))
	_, result := ledger.Snapshot()
	assertConservation(t, result, 5)
	ledger.ReplaceResult(ctx, "b", "b", NewResult(2, true, Outcomes{Imported: 2}))
	require.NoError(t, ledger.Close(ctx))
	ledger = OpenSnapshots(ctx, store, "group")
	_, result = ledger.Snapshot()
	assertConservation(t, result, 5)
	require.Equal(t, uint64(4), result.Imported)
	fresh := OpenSnapshots(ctx, store, "another-group")
	_, result = fresh.Snapshot()
	assertConservation(t, result, 0)
}

// TestLedgerStatisticsFailure verifies that confirmed writes remain available
// when only the statistics store fails, and that Close reports incompleteness.
func TestLedgerStatisticsFailure(t *testing.T) {
	ctx := context.Background()
	store := &memoryStore{}
	ledger := Open(ctx, store, false)
	ledger.Ensure("chunk", "table", 0, 0)
	ledger.Observe("chunk", 0, Outcomes{})
	store.fail = true
	ledger.BeginWrite(ctx, "chunk", 0)
	ledger.FinishWrite(ctx, "chunk", 0, Outcomes{Imported: 1}, 0, 0)
	require.Error(t, ledger.Close(ctx))
	_, result := ledger.Snapshot()
	require.Equal(t, uint64(1), result.Imported)
	require.False(t, result.Complete)
}

// TestSQLStatisticsStore restores independent chunk entries, then verifies that
// metadata and dirty chunks commit together and roll back together on failure.
func TestSQLStatisticsStore(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := NewSQLStore(db, "checkpoint")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	metadata := []byte(`{"version":1,"known":true}`)
	unitJSON := []byte(`{"table":"table","known":true,"total":1}`)
	mock.ExpectQuery("SELECT unit_key,statistics").WillReturnRows(sqlmock.NewRows([]string{"unit_key", "statistics"}).AddRow("", metadata).AddRow("chunk", unitJSON))
	data, err := store.Load(ctx)
	require.NoError(t, err)
	var saved state
	require.NoError(t, json.Unmarshal(data, &saved))
	require.Equal(t, uint64(1), saved.Units["chunk"].Total)
	id := sha256.Sum256([]byte("chunk"))
	rootID := sha256.Sum256(nil)
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("REPLACE INTO").WithArgs(rootID[:], "", metadata).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("REPLACE INTO").WithArgs(id[:], "chunk", unitJSON).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	require.NoError(t, store.SaveEntries(ctx, metadata, map[string][]byte{"chunk": unitJSON}, true))
	mock.ExpectBegin()
	mock.ExpectExec("REPLACE INTO").WithArgs(rootID[:], "", metadata).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("REPLACE INTO").WithArgs(id[:], "chunk", unitJSON).WillReturnError(errors.New("full"))
	mock.ExpectRollback()
	require.Error(t, store.SaveEntries(ctx, metadata, map[string][]byte{"chunk": unitJSON}, false))
	require.NoError(t, mock.ExpectationsWereMet())
}

type entryCountStore struct {
	memoryStore
	lastEntries int
}

func (s *entryCountStore) SaveEntries(_ context.Context, _ []byte, entries map[string][]byte, _ bool) error {
	s.lastEntries = len(entries)
	return nil
}

// TestLedgerStoresOnlyChangedChunks creates many chunks and acknowledges one
// source batch; subsequent checkpoints serialize only the changed chunk.
func TestLedgerStoresOnlyChangedChunks(t *testing.T) {
	ctx := context.Background()
	store := &entryCountStore{}
	ledger := Open(ctx, store, false)
	for i := 0; i < 100; i++ {
		ledger.Ensure(fmt.Sprint(i), "table", 0, 0)
	}
	ledger.Observe("0", 0, Outcomes{})
	ledger.BeginWrite(ctx, "0", 0)
	require.Equal(t, 100, store.lastEntries)
	ledger.FinishWrite(ctx, "0", 0, Outcomes{Imported: 1}, 0, 0)
	require.Equal(t, 1, store.lastEntries)
}

// TestLedgerConcurrentChunks concurrently reads and acknowledges independent
// chunks, then freezes and checks one coherent conserved task result.
func TestLedgerConcurrentChunks(t *testing.T) {
	ctx := context.Background()
	ledger := Open(ctx, nil, false)
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		key := fmt.Sprint(i)
		ledger.Ensure(key, "table", 0, 0)
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := int64(0); j < 100; j++ {
				ledger.Observe(key, j, Outcomes{})
				ledger.FinishWrite(ctx, key, j, Outcomes{Imported: 1}, 0, 0)
			}
		}()
	}
	workers.Wait()
	require.NoError(t, ledger.Close(ctx))
	_, result := ledger.Snapshot()
	assertConservation(t, result, 800)
	require.Equal(t, uint64(800), result.Imported)
	// Late cancellation acknowledgements cannot mutate the frozen final result.
	ledger.Observe("0", 100, Outcomes{})
	_, late := ledger.Snapshot()
	require.Equal(t, result, late)
}

// BenchmarkLedgerCheckpoint measures a changed chunk in a task with 10,000
// units; persistence work should depend on dirty chunks, not task size.
func BenchmarkLedgerCheckpoint(b *testing.B) {
	ctx := context.Background()
	store := &entryCountStore{}
	ledger := Open(ctx, store, false)
	for i := 0; i < 10000; i++ {
		ledger.Ensure(fmt.Sprint(i), "table", 0, 0)
	}
	ledger.BeginWrite(ctx, "0", -1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ledger.Observe("0", int64(i), Outcomes{})
		ledger.FinishWrite(ctx, "0", int64(i), Outcomes{Imported: 1}, 0, 0)
	}
}

// TestUncleanHistoryDoesNotReclassifyUnfencedReplay simulates a failed statistics
// fence followed by a process crash. The next process must not call its replay a
// replacement merely because the original unacknowledged insert is now visible.
func TestUncleanHistoryDoesNotReclassifyUnfencedReplay(t *testing.T) {
	ctx := context.Background()
	store := &memoryStore{}
	ledger := Open(ctx, store, false)
	ledger.Ensure("chunk", "table", 0, 0)
	ledger.Observe("chunk", 0, Outcomes{})
	ledger.BeginWrite(ctx, "chunk", 0)
	ledger.FinishWrite(ctx, "chunk", 0, Outcomes{Imported: 1}, 0, 0)
	store.fail = true
	ledger.Observe("chunk", 1, Outcomes{})
	ledger.BeginWrite(ctx, "chunk", 1)
	// The source write can commit despite the optional statistics save failing.
	// No Close: its incomplete-history flag could not be persisted either.
	store.fail = false
	resumed := Open(ctx, store, true)
	resumed.Ensure("chunk", "table", 0, 0)
	resumed.Observe("chunk", 1, Outcomes{})
	resumed.FinishWrite(ctx, "chunk", 1, Outcomes{Replaced: 1}, 0, 0)
	_, result := resumed.Snapshot()
	require.Nil(t, result.Total)
	require.Equal(t, uint64(1), result.Imported)
	require.Zero(t, result.Replaced)
	require.False(t, result.Complete)
}

// Resetting a table removes every matching unit by its stored table identity,
// retains task metadata and other tables, and lets corrected input start fresh.
func TestSQLStatisticsRemoveTables(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := NewSQLStore(db, "checkpoint")
	metadata := []byte(`{"version":1,"known":true,"running":false}`)
	removed := []byte(`{"table":"table","high":10,"ack":10,"fence":10,"known":true,"total":2,"outcomes":{"imported":1,"error":1}}`)
	kept := []byte(`{"table":"table2","high":10,"ack":10,"fence":10,"known":true,"total":2,"outcomes":{"imported":2}}`)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT unit_key,statistics .* FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"unit_key", "statistics"}).AddRow("arbitrary", removed).AddRow("another-chunk", removed).AddRow("table/prefix-collision", kept))
	firstID := sha256.Sum256([]byte("arbitrary"))
	secondID := sha256.Sum256([]byte("another-chunk"))
	mock.ExpectExec("DELETE FROM .* WHERE id IN ").WithArgs(firstID[:], secondID[:]).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.RemoveTables(ctx, tx, []string{"table"}))
	require.NoError(t, tx.Commit())
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT unit_key,statistics").WillReturnRows(sqlmock.NewRows([]string{"unit_key", "statistics"}).AddRow("", metadata).AddRow("table/prefix-collision", kept))
	data, err := store.Load(ctx)
	require.NoError(t, err)
	persistence := &memoryStore{data: data}
	ledger := Open(ctx, persistence, true)
	ledger.Ensure("table/prefix-collision", "table2", 0, 0)
	require.False(t, ledger.Observe("table/prefix-collision", 0, Outcomes{}))
	ledger.Ensure("arbitrary", "table", 0, 0)
	require.True(t, ledger.Observe("arbitrary", 0, Outcomes{}))
	require.True(t, ledger.Observe("arbitrary", 10, Outcomes{}))
	ledger.FinishWrite(ctx, "arbitrary", 10, Outcomes{Imported: 2}, 0, 0)
	require.NoError(t, ledger.Close(ctx))
	ledger = Open(ctx, persistence, true)
	tables, result := ledger.Snapshot()
	require.Equal(t, uint64(2), tables["table"].Imported)
	require.Zero(t, tables["table"].Error)
	require.Equal(t, uint64(2), tables["table2"].Imported)
	require.Equal(t, uint64(4), *result.Total)
	require.True(t, result.Complete)
	require.NoError(t, ledger.Close(ctx))
	require.NoError(t, mock.ExpectationsWereMet())
}

// A malformed/unreadable unit or failed deletion aborts the caller's reset
// transaction, rather than allowing a checkpoint-only reset to be committed.
func TestSQLStatisticsRemoveTablesFailure(t *testing.T) {
	for _, phase := range []string{"read", "decode", "delete"} {
		t.Run(phase, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewSQLStore(db, "checkpoint")
			mock.ExpectBegin()
			query := mock.ExpectQuery("SELECT unit_key,statistics .* FOR UPDATE")
			if phase == "read" {
				query.WillReturnError(errors.New("statistics unavailable"))
			} else {
				payload := []byte(`{"table":"table"}`)
				if phase == "decode" {
					payload = []byte(`{`)
				}
				query.WillReturnRows(sqlmock.NewRows([]string{"unit_key", "statistics"}).AddRow("chunk", payload))
			}
			if phase == "delete" {
				id := sha256.Sum256([]byte("chunk"))
				mock.ExpectExec("DELETE FROM .* WHERE id IN ").WithArgs(id[:]).WillReturnError(errors.New("statistics delete failed"))
			}
			mock.ExpectRollback()
			tx, err := db.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			require.Error(t, store.RemoveTables(context.Background(), tx, []string{"table"}))
			require.NoError(t, tx.Rollback())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// A table with many chunks must not require one SQL round trip per removed unit.
// Keep the bounded deletes in the caller's transaction, including a middle-batch failure.
func TestSQLStatisticsRemoveTablesBatches(t *testing.T) {
	for _, tc := range []struct {
		name             string
		units, failBatch int
	}{
		{name: "no matching units"}, {name: "one", units: 1},
		{name: "full batch", units: 256}, {name: "boundary", units: 257},
		{name: "multiple batches", units: 513}, {name: "middle batch error", units: 513, failBatch: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewSQLStore(db, "checkpoint")
			rows := sqlmock.NewRows([]string{"unit_key", "statistics"}).AddRow("keep", []byte(`{"table":"other"}`))
			var ids []driver.Value
			for i := range tc.units {
				key := fmt.Sprintf("table/chunk-%d", i)
				rows.AddRow(key, []byte(`{"table":"table"}`))
				id := sha256.Sum256([]byte(key))
				ids = append(ids, id[:])
			}
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT unit_key,statistics .* FOR UPDATE").WillReturnRows(rows).RowsWillBeClosed()
			batch := 0
			for len(ids) > 0 {
				batch++
				count := min(len(ids), 256)
				placeholders := "(" + strings.TrimSuffix(strings.Repeat("?,", count), ",") + ")"
				deletion := mock.ExpectExec("DELETE FROM .* WHERE id IN " + regexp.QuoteMeta(placeholders)).WithArgs(ids[:count]...)
				if batch == tc.failBatch {
					deletion.WillReturnError(errors.New("middle batch failed"))
					break
				}
				deletion.WillReturnResult(sqlmock.NewResult(0, int64(count)))
				ids = ids[count:]
			}
			if tc.failBatch > 0 {
				mock.ExpectRollback()
			} else {
				mock.ExpectCommit()
			}
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			err = store.RemoveTables(ctx, tx, []string{"table"})
			if tc.failBatch > 0 {
				require.EqualError(t, err, "middle batch failed")
				require.NoError(t, tx.Rollback())
			} else {
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
