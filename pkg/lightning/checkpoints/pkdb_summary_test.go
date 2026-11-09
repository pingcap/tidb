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

package checkpoints

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints/checkpointspb"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/pkdbsummary"
	"github.com/stretchr/testify/require"
)

type statisticsCountingStorage struct {
	storage.ExternalStorage
	writes          int
	bytes           int
	largest         int
	writeError      func(string) error
	afterWriteError func(string) error
	failDelete      bool
}

func (s *statisticsCountingStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	s.writes++
	s.bytes += len(data)
	s.largest = max(s.largest, len(data))
	if s.writeError != nil {
		if err := s.writeError(name); err != nil {
			return err
		}
	}
	if err := s.ExternalStorage.WriteFile(ctx, name, data); err != nil {
		return err
	}
	if s.afterWriteError != nil {
		return s.afterWriteError(name)
	}
	return nil
}

func (s *statisticsCountingStorage) DeleteFiles(ctx context.Context, names []string) error {
	if s.failDelete {
		return errors.New("statistics cleanup unavailable")
	}
	return s.ExternalStorage.DeleteFiles(ctx, names)
}

func newStatisticsTestDB(t *testing.T) (*FileCheckpointsDB, *statisticsCountingStorage) {
	t.Helper()
	ctx := context.Background()
	local, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	storage := &statisticsCountingStorage{ExternalStorage: local}
	db, err := NewFileCheckpointsDBWithExstorageFileName(ctx, "", storage, "checkpoint.pb")
	require.NoError(t, err)
	return db, storage
}

func reopenStatisticsTestDB(t *testing.T, storage storage.ExternalStorage, name string) *FileCheckpointsDB {
	t.Helper()
	db, err := NewFileCheckpointsDBWithExstorageFileName(context.Background(), "", storage, name)
	require.NoError(t, err)
	return db
}

// A fixed number of writes to one chunk must not rewrite unrelated chunks or
// the import control checkpoint on every source batch.
func TestFileStatisticsWriteVolume(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	storage := &statisticsCountingStorage{ExternalStorage: local}
	db, err := NewFileCheckpointsDBWithExstorageFileName(ctx, "", storage, "checkpoint.pb")
	require.NoError(t, err)
	ledger := pkdbsummary.Open(ctx, ImportStatisticsStore(db), false)
	for i := range 10000 {
		ledger.Ensure(fmt.Sprintf("table/dump.%05d.csv:0", i), "table", 0, 0)
	}
	const key = "table/dump.00000.csv:0"
	ledger.BeginWrite(ctx, key, -1)
	storage.writes, storage.bytes, storage.largest = 0, 0, 0
	for i := int64(0); i < 32; i++ {
		ledger.Observe(key, i, pkdbsummary.Outcomes{})
		ledger.BeginWrite(ctx, key, i)
		ledger.FinishWrite(ctx, key, i, pkdbsummary.Outcomes{Imported: 1}, 0, 0)
	}
	t.Logf("10000 chunks, 32 batches: writes=%d bytes=%d largest=%d", storage.writes, storage.bytes, storage.largest)
	require.Less(t, storage.bytes, 128<<10)
	require.Less(t, storage.largest, 4096)
	require.NoError(t, ledger.Close(ctx))
	restored, err := NewFileCheckpointsDB(ctx, filepath.Join(local.Base(), "checkpoint.pb"))
	require.NoError(t, err)
	resumed := pkdbsummary.Open(ctx, ImportStatisticsStore(restored), true)
	_, result := resumed.Snapshot()
	require.True(t, result.Complete)
	require.Equal(t, uint64(32), result.Imported)
	require.Equal(t, uint64(32), *result.Total)
	require.NoError(t, resumed.Close(ctx))
}

// The durable fence and earlier acknowledgements must survive both an orderly
// pause and an unclean process exit. Optional storage failures cannot turn a
// replayed input into a confirmed replacement.
func TestFileStatisticsResume(t *testing.T) {
	for _, scenario := range []string{"closed", "crashed", "failed-fence", "uncertain-fence"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			db, storage := newStatisticsTestDB(t)
			ledger := pkdbsummary.Open(ctx, ImportStatisticsStore(db), false)
			ledger.Ensure("chunk", "table", 0, 0)
			ledger.Observe("chunk", 0, pkdbsummary.Outcomes{})
			ledger.BeginWrite(ctx, "chunk", 0)
			ledger.FinishWrite(ctx, "chunk", 0, pkdbsummary.Outcomes{Imported: 1}, 0, 0)
			ledger.Observe("chunk", 1, pkdbsummary.Outcomes{})
			fail := func(string) error { return errors.New("fence unavailable") }
			switch scenario {
			case "failed-fence":
				storage.writeError = fail
			case "uncertain-fence":
				storage.afterWriteError = fail
			}
			ledger.BeginWrite(ctx, "chunk", 1)
			storage.writeError, storage.afterWriteError = nil, nil
			if scenario == "closed" {
				require.NoError(t, ledger.Close(ctx))
			}
			// Open another DB instance without closing the first DB: only
			// completed durable writes are available to the resumed process.
			restored := reopenStatisticsTestDB(t, storage, "checkpoint.pb")
			resumed := pkdbsummary.Open(ctx, ImportStatisticsStore(restored), true)
			resumed.Ensure("chunk", "table", 0, 0)
			resumed.Observe("chunk", 1, pkdbsummary.Outcomes{})
			require.False(t, resumed.Eligible("chunk", 1))
			resumed.FinishWrite(ctx, "chunk", 1, pkdbsummary.Outcomes{Replaced: 1}, 0, 0)
			_, result := resumed.Snapshot()
			require.Equal(t, uint64(1), result.Imported)
			require.Zero(t, result.Replaced)
			require.False(t, result.Complete)
			if scenario == "closed" {
				require.Equal(t, uint64(2), *result.Total)
				require.Equal(t, uint64(1), *result.Unresolved)
			} else {
				require.Nil(t, result.Total)
			}
			require.NoError(t, resumed.Close(ctx))
		})
	}
}

func TestFileStatisticsLegacyMigrationAndReset(t *testing.T) {
	ctx := context.Background()
	db, storage := newStatisticsTestDB(t)
	// This is the embedded format written by the original PR head.
	legacy := []byte(`{"version":1,"running":false,"known":true,"units":{"old":{"table":"table","high":0,"ack":0,"fence":0,"total":1,"known":true,"outcomes":{"imported":1}}}}`)
	db.checkpoints.ImportStatistics = legacy
	require.NoError(t, db.Close())
	db = reopenStatisticsTestDB(t, storage, "checkpoint.pb")
	ledger := pkdbsummary.Open(ctx, ImportStatisticsStore(db), true)
	ledger.Ensure("new", "table", 0, 0)
	ledger.Observe("new", 0, pkdbsummary.Outcomes{})
	ledger.BeginWrite(ctx, "new", 0)
	ledger.FinishWrite(ctx, "new", 0, pkdbsummary.Outcomes{Imported: 1}, 0, 0)
	require.NoError(t, ledger.Close(ctx))
	generation, err := statisticsGeneration(db.checkpoints.ImportStatistics)
	require.NoError(t, err)
	require.NotEmpty(t, generation)
	db = reopenStatisticsTestDB(t, storage, "checkpoint.pb")
	ledger = pkdbsummary.Open(ctx, ImportStatisticsStore(db), true)
	_, result := ledger.Snapshot()
	require.True(t, result.Complete)
	require.Equal(t, uint64(2), result.Imported)
	require.Equal(t, uint64(2), *result.Total)
	require.NoError(t, ledger.Close(ctx))
	ledger = pkdbsummary.Open(ctx, ImportStatisticsStore(db), false)
	_, result = ledger.Snapshot()
	require.Zero(t, result.Imported)
	require.Zero(t, *result.Total)
	exists, err := storage.FileExists(ctx, statisticsDirectory(generation)+"snapshot.json")
	require.NoError(t, err)
	require.False(t, exists)
	require.NoError(t, ledger.Close(ctx))
}

// Compaction publishes its snapshot before removing journals. A failed cleanup
// leaves redundant records, and a failed snapshot write leaves the old snapshot
// plus a complete journal; both must load the same latest metadata and units.
func TestFileStatisticsCompactionRecovery(t *testing.T) {
	for _, scenario := range []string{"success", "delete-fails", "snapshot-fails"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			db, storage := newStatisticsTestDB(t)
			store := ImportStatisticsStore(db).(*fileStatisticsStore)
			metadata := []byte(`{"version":1,"running":false,"known":true}`)
			require.NoError(t, store.SaveEntries(ctx, metadata, nil, true))
			switch scenario {
			case "delete-fails":
				storage.failDelete = true
			case "snapshot-fails":
				storage.writeError = func(name string) error {
					if strings.HasSuffix(name, "snapshot.json") {
						return errors.New("snapshot unavailable")
					}
					return nil
				}
			}
			large, err := json.Marshal(map[string]string{"padding": strings.Repeat("x", 1<<20)})
			require.NoError(t, err)
			require.NoError(t, store.SaveEntries(ctx, metadata, map[string][]byte{"large": large}, false))
			if scenario == "snapshot-fails" {
				require.Positive(t, store.journalBytes)
			} else {
				require.Zero(t, store.journalBytes)
			}
			storage.writeError, storage.failDelete = nil, false
			latest := []byte(`{"version":1,"running":true,"known":false}`)
			require.NoError(t, store.SaveEntries(ctx, latest, map[string][]byte{"small": []byte(`{"value":2}`)}, false))
			restored := reopenStatisticsTestDB(t, storage, "checkpoint.pb")
			data, err := ImportStatisticsStore(restored).Load(ctx)
			require.NoError(t, err)
			var root struct {
				Running bool                       `json:"running"`
				Known   bool                       `json:"known"`
				Units   map[string]json.RawMessage `json:"units"`
			}
			require.NoError(t, json.Unmarshal(data, &root))
			require.True(t, root.Running)
			require.False(t, root.Known)
			require.JSONEq(t, string(large), string(root.Units["large"]))
			require.JSONEq(t, `{"value":2}`, string(root.Units["small"]))
		})
	}
}

func TestFileStatisticsResetPublishFailure(t *testing.T) {
	ctx := context.Background()
	db, storage := newStatisticsTestDB(t)
	store := ImportStatisticsStore(db).(*fileStatisticsStore)
	require.NoError(t, store.Save(ctx, []byte(`{"version":1,"units":{"old":{"value":1}}}`)))
	previous := append([]byte(nil), db.checkpoints.ImportStatistics...)
	storage.writeError = func(name string) error {
		if name == "checkpoint.pb" {
			return errors.New("checkpoint publication failed")
		}
		return nil
	}
	require.Error(t, store.Save(ctx, []byte(`{"version":1,"units":{"new":{"value":2}}}`)))
	require.Equal(t, previous, db.checkpoints.ImportStatistics)
	storage.writeError = nil
	restored := reopenStatisticsTestDB(t, storage, "checkpoint.pb")
	data, err := ImportStatisticsStore(restored).Load(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `{"version":1,"units":{"old":{"value":1}}}`, string(data))
}

func TestFileStatisticsArchiveAndRemove(t *testing.T) {
	for _, scenario := range []string{"archive", "archive-embedding-fails", "remove"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			db, storage := newStatisticsTestDB(t)
			ledger := pkdbsummary.Open(ctx, ImportStatisticsStore(db), false)
			ledger.Ensure("chunk", "table", 0, 0)
			ledger.Observe("chunk", 0, pkdbsummary.Outcomes{})
			ledger.BeginWrite(ctx, "chunk", 0)
			ledger.FinishWrite(ctx, "chunk", 0, pkdbsummary.Outcomes{Imported: 1}, 0, 0)
			require.NoError(t, ledger.Close(ctx))
			generation, err := statisticsGeneration(db.checkpoints.ImportStatistics)
			require.NoError(t, err)
			if scenario == "remove" {
				require.NoError(t, db.RemoveCheckpoint(ctx, allTables))
				exists, err := storage.FileExists(ctx, statisticsDirectory(generation)+"snapshot.json")
				require.NoError(t, err)
				require.False(t, exists)
				return
			}
			if scenario == "archive-embedding-fails" {
				storage.writeError = func(name string) error {
					if name == "checkpoint.pb" {
						return errors.New("embedding unavailable")
					}
					return nil
				}
			}
			require.NoError(t, db.MoveCheckpoints(ctx, 123))
			storage.writeError = nil
			var restored *FileCheckpointsDB
			if scenario == "archive" {
				// Copy only the archived checkpoint to a different directory.
				// No journal directory is required after a successful embedding.
				data, err := storage.ReadFile(ctx, "checkpoint.pb.123.bak")
				require.NoError(t, err)
				copyPath := filepath.Join(t.TempDir(), "copy.pb")
				require.NoError(t, os.WriteFile(copyPath, data, 0600))
				restored, err = NewFileCheckpointsDB(ctx, copyPath)
				require.NoError(t, err)
				exists, err := storage.FileExists(ctx, statisticsDirectory(generation)+"snapshot.json")
				require.NoError(t, err)
				require.False(t, exists)
			} else {
				restored = reopenStatisticsTestDB(t, storage, "checkpoint.pb.123.bak")
			}
			resumed := pkdbsummary.Open(ctx, ImportStatisticsStore(restored), true)
			_, result := resumed.Snapshot()
			require.True(t, result.Complete)
			require.Equal(t, uint64(1), result.Imported)
			require.Equal(t, uint64(1), *result.Total)
			require.NoError(t, resumed.Close(ctx))
		})
	}
}

func TestFileStatisticsRejectsMissingJournal(t *testing.T) {
	ctx := context.Background()
	db, storage := newStatisticsTestDB(t)
	store := ImportStatisticsStore(db).(*fileStatisticsStore)
	metadata := []byte(`{"version":1,"running":false,"known":true}`)
	require.NoError(t, store.SaveEntries(ctx, metadata, nil, true))
	require.NoError(t, store.SaveEntries(ctx, metadata, map[string][]byte{"first": []byte(`{"value":1}`)}, false))
	require.NoError(t, store.SaveEntries(ctx, metadata, map[string][]byte{"second": []byte(`{"value":2}`)}, false))
	require.NoError(t, storage.DeleteFile(ctx, store.journals[0]))
	restored := reopenStatisticsTestDB(t, storage, "checkpoint.pb")
	_, err := ImportStatisticsStore(restored).Load(ctx)
	require.ErrorContains(t, err, "missing import statistics journal")
}

func TestFileStatisticsConcurrentCheckpointWrites(t *testing.T) {
	ctx := context.Background()
	db, storage := newStatisticsTestDB(t)
	ledger := pkdbsummary.Open(ctx, ImportStatisticsStore(db), false)
	var workers sync.WaitGroup
	for i := range 4 {
		key := fmt.Sprint(i)
		ledger.Ensure(key, "table", 0, 0)
		workers.Add(1)
		go func() {
			defer workers.Done()
			for offset := int64(0); offset < 8; offset++ {
				ledger.Observe(key, offset, pkdbsummary.Outcomes{})
				ledger.BeginWrite(ctx, key, offset)
				ledger.FinishWrite(ctx, key, offset, pkdbsummary.Outcomes{Imported: 1}, 0, 0)
			}
		}()
	}
	checkpointErrors := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		for range 8 {
			// Close only saves the control model; it does not close the store.
			if err := db.Close(); err != nil {
				checkpointErrors <- err
				return
			}
		}
	}()
	workers.Wait()
	close(checkpointErrors)
	for err := range checkpointErrors {
		require.NoError(t, err)
	}
	require.NoError(t, ledger.Close(ctx))
	restored := reopenStatisticsTestDB(t, storage, "checkpoint.pb")
	resumed := pkdbsummary.Open(ctx, ImportStatisticsStore(restored), true)
	_, result := resumed.Snapshot()
	require.True(t, result.Complete)
	require.Equal(t, uint64(32), result.Imported)
	require.Equal(t, uint64(32), *result.Total)
	require.NoError(t, resumed.Close(ctx))
}

// Explicitly resetting one table starts a new source generation. Exercise both
// reset commands and every file format, then reload twice to reject stale
// journals while preserving another table's outcomes and acknowledged frontier.
func TestFileStatisticsTableReset(t *testing.T) {
	for _, format := range []string{"journal", "snapshot", "embedded"} {
		for _, mode := range []string{"remove", "destroy-table", "destroy-all-errors"} {
			for _, scenario := range []string{"shorter-source", "corrected-error"} {
				t.Run(format+"/"+mode+"/"+scenario, func(t *testing.T) {
					ctx := context.Background()
					db, storage := newStatisticsTestDB(t)
					cfg := config.NewConfig()
					cfg.TaskID = 42
					require.NoError(t, db.Initialize(ctx, cfg, nil))
					const table, keep = "`db`.`t`", "`db`.`t2`"
					const key, keepKey = "`db`.`t`/source.csv:0", "`db`.`t2`/source.csv:0"
					status := CheckpointStatusLoaded
					if mode != "remove" {
						status = CheckpointStatusMaxInvalid
					}
					db.checkpoints.Checkpoints[table] = &checkpointspb.TableCheckpointModel{Status: uint32(status)}
					db.checkpoints.Checkpoints[keep] = &checkpointspb.TableCheckpointModel{Status: uint32(CheckpointStatusLoaded)}
					ledger := pkdbsummary.Open(ctx, ImportStatisticsStore(db), false)
					ledger.Ensure(keepKey, keep, 0, 0)
					ledger.Observe(keepKey, 0, pkdbsummary.Outcomes{})
					ledger.Observe(keepKey, 10, pkdbsummary.Outcomes{})
					ledger.FinishWrite(ctx, keepKey, 10, pkdbsummary.Outcomes{Imported: 2}, 0, 0)
					ledger.Ensure(key, table, 0, 0)
					fresh := ledger.Observe(key, 0, pkdbsummary.Outcomes{})
					if scenario == "corrected-error" {
						ledger.ClassifyRead(key, 0, fresh, pkdbsummary.Outcomes{Error: 1})
					}
					ledger.Observe(key, 10, pkdbsummary.Outcomes{})
					imported := uint64(2)
					if scenario == "corrected-error" {
						imported = 1
					}
					ledger.FinishWrite(ctx, key, 10, pkdbsummary.Outcomes{Imported: imported}, 0, 0)
					require.NoError(t, ledger.Close(ctx))
					db.lock.Lock()
					switch format {
					case "snapshot":
						require.NoError(t, db.statistics.compact(ctx))
					case "embedded":
						db.embedImportStatistics(ctx)
					}
					db.lock.Unlock()
					// Use a fresh command-side DB: CLI resets do not first open a ledger.
					db = reopenStatisticsTestDB(t, storage, "checkpoint.pb")
					if mode == "remove" {
						require.NoError(t, db.RemoveCheckpoint(ctx, table))
					} else {
						target := table
						if mode == "destroy-all-errors" {
							target = allTables
						}
						removed, err := db.DestroyErrorCheckpoint(ctx, target)
						require.NoError(t, err)
						require.Len(t, removed, 1)
						require.Equal(t, table, removed[0].TableName)
					}
					db = reopenStatisticsTestDB(t, storage, "checkpoint.pb")
					_, err := db.Get(ctx, table)
					require.Error(t, err)
					_, err = db.Get(ctx, keep)
					require.NoError(t, err)
					task, err := db.TaskCheckpoint(ctx)
					require.NoError(t, err)
					require.Equal(t, int64(42), task.TaskID)
					resumed := pkdbsummary.Open(ctx, ImportStatisticsStore(db), true)
					resumed.Ensure(keepKey, keep, 0, 0)
					require.False(t, resumed.Observe(keepKey, 0, pkdbsummary.Outcomes{}))
					resumed.Ensure(key, table, 0, 0)
					require.True(t, resumed.Observe(key, 0, pkdbsummary.Outcomes{}))
					rows, end := uint64(1), int64(0)
					if scenario == "corrected-error" {
						rows, end = 2, 10
						require.True(t, resumed.Observe(key, end, pkdbsummary.Outcomes{}))
					}
					resumed.FinishWrite(ctx, key, end, pkdbsummary.Outcomes{Imported: rows}, 0, 0)
					require.NoError(t, resumed.Close(ctx))
					db = reopenStatisticsTestDB(t, storage, "checkpoint.pb")
					resumed = pkdbsummary.Open(ctx, ImportStatisticsStore(db), true)
					tables, result := resumed.Snapshot()
					require.Equal(t, rows, tables[table].Imported)
					require.Zero(t, tables[table].Error)
					require.True(t, tables[table].Complete)
					require.Equal(t, uint64(2), tables[keep].Imported)
					require.True(t, tables[keep].Complete)
					require.Equal(t, rows+2, *result.Total)
					require.Equal(t, rows+2, result.Imported)
					require.True(t, result.Complete)
					require.NoError(t, resumed.Close(ctx))
				})
			}
		}
	}
}

// A failed checkpoint publication must preserve both old generations. After a
// retry succeeds, even undeletable obsolete journals must not resurrect rows.
func TestFileStatisticsTableResetPublicationFailure(t *testing.T) {
	ctx := context.Background()
	db, storage := newStatisticsTestDB(t)
	const table = "table"
	db.checkpoints.Checkpoints[table] = &checkpointspb.TableCheckpointModel{Status: uint32(CheckpointStatusLoaded)}
	ledger := pkdbsummary.Open(ctx, ImportStatisticsStore(db), false)
	ledger.Ensure("chunk", table, 0, 0)
	ledger.Observe("chunk", 0, pkdbsummary.Outcomes{})
	ledger.FinishWrite(ctx, "chunk", 0, pkdbsummary.Outcomes{Imported: 1}, 0, 0)
	require.NoError(t, ledger.Close(ctx))
	generation, err := statisticsGeneration(db.checkpoints.ImportStatistics)
	require.NoError(t, err)
	storage.writeError = func(name string) error {
		if name == "checkpoint.pb" {
			return errors.New("reset publication failed")
		}
		return nil
	}
	require.Error(t, db.RemoveCheckpoint(ctx, table))
	_, err = db.Get(ctx, table)
	require.NoError(t, err)
	storage.writeError = nil
	db = reopenStatisticsTestDB(t, storage, "checkpoint.pb")
	_, err = db.Get(ctx, table)
	require.NoError(t, err)
	data, err := ImportStatisticsStore(db).Load(ctx)
	require.NoError(t, err)
	require.Contains(t, string(data), "chunk")
	storage.failDelete = true
	require.NoError(t, db.RemoveCheckpoint(ctx, table))
	exists, err := storage.FileExists(ctx, statisticsDirectory(generation)+"snapshot.json")
	require.NoError(t, err)
	require.True(t, exists)
	storage.failDelete = false
	db = reopenStatisticsTestDB(t, storage, "checkpoint.pb")
	resumed := pkdbsummary.Open(ctx, ImportStatisticsStore(db), true)
	resumed.Ensure("chunk", table, 0, 0)
	require.True(t, resumed.Observe("chunk", 0, pkdbsummary.Outcomes{}))
	_, result := resumed.Snapshot()
	require.Zero(t, result.Imported)
	require.NoError(t, resumed.Close(ctx))
}
