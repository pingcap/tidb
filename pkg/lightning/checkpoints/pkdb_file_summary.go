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
	"fmt"
	"maps"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

// fileStatisticsManifest keeps control checkpoint writes independent of ledger
// size. A generation is published only after its initial snapshot is durable.
// Its directory is independent of the checkpoint name, so renaming a checkpoint
// cannot invalidate the reference even if best-effort embedding fails.
type fileStatisticsManifest struct {
	Version    int    `json:"file-version"`
	Generation string `json:"generation"`
}

// Each journal object commits metadata and changed units in one atomic storage
// WriteFile. Sequence numbers reject gaps; snapshots supersede older records.
type fileStatisticsRecord struct {
	Version  int                        `json:"version"`
	Sequence uint64                     `json:"sequence"`
	Metadata json.RawMessage            `json:"metadata"`
	Entries  map[string]json.RawMessage `json:"entries"`
}

// All fields are protected by db.lock, including normal checkpoint writes.
type fileStatisticsStore struct {
	db            *FileCheckpointsDB
	loaded        bool
	generation    string
	sequence      uint64
	metadata      json.RawMessage
	entries       map[string]json.RawMessage
	snapshotBytes int
	journalBytes  int64
	journals      []string
}

func statisticsDirectory(generation string) string {
	return ".pkdb-import-statistics/" + generation + "/"
}

func statisticsGeneration(data []byte) (string, error) {
	if len(data) == 0 {
		return "", nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return "", err
	}
	if _, ok := root["file-version"]; !ok {
		return "", nil // The previous format embedded the entire ledger.
	}
	var manifest fileStatisticsManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return "", err
	}
	id, err := uuid.Parse(manifest.Generation)
	if err != nil || id.String() != manifest.Generation || manifest.Version != 1 {
		return "", fmt.Errorf("unsupported import statistics manifest")
	}
	return manifest.Generation, nil
}

func splitStatistics(data []byte) (json.RawMessage, map[string]json.RawMessage, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, nil, err
	}
	if root == nil {
		return nil, nil, fmt.Errorf("invalid import statistics metadata")
	}
	entries := make(map[string]json.RawMessage)
	if err := json.Unmarshal(root["units"], &entries); err != nil {
		return nil, nil, err
	}
	if entries == nil {
		return nil, nil, fmt.Errorf("invalid import statistics units")
	}
	delete(root, "units")
	metadata, err := json.Marshal(root)
	return metadata, entries, err
}

func (s *fileStatisticsStore) document() ([]byte, error) {
	if len(s.metadata) == 0 {
		return nil, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(s.metadata, &root); err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("invalid import statistics metadata")
	}
	entries, err := json.Marshal(s.entries)
	if err != nil {
		return nil, err
	}
	root["units"] = entries
	return json.Marshal(root)
}

func (s *fileStatisticsStore) readRecord(ctx context.Context, name string) (fileStatisticsRecord, int, error) {
	data, err := s.db.exStorage.ReadFile(ctx, name)
	if err != nil {
		return fileStatisticsRecord{}, 0, err
	}
	var record fileStatisticsRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return record, 0, err
	}
	if record.Version != 1 || len(record.Metadata) == 0 || record.Entries == nil {
		return record, 0, fmt.Errorf("invalid import statistics record %s", name)
	}
	return record, len(data), nil
}

func (s *fileStatisticsStore) Load(ctx context.Context) ([]byte, error) {
	s.db.lock.Lock()
	defer s.db.lock.Unlock()
	return s.load(ctx)
}

func (s *fileStatisticsStore) load(ctx context.Context) ([]byte, error) {
	data := s.db.checkpoints.ImportStatistics
	generation, err := statisticsGeneration(data)
	if err != nil {
		return nil, err
	}
	restored := fileStatisticsStore{db: s.db, loaded: true, generation: generation, entries: make(map[string]json.RawMessage)}
	if generation == "" {
		if len(data) > 0 {
			restored.metadata, restored.entries, err = splitStatistics(data)
			if err != nil {
				return nil, err
			}
		}
		*s = restored
		return append([]byte(nil), data...), nil
	}
	directory := statisticsDirectory(generation)
	snapshot, size, err := s.readRecord(ctx, directory+"snapshot.json")
	if err != nil {
		return nil, err
	}
	restored.metadata, restored.entries = snapshot.Metadata, snapshot.Entries
	restored.sequence, restored.snapshotBytes = snapshot.Sequence, size
	type journal struct {
		name     string
		sequence uint64
		size     int64
	}
	var journals []journal
	err = s.db.exStorage.WalkDir(ctx, &storage.WalkOption{SubDir: directory}, func(name string, size int64) error {
		if !strings.HasPrefix(name, directory) || !strings.HasSuffix(name, ".json") || name == directory+"snapshot.json" {
			return nil
		}
		sequence, err := strconv.ParseUint(strings.TrimSuffix(path.Base(name), ".json"), 10, 64)
		if err != nil {
			return err
		}
		restored.journals = append(restored.journals, name)
		if sequence > snapshot.Sequence {
			journals = append(journals, journal{name: name, sequence: sequence, size: size})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(journals, func(i, j int) bool { return journals[i].sequence < journals[j].sequence })
	for _, journal := range journals {
		if journal.sequence != restored.sequence+1 {
			return nil, fmt.Errorf("missing import statistics journal after %d", restored.sequence)
		}
		record, _, err := s.readRecord(ctx, journal.name)
		if err != nil {
			return nil, err
		}
		if record.Sequence != journal.sequence {
			return nil, fmt.Errorf("invalid import statistics sequence in %s", journal.name)
		}
		restored.metadata = record.Metadata
		for key, entry := range record.Entries {
			restored.entries[key] = entry
		}
		restored.sequence = record.Sequence
		restored.journalBytes += journal.size
	}
	data, err = restored.document()
	if err == nil {
		*s = restored
	}
	return data, err
}

func (s *fileStatisticsStore) Save(ctx context.Context, data []byte) error {
	metadata, entries, err := splitStatistics(data)
	if err != nil {
		return err
	}
	raw := make(map[string][]byte, len(entries))
	for key, entry := range entries {
		raw[key] = entry
	}
	return s.SaveEntries(ctx, metadata, raw, true)
}

func (s *fileStatisticsStore) SaveEntries(ctx context.Context, metadata []byte, entries map[string][]byte, reset bool) error {
	s.db.lock.Lock()
	defer s.db.lock.Unlock()
	if !s.loaded && !reset {
		if _, err := s.load(ctx); err != nil {
			return err
		}
	}
	changed := make(map[string]json.RawMessage, len(entries))
	for key, entry := range entries {
		changed[key] = append(json.RawMessage(nil), entry...)
	}
	record := fileStatisticsRecord{Version: 1, Sequence: s.sequence + 1, Metadata: metadata, Entries: changed}
	if reset || s.generation == "" {
		// Copy the previous embedded ledger only once when upgrading its format.
		if !reset {
			for key, entry := range s.entries {
				if _, ok := changed[key]; !ok {
					changed[key] = entry
				}
			}
		}
		record.Sequence = 0
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}
		generation := uuid.NewString()
		if err := s.db.exStorage.WriteFile(ctx, statisticsDirectory(generation)+"snapshot.json", data); err != nil {
			return err
		}
		manifest, err := json.Marshal(fileStatisticsManifest{Version: 1, Generation: generation})
		if err != nil {
			return err
		}
		previous := s.db.checkpoints.ImportStatistics
		s.db.checkpoints.ImportStatistics = manifest
		if err := s.db.save(); err != nil {
			s.db.checkpoints.ImportStatistics = previous
			return err
		}
		previousGeneration := s.generation
		*s = fileStatisticsStore{db: s.db, loaded: true, generation: generation, metadata: append(json.RawMessage(nil), metadata...), entries: changed, snapshotBytes: len(data)}
		if previousGeneration != "" {
			s.db.removeStatisticsFiles(ctx, previousGeneration)
		}
		return nil
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("%s%020d.json", statisticsDirectory(s.generation), record.Sequence)
	if err := s.db.exStorage.WriteFile(ctx, name, data); err != nil {
		return err
	}
	s.metadata = append(json.RawMessage(nil), metadata...)
	for key, entry := range changed {
		s.entries[key] = entry
	}
	s.sequence = record.Sequence
	s.journalBytes += int64(len(data))
	s.journals = append(s.journals, name)
	// Amortize whole-ledger compaction over at least as many journal bytes as
	// the preceding snapshot. Small ledgers compact after 1 MiB, not every batch.
	// The current record is already durable: maintenance failures can be retried
	// at the next save without invalidating this acknowledged fence or outcome.
	if s.journalBytes >= int64(max(1<<20, s.snapshotBytes)) {
		if err := s.compact(ctx); err != nil {
			log.L().Warn("could not compact import statistics", zap.Error(err))
		}
	}
	return nil
}

func (s *fileStatisticsStore) compact(ctx context.Context) error {
	record := fileStatisticsRecord{Version: 1, Sequence: s.sequence, Metadata: s.metadata, Entries: s.entries}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := s.db.exStorage.WriteFile(ctx, statisticsDirectory(s.generation)+"snapshot.json", data); err != nil {
		return err
	}
	// A crash or failed deletion is safe: Load ignores journals covered by the
	// durable snapshot. Never remove them before publishing that snapshot.
	if err := s.db.exStorage.DeleteFiles(ctx, s.journals); err != nil {
		log.L().Warn("could not clean compacted import statistics", zap.Error(err))
	}
	s.snapshotBytes, s.journalBytes, s.journals = len(data), 0, nil
	return nil
}

func (cpdb *FileCheckpointsDB) removeStatisticsFiles(ctx context.Context, generation string) {
	if generation == "" {
		return
	}
	directory := statisticsDirectory(generation)
	var files []string
	err := cpdb.exStorage.WalkDir(ctx, &storage.WalkOption{SubDir: directory}, func(name string, _ int64) error {
		if strings.HasPrefix(name, directory) {
			files = append(files, name)
		}
		return nil
	})
	if err == nil {
		err = cpdb.exStorage.DeleteFiles(ctx, files)
	}
	if err != nil {
		log.L().Warn("could not clean import statistics", zap.Error(err))
	}
}

// embedImportStatistics makes an archived checkpoint self-contained. This is
// called only at task completion, never on the source-write critical path.
// Statistics failures do not change the existing checkpoint rename decision.
func (cpdb *FileCheckpointsDB) embedImportStatistics(ctx context.Context) {
	s := cpdb.statistics
	if s == nil {
		s = &fileStatisticsStore{db: cpdb}
		cpdb.statistics = s
	}
	if !s.loaded {
		if _, err := s.load(ctx); err != nil {
			log.L().Warn("could not load import statistics for backup", zap.Error(err))
			return
		}
	}
	if s.generation == "" {
		return
	}
	data, err := s.document()
	if err != nil {
		log.L().Warn("could not back up import statistics", zap.Error(err))
		return
	}
	previous := cpdb.checkpoints.ImportStatistics
	cpdb.checkpoints.ImportStatistics = data
	if err := cpdb.save(); err != nil {
		cpdb.checkpoints.ImportStatistics = previous
		log.L().Warn("could not back up import statistics", zap.Error(err))
		return
	}
	cpdb.removeStatisticsFiles(ctx, s.generation)
	s.generation = ""
}

// removeTableCheckpoints publishes the filtered ledger and control checkpoints
// together. This rare maintenance operation embeds one snapshot; subsequent
// imports migrate it back to journals. The caller holds cpdb.lock.
func (cpdb *FileCheckpointsDB) removeTableCheckpoints(ctx context.Context, tables []string) error {
	previousStatistics := cpdb.checkpoints.ImportStatistics
	previousTables := cpdb.checkpoints.Checkpoints
	generation := ""
	if len(tables) > 0 && len(previousStatistics) > 0 {
		store := &fileStatisticsStore{db: cpdb}
		if _, err := store.load(ctx); err != nil {
			return err
		}
		generation = store.generation
		removed := make(map[string]struct{}, len(tables))
		for _, table := range tables {
			removed[table] = struct{}{}
		}
		for key, data := range store.entries {
			var entry struct {
				Table string `json:"table"`
			}
			if err := json.Unmarshal(data, &entry); err != nil {
				return err
			}
			if _, ok := removed[entry.Table]; ok {
				delete(store.entries, key)
			}
		}
		data, err := store.document()
		if err != nil {
			return err
		}
		cpdb.checkpoints.ImportStatistics = data
	}
	cpdb.checkpoints.Checkpoints = maps.Clone(previousTables)
	for _, table := range tables {
		delete(cpdb.checkpoints.Checkpoints, table)
	}
	if err := cpdb.save(); err != nil {
		cpdb.checkpoints.ImportStatistics = previousStatistics
		cpdb.checkpoints.Checkpoints = previousTables
		return err
	}
	if len(tables) > 0 {
		// A cached store must not retain units/frontiers from the discarded generation.
		cpdb.statistics = nil
		// Obsolete files are removed only after the replacement checkpoint is durable.
		cpdb.removeStatisticsFiles(ctx, generation)
	}
	return nil
}
