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

package stream

import (
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// MaterializedViewTable retains the source definition, including SQL context,
// indexes and schedules, independently of the stripped metadata being restored.
type MaterializedViewTable struct {
	DBID int64
	Info *model.TableInfo
}

type materializedViewWrite struct {
	dbID     int64
	commitTS uint64
	startTS  uint64
	deleted  bool
}

// MaterializedViewRecorder tracks the final committed materialized definitions.
// Write CF is scanned before default CF during ID-map construction, so only
// committed values can replace definitions. Skipped IDs include dropped objects.
type MaterializedViewRecorder struct {
	Tables  map[int64]*MaterializedViewTable
	Skipped map[int64]bool
	writes  map[int64]materializedViewWrite
}

// NewMaterializedViewRecorder creates an empty recorder.
func NewMaterializedViewRecorder() *MaterializedViewRecorder {
	return &MaterializedViewRecorder{
		Tables:  make(map[int64]*MaterializedViewTable),
		Skipped: make(map[int64]bool),
		writes:  make(map[int64]materializedViewWrite),
	}
}

// Seed adds a snapshot definition. IDs are always upstream IDs.
func (r *MaterializedViewRecorder) Seed(dbID int64, info *model.TableInfo) {
	if info == nil {
		return
	}
	r.classify(info)
	if hasMaterializedViewDefinition(info) {
		r.Tables[info.ID] = &MaterializedViewTable{DBID: dbID, Info: info.Clone()}
	}
}

func (r *MaterializedViewRecorder) classify(info *model.TableInfo) {
	if info.MaterializedView != nil || info.MaterializedViewLog != nil || info.MaterializedViewShadow != nil {
		r.Skipped[info.ID] = true
	}
}

// RecordEntry consumes upstream metadata without changing it. Callers serialize
// access because metadata files can be read concurrently and out of TSO order.
func (r *MaterializedViewRecorder) RecordEntry(e *kv.Entry, cf string) error {
	if !IsMetaDBKey(e.Key) {
		return nil
	}
	key, err := ParseTxnMetaKeyFrom(e.Key)
	if err != nil {
		return err
	}
	if !meta.IsDBkey(key.Key) || !meta.IsTableKey(key.Field) {
		return nil
	}
	dbID, err := meta.ParseDBKey(key.Key)
	if err != nil {
		return err
	}
	tableID, err := meta.ParseTableKey(key.Field)
	if err != nil {
		return err
	}
	if r.writes == nil {
		r.writes = make(map[int64]materializedViewWrite)
	}
	if cf == DefaultCF {
		info, err := r.decodeTableInfo(e.Value)
		if err != nil {
			return err
		}
		write, exists := r.writes[tableID]
		if exists && !write.deleted && write.dbID == dbID && write.startTS == key.Ts {
			r.recordTable(dbID, tableID, info)
		}
		return nil
	}
	if cf != WriteCF {
		return nil
	}
	write := new(RawWriteCFValue)
	if err := write.ParseFrom(e.Value); err != nil {
		return err
	}
	if write.IsRollback() || write.GetWriteType() == WriteTypeLock {
		return nil
	}
	var info *model.TableInfo
	if write.HasShortValue() {
		// Classify every version, even if a later version drops the object.
		info, err = r.decodeTableInfo(write.GetShortValue())
		if err != nil {
			return err
		}
	}
	previous, exists := r.writes[tableID]
	// A cross-schema rename deletes and inserts the same ID in one commit.
	// The insertion wins regardless of the metadata file read order.
	if exists && (previous.commitTS > key.Ts || previous.commitTS == key.Ts && !previous.deleted) {
		return nil
	}
	r.writes[tableID] = materializedViewWrite{
		dbID: dbID, commitTS: key.Ts, startTS: write.GetStartTs(), deleted: write.IsDelete(),
	}
	if write.IsDelete() {
		delete(r.Tables, tableID)
		return nil
	}
	// Long values are applied when the matching default entry arrives.
	r.recordTable(dbID, tableID, info)
	return nil
}

func (r *MaterializedViewRecorder) decodeTableInfo(value []byte) (*model.TableInfo, error) {
	var info model.TableInfo
	if err := json.Unmarshal(value, &info); err != nil {
		return nil, errors.Trace(err)
	}
	r.classify(&info)
	return &info, nil
}

func hasMaterializedViewDefinition(info *model.TableInfo) bool {
	return info != nil &&
		(info.MaterializedView != nil || info.MaterializedViewLog != nil || info.MaterializedViewBase != nil)
}

func (r *MaterializedViewRecorder) recordTable(dbID, tableID int64, info *model.TableInfo) {
	if hasMaterializedViewDefinition(info) {
		r.Tables[tableID] = &MaterializedViewTable{DBID: dbID, Info: info}
	} else {
		delete(r.Tables, tableID)
	}
}

// FinishRecording releases transient MVCC state after the ID-map scan completes.
// Only final definitions and skipped IDs are needed during data restore/rebuild.
func (r *MaterializedViewRecorder) FinishRecording() {
	r.writes = nil
}
