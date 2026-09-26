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

package snapclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"slices"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/restore"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
)

// classicRestoreTaskID hashes the original logical batch, never a Region crop or
// a checkpoint-filtered subset. Version 1 uses length-prefixed bytes and big-endian
// uint64 values; changing this encoding requires a new domain version and a resume
// compatibility decision. Effective rewrite rules are encoded with their sources,
// so reordering independent rules or file sets does not change identity.
func classicRestoreTaskID(restoreID uuid.UUID, batch restore.BatchBackupFileSet) ([32]byte, error) {
	var zero [32]byte
	if restoreID == uuid.Nil {
		return zero, errors.New("Classic Restore task identity requires a restore UUID")
	}
	var records [][]byte
	var start, end []byte
	for _, set := range batch {
		for _, file := range set.SSTFiles {
			if file == nil || set.RewriteRules == nil {
				return zero, errors.New("Classic Restore task identity requires files and rewrite rules")
			}
			rule := restoreutils.FindMatchedRewriteRule(file, set.RewriteRules)
			if rule == nil {
				return zero, errors.New("Classic Restore task identity has no matched rewrite rule")
			}
			effective := *rule
			if err := restoreutils.SetTimeRangeFilter(set.RewriteRules, &effective, file.Cf); err != nil {
				return zero, err
			}
			if effective.IgnoreBeforeTimestamp != 0 || effective.IgnoreAfterTimestamp != 0 {
				return zero, errors.New("RestoreRegion does not support timestamp filtering")
			}
			fileStart := restoreutils.RewriteAndEncodeRawKey(file.StartKey, &effective)
			fileEnd := restoreutils.RewriteAndEncodeRawKey(file.EndKey, &effective)
			if start == nil || bytes.Compare(fileStart, start) < 0 {
				start = fileStart
			}
			if end == nil || bytes.Compare(fileEnd, end) > 0 {
				end = fileEnd
			}
			record := binary.BigEndian.AppendUint64(nil, uint64(set.TableID))
			record = appendTaskIDBytes(record, []byte(file.Name))
			record = appendTaskIDBytes(record, []byte(file.Cf))
			record = binary.BigEndian.AppendUint64(record, file.Size_)
			record = appendTaskIDBytes(record, file.Sha256)
			record = appendTaskIDBytes(record, file.StartKey)
			record = appendTaskIDBytes(record, file.EndKey)
			record = appendTaskIDBytes(record, effective.OldKeyPrefix)
			record = appendTaskIDBytes(record, effective.NewKeyPrefix)
			record = binary.BigEndian.AppendUint64(record, effective.NewTimestamp)
			record = binary.BigEndian.AppendUint64(record, effective.IgnoreAfterTimestamp)
			record = binary.BigEndian.AppendUint64(record, effective.IgnoreBeforeTimestamp)
			records = append(records, record)
		}
	}
	if len(records) == 0 {
		return zero, errors.New("Classic Restore task identity requires a nonempty batch")
	}
	slices.SortFunc(records, bytes.Compare)
	encoded := appendTaskIDBytes(nil, []byte("tidb/br/classic-restore-task/v1"))
	encoded = appendTaskIDBytes(encoded, restoreID[:])
	// This identity format is for the API V2 Classic Restore path only.
	encoded = binary.BigEndian.AppendUint64(encoded, 2)
	encoded = appendTaskIDBytes(encoded, start)
	encoded = appendTaskIDBytes(encoded, end)
	encoded = binary.BigEndian.AppendUint64(encoded, uint64(len(records)))
	hash := sha256.New()
	_, _ = hash.Write(encoded)
	for _, record := range records {
		_, _ = hash.Write(binary.BigEndian.AppendUint64(nil, uint64(len(record))))
		_, _ = hash.Write(record)
	}
	var id [32]byte
	hash.Sum(id[:0])
	return id, nil
}

func appendTaskIDBytes(dst, value []byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(len(value)))
	return append(dst, value...)
}

// prepareRestoreBatch retains the complete batch identity on every surviving set.
// It also handles old restore planning, where a zero restoreID disables task IDs.
func prepareRestoreBatch(batch restore.BatchBackupFileSet, checkpoints map[int64]map[string]struct{}, restoreID uuid.UUID) (restore.BatchBackupFileSet, error) {
	var id [32]byte
	if restoreID != uuid.Nil {
		var err error
		id, err = classicRestoreTaskID(restoreID, batch)
		if err != nil {
			return nil, err
		}
	}
	remaining := make(restore.BatchBackupFileSet, 0, len(batch))
	for _, set := range batch {
		set.SSTFiles = filterOutFiles(checkpoints[set.TableID], set.SSTFiles)
		if len(set.SSTFiles) > 0 {
			set.RestoreTaskID = id
			remaining = append(remaining, set)
		}
	}
	return remaining, nil
}
