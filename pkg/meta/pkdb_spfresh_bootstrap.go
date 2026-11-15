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

package meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/structure"
)

var mSPFreshBootstrap = []byte("SPFreshBootstrap")

// SPFreshBootstrapTarget records an empty index's identity before remote I/O.
// A retry retains the first snapshot and owner, even after configuration changes.
type SPFreshBootstrapTarget struct {
	TableID         int64    `json:"table_id"`
	IndexIDs        []int64  `json:"index_ids"`
	ClusterID       uint64   `json:"cluster_id"`
	KeyspaceID      uint32   `json:"keyspace_id"`
	WorkerEndpoints []string `json:"worker_endpoints"`
	Owner           string   `json:"owner"`
	BaseSnapshotTS  uint64   `json:"base_snapshot_ts"`
}

// SPFreshBootstrapJob survives lost bootstrap responses and owner replacement.
// Successful and failed terminal records both fence delayed admission; only
// failed records are eligible for cleanup. Records are never removed by a sweep.
type SPFreshBootstrapJob struct {
	JobID                int64                    `json:"job_id"`
	Targets              []SPFreshBootstrapTarget `json:"targets"`
	Finished             bool                     `json:"finished"`
	Succeeded            bool                     `json:"succeeded"`
	NextSweepAtUnixMilli int64                    `json:"next_sweep_at_unix_milli"`
}

func spfreshBootstrapKey(jobID int64) []byte { return []byte(fmt.Sprintf("%020d", jobID)) }

func decodeSPFreshBootstrap(data []byte) (*SPFreshBootstrapJob, error) {
	var record SPFreshBootstrapJob
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, errors.Trace(err)
	}
	if record.JobID <= 0 || (!record.Finished && record.Succeeded) {
		return nil, errors.New("invalid SPFresh bootstrap job")
	}
	seen := make(map[int64]struct{}, len(record.Targets))
	for _, target := range record.Targets {
		if target.TableID <= 0 || len(target.IndexIDs) == 0 || target.ClusterID == 0 || target.BaseSnapshotTS == 0 || target.Owner == "" || !slices.Contains(target.WorkerEndpoints, target.Owner) {
			return nil, errors.New("invalid SPFresh bootstrap target")
		}
		if _, ok := seen[target.TableID]; ok {
			return nil, errors.New("duplicate SPFresh bootstrap table")
		}
		seen[target.TableID] = struct{}{}
		for i, id := range target.IndexIDs {
			if id <= 0 || (i > 0 && id <= target.IndexIDs[i-1]) {
				return nil, errors.New("invalid SPFresh bootstrap index IDs")
			}
		}
	}
	return &record, nil
}

// RegisterSPFreshBootstrap must commit before sending any bootstrap RPC. The
// terminal transaction reads the same key, so admission cannot race completion.
func (m *Mutator) RegisterSPFreshBootstrap(jobID int64, target SPFreshBootstrapTarget) (SPFreshBootstrapTarget, error) {
	key := spfreshBootstrapKey(jobID)
	data, err := m.txn.HGet(mSPFreshBootstrap, key)
	if err != nil {
		return target, errors.Trace(err)
	}
	record := &SPFreshBootstrapJob{JobID: jobID}
	if len(data) != 0 {
		record, err = decodeSPFreshBootstrap(data)
		if err != nil {
			return target, err
		}
		if record.Finished {
			return target, errors.New("SPFresh bootstrap job is already finished")
		}
	}
	target.IndexIDs = slices.Clone(target.IndexIDs)
	slices.Sort(target.IndexIDs)
	for _, old := range record.Targets {
		if old.TableID != target.TableID {
			continue
		}
		if old.ClusterID != target.ClusterID || old.KeyspaceID != target.KeyspaceID || !slices.Equal(old.IndexIDs, target.IndexIDs) {
			return target, errors.New("SPFresh bootstrap identity changed")
		}
		return old, nil
	}
	record.Targets = append(record.Targets, target)
	data, err = json.Marshal(record)
	if err != nil {
		return target, errors.Trace(err)
	}
	if _, err = decodeSPFreshBootstrap(data); err != nil {
		return target, err
	}
	return target, m.txn.HSet(mSPFreshBootstrap, key, data)
}

// FinishSPFreshBootstrap runs in the DDL terminal transaction. keepFence closes
// admission even if cancellation won the race with the first bootstrap attempt.
func (m *Mutator) FinishSPFreshBootstrap(jobID int64, succeeded, keepFence bool) error {
	key := spfreshBootstrapKey(jobID)
	data, err := m.txn.HGet(mSPFreshBootstrap, key)
	if err != nil {
		return errors.Trace(err)
	}
	if len(data) == 0 && !keepFence {
		return nil
	}
	record := &SPFreshBootstrapJob{JobID: jobID}
	if len(data) != 0 {
		record, err = decodeSPFreshBootstrap(data)
		if err != nil {
			return err
		}
		if record.Finished {
			if record.Succeeded != succeeded {
				return errors.New("SPFresh bootstrap terminal outcome changed")
			}
			return nil
		}
	}
	record.Finished, record.Succeeded = true, succeeded
	data, err = json.Marshal(record)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.HSet(mSPFreshBootstrap, key, data)
}

// ScheduleSPFreshBootstrapCleanup keeps failed identities for late RPCs.
func (m *Mutator) ScheduleSPFreshBootstrapCleanup(jobID int64, nextSweepAtUnixMilli int64) error {
	key := spfreshBootstrapKey(jobID)
	data, err := m.txn.HGet(mSPFreshBootstrap, key)
	if err != nil {
		return errors.Trace(err)
	}
	record, err := decodeSPFreshBootstrap(data)
	if err != nil {
		return err
	}
	if !record.Finished || record.Succeeded || nextSweepAtUnixMilli <= 0 {
		return errors.New("SPFresh bootstrap job is not eligible for cleanup")
	}
	record.NextSweepAtUnixMilli = max(record.NextSweepAtUnixMilli, nextSweepAtUnixMilli)
	data, err = json.Marshal(record)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.HSet(mSPFreshBootstrap, key, data)
}

// ReadSPFreshBootstrapJob looks up one durable admission without relying on
// terminal DDL arguments, which may be cleared when a job is canceled.
func ReadSPFreshBootstrapJob(snapshot kv.Snapshot, jobID int64) (*SPFreshBootstrapJob, error) {
	snapshot.SetOption(kv.RequestSourceInternal, true)
	snapshot.SetOption(kv.RequestSourceType, kv.InternalTxnMeta)
	t := structure.NewStructure(snapshot, nil, mMetaPrefix)
	data, err := t.HGet(mSPFreshBootstrap, spfreshBootstrapKey(jobID))
	if err != nil || len(data) == 0 {
		return nil, errors.Trace(err)
	}
	record, err := decodeSPFreshBootstrap(data)
	if err != nil {
		return nil, err
	}
	if record.JobID != jobID {
		return nil, errors.New("SPFresh bootstrap key mismatch")
	}
	return record, nil
}

// ReadSPFreshBootstrapJobs scans a bounded page, including active/successful
// records so arbitrarily many terminal fences cannot make one tick unbounded.
func ReadSPFreshBootstrapJobs(snapshot kv.Snapshot, after int64, limit int) (records []*SPFreshBootstrapJob, err error) {
	if limit <= 0 {
		return nil, errors.New("SPFresh bootstrap page limit must be positive")
	}
	snapshot.SetOption(kv.RequestSourceInternal, true)
	snapshot.SetOption(kv.RequestSourceType, kv.InternalTxnMeta)
	t := structure.NewStructure(snapshot, nil, mMetaPrefix)
	err = t.HGetIter(mSPFreshBootstrap, func(pair structure.HashPair) error {
		if bytes.Compare(pair.Field, spfreshBootstrapKey(after)) <= 0 {
			return nil
		}
		record, err := decodeSPFreshBootstrap(pair.Value)
		if err != nil {
			return err
		}
		if !bytes.Equal(pair.Field, spfreshBootstrapKey(record.JobID)) {
			return errors.New("SPFresh bootstrap key mismatch")
		}
		records = append(records, record)
		if len(records) == limit {
			return errSPFreshBulkScanDone
		}
		return nil
	})
	if errors.Cause(err) == errSPFreshBulkScanDone {
		err = nil
	}
	return records, errors.Trace(err)
}
