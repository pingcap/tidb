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
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/structure"
)

var (
	mSPFreshBulkActive     = []byte("SPFreshBulkActive")
	mSPFreshBulkCleanup    = []byte("SPFreshBulkCleanup")
	errSPFreshBulkScanDone = errors.New("SPFresh bulk cleanup scan complete")
)

// SPFreshBulkCleanup binds one DDL job to its exclusively owned temporary objects.
// It is registered in the admission transaction and moved to the cleanup queue
// in the same transaction that finishes the DDL job. No credentials are logged.
type SPFreshBulkCleanup struct {
	JobID                int64    `json:"job_id"`
	RunID                string   `json:"run_id"`
	StorageURI           string   `json:"storage_uri"`
	WorkerEndpoints      []string `json:"worker_endpoints"`
	FinishedAtUnixMilli  int64    `json:"finished_at_unix_milli"`
	NextSweepAtUnixMilli int64    `json:"next_sweep_at_unix_milli"`
}

func decodeSPFreshBulkCleanup(data []byte) (*SPFreshBulkCleanup, error) {
	var record SPFreshBulkCleanup
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return nil, errors.Trace(err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("invalid SPFresh bulk cleanup trailing data")
	}
	if record.JobID <= 0 || record.RunID == "" || record.StorageURI == "" || len(record.WorkerEndpoints) == 0 || record.NextSweepAtUnixMilli <= 0 {
		return nil, errors.New("incomplete SPFresh bulk cleanup record")
	}
	return &record, nil
}

// RegisterSPFreshBulkCleanup records ownership before any object is uploaded.
// Repeating admission in an uncommitted DDL step must bind exactly the same run.
func (m *Mutator) RegisterSPFreshBulkCleanup(record SPFreshBulkCleanup) error {
	// Required current-format field. Active records use a stable due sentinel so
	// repeating admission in another transaction preserves byte equality.
	record.NextSweepAtUnixMilli = 1
	data, err := json.Marshal(record)
	if err != nil {
		return errors.Trace(err)
	}
	if record.FinishedAtUnixMilli != 0 {
		return errors.New("SPFresh bulk admission cannot register a finished run")
	}
	if _, err = decodeSPFreshBulkCleanup(data); err != nil {
		return err
	}
	key := []byte(record.RunID)
	old, err := m.txn.HGet(mSPFreshBulkActive, key)
	if err != nil {
		return errors.Trace(err)
	}
	if len(old) != 0 && !bytes.Equal(old, data) {
		return errors.New("SPFresh bulk prefix already belongs to another build")
	}
	pending, err := m.txn.HGet(mSPFreshBulkCleanup, key)
	if err != nil {
		return errors.Trace(err)
	}
	if len(pending) != 0 {
		return errors.New("SPFresh bulk prefix is already pending cleanup")
	}
	return m.txn.HSet(mSPFreshBulkActive, key, data)
}

// FinishSPFreshBulkCleanup makes a terminal job's objects eligible for background
// deletion. The enclosing DDL transaction also commits Public or rollback/history.
func (m *Mutator) FinishSPFreshBulkCleanup(jobID int64, runID string) error {
	key := []byte(runID)
	data, err := m.txn.HGet(mSPFreshBulkActive, key)
	if err != nil {
		return errors.Trace(err)
	}
	if len(data) == 0 {
		// Retrying a terminal transaction may observe its already committed queue
		// record. Accept only that exact current-job outcome, never missing state.
		pending, err := m.txn.HGet(mSPFreshBulkCleanup, key)
		if err != nil {
			return errors.Trace(err)
		}
		record, err := decodeSPFreshBulkCleanup(pending)
		if err != nil {
			return err
		}
		if record.JobID != jobID || record.RunID != runID || record.FinishedAtUnixMilli <= 0 {
			return errors.New("SPFresh bulk cleanup terminal identity mismatch")
		}
		return nil
	}
	record, err := decodeSPFreshBulkCleanup(data)
	if err != nil {
		return err
	}
	if record.JobID != jobID || record.RunID != runID {
		return errors.New("SPFresh bulk cleanup job identity mismatch")
	}
	record.FinishedAtUnixMilli = int64(m.StartTS >> 18)
	data, err = json.Marshal(record)
	if err != nil {
		return errors.Trace(err)
	}
	if err := m.txn.HSet(mSPFreshBulkCleanup, key, data); err != nil {
		return err
	}
	return m.txn.HDel(mSPFreshBulkActive, key)
}

// ScheduleSPFreshBulkCleanup retains the terminal run after an empty sweep.
// A timed-out PUT can still materialize remotely, so emptiness never retires this
// ownership record. Only an explicit offline reset may remove these tombstones.
func (m *Mutator) ScheduleSPFreshBulkCleanup(runID string, nextSweepAtUnixMilli int64) error {
	data, err := m.txn.HGet(mSPFreshBulkCleanup, []byte(runID))
	if err != nil {
		return errors.Trace(err)
	}
	record, err := decodeSPFreshBulkCleanup(data)
	if err != nil {
		return err
	}
	if record.RunID != runID || record.FinishedAtUnixMilli <= 0 || nextSweepAtUnixMilli <= record.FinishedAtUnixMilli {
		return errors.New("invalid SPFresh cleanup sweep identity or time")
	}
	record.NextSweepAtUnixMilli = max(record.NextSweepAtUnixMilli, nextSweepAtUnixMilli)
	data, err = json.Marshal(record)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.HSet(mSPFreshBulkCleanup, []byte(runID), data)
}

// ReadSPFreshBulkCleanup returns a bounded queue page after a run ID, or reports an active build.
// An empty after starts a new pass; callers rotate pages so failed runs cannot starve others.
// Active builds take priority; object-store deletion never discovers build inputs.
func ReadSPFreshBulkCleanup(snapshot kv.Snapshot, after string, limit int) (active bool, pending []*SPFreshBulkCleanup, err error) {
	if limit <= 0 {
		return false, nil, errors.New("SPFresh cleanup page limit must be positive")
	}
	snapshot.SetOption(kv.RequestSourceInternal, true)
	snapshot.SetOption(kv.RequestSourceType, kv.InternalTxnMeta)
	t := structure.NewStructure(snapshot, nil, mMetaPrefix)
	err = t.HGetIter(mSPFreshBulkActive, func(structure.HashPair) error {
		active = true
		return errSPFreshBulkScanDone
	})
	if errors.Cause(err) == errSPFreshBulkScanDone {
		return true, nil, nil
	}
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	err = t.HGetIter(mSPFreshBulkCleanup, func(pair structure.HashPair) error {
		if string(pair.Field) <= after {
			return nil
		}
		record, err := decodeSPFreshBulkCleanup(pair.Value)
		if err != nil {
			return err
		}
		if string(pair.Field) != record.RunID || record.FinishedAtUnixMilli <= 0 {
			return errors.New("SPFresh cleanup key, run identity or terminal timestamp is invalid")
		}
		pending = append(pending, record)
		if len(pending) == limit {
			return errSPFreshBulkScanDone
		}
		return nil
	})
	if errors.Cause(err) == errSPFreshBulkScanDone {
		err = nil
	}
	return false, pending, errors.Trace(err)
}
