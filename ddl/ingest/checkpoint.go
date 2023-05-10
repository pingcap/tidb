// Copyright 2023 PingCAP, Inc.
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

package ingest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	sess "github.com/pingcap/tidb/ddl/internal/session"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// CheckpointManager is a checkpoint manager implementation that used by non-distributed reorganization.
type CheckpointManager struct {
	ctx        context.Context
	flushCtrl  FlushController
	sessPool   *sess.Pool
	jobID      int64
	indexID    int64
	physicalID int64

	// Derived and unchanged after the initialization.
	instanceAddr     string
	localDataIsValid bool

	// Live in memory.
	checkpoints     map[int]*TaskCheckpoint // task ID -> checkpoint
	mu              sync.Mutex
	minTaskIDSynced int
	dirty           bool

	// Persisted to the storage.
	minKeySyncLocal  kv.Key
	minKeySyncGlobal kv.Key
	localCnt         int
	globalCnt        int

	// For persisting the checkpoint periodically.
	updating      bool
	updaterWg     sync.WaitGroup
	updaterCh     chan *sync.WaitGroup
	updaterExitCh chan struct{}
}

// TaskCheckpoint is the checkpoint for a single task.
type TaskCheckpoint struct {
	totalKeys     int
	currentKeys   int
	checksum      int64
	endKey        kv.Key
	lastBatchSent bool
}

// FlushController is an interface to control the flush of the checkpoint.
type FlushController interface {
	Flush(indexID int64, mode FlushMode) (flushed, imported bool, err error)
}

// NewCheckpointManager creates a new checkpoint manager.
func NewCheckpointManager(ctx context.Context, flushCtrl FlushController,
	sessPool *sess.Pool, jobID, indexID int64) (*CheckpointManager, error) {
	instanceAddr := InitInstanceAddr()
	cm := &CheckpointManager{
		ctx:           ctx,
		flushCtrl:     flushCtrl,
		sessPool:      sessPool,
		jobID:         jobID,
		indexID:       indexID,
		checkpoints:   make(map[int]*TaskCheckpoint, 16),
		mu:            sync.Mutex{},
		instanceAddr:  instanceAddr,
		updaterWg:     sync.WaitGroup{},
		updaterExitCh: make(chan struct{}),
		updaterCh:     make(chan *sync.WaitGroup),
	}
	err := cm.resumeCheckpoint()
	if err != nil {
		return nil, err
	}
	cm.updaterWg.Add(1)
	go func() {
		cm.updateCheckpointLoop()
		cm.updaterWg.Done()
	}()
	return cm, nil
}

// InitInstanceAddr returns the string concat with instance address and temp-dir.
func InitInstanceAddr() string {
	cfg := config.GetGlobalConfig()
	dsn := net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port)))
	return fmt.Sprintf("%s:%s", dsn, cfg.TempDir)
}

// IsComplete checks if the task is complete.
// This is called before the reader reads the data and decides whether to skip the current task.
func (s *CheckpointManager) IsComplete(end kv.Key) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.minKeySyncGlobal) > 0 && end.Cmp(s.minKeySyncGlobal) <= 0 {
		return true
	}
	return s.localDataIsValid && len(s.minKeySyncLocal) > 0 && end.Cmp(s.minKeySyncLocal) <= 0
}

// Status returns the status of the checkpoint.
func (s *CheckpointManager) Status() (int, kv.Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, cp := range s.checkpoints {
		total += cp.currentKeys
	}
	return s.localCnt + total, s.minKeySyncGlobal
}

// Register registers a new task.
func (s *CheckpointManager) Register(taskID int, end kv.Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[taskID] = &TaskCheckpoint{
		endKey: end,
	}
}

// UpdateTotal updates the total keys of the task.
// This is called by the reader after reading the data to update the number of rows contained in the current chunk.
func (s *CheckpointManager) UpdateTotal(taskID int, added int, last bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := s.checkpoints[taskID]
	cp.totalKeys += added
	cp.lastBatchSent = last
}

// UpdateCurrent updates the current keys of the task.
// This is called by the writer after writing the local engine to update the current number of rows written.
func (s *CheckpointManager) UpdateCurrent(taskID int, added int) error {
	s.mu.Lock()
	cp := s.checkpoints[taskID]
	cp.currentKeys += added
	s.mu.Unlock()

	flushed, imported, err := s.flushCtrl.Flush(s.indexID, FlushModeAuto)
	if !flushed || err != nil {
		return err
	}
	// Progress the minimum synced key.
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		cp := s.checkpoints[s.minTaskIDSynced+1]
		if cp == nil || !cp.lastBatchSent || cp.currentKeys < cp.totalKeys {
			break
		}
		s.minTaskIDSynced++
		s.minKeySyncLocal = cp.endKey
		s.localCnt += cp.totalKeys
		delete(s.checkpoints, s.minTaskIDSynced)
		s.dirty = true
	}
	if imported && s.minKeySyncGlobal.Cmp(s.minKeySyncLocal) != 0 {
		s.minKeySyncGlobal = s.minKeySyncLocal
		s.globalCnt = s.localCnt
		s.dirty = true
	}
	return nil
}

// Close closes the checkpoint manager.
func (s *CheckpointManager) Close() {
	s.updaterExitCh <- struct{}{}
	s.updaterWg.Wait()
}

// Sync syncs the checkpoint.
func (s *CheckpointManager) Sync() {
	wg := sync.WaitGroup{}
	wg.Add(1)
	s.updaterCh <- &wg
	wg.Wait()
}

// Reset resets the checkpoint manager between two partitions.
func (s *CheckpointManager) Reset(newPhysicalID int64) {
	if s.physicalID != 0 {
		_, _, err := s.flushCtrl.Flush(s.indexID, FlushModeForceLocal)
		if err != nil {
			logutil.BgLogger().Warn("[ddl-ingest] flush local engine failed", zap.Error(err))
		}
	}
	s.mu.Lock()
	if s.physicalID != newPhysicalID {
		s.minKeySyncLocal = nil
		s.minKeySyncGlobal = nil
		s.minTaskIDSynced = 0
		s.physicalID = newPhysicalID
		for id, cp := range s.checkpoints {
			s.localCnt += cp.totalKeys
			delete(s.checkpoints, id)
		}
	}
	s.mu.Unlock()
}

// JobReorgMeta is the metadata for a reorg job.
type JobReorgMeta struct {
	Checkpoint *ReorgCheckpoint `json:"reorg_checkpoint"`
}

// ReorgCheckpoint is the checkpoint for a reorg job.
type ReorgCheckpoint struct {
	LocalSyncKey   kv.Key `json:"local_sync_key"`
	LocalKeyCount  int    `json:"local_key_count"`
	GlobalSyncKey  kv.Key `json:"global_sync_key"`
	GlobalKeyCount int    `json:"global_key_count"`
	InstanceAddr   string `json:"instance_addr"`
	PhysicalID     int64  `json:"physical_id"`
	Version        int64  `json:"version"`
}

// JobCheckpointVersionCurrent is the current version of the checkpoint.
const (
	JobCheckpointVersionCurrent = JobCheckpointVersion1
	JobCheckpointVersion1       = 1
)

func (s *CheckpointManager) resumeCheckpoint() error {
	sessCtx, err := s.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.sessPool.Put(sessCtx)
	ddlSess := sess.NewSession(sessCtx)
	return ddlSess.RunInTxn(func(se *sess.Session) error {
		template := "select reorg_meta from mysql.tidb_ddl_reorg where job_id = %d and ele_id = %d and ele_type = %s;"
		sql := fmt.Sprintf(template, s.jobID, s.indexID, util.WrapKey2String(meta.IndexElementKey))
		ctx := kv.WithInternalSourceType(s.ctx, kv.InternalTxnBackfillDDLPrefix+"add_index")
		rows, err := se.Execute(ctx, sql, "get_checkpoint")
		if err != nil {
			return errors.Trace(err)
		}
		if len(rows) == 0 || rows[0].IsNull(0) {
			return nil
		}
		rawReorgMeta := rows[0].GetBytes(0)
		var reorgMeta JobReorgMeta
		err = json.Unmarshal(rawReorgMeta, &reorgMeta)
		if err != nil {
			return errors.Trace(err)
		}
		if cp := reorgMeta.Checkpoint; cp != nil {
			s.minKeySyncGlobal = cp.GlobalSyncKey
			s.globalCnt = cp.GlobalKeyCount
			if s.instanceAddr == cp.InstanceAddr {
				s.localDataIsValid = true
				s.minKeySyncLocal = cp.LocalSyncKey
				s.localCnt = cp.LocalKeyCount
				s.physicalID = cp.PhysicalID
			}
			logutil.BgLogger().Info("[ddl-ingest] resume checkpoint",
				zap.Int64("job ID", s.jobID), zap.Int64("index ID", s.indexID),
				zap.String("local checkpoint", hex.EncodeToString(s.minKeySyncLocal)),
				zap.String("global checkpoint", hex.EncodeToString(s.minKeySyncGlobal)),
				zap.String("previous instance", cp.InstanceAddr),
				zap.String("current instance", s.instanceAddr))
			return nil
		}
		logutil.BgLogger().Info("[ddl-ingest] checkpoint is empty",
			zap.Int64("job ID", s.jobID), zap.Int64("index ID", s.indexID))
		return nil
	})
}

func (s *CheckpointManager) updateCheckpoint() error {
	s.mu.Lock()
	currentLocalKey := s.minKeySyncLocal
	currentGlobalKey := s.minKeySyncGlobal
	currentLocalCnt := s.localCnt
	currentGlobalCnt := s.globalCnt
	s.updating = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.updating = false
		s.mu.Unlock()
	}()

	sessCtx, err := s.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.sessPool.Put(sessCtx)
	ddlSess := sess.NewSession(sessCtx)
	err = ddlSess.RunInTxn(func(se *sess.Session) error {
		template := "update mysql.tidb_ddl_reorg set reorg_meta = %s where job_id = %d and ele_id = %d and ele_type = %s;"
		cp := &ReorgCheckpoint{
			LocalSyncKey:   currentLocalKey,
			GlobalSyncKey:  currentGlobalKey,
			LocalKeyCount:  currentLocalCnt,
			GlobalKeyCount: currentGlobalCnt,
			InstanceAddr:   s.instanceAddr,
			PhysicalID:     s.physicalID,
			Version:        JobCheckpointVersionCurrent,
		}
		rawReorgMeta, err := json.Marshal(JobReorgMeta{Checkpoint: cp})
		if err != nil {
			return errors.Trace(err)
		}
		sql := fmt.Sprintf(template, util.WrapKey2String(rawReorgMeta), s.jobID, s.indexID, util.WrapKey2String(meta.IndexElementKey))
		ctx := kv.WithInternalSourceType(s.ctx, kv.InternalTxnBackfillDDLPrefix+"add_index")
		_, err = se.Execute(ctx, sql, "update_checkpoint")
		if err != nil {
			return errors.Trace(err)
		}
		s.mu.Lock()
		s.dirty = false
		s.mu.Unlock()
		return nil
	})
	logutil.BgLogger().Info("[ddl-ingest] update checkpoint",
		zap.Int64("job ID", s.jobID), zap.Int64("index ID", s.indexID),
		zap.String("local checkpoint", hex.EncodeToString(currentLocalKey)),
		zap.String("global checkpoint", hex.EncodeToString(currentGlobalKey)),
		zap.Error(err))
	return err
}

func (s *CheckpointManager) updateCheckpointLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case wg := <-s.updaterCh:
			err := s.updateCheckpoint()
			if err != nil {
				logutil.BgLogger().Error("[ddl-ingest] update checkpoint failed", zap.Error(err))
			}
			wg.Done()
		case <-ticker.C:
			s.mu.Lock()
			if !s.dirty || s.updating {
				s.mu.Unlock()
				continue
			}
			s.mu.Unlock()
			err := s.updateCheckpoint()
			if err != nil {
				logutil.BgLogger().Error("[ddl-ingest] update checkpoint failed", zap.Error(err))
			}
		case <-s.updaterExitCh:
			return
		}
	}
}
