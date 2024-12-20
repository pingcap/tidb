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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// CheckpointManager is a checkpoint manager implementation that used by
// non-distributed reorganization. It manages the data as two-level checkpoints:
// "flush"ed to local storage and "import"ed to TiKV. The checkpoint is saved in
// a table in the TiDB cluster.
type CheckpointManager struct {
	ctx           context.Context
	cancel        context.CancelFunc
	sessPool      *sess.Pool
	jobID         int64
	indexIDs      []int64
	localStoreDir string
	pdCli         pd.Client
	logger        *zap.Logger
	physicalID    int64

	// Derived and unchanged after the initialization.
	instanceAddr     string
	localDataIsValid bool

	// Live in memory.
	mu          sync.Mutex
	checkpoints map[int]*taskCheckpoint // task ID -> checkpoint
	// we require each task ID to be continuous and start from 0.
	minTaskIDFinished int
	dirty             bool

	// Persisted to the storage.
	flushedKeyLowWatermark  kv.Key
	importedKeyLowWatermark kv.Key
	flushedKeyCnt           int
	importedKeyCnt          int

	ts uint64

	// For persisting the checkpoint periodically.
	updaterWg sync.WaitGroup
	updaterCh chan chan struct{}
}

// taskCheckpoint is the checkpoint for a single task.
type taskCheckpoint struct {
	totalKeys     int
	writtenKeys   int
	checksum      int64
	endKey        kv.Key
	lastBatchRead bool
}

// FlushController is an interface to control the flush of data so after it
// returns caller can save checkpoint.
type FlushController interface {
	// Flush checks if al engines need to be flushed and imported based on given
	// FlushMode. It's concurrent safe.
	Flush(ctx context.Context, mode FlushMode) (flushed, imported bool, err error)
}

// NewCheckpointManager creates a new checkpoint manager.
func NewCheckpointManager(
	ctx context.Context,
	sessPool *sess.Pool,
	physicalID int64,
	jobID int64,
	indexIDs []int64,
	localStoreDir string,
	pdCli pd.Client,
) (*CheckpointManager, error) {
	instanceAddr := InstanceAddr()
	ctx2, cancel := context.WithCancel(ctx)
	logger := logutil.DDLIngestLogger().With(
		zap.Int64("jobID", jobID), zap.Int64s("indexIDs", indexIDs))

	cm := &CheckpointManager{
		ctx:           ctx2,
		cancel:        cancel,
		sessPool:      sessPool,
		jobID:         jobID,
		indexIDs:      indexIDs,
		localStoreDir: localStoreDir,
		pdCli:         pdCli,
		logger:        logger,
		checkpoints:   make(map[int]*taskCheckpoint, 16),
		mu:            sync.Mutex{},
		instanceAddr:  instanceAddr,
		physicalID:    physicalID,
		updaterWg:     sync.WaitGroup{},
		updaterCh:     make(chan chan struct{}),
	}
	err := cm.resumeOrInitCheckpoint()
	if err != nil {
		return nil, err
	}
	cm.updaterWg.Add(1)
	go func() {
		cm.updateCheckpointLoop()
		cm.updaterWg.Done()
	}()
	logger.Info("create checkpoint manager")
	return cm, nil
}

// InstanceAddr returns the string concat with instance address and temp-dir.
func InstanceAddr() string {
	cfg := config.GetGlobalConfig()
	dsn := net.JoinHostPort(cfg.AdvertiseAddress, strconv.Itoa(int(cfg.Port)))
	return fmt.Sprintf("%s:%s", dsn, cfg.TempDir)
}

// IsKeyProcessed checks if the key is processed. The key may not be imported.
// This is called before the reader reads the data and decides whether to skip
// the current task.
func (s *CheckpointManager) IsKeyProcessed(end kv.Key) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.importedKeyLowWatermark) > 0 && end.Cmp(s.importedKeyLowWatermark) <= 0 {
		return true
	}
	return s.localDataIsValid && len(s.flushedKeyLowWatermark) > 0 && end.Cmp(s.flushedKeyLowWatermark) <= 0
}

// NextKeyToProcess finds the next unprocessed key in checkpoint.
// If there is no such key, it returns nil.
func (s *CheckpointManager) NextKeyToProcess() kv.Key {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.localDataIsValid && len(s.flushedKeyLowWatermark) > 0 {
		return s.flushedKeyLowWatermark.Clone()
	}
	if len(s.importedKeyLowWatermark) > 0 {
		return s.importedKeyLowWatermark.Clone()
	}
	return nil
}

// Status returns the status of the checkpoint.
func (s *CheckpointManager) Status() (keyCnt int, minKeyImported kv.Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, cp := range s.checkpoints {
		total += cp.writtenKeys
	}
	// TODO(lance6716): ???
	return s.flushedKeyCnt + total, s.importedKeyLowWatermark
}

// Register registers a new task. taskID MUST be continuous ascending and start
// from 0.
//
// TODO(lance6716): remove this constraint, use endKey as taskID and use
// ordered map type for checkpoints.
func (s *CheckpointManager) Register(taskID int, end kv.Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[taskID] = &taskCheckpoint{
		endKey: end,
	}
}

// UpdateTotalKeys updates the total keys of the task.
// This is called by the reader after reading the data to update the number of rows contained in the current chunk.
func (s *CheckpointManager) UpdateTotalKeys(taskID int, delta int, last bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := s.checkpoints[taskID]
	cp.totalKeys += delta
	cp.lastBatchRead = last
}

// UpdateWrittenKeys updates the written keys of the task.
// This is called by the writer after writing the local engine to update the current number of rows written.
func (s *CheckpointManager) UpdateWrittenKeys(taskID int, delta int) {
	s.mu.Lock()
	cp := s.checkpoints[taskID]
	cp.writtenKeys += delta
	s.mu.Unlock()
}

// AdvanceWatermark advances the watermark according to flushed or imported status.
func (s *CheckpointManager) AdvanceWatermark(flushed, imported bool) {
	if !flushed {
		return
	}

	failpoint.Inject("resignAfterFlush", func() {
		// used in a manual test
		ResignOwnerForTest.Store(true)
		// wait until ResignOwnerForTest is processed
		for ResignOwnerForTest.Load() {
			time.Sleep(100 * time.Millisecond)
		}
	})

	s.mu.Lock()
	defer s.mu.Unlock()
	s.afterFlush()

	if imported {
		s.afterImport()
	}
}

// afterFlush should be called after all engine is flushed.
func (s *CheckpointManager) afterFlush() {
	for {
		cp := s.checkpoints[s.minTaskIDFinished]
		if cp == nil || !cp.lastBatchRead || cp.writtenKeys < cp.totalKeys {
			break
		}
		delete(s.checkpoints, s.minTaskIDFinished)
		s.minTaskIDFinished++
		s.flushedKeyLowWatermark = cp.endKey
		s.flushedKeyCnt += cp.totalKeys
		s.dirty = true
	}
}

func (s *CheckpointManager) afterImport() {
	if s.importedKeyLowWatermark.Cmp(s.flushedKeyLowWatermark) > 0 {
		s.logger.Warn("lower watermark of flushed key is less than imported key",
			zap.String("flushed", hex.EncodeToString(s.flushedKeyLowWatermark)),
			zap.String("imported", hex.EncodeToString(s.importedKeyLowWatermark)),
		)
		return
	}
	s.importedKeyLowWatermark = s.flushedKeyLowWatermark
	s.importedKeyCnt = s.flushedKeyCnt
	s.dirty = true
}

// Close closes the checkpoint manager.
func (s *CheckpointManager) Close() {
	err := s.updateCheckpoint()
	if err != nil {
		s.logger.Error("update checkpoint failed", zap.Error(err))
	}

	s.cancel()
	s.updaterWg.Wait()
	s.logger.Info("checkpoint manager closed")
}

// GetTS returns the TS saved in checkpoint.
func (s *CheckpointManager) GetTS() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ts
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

	PhysicalID int64 `json:"physical_id"`
	// TS of next engine ingest.
	TS uint64 `json:"ts"`

	Version int64 `json:"version"`
}

// JobCheckpointVersionCurrent is the current version of the checkpoint.
const (
	JobCheckpointVersionCurrent = JobCheckpointVersion1
	JobCheckpointVersion1       = 1
)

func (s *CheckpointManager) resumeOrInitCheckpoint() error {
	sessCtx, err := s.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.sessPool.Put(sessCtx)
	ddlSess := sess.NewSession(sessCtx)
	err = ddlSess.RunInTxn(func(se *sess.Session) error {
		template := "select reorg_meta from mysql.tidb_ddl_reorg where job_id = %d and ele_type = %s;"
		sql := fmt.Sprintf(template, s.jobID, util.WrapKey2String(meta.IndexElementKey))
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
			if cp.PhysicalID != s.physicalID {
				s.logger.Info("checkpoint physical table ID mismatch",
					zap.Int64("current", s.physicalID),
					zap.Int64("get", cp.PhysicalID))
				return nil
			}
			s.importedKeyLowWatermark = cp.GlobalSyncKey
			s.importedKeyCnt = cp.GlobalKeyCount
			s.ts = cp.TS
			folderNotEmpty := util.FolderNotEmpty(s.localStoreDir)
			if folderNotEmpty &&
				(s.instanceAddr == cp.InstanceAddr || cp.InstanceAddr == "" /* initial state */) {
				s.localDataIsValid = true
				s.flushedKeyLowWatermark = cp.LocalSyncKey
				s.flushedKeyCnt = cp.LocalKeyCount
			}
			s.logger.Info("resume checkpoint",
				zap.String("flushed key low watermark", hex.EncodeToString(s.flushedKeyLowWatermark)),
				zap.String("imported key low watermark", hex.EncodeToString(s.importedKeyLowWatermark)),
				zap.Int64("physical table ID", cp.PhysicalID),
				zap.String("previous instance", cp.InstanceAddr),
				zap.String("current instance", s.instanceAddr),
				zap.Bool("folder is empty", !folderNotEmpty))
			return nil
		}
		s.logger.Info("checkpoint not found")
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	if s.ts > 0 {
		return nil
	}
	// if TS is not set, we need to allocate a TS and save it to the storage before
	// continue.
	p, l, err := s.pdCli.GetTS(s.ctx)
	if err != nil {
		return errors.Trace(err)
	}
	ts := oracle.ComposeTS(p, l)
	s.ts = ts
	return s.updateCheckpointImpl()
}

// updateCheckpointImpl is only used by updateCheckpointLoop goroutine or in
// NewCheckpointManager. In other cases, use updateCheckpoint instead.
func (s *CheckpointManager) updateCheckpointImpl() error {
	s.mu.Lock()
	flushedKeyLowWatermark := s.flushedKeyLowWatermark
	importedKeyLowWatermark := s.importedKeyLowWatermark
	flushedKeyCnt := s.flushedKeyCnt
	importedKeyCnt := s.importedKeyCnt
	physicalID := s.physicalID
	ts := s.ts
	s.mu.Unlock()

	sessCtx, err := s.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.sessPool.Put(sessCtx)
	ddlSess := sess.NewSession(sessCtx)
	err = ddlSess.RunInTxn(func(se *sess.Session) error {
		template := "update mysql.tidb_ddl_reorg set reorg_meta = %s where job_id = %d and ele_type = %s;"
		cp := &ReorgCheckpoint{
			LocalSyncKey:   flushedKeyLowWatermark,
			GlobalSyncKey:  importedKeyLowWatermark,
			LocalKeyCount:  flushedKeyCnt,
			GlobalKeyCount: importedKeyCnt,
			InstanceAddr:   s.instanceAddr,
			PhysicalID:     physicalID,
			TS:             ts,
			Version:        JobCheckpointVersionCurrent,
		}
		rawReorgMeta, err := json.Marshal(JobReorgMeta{Checkpoint: cp})
		if err != nil {
			return errors.Trace(err)
		}
		sql := fmt.Sprintf(template, util.WrapKey2String(rawReorgMeta), s.jobID, util.WrapKey2String(meta.IndexElementKey))
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

	logFunc := s.logger.Info
	if err != nil {
		logFunc = s.logger.With(zap.Error(err)).Error
	}
	logFunc("update checkpoint",
		zap.String("local checkpoint", hex.EncodeToString(flushedKeyLowWatermark)),
		zap.String("global checkpoint", hex.EncodeToString(importedKeyLowWatermark)),
		zap.Int("flushed keys", flushedKeyCnt),
		zap.Int("imported keys", importedKeyCnt),
		zap.Int64("global physical ID", physicalID),
		zap.Uint64("ts", ts))
	return err
}

func (s *CheckpointManager) updateCheckpointLoop() {
	failpoint.Inject("checkpointLoopExit", func() {
		// used in a manual test
		failpoint.Return()
	})
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case finishCh := <-s.updaterCh:
			err := s.updateCheckpointImpl()
			if err != nil {
				s.logger.Error("update checkpoint failed", zap.Error(err))
			}
			close(finishCh)
		case <-ticker.C:
			s.mu.Lock()
			if !s.dirty {
				s.mu.Unlock()
				continue
			}
			s.mu.Unlock()
			err := s.updateCheckpointImpl()
			if err != nil {
				s.logger.Error("periodically update checkpoint failed", zap.Error(err))
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *CheckpointManager) updateCheckpoint() error {
	failpoint.Inject("checkpointLoopExit", func() {
		// used in a manual test
		failpoint.Return(errors.New("failpoint triggered so can't update checkpoint"))
	})
	finishCh := make(chan struct{})
	select {
	case s.updaterCh <- finishCh:
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	// wait updateCheckpointLoop to finish checkpoint update.
	select {
	case <-finishCh:
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	return nil
}

func (s *CheckpointManager) refreshTSAndUpdateCP() (uint64, error) {
	p, l, err := s.pdCli.GetTS(s.ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	newTS := oracle.ComposeTS(p, l)
	s.mu.Lock()
	s.ts = newTS
	s.mu.Unlock()
	return newTS, s.updateCheckpoint()
}
