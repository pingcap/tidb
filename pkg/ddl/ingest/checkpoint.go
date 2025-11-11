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
	"os"
	"sort"
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
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// CheckpointStorage defines the interface for checkpoint storage operations
type CheckpointStorage interface {
	// LoadCheckpoint loads checkpoint from storage
	LoadCheckpoint(ctx context.Context) (*ReorgCheckpoint, error)
	// SaveCheckpoint saves checkpoint to storage
	SaveCheckpoint(ctx context.Context, checkpoint *ReorgCheckpoint) error
}

// NormalCheckpointStorage implements CheckpointStorage for the Non-DXF reorg table storage
type NormalCheckpointStorage struct {
	sessPool   *sess.Pool
	jobID      int64
	physicalID int64
}

// DistTaskCheckpointStorage implements CheckpointStorage for distributed task storage
type DistTaskCheckpointStorage struct {
	updateFunc func(context.Context, int64, any) error
	getFunc    func(context.Context, int64) (string, error)
	subtaskID  int64
}

// LoadCheckpoint loads the checkpoint from the normal storage strategy.
func (s *NormalCheckpointStorage) LoadCheckpoint(ctx context.Context) (*ReorgCheckpoint, error) {
	sessCtx, err := s.sessPool.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer s.sessPool.Put(sessCtx)

	ddlSess := sess.NewSession(sessCtx)
	var checkpoint *ReorgCheckpoint

	err = ddlSess.RunInTxn(func(se *sess.Session) error {
		template := "select reorg_meta from mysql.tidb_ddl_reorg where job_id = %d and ele_type = %s;"
		sql := fmt.Sprintf(template, s.jobID, util.WrapKey2String(meta.IndexElementKey))
		ctx := kv.WithInternalSourceType(ctx, kv.InternalTxnBackfillDDLPrefix+"add_index")
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
				return nil // Skip mismatched physical ID
			}
			checkpoint = cp
		}
		return nil
	})

	return checkpoint, err
}

// SaveCheckpoint saves the checkpoint to the normal storage strategy.
func (s *NormalCheckpointStorage) SaveCheckpoint(ctx context.Context, checkpoint *ReorgCheckpoint) error {
	sessCtx, err := s.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.sessPool.Put(sessCtx)

	ddlSess := sess.NewSession(sessCtx)
	return ddlSess.RunInTxn(func(se *sess.Session) error {
		template := "update mysql.tidb_ddl_reorg set reorg_meta = %s where job_id = %d and ele_type = %s;"
		rawReorgMeta, err := json.Marshal(JobReorgMeta{Checkpoint: checkpoint})
		if err != nil {
			return errors.Trace(err)
		}
		sql := fmt.Sprintf(template, util.WrapKey2String(rawReorgMeta), s.jobID, util.WrapKey2String(meta.IndexElementKey))
		ctx := kv.WithInternalSourceType(ctx, kv.InternalTxnBackfillDDLPrefix+"add_index")
		_, err = se.Execute(ctx, sql, "update_checkpoint")
		return err
	})
}

// LoadCheckpoint loads the checkpoint from the distributed task storage strategy.
func (s *DistTaskCheckpointStorage) LoadCheckpoint(ctx context.Context) (*ReorgCheckpoint, error) {
	if s.getFunc == nil {
		return nil, nil
	}

	checkpointJSON, err := s.getFunc(ctx, s.subtaskID)
	if err != nil {
		return nil, err
	}

	if checkpointJSON == "" || checkpointJSON == "{}" {
		return nil, nil
	}

	var checkpoint ReorgCheckpoint
	err = json.Unmarshal([]byte(checkpointJSON), &checkpoint)
	if err != nil {
		return nil, err
	}

	return &checkpoint, nil
}

// SaveCheckpoint saves the checkpoint to the distributed task storage strategy.
func (s *DistTaskCheckpointStorage) SaveCheckpoint(ctx context.Context, checkpoint *ReorgCheckpoint) error {
	if s.updateFunc == nil {
		return nil
	}

	return s.updateFunc(ctx, s.subtaskID, checkpoint)
}

// ProcessedRange represents a processed key range with closed interval [StartKey, EndKey].
// PrevTailKey is the last key of the previous interval (if known) to help stitch/merge continuous ranges.
type ProcessedRange struct {
	StartKey    kv.Key
	EndKey      kv.Key
	PrevTailKey kv.Key
}

// CheckpointManager is a checkpoint manager implementation that used by reorganization.
type CheckpointManager struct {
	ctx           context.Context
	cancel        context.CancelFunc
	localStoreDir string
	pdCli         pd.Client
	logger        *zap.Logger
	physicalID    int64

	// strategy for checkpoint storage
	storage CheckpointStorage

	// Derived and unchanged after the initialization.
	instanceAddr string

	// Live in memory.
	mu    sync.Mutex
	dirty bool

	// pendingFinishedRanges: locally written, not yet merged, closed intervals
	pendingFinishedRanges []ProcessedRange
	// importedRanges: imported but not fully contiguous from watermark, closed intervals
	importedRanges   []ProcessedRange
	localWrittenRows int

	// Persisted to the storage.
	// importedKeyLowWatermark: first not-yet-imported key
	importedKeyLowWatermark kv.Key
	importedKeyCnt          int

	ts uint64

	// For persisting the checkpoint periodically.
	updaterWg sync.WaitGroup
	updaterCh chan chan struct{}

	// initial start key to set low watermark in resumeOrInitCheckpoint
	initialStartKey kv.Key
}

// newCheckpointManagerWithStorage is the common constructor
func newCheckpointManagerWithStorage(
	ctx context.Context,
	storage CheckpointStorage,
	physicalID int64,
	localStoreDir string,
	pdCli pd.Client,
	startKey kv.Key,
) (*CheckpointManager, error) {
	instanceAddr := InstanceAddr()
	ctx2, cancel := context.WithCancel(ctx)
	logger := logutil.DDLIngestLogger().With(zap.Int64("physicalID", physicalID))

	cm := &CheckpointManager{
		ctx:                   ctx2,
		cancel:                cancel,
		storage:               storage,
		localStoreDir:         localStoreDir,
		pdCli:                 pdCli,
		logger:                logger,
		instanceAddr:          instanceAddr,
		physicalID:            physicalID,
		updaterWg:             sync.WaitGroup{},
		updaterCh:             make(chan chan struct{}),
		pendingFinishedRanges: make([]ProcessedRange, 0),
		importedRanges:        make([]ProcessedRange, 0),
		initialStartKey:       startKey,
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

// NewCheckpointManager creates a new checkpoint manager with normal storage
func NewCheckpointManager(
	ctx context.Context,
	sessPool *sess.Pool,
	physicalID int64,
	jobID int64,
	localStoreDir string,
	pdCli pd.Client,
	startKey kv.Key,
) (*CheckpointManager, error) {
	storage := &NormalCheckpointStorage{
		sessPool:   sessPool,
		jobID:      jobID,
		physicalID: physicalID,
	}

	return newCheckpointManagerWithStorage(ctx, storage, physicalID, localStoreDir, pdCli, startKey)
}

// NewCheckpointManagerForDistTask creates a new checkpoint manager with DXF task storage
func NewCheckpointManagerForDistTask(
	ctx context.Context,
	subtaskID int64,
	physicalID int64,
	localStoreDir string,
	pdCli pd.Client,
	updateFunc func(context.Context, int64, any) error,
	getFunc func(context.Context, int64) (string, error),
	startKey kv.Key,
) (*CheckpointManager, error) {
	storage := &DistTaskCheckpointStorage{
		updateFunc: updateFunc,
		getFunc:    getFunc,
		subtaskID:  subtaskID,
	}

	return newCheckpointManagerWithStorage(ctx, storage, physicalID, localStoreDir, pdCli, startKey)
}

// InstanceAddr returns the string concat with instance address and temp-dir.
func InstanceAddr() string {
	cfg := config.GetGlobalConfig()
	dsn := net.JoinHostPort(cfg.AdvertiseAddress, strconv.Itoa(int(cfg.Port)))
	return fmt.Sprintf("%s:%s", dsn, cfg.TempDir)
}

// NextStartKey finds the next unprocessed key in checkpoint.
// If there is no such key, it returns nil.
func (s *CheckpointManager) NextStartKey() kv.Key {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.importedKeyLowWatermark) > 0 {
		return s.importedKeyLowWatermark.Clone()
	}
	return nil
}

// TotalKeyCount returns the key counts that have processed.
// It contains the keys that is not sync to checkpoint.
func (s *CheckpointManager) TotalKeyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.importedKeyCnt + s.localWrittenRows
}

// FinishChunk records write-to-local progress for a range.
// delta: written rows in this batch;
// prevTailKey: the last key of the previous chunk to help stitch/merge continuous ranges.
func (s *CheckpointManager) FinishChunk(rg kv.KeyRange, delta int, prevTailKey kv.Key) {
	if len(rg.StartKey) == 0 || len(rg.EndKey) == 0 || rg.StartKey.Cmp(rg.EndKey) > 0 {
		s.logger.Warn("FinishChunk skip invalid range",
			zap.String("start", hex.EncodeToString(rg.StartKey)),
			zap.String("end", hex.EncodeToString(rg.EndKey)),
			zap.Int("delta", delta))
		return
	}

	s.mu.Lock()
	s.localWrittenRows += delta
	s.pendingFinishedRanges = append(s.pendingFinishedRanges, ProcessedRange{
		StartKey:    rg.StartKey.Clone(),
		EndKey:      rg.EndKey.Clone(),
		PrevTailKey: prevTailKey.Clone(),
	})
	s.mu.Unlock()
}

// AdvanceWatermark merges finished local ranges, advances the low watermark if contiguous,
// allocates a new TS and persists checkpoint.
func (s *CheckpointManager) AdvanceWatermark() error {
	failpoint.Inject("resignAfterFlush", func() {
		// used in a manual test
		ResignOwnerForTest.Store(true)
		// wait until ResignOwnerForTest is processed
		for ResignOwnerForTest.Load() {
			time.Sleep(100 * time.Millisecond)
		}
	})

	s.mu.Lock()
	if len(s.pendingFinishedRanges) == 0 {
		s.mu.Unlock()
		return nil
	}
	// Detach pending ranges.
	pend := append([]ProcessedRange(nil), s.pendingFinishedRanges...)
	s.pendingFinishedRanges = s.pendingFinishedRanges[:0]
	oldWM := s.importedKeyLowWatermark.Clone()

	s.importedRanges = MergeAndCompactRanges(append(s.importedRanges, pend...))
	s.importedRanges = PruneRanges(s.importedRanges, s.importedKeyLowWatermark)

	consumed := 0
	for len(s.importedRanges) > 0 && CanPromoteWatermark(s.importedRanges[0], s.importedKeyLowWatermark) {
		s.importedKeyLowWatermark = rangeExclusiveUpper(s.importedRanges[0]).Clone()
		s.importedRanges = s.importedRanges[1:]
		consumed++
	}

	s.importedKeyCnt += s.localWrittenRows
	s.localWrittenRows = 0
	newWM := s.importedKeyLowWatermark.Clone()
	s.dirty = true
	remain := len(s.importedRanges)
	s.mu.Unlock()

	s.logger.Info("watermark advanced",
		zap.String("old", hex.EncodeToString(oldWM)),
		zap.String("new", hex.EncodeToString(newWM)),
		zap.Int("consumed_ranges", consumed),
		zap.Int("remaining_ranges", remain),
		zap.Int("imported_keys", s.importedKeyCnt),
	)

	if err := s.afterImport(); err != nil {
		return err
	}
	return s.updateCheckpoint()
}

// CanPromoteWatermark reports whether the first imported range can extend the low watermark.
// export for test.
func CanPromoteWatermark(r ProcessedRange, low kv.Key) bool {
	if len(r.PrevTailKey) > 0 {
		return r.PrevTailKey.Cmp(low) <= 0
	}
	return r.StartKey.Cmp(low) <= 0
}

// MergeAndCompactRanges merges (pending + existing) ranges into non-overlapping sorted segments.
// It:
//  1. Filters invalid ranges
//  2. Sorts by [StartKey, EndKey]
//  3. Merges overlapping or PrevTailKey-stitched segments
//
// export for test.
func MergeAndCompactRanges(rs []ProcessedRange) []ProcessedRange {
	if len(rs) <= 1 {
		return rs
	}
	tmp := make([]ProcessedRange, 0, len(rs))
	for _, r := range rs {
		if len(r.StartKey) == 0 || len(r.EndKey) == 0 || r.StartKey.Cmp(r.EndKey) > 0 {
			continue
		}
		tmp = append(tmp, r)
	}
	if len(tmp) <= 1 {
		return tmp
	}
	sort.Slice(tmp, func(i, j int) bool {
		if c := tmp[i].StartKey.Cmp(tmp[j].StartKey); c != 0 {
			return c < 0
		}
		return tmp[i].EndKey.Cmp(tmp[j].EndKey) < 0
	})
	out := tmp[:0]
	cur := tmp[0]
	for i := 1; i < len(tmp); i++ {
		n := tmp[i]
		overlap := n.StartKey.Cmp(cur.EndKey) <= 0
		stitch := len(n.PrevTailKey) > 0 &&
			(n.PrevTailKey.Cmp(cur.EndKey) == 0 || n.PrevTailKey.Next().Cmp(cur.EndKey) == 0)
		if overlap || stitch {
			if n.EndKey.Cmp(cur.EndKey) > 0 {
				cur.EndKey = n.EndKey.Clone()
			}
			continue
		}
		out = append(out, cur)
		cur = n
	}
	out = append(out, cur)
	return out
}

// PruneRanges discards ranges fully below the global low watermark.
// export for test.
func PruneRanges(rs []ProcessedRange, low kv.Key) []ProcessedRange {
	if len(low) == 0 || len(rs) == 0 {
		return rs
	}
	i := 0
	for i < len(rs) && rs[i].EndKey.Cmp(low) < 0 {
		i++
	}
	if i == 0 {
		return rs
	}
	if i >= len(rs) {
		return nil
	}
	return append([]ProcessedRange(nil), rs[i:]...)
}

func rangeExclusiveUpper(r ProcessedRange) kv.Key {
	if len(r.EndKey) == 0 {
		return nil
	}
	return r.EndKey.Next()
}

// SubtractRanges subtracts imported ranges from input
// input: half-open [StartKey, EndKey)
// imported: closed [StartKey, EndKey]
// export for test.
func SubtractRanges(input []kv.KeyRange, imported []ProcessedRange) []kv.KeyRange {
	if len(imported) == 0 || len(input) == 0 {
		return input
	}
	res := make([]kv.KeyRange, 0, len(input))
	for _, r := range input {
		cur := []kv.KeyRange{r}
		for _, im := range imported {
			next := cur[:0]
			for _, piece := range cur {
				next = append(next, SubtractOne(piece, im)...)
			}
			cur = next
			if len(cur) == 0 {
				break
			}
		}
		res = append(res, cur...)
	}
	return res
}

// SubtractOne subtracts range b from a and may return up to two residual pieces.
// a is half-open [StartKey, EndKey)
// b is closed [StartKey, EndKey]
// export for test.
func SubtractOne(a kv.KeyRange, b ProcessedRange) []kv.KeyRange {
	// Non-overlap cases:
	// 1) b entirely before a: b.End < a.Start  (strict < because if == they share a.Start key -> overlap)
	// 2) b starts at or after a's exclusive end: b.Start >= a.End
	if b.EndKey.Cmp(a.StartKey) < 0 || b.StartKey.Cmp(a.EndKey) >= 0 {
		return []kv.KeyRange{a}
	}
	out := make([]kv.KeyRange, 0, 2)
	if b.StartKey.Cmp(a.StartKey) > 0 {
		// left residual: [a.Start, b.Start)
		out = append(out, kv.KeyRange{StartKey: a.StartKey.Clone(), EndKey: b.StartKey.Clone()})
	}
	if b.EndKey.Cmp(a.EndKey) < 0 {
		// right residual: start after b.End
		out = append(out, kv.KeyRange{StartKey: b.EndKey.Clone().Next(), EndKey: a.EndKey.Clone()})
	}
	return out
}

// FilterUnimportedRanges removes already imported (or below watermark) portions from the input ranges.
func (s *CheckpointManager) FilterUnimportedRanges(ranges []kv.KeyRange) []kv.KeyRange {
	s.mu.Lock()
	global := s.importedKeyLowWatermark.Clone()
	s.mu.Unlock()

	out := make([]kv.KeyRange, 0, len(ranges))
	for _, r := range ranges {
		if len(global) > 0 && r.EndKey.Cmp(global) <= 0 {
			continue
		}
		if len(global) > 0 && r.StartKey.Cmp(global) < 0 {
			r.StartKey = global.Clone()
			if r.StartKey.Cmp(r.EndKey) >= 0 {
				continue
			}
		}
		out = append(out, r)
	}
	if len(out) == 0 {
		s.logger.Info("filter ranges result empty")
		return nil
	}
	res := SubtractRanges(out, s.importedRanges)
	s.logger.Info("filter ranges done",
		zap.Int("input_count", len(ranges)),
		zap.Int("output_count", len(res)),
		zap.Int("imported_segments", len(s.importedRanges)),
	)
	return res
}

func (s *CheckpointManager) afterImport() error {
	p, l, err := s.pdCli.GetTS(s.ctx)
	failpoint.Inject("mockAfterImportAllocTSFailed", func(_ failpoint.Value) {
		err = errors.Errorf("mock err")
	})
	if err != nil {
		s.logger.Warn("advance watermark get ts failed", zap.Error(err))
		return err
	}
	newTS := oracle.ComposeTS(p, l)

	s.mu.Lock()
	defer s.mu.Unlock()
	intest.Assert(s.ts < newTS)
	if s.ts < newTS {
		s.ts = newTS
	}
	s.dirty = true
	return nil
}

// cleanupLocalStoreDirUnsafe removes the on-disk sorted data to avoid re-importing
// lingering SSTs after a resume. It recreates the directory empty.
func (s *CheckpointManager) cleanupLocalStoreDirUnsafe() {
	if s.localStoreDir == "" {
		return
	}
	if err := os.RemoveAll(s.localStoreDir); err != nil {
		s.logger.Warn("failed to cleanup local store dir", zap.String("dir", s.localStoreDir), zap.Error(err))
		return
	}
	if err := os.MkdirAll(s.localStoreDir, 0o700); err != nil {
		s.logger.Warn("failed to recreate local store dir", zap.String("dir", s.localStoreDir), zap.Error(err))
		return
	}
	s.logger.Info("cleaned local sorted dir on resume to avoid re-importing lingering SSTs", zap.String("dir", s.localStoreDir))
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

// GetImportTS returns the TS saved in checkpoint.
func (s *CheckpointManager) GetImportTS() uint64 {
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

	ImportedRanges []ProcessedRange `json:"imported_ranges,omitempty"`
}

// JobCheckpointVersionCurrent is the current version of the checkpoint.
const (
	JobCheckpointVersionCurrent = JobCheckpointVersion2
	JobCheckpointVersion1       = 1
	JobCheckpointVersion2       = 2
)

func (s *CheckpointManager) resumeOrInitCheckpoint() error {
	cp, err := s.storage.LoadCheckpoint(s.ctx)
	if err != nil {
		return err
	}

	// Always discard any existing local sorted data to avoid re‑ingesting stale SSTs.
	s.cleanupLocalStoreDirUnsafe()

	if cp != nil {
		if cp.PhysicalID != s.physicalID {
			s.logger.Info("checkpoint physical table ID mismatch",
				zap.Int64("current", s.physicalID),
				zap.Int64("get", cp.PhysicalID))
			return nil
		}

		if len(cp.GlobalSyncKey) > 0 {
			s.importedKeyLowWatermark = cp.GlobalSyncKey
		} else if len(s.initialStartKey) > 0 {
			s.importedKeyLowWatermark = s.initialStartKey.Clone()
		}

		s.importedKeyCnt = cp.GlobalKeyCount
		s.ts = cp.TS

		// Load imported ranges.
		var imp []ProcessedRange
		if len(cp.ImportedRanges) > 0 {
			imp = make([]ProcessedRange, 0, len(cp.ImportedRanges))
			for _, r := range cp.ImportedRanges {
				imp = append(imp, ProcessedRange{
					StartKey: r.StartKey, EndKey: r.EndKey, PrevTailKey: r.PrevTailKey,
				})
			}
		}
		if len(imp) > 0 {
			imp = MergeAndCompactRanges(imp)
			imp = PruneRanges(imp, s.importedKeyLowWatermark)
			s.importedRanges = imp
		}

		s.logger.Info("resume checkpoint",
			zap.String("imported key low watermark", hex.EncodeToString(s.importedKeyLowWatermark)),
			zap.Int64("physical table ID", cp.PhysicalID),
			zap.String("previous instance", cp.InstanceAddr),
			zap.String("current instance", s.instanceAddr),
			zap.Int("imported_range_count", len(s.importedRanges)))
		return nil
	}
	s.logger.Info("checkpoint not found")
	if len(s.initialStartKey) > 0 {
		s.importedKeyLowWatermark = s.initialStartKey.Clone()
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
	checkpoint := &ReorgCheckpoint{
		GlobalSyncKey:  s.importedKeyLowWatermark,
		GlobalKeyCount: s.importedKeyCnt,
		InstanceAddr:   s.instanceAddr,
		PhysicalID:     s.physicalID,
		TS:             s.ts,
		Version:        JobCheckpointVersionCurrent,
		ImportedRanges: append([]ProcessedRange(nil), s.importedRanges...),
	}
	s.mu.Unlock()

	err := s.storage.SaveCheckpoint(s.ctx, checkpoint)
	logFunc := s.logger.Info
	if err != nil {
		logFunc = s.logger.With(zap.Error(err)).Error
	}
	logFunc("update checkpoint",
		zap.String("global checkpoint", hex.EncodeToString(checkpoint.GlobalSyncKey)),
		zap.Int("imported keys", checkpoint.GlobalKeyCount),
		zap.Int64("global physical ID", checkpoint.PhysicalID),
		zap.Uint64("ts", checkpoint.TS),
		zap.Int("imported_range_count", len(checkpoint.ImportedRanges)),
	)

	if err == nil {
		s.mu.Lock()
		s.dirty = false
		s.mu.Unlock()
	}

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

var _ CheckpointOperator = (*CheckpointManager)(nil)
