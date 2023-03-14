// Copyright 2022 PingCAP, Inc.
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

package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	CheckpointMetaPath = "checkpoint.meta"
	CheckpointDir      = "/checkpoints"

	CheckpointDataDirForBackup     = CheckpointDir + "/data"
	CheckpointChecksumDirForBackup = CheckpointDir + "/checksum"
	CheckpointLockPathForBackup    = CheckpointDir + "/checkpoint.lock"

	RestoreGroupKey                       = "restore"
	CheckpointDataDirForRestoreFormat     = CheckpointDir + "/restore-%s/data"
	CheckpointChecksumDirForRestoreFormat = CheckpointDir + "/restore-%s/checksum"
	CheckpointMetaPathForRestore          = CheckpointDir + "/restore-%s/checkpoint.meta"
)

type FlushPosition struct {
	CheckpointDataDir     string
	CheckpointChecksumDir string
	CheckpointLockPath    string
}

func FlushPositionForBackup() FlushPosition {
	return FlushPosition{
		CheckpointDataDir:     CheckpointDataDirForBackup,
		CheckpointChecksumDir: CheckpointChecksumDirForBackup,
		CheckpointLockPath:    CheckpointLockPathForBackup,
	}
}

func getCheckpointMetaPathByName(taskName string) string {
	return fmt.Sprintf(CheckpointMetaPathForRestore, taskName)
}

func getCheckpointDataDirByName(taskName string) string {
	return fmt.Sprintf(CheckpointDataDirForRestoreFormat, taskName)
}

func getCheckpointChecksumDirByName(taskName string) string {
	return fmt.Sprintf(CheckpointChecksumDirForRestoreFormat, taskName)
}

func FlushPositionForRestore(taskName string) FlushPosition {
	return FlushPosition{
		CheckpointDataDir:     getCheckpointDataDirByName(taskName),
		CheckpointChecksumDir: fmt.Sprintf(CheckpointChecksumDirForRestoreFormat, taskName),
	}
}

const MaxChecksumTotalCost float64 = 60.0

const tickDurationForFlush = 30 * time.Second

const tickDurationForLock = 4 * time.Minute

const lockTimeToLive = 5 * time.Minute

type CheckpointMessage struct {
	// start-key of the origin range
	GroupKey string

	Group []*rtree.Range
}

// A Checkpoint Range File is like this:
//
//    ChecksumData
// +----------------+           RangeGroupData                      RangeGroups
// |    DureTime    |     +--------------------------+ encrypted  +-------------+
// | RangeGroupData-+---> | RangeGroupsEncriptedData-+----------> |   GroupKey  |
// | RangeGroupData |     | Checksum                 |            |    Range    |
// |      ...       |     | CipherIv                 |            |     ...     |
// | RangeGroupData |     | Size                     |            |    Range    |
// +----------------+     +--------------------------+            +-------------+
//
// For restore, because there is no group key, so there is only one RangeGroupData
// with multi-ranges in the ChecksumData.

type RangeGroups struct {
	GroupKey string         `json:"group-key"`
	Groups   []*rtree.Range `json:"groups"`
}

type RangeGroupData struct {
	RangeGroupsEncriptedData []byte
	Checksum                 []byte
	CipherIv                 []byte

	Size int
}

type CheckpointData struct {
	DureTime        time.Duration     `json:"dure-time"`
	RangeGroupMetas []*RangeGroupData `json:"range-group-metas"`
}

// A Checkpoint Checksum File is like this:
//
//  ChecksumInfo       ChecksumItems         ChecksumItem
// +-------------+   +--------------+     +--------------+
// |   Content---+-> | ChecksumItem-+---> |   TableID    |
// |  Checksum   |   | ChecksumItem |     |   Crc64xor   |
// +-------------+   |     ...      |     |   TotalKvs   |
//                   | ChecksumItem |     |  TotalBytes  |
//                   +--------------+     +--------------+

type ChecksumItem struct {
	TableID    int64  `json:"table-id"`
	Crc64xor   uint64 `json:"crc64-xor"`
	TotalKvs   uint64 `json:"total-kvs"`
	TotalBytes uint64 `json:"total-bytes"`
}

type ChecksumItems struct {
	Items []*ChecksumItem `json:"checksum-items"`
}

type ChecksumInfo struct {
	Content  []byte `json:"content"`
	Checksum []byte `json:"checksum"`
}

type ChecksumRunner struct {
	sync.Mutex

	checkpointChecksumDir string
	checksumItems         ChecksumItems

	// when the total time cost is large than the threshold,
	// begin to flush checksum
	totalCost float64

	err        error
	wg         sync.WaitGroup
	workerPool utils.WorkerPool
}

func NewChecksumRunner(checkpointChecksumDir string) *ChecksumRunner {
	return &ChecksumRunner{
		checkpointChecksumDir: checkpointChecksumDir,
		workerPool:            *utils.NewWorkerPool(4, "checksum flush worker"),
	}
}

func (cr *ChecksumRunner) RecordError(err error) {
	cr.Lock()
	cr.err = err
	cr.Unlock()
}

// FlushChecksum save the checksum in the memory temporarily
// and flush to the external storage if checksum take much time
func (cr *ChecksumRunner) FlushChecksum(
	ctx context.Context,
	s storage.ExternalStorage,
	tableID int64,
	crc64xor uint64,
	totalKvs uint64,
	totalBytes uint64,
	timeCost float64,
) error {
	checksumItem := &ChecksumItem{
		TableID:    tableID,
		Crc64xor:   crc64xor,
		TotalKvs:   totalKvs,
		TotalBytes: totalBytes,
	}
	var toBeFlushedChecksumItems *ChecksumItems = nil
	cr.Lock()
	if cr.err != nil {
		err := cr.err
		cr.Unlock()
		return err
	}
	if cr.checksumItems.Items == nil {
		// reset the checksumInfo
		cr.totalCost = 0
		cr.checksumItems.Items = make([]*ChecksumItem, 0)
	}
	cr.totalCost += timeCost
	cr.checksumItems.Items = append(cr.checksumItems.Items, checksumItem)
	// When the table is large, the time spent executing checksum will span
	// several breakpoint persistence cycles. So we'd better to immediately persist
	// the checksum of these large tables.
	if cr.totalCost > MaxChecksumTotalCost {
		toBeFlushedChecksumItems = &ChecksumItems{
			Items: cr.checksumItems.Items,
		}
		cr.checksumItems.Items = nil
	}
	cr.Unlock()

	// now lock is free
	if toBeFlushedChecksumItems == nil {
		return nil
	}

	// create a goroutine to flush checksumInfo to external storage
	cr.wg.Add(1)
	cr.workerPool.Apply(func() {
		defer cr.wg.Done()

		content, err := json.Marshal(toBeFlushedChecksumItems)
		if err != nil {
			cr.RecordError(err)
			return
		}

		checksum := sha256.Sum256(content)
		checksumInfo := &ChecksumInfo{
			Content:  content,
			Checksum: checksum[:],
		}

		data, err := json.Marshal(checksumInfo)
		if err != nil {
			cr.RecordError(err)
			return
		}

		fname := fmt.Sprintf("%s/t%d_and__", cr.checkpointChecksumDir, tableID)
		err = s.WriteFile(ctx, fname, data)
		if err != nil {
			cr.RecordError(err)
			return
		}
	})
	return nil
}

type GlobalTimer interface {
	GetTS(context.Context) (int64, int64, error)
}

type CheckpointRunner struct {
	FlushPosition
	lockId uint64

	meta map[string]*RangeGroups

	checksumRunner *ChecksumRunner

	storage storage.ExternalStorage
	cipher  *backuppb.CipherInfo
	timer   GlobalTimer

	appendCh chan *CheckpointMessage
	metaCh   chan map[string]*RangeGroups
	lockCh   chan struct{}
	errCh    chan error

	wg sync.WaitGroup
}

func newCheckpointRunner(ctx context.Context, storage storage.ExternalStorage, cipher *backuppb.CipherInfo, timer GlobalTimer, f FlushPosition) *CheckpointRunner {
	return &CheckpointRunner{
		FlushPosition: f,

		meta: make(map[string]*RangeGroups),

		checksumRunner: NewChecksumRunner(f.CheckpointChecksumDir),

		storage: storage,
		cipher:  cipher,
		timer:   timer,

		appendCh: make(chan *CheckpointMessage),
		metaCh:   make(chan map[string]*RangeGroups),
		lockCh:   make(chan struct{}),
		errCh:    make(chan error, 1),
	}
}

// only for test
func StartCheckpointRunnerForTest(ctx context.Context, storage storage.ExternalStorage, cipher *backuppb.CipherInfo, tick time.Duration, timer GlobalTimer) (*CheckpointRunner, error) {
	runner := newCheckpointRunner(ctx, storage, cipher, timer, FlushPositionForBackup())

	err := runner.initialLock(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to initialize checkpoint lock.")
	}
	runner.startCheckpointMainLoop(ctx, tick, tick)
	return runner, nil
}

func StartCheckpointRunnerForBackup(ctx context.Context, storage storage.ExternalStorage, cipher *backuppb.CipherInfo, timer GlobalTimer) (*CheckpointRunner, error) {
	runner := newCheckpointRunner(ctx, storage, cipher, timer, FlushPositionForBackup())

	err := runner.initialLock(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	runner.startCheckpointMainLoop(ctx, tickDurationForFlush, tickDurationForLock)
	return runner, nil
}

func StartCheckpointRunnerForRestore(ctx context.Context, storage storage.ExternalStorage, cipher *backuppb.CipherInfo, taskName string) (*CheckpointRunner, error) {
	runner := newCheckpointRunner(ctx, storage, cipher, nil, FlushPositionForRestore(taskName))

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(ctx, tickDurationForFlush, 0)
	return runner, nil
}

func (r *CheckpointRunner) FlushChecksum(ctx context.Context, tableID int64, crc64xor uint64, totalKvs uint64, totalBytes uint64, timeCost float64) error {
	return r.checksumRunner.FlushChecksum(ctx, r.storage, tableID, crc64xor, totalKvs, totalBytes, timeCost)
}

func (r *CheckpointRunner) append(
	ctx context.Context,
	message *CheckpointMessage,
) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.errCh:
		return err
	case r.appendCh <- message:
		return nil
	}
}

func (r *CheckpointRunner) Append(
	ctx context.Context,
	groupKey string,
	startKey []byte,
	endKey []byte,
	files []*backuppb.File,
) error {
	return r.append(ctx, &CheckpointMessage{
		GroupKey: groupKey,
		Group: []*rtree.Range{
			{
				StartKey: startKey,
				EndKey:   endKey,
				Files:    files,
			},
		},
	})
}

func (r *CheckpointRunner) AppendRanges(
	ctx context.Context,
	ranges []rtree.Range,
) error {
	// no need to persist the file information
	group := make([]*rtree.Range, 0, len(ranges))
	for _, rg := range ranges {
		group = append(group, &rtree.Range{
			StartKey: rg.StartKey,
			EndKey:   rg.EndKey,
		})
	}
	return r.append(ctx, &CheckpointMessage{
		GroupKey: RestoreGroupKey,
		Group:    group,
	})
}

// Note: Cannot be parallel with `Append` function
func (r *CheckpointRunner) WaitForFinish(ctx context.Context) {
	// can not append anymore
	close(r.appendCh)
	// wait the range flusher exit
	r.wg.Wait()
	// wait the checksum flusher exit
	r.checksumRunner.wg.Wait()
	// remove the checkpoint lock
	if r.lockId > 0 {
		err := r.storage.DeleteFile(ctx, r.CheckpointLockPath)
		if err != nil {
			log.Warn("failed to remove the checkpoint lock", zap.Error(err))
		}
	}
}

// Send the meta to the flush goroutine, and reset the CheckpointRunner's meta
func (r *CheckpointRunner) flushMeta(ctx context.Context, errCh chan error) error {
	meta := r.meta
	r.meta = make(map[string]*RangeGroups)
	// do flush
	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	case r.metaCh <- meta:
	}
	return nil
}

func (r *CheckpointRunner) setLock(ctx context.Context, errCh chan error) error {
	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	case r.lockCh <- struct{}{}:
	}
	return nil
}

// start a goroutine to flush the meta, which is sent from `checkpoint looper`, to the external storage
func (r *CheckpointRunner) startCheckpointFlushLoop(ctx context.Context, wg *sync.WaitGroup) chan error {
	errCh := make(chan error, 1)
	wg.Add(1)
	flushWorker := func(ctx context.Context, errCh chan error) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case meta, ok := <-r.metaCh:
				if !ok {
					log.Info("stop checkpoint flush worker")
					return
				}
				if err := r.doFlush(ctx, meta); err != nil {
					errCh <- err
					return
				}
			case _, ok := <-r.lockCh:
				if !ok {
					log.Info("stop checkpoint flush worker")
					return
				}
				if err := r.updateLock(ctx); err != nil {
					errCh <- errors.Annotate(err, "Failed to update checkpoint lock.")
					return
				}
			}
		}
	}

	go flushWorker(ctx, errCh)
	return errCh
}

func (r *CheckpointRunner) sendError(err error) {
	select {
	case r.errCh <- err:
	default:
		log.Error("errCh is blocked", logutil.ShortError(err))
	}
	r.checksumRunner.RecordError(err)
}

func (r *CheckpointRunner) startCheckpointMainLoop(ctx context.Context, tickDurationForFlush, tickDurationForLock time.Duration) {
	r.wg.Add(1)
	checkpointLoop := func(ctx context.Context) {
		defer r.wg.Done()
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		errCh := r.startCheckpointFlushLoop(cctx, &wg)
		flushTicker := time.NewTicker(tickDurationForFlush)
		defer flushTicker.Stop()
		// register time ticker, the lock ticker is optional
		lockTicker := dispatcherTicker(tickDurationForLock)
		defer lockTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-lockTicker.Ch():
				if err := r.setLock(ctx, errCh); err != nil {
					r.sendError(err)
					return
				}
			case <-flushTicker.C:
				if err := r.flushMeta(ctx, errCh); err != nil {
					r.sendError(err)
					return
				}
			case msg, ok := <-r.appendCh:
				if !ok {
					log.Info("stop checkpoint runner")
					if err := r.flushMeta(ctx, errCh); err != nil {
						r.sendError(err)
					}
					// close the channel to flush worker
					// and wait it to consumes all the metas
					close(r.metaCh)
					close(r.lockCh)
					wg.Wait()
					return
				}
				groups, exist := r.meta[msg.GroupKey]
				if !exist {
					groups = &RangeGroups{
						GroupKey: msg.GroupKey,
						Groups:   make([]*rtree.Range, 0),
					}
					r.meta[msg.GroupKey] = groups
				}
				groups.Groups = append(groups.Groups, msg.Group...)
			case err := <-errCh:
				// pass flush worker's error back
				r.sendError(err)
				return
			}
		}
	}

	go checkpointLoop(ctx)
}

// flush the meta to the external storage
func (r *CheckpointRunner) doFlush(ctx context.Context, meta map[string]*RangeGroups) error {
	if len(meta) == 0 {
		return nil
	}

	checkpointData := &CheckpointData{
		DureTime:        summary.NowDureTime(),
		RangeGroupMetas: make([]*RangeGroupData, 0, len(meta)),
	}

	var fname []byte = nil

	for _, group := range meta {
		if len(group.Groups) == 0 {
			continue
		}

		// use the first item's group-key and sub-range-key as the filename
		if len(fname) == 0 {
			fname = append(append([]byte(group.GroupKey), '.', '.'), group.Groups[0].StartKey...)
		}

		// Flush the metaFile to storage
		content, err := json.Marshal(group)
		if err != nil {
			return errors.Trace(err)
		}

		encryptBuff, iv, err := metautil.Encrypt(content, r.cipher)
		if err != nil {
			return errors.Trace(err)
		}

		checksum := sha256.Sum256(content)

		checkpointData.RangeGroupMetas = append(checkpointData.RangeGroupMetas, &RangeGroupData{
			RangeGroupsEncriptedData: encryptBuff,
			Checksum:                 checksum[:],
			Size:                     len(content),
			CipherIv:                 iv,
		})
	}

	if len(checkpointData.RangeGroupMetas) > 0 {
		data, err := json.Marshal(checkpointData)
		if err != nil {
			return errors.Trace(err)
		}

		checksum := sha256.Sum256(fname)
		checksumEncoded := base64.URLEncoding.EncodeToString(checksum[:])
		path := fmt.Sprintf("%s/%s_%d.cpt", r.CheckpointDataDir, checksumEncoded, rand.Uint64())
		if err := r.storage.WriteFile(ctx, path, data); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type CheckpointLock struct {
	LockId   uint64 `json:"lock-id"`
	ExpireAt int64  `json:"expire-at"`
}

// get ts with retry
func (r *CheckpointRunner) getTS(ctx context.Context) (int64, int64, error) {
	var (
		p     int64 = 0
		l     int64 = 0
		retry int   = 0
	)
	errRetry := utils.WithRetry(ctx, func() error {
		var err error
		p, l, err = r.timer.GetTS(ctx)
		if err != nil {
			retry++
			log.Info("failed to get ts", zap.Int("retry", retry), zap.Error(err))
			return err
		}

		return nil
	}, utils.NewPDReqBackoffer())

	return p, l, errors.Trace(errRetry)
}

// flush the lock to the external storage
func (r *CheckpointRunner) flushLock(ctx context.Context, p int64) error {
	lock := &CheckpointLock{
		LockId:   r.lockId,
		ExpireAt: p + lockTimeToLive.Milliseconds(),
	}
	log.Info("start to flush the checkpoint lock", zap.Int64("lock-at", p), zap.Int64("expire-at", lock.ExpireAt))
	data, err := json.Marshal(lock)
	if err != nil {
		return errors.Trace(err)
	}

	err = r.storage.WriteFile(ctx, r.CheckpointLockPath, data)
	return errors.Trace(err)
}

// check whether this lock belongs to this BR
func (r *CheckpointRunner) checkLockFile(ctx context.Context, now int64) error {
	data, err := r.storage.ReadFile(ctx, r.CheckpointLockPath)
	if err != nil {
		return errors.Trace(err)
	}
	lock := &CheckpointLock{}
	err = json.Unmarshal(data, lock)
	if err != nil {
		return errors.Trace(err)
	}
	if lock.ExpireAt <= now {
		if lock.LockId > r.lockId {
			return errors.Errorf("There are another BR(%d) running after but setting lock before this one(%d). "+
				"Please check whether the BR is running. If not, you can retry.", lock.LockId, r.lockId)
		}
		if lock.LockId == r.lockId {
			log.Warn("The lock has expired.", zap.Int64("expire-at(ms)", lock.ExpireAt), zap.Int64("now(ms)", now))
		}
	} else if lock.LockId != r.lockId {
		return errors.Errorf("The existing lock will expire in %d seconds. "+
			"There may be another BR(%d) running. If not, you can wait for the lock to expire, or delete the file `%s%s` manually.",
			(lock.ExpireAt-now)/1000, lock.LockId, strings.TrimRight(r.storage.URI(), "/"), r.CheckpointLockPath)
	}

	return nil
}

// generate a new lock and flush the lock to the external storage
func (r *CheckpointRunner) updateLock(ctx context.Context) error {
	p, _, err := r.getTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if err = r.checkLockFile(ctx, p); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(r.flushLock(ctx, p))
}

// Attempt to initialize the lock. Need to stop the backup when there is an unexpired locks.
func (r *CheckpointRunner) initialLock(ctx context.Context) error {
	p, l, err := r.getTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	r.lockId = oracle.ComposeTS(p, l)
	exist, err := r.storage.FileExists(ctx, r.CheckpointLockPath)
	if err != nil {
		return errors.Trace(err)
	}
	if exist {
		if err := r.checkLockFile(ctx, p); err != nil {
			return errors.Trace(err)
		}
	}
	if err = r.flushLock(ctx, p); err != nil {
		return errors.Trace(err)
	}

	// wait for 3 seconds to check whether the lock file is overwritten by another BR
	time.Sleep(3 * time.Second)
	err = r.checkLockFile(ctx, p)
	return errors.Trace(err)
}

// walk the whole checkpoint range files and retrieve the metadata of backed up ranges
// and return the total time cost in the past executions
func WalkCheckpointFileForBackup(ctx context.Context, s storage.ExternalStorage, cipher *backuppb.CipherInfo, fn func(groupKey string, rg *rtree.Range)) (time.Duration, error) {
	return walkCheckpointFile(ctx, s, cipher, CheckpointDataDirForBackup, fn)
}

func WalkCheckpointFileForRestore(ctx context.Context, s storage.ExternalStorage, cipher *backuppb.CipherInfo, taskName string, fn func(groupKey string, rg *rtree.Range)) (time.Duration, error) {
	return walkCheckpointFile(ctx, s, cipher, getCheckpointDataDirByName(taskName), fn)
}

func walkCheckpointFile(ctx context.Context, s storage.ExternalStorage, cipher *backuppb.CipherInfo, subDir string, fn func(groupKey string, rg *rtree.Range)) (time.Duration, error) {
	// records the total time cost in the past executions
	var pastDureTime time.Duration = 0
	err := s.WalkDir(ctx, &storage.WalkOption{SubDir: CheckpointDataDirForBackup}, func(path string, size int64) error {
		if strings.HasSuffix(path, ".cpt") {
			content, err := s.ReadFile(ctx, path)
			if err != nil {
				return errors.Trace(err)
			}

			checkpointData := &CheckpointData{}
			if err = json.Unmarshal(content, checkpointData); err != nil {
				return errors.Trace(err)
			}

			if checkpointData.DureTime > pastDureTime {
				pastDureTime = checkpointData.DureTime
			}
			for _, meta := range checkpointData.RangeGroupMetas {
				decryptContent, err := metautil.Decrypt(meta.RangeGroupsEncriptedData, cipher, meta.CipherIv)
				if err != nil {
					return errors.Trace(err)
				}

				checksum := sha256.Sum256(decryptContent)
				if !bytes.Equal(meta.Checksum, checksum[:]) {
					log.Error("checkpoint checksum info's checksum mismatch, skip it",
						zap.ByteString("expect", meta.Checksum),
						zap.ByteString("got", checksum[:]),
					)
					continue
				}

				group := &RangeGroups{}
				if err = json.Unmarshal(decryptContent, group); err != nil {
					return errors.Trace(err)
				}

				for _, g := range group.Groups {
					fn(group.GroupKey, g)
				}
			}
		}
		return nil
	})

	return pastDureTime, errors.Trace(err)
}

type CheckpointMetadata struct {
	GCServiceId string        `json:"gc-service-id"`
	ConfigHash  []byte        `json:"config-hash"`
	BackupTS    uint64        `json:"backup-ts"`
	Ranges      []rtree.Range `json:"ranges"`

	CheckpointChecksum map[int64]*ChecksumItem    `json:"-"`
	CheckpointDataMap  map[string]rtree.RangeTree `json:"-"`
}

// load checkpoint metadata from the external storage
func LoadCheckpointMetadata(ctx context.Context, s storage.ExternalStorage) (*CheckpointMetadata, error) {
	m, err := loadCheckpointMeta(ctx, s, CheckpointMetaPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m.CheckpointChecksum, err = loadCheckpointChecksum(ctx, s, CheckpointChecksumDirForBackup)
	return m, errors.Trace(err)
}

func LoadCheckpointChecksumForRestore(ctx context.Context, s storage.ExternalStorage, taskName string) (map[int64]*ChecksumItem, error) {
	return loadCheckpointChecksum(ctx, s, getCheckpointChecksumDirByName(taskName))
}

func LoadCheckpointMetaForRestore(ctx context.Context, s storage.ExternalStorage, taskName string) (*CheckpointMetadata, error) {
	return loadCheckpointMeta(ctx, s, getCheckpointMetaPathByName(taskName))
}

func loadCheckpointMeta(ctx context.Context, s storage.ExternalStorage, path string) (*CheckpointMetadata, error) {
	data, err := s.ReadFile(ctx, path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m := &CheckpointMetadata{}
	err = json.Unmarshal(data, m)
	return m, errors.Trace(err)
}

// walk the whole checkpoint checksum files and retrieve checksum information of tables calculated
func loadCheckpointChecksum(ctx context.Context, s storage.ExternalStorage, subDir string) (map[int64]*ChecksumItem, error) {
	checkpointChecksum := make(map[int64]*ChecksumItem)

	err := s.WalkDir(ctx, &storage.WalkOption{SubDir: subDir}, func(path string, size int64) error {
		data, err := s.ReadFile(ctx, path)
		if err != nil {
			return errors.Trace(err)
		}
		info := &ChecksumInfo{}
		err = json.Unmarshal(data, info)
		if err != nil {
			return errors.Trace(err)
		}

		checksum := sha256.Sum256(info.Content)
		if !bytes.Equal(info.Checksum, checksum[:]) {
			log.Error("checkpoint checksum info's checksum mismatch, skip it",
				zap.ByteString("expect", info.Checksum),
				zap.ByteString("got", checksum[:]),
			)
			return nil
		}

		items := &ChecksumItems{}
		err = json.Unmarshal(info.Content, items)
		if err != nil {
			return errors.Trace(err)
		}

		for _, c := range items.Items {
			checkpointChecksum[c.TableID] = c
		}
		return nil
	})
	return checkpointChecksum, errors.Trace(err)
}

// save the checkpoint metadata into the external storage
func SaveCheckpointMetadata(ctx context.Context, s storage.ExternalStorage, meta *CheckpointMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	err = s.WriteFile(ctx, CheckpointMetaPath, data)
	return errors.Trace(err)
}

func SaveCheckpointMetadataForRestore(ctx context.Context, s storage.ExternalStorage, meta *CheckpointMetadata, taskName string) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	err = s.WriteFile(ctx, getCheckpointMetaPathByName(taskName), data)
	return errors.Trace(err)
}

func ExistsRestoreCheckpoint(ctx context.Context, s storage.ExternalStorage, taskName string) (bool, error) {
	return s.FileExists(ctx, getCheckpointMetaPathByName(taskName))
}
