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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const CheckpointDir = "/checkpoints"

type flushPosition struct {
	CheckpointDataDir     string
	CheckpointChecksumDir string
	CheckpointLockPath    string
}

const MaxChecksumTotalCost float64 = 60.0

const defaultTickDurationForFlush = 30 * time.Second

const defaultTckDurationForChecksum = 5 * time.Second

const defaultTickDurationForLock = 4 * time.Minute

const lockTimeToLive = 5 * time.Minute

type KeyType interface {
	~BackupKeyType | ~RestoreKeyType
}

type RangeType struct {
	*rtree.Range
}

func (r RangeType) IdentKey() []byte {
	return r.StartKey
}

type ValueType interface {
	IdentKey() []byte
}

type CheckpointMessage[K KeyType, V ValueType] struct {
	// start-key of the origin range
	GroupKey K

	Group []V
}

// A Checkpoint Range File is like this:
//
//   CheckpointData
// +----------------+           RangeGroupData                          RangeGroup
// |    DureTime    |     +--------------------------+ encrypted  +--------------------+
// | RangeGroupData-+---> | RangeGroupsEncriptedData-+----------> |  GroupKey/TableID  |
// | RangeGroupData |     | Checksum                 |            |       Range        |
// |      ...       |     | CipherIv                 |            |        ...         |
// | RangeGroupData |     | Size                     |            |       Range        |
// +----------------+     +--------------------------+            +--------------------+
//
// For restore, because there is no group key, so there is only one RangeGroupData
// with multi-ranges in the ChecksumData.

type RangeGroup[K KeyType, V ValueType] struct {
	GroupKey K   `json:"group-key"`
	Group    []V `json:"groups"`
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
// +------------+    +--------------+     +--------------+
// |   Content--+--> | ChecksumItem-+---> |   TableID    |
// |  Checksum  |    | ChecksumItem |     |   Crc64xor   |
// |  DureTime  |    |     ...      |     |   TotalKvs   |
// +------------+    | ChecksumItem |     |  TotalBytes  |
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
	Content  []byte        `json:"content"`
	Checksum []byte        `json:"checksum"`
	DureTime time.Duration `json:"dure-time"`
}

type GlobalTimer interface {
	GetTS(context.Context) (int64, int64, error)
}

type CheckpointRunner[K KeyType, V ValueType] struct {
	flushPosition
	lockId uint64

	meta     map[K]*RangeGroup[K, V]
	checksum ChecksumItems

	valueMarshaler func(*RangeGroup[K, V]) ([]byte, error)

	storage storage.ExternalStorage
	cipher  *backuppb.CipherInfo
	timer   GlobalTimer

	appendCh       chan *CheckpointMessage[K, V]
	checksumCh     chan *ChecksumItem
	doneCh         chan bool
	metaCh         chan map[K]*RangeGroup[K, V]
	checksumMetaCh chan ChecksumItems
	lockCh         chan struct{}
	errCh          chan error
	err            error
	errLock        sync.RWMutex

	wg sync.WaitGroup
}

func newCheckpointRunner[K KeyType, V ValueType](
	ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	timer GlobalTimer,
	f flushPosition,
	vm func(*RangeGroup[K, V]) ([]byte, error),
) *CheckpointRunner[K, V] {
	return &CheckpointRunner[K, V]{
		flushPosition: f,

		meta:     make(map[K]*RangeGroup[K, V]),
		checksum: ChecksumItems{Items: make([]*ChecksumItem, 0)},

		valueMarshaler: vm,

		storage: storage,
		cipher:  cipher,
		timer:   timer,

		appendCh:       make(chan *CheckpointMessage[K, V]),
		checksumCh:     make(chan *ChecksumItem),
		doneCh:         make(chan bool, 1),
		metaCh:         make(chan map[K]*RangeGroup[K, V]),
		checksumMetaCh: make(chan ChecksumItems),
		lockCh:         make(chan struct{}),
		errCh:          make(chan error, 1),
		err:            nil,
	}
}

func (r *CheckpointRunner[K, V]) FlushChecksum(
	ctx context.Context,
	tableID int64,
	crc64xor uint64,
	totalKvs uint64,
	totalBytes uint64,
) error {
	checksumItem := &ChecksumItem{
		TableID:    tableID,
		Crc64xor:   crc64xor,
		TotalKvs:   totalKvs,
		TotalBytes: totalBytes,
	}
	return r.FlushChecksumItem(ctx, checksumItem)
}

func (r *CheckpointRunner[K, V]) FlushChecksumItem(
	ctx context.Context,
	checksumItem *ChecksumItem,
) error {
	select {
	case <-ctx.Done():
		return errors.Annotatef(ctx.Err(), "failed to append checkpoint checksum item")
	case err, ok := <-r.errCh:
		if !ok {
			r.errLock.RLock()
			err = r.err
			r.errLock.RUnlock()
			return errors.Annotate(err, "[checkpoint] Checksum: failed to append checkpoint checksum item")
		}
		return err
	case r.checksumCh <- checksumItem:
		return nil
	}
}

func (r *CheckpointRunner[K, V]) Append(
	ctx context.Context,
	message *CheckpointMessage[K, V],
) error {
	select {
	case <-ctx.Done():
		return errors.Annotatef(ctx.Err(), "failed to append checkpoint message")
	case err, ok := <-r.errCh:
		if !ok {
			r.errLock.RLock()
			err = r.err
			r.errLock.RUnlock()
			return errors.Annotate(err, "[checkpoint] Append: failed to append checkpoint message")
		}
		return err
	case r.appendCh <- message:
		return nil
	}
}

// Note: Cannot be parallel with `Append` function
func (r *CheckpointRunner[K, V]) WaitForFinish(ctx context.Context, flush bool) {
	if r.doneCh != nil {
		select {
		case r.doneCh <- flush:

		default:
			log.Warn("not the first close the checkpoint runner", zap.String("category", "checkpoint"))
		}
	}
	// wait the range flusher exit
	r.wg.Wait()
	// remove the checkpoint lock
	if r.lockId > 0 {
		err := r.storage.DeleteFile(ctx, r.CheckpointLockPath)
		if err != nil {
			log.Warn("failed to remove the checkpoint lock", zap.Error(err))
		}
	}
}

// Send the checksum to the flush goroutine, and reset the CheckpointRunner's checksum
func (r *CheckpointRunner[K, V]) flushChecksum(ctx context.Context, errCh chan error) error {
	checksum := ChecksumItems{
		Items: r.checksum.Items,
	}
	r.checksum.Items = make([]*ChecksumItem, 0)
	// do flush
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	case r.checksumMetaCh <- checksum:
	}
	return nil
}

// Send the meta to the flush goroutine, and reset the CheckpointRunner's meta
func (r *CheckpointRunner[K, V]) flushMeta(ctx context.Context, errCh chan error) error {
	meta := r.meta
	r.meta = make(map[K]*RangeGroup[K, V])
	// do flush
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	case r.metaCh <- meta:
	}
	return nil
}

func (r *CheckpointRunner[K, V]) setLock(ctx context.Context, errCh chan error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	case r.lockCh <- struct{}{}:
	}
	return nil
}

// start a goroutine to flush the meta, which is sent from `checkpoint looper`, to the external storage
func (r *CheckpointRunner[K, V]) startCheckpointFlushLoop(ctx context.Context, wg *sync.WaitGroup) chan error {
	errCh := make(chan error, 1)
	wg.Add(1)
	flushWorker := func(ctx context.Context, errCh chan error) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					errCh <- err
				}
				return
			case meta, ok := <-r.metaCh:
				if !ok {
					log.Info("stop checkpoint flush worker")
					return
				}
				if err := r.doFlush(ctx, meta); err != nil {
					errCh <- errors.Annotate(err, "failed to flush checkpoint data.")
					return
				}
			case checksums, ok := <-r.checksumMetaCh:
				if !ok {
					log.Info("stop checkpoint flush worker")
					return
				}
				if err := r.doChecksumFlush(ctx, checksums); err != nil {
					errCh <- errors.Annotate(err, "failed to flush checkpoint checksum.")
					return
				}
			case _, ok := <-r.lockCh:
				if !ok {
					log.Info("stop checkpoint flush worker")
					return
				}
				if err := r.updateLock(ctx); err != nil {
					errCh <- errors.Annotate(err, "failed to update checkpoint lock.")
					return
				}
			}
		}
	}

	go flushWorker(ctx, errCh)
	return errCh
}

func (r *CheckpointRunner[K, V]) sendError(err error) {
	select {
	case r.errCh <- err:
		log.Error("send the error", zap.String("category", "checkpoint"), zap.Error(err))
		r.errLock.Lock()
		r.err = err
		r.errLock.Unlock()
		close(r.errCh)
	default:
		log.Error("errCh is blocked", logutil.ShortError(err))
	}
}

func (r *CheckpointRunner[K, V]) startCheckpointMainLoop(
	ctx context.Context,
	tickDurationForFlush,
	tickDurationForChecksum,
	tickDurationForLock time.Duration,
) {
	failpoint.Inject("checkpoint-more-quickly-flush", func(_ failpoint.Value) {
		tickDurationForChecksum = 1 * time.Second
		tickDurationForFlush = 3 * time.Second
		if tickDurationForLock > 0 {
			tickDurationForLock = 1 * time.Second
		}
		log.Info("adjust the tick duration for flush or lock",
			zap.Duration("flush", tickDurationForFlush),
			zap.Duration("checksum", tickDurationForChecksum),
			zap.Duration("lock", tickDurationForLock),
		)
	})
	r.wg.Add(1)
	checkpointLoop := func(ctx context.Context) {
		defer r.wg.Done()
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		errCh := r.startCheckpointFlushLoop(cctx, &wg)
		flushTicker := time.NewTicker(tickDurationForFlush)
		defer flushTicker.Stop()
		checksumTicker := time.NewTicker(tickDurationForChecksum)
		defer checksumTicker.Stop()
		// register time ticker, the lock ticker is optional
		lockTicker := dispatcherTicker(tickDurationForLock)
		defer lockTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					r.sendError(err)
				}
				return
			case <-lockTicker.Ch():
				if err := r.setLock(ctx, errCh); err != nil {
					r.sendError(err)
					return
				}
			case <-checksumTicker.C:
				if err := r.flushChecksum(ctx, errCh); err != nil {
					r.sendError(err)
					return
				}
			case <-flushTicker.C:
				if err := r.flushMeta(ctx, errCh); err != nil {
					r.sendError(err)
					return
				}
			case msg := <-r.appendCh:
				groups, exist := r.meta[msg.GroupKey]
				if !exist {
					groups = &RangeGroup[K, V]{
						GroupKey: msg.GroupKey,
						Group:    make([]V, 0),
					}
					r.meta[msg.GroupKey] = groups
				}
				groups.Group = append(groups.Group, msg.Group...)
			case msg := <-r.checksumCh:
				r.checksum.Items = append(r.checksum.Items, msg)
			case flush := <-r.doneCh:
				log.Info("stop checkpoint runner")
				if flush {
					// NOTE: the exit step, don't send error any more.
					if err := r.flushMeta(ctx, errCh); err != nil {
						log.Error("failed to flush checkpoint meta", zap.Error(err))
					} else if err := r.flushChecksum(ctx, errCh); err != nil {
						log.Error("failed to flush checkpoint checksum", zap.Error(err))
					}
				}
				// close the channel to flush worker
				// and wait it to consumes all the metas
				close(r.metaCh)
				close(r.checksumMetaCh)
				close(r.lockCh)
				wg.Wait()
				return
			case err := <-errCh:
				// pass flush worker's error back
				r.sendError(err)
				return
			}
		}
	}

	go checkpointLoop(ctx)
}

// flush the checksum to the external storage
func (r *CheckpointRunner[K, V]) doChecksumFlush(ctx context.Context, checksumItems ChecksumItems) error {
	if len(checksumItems.Items) == 0 {
		return nil
	}
	content, err := json.Marshal(checksumItems)
	if err != nil {
		return errors.Trace(err)
	}

	checksum := sha256.Sum256(content)
	checksumInfo := &ChecksumInfo{
		Content:  content,
		Checksum: checksum[:],
		DureTime: summary.NowDureTime(),
	}

	data, err := json.Marshal(checksumInfo)
	if err != nil {
		return errors.Trace(err)
	}

	fname := fmt.Sprintf("%s/t%d_and__.cpt", r.CheckpointChecksumDir, checksumItems.Items[0].TableID)
	if err = r.storage.WriteFile(ctx, fname, data); err != nil {
		return errors.Annotatef(err, "failed to write file %s for checkpoint checksum", fname)
	}

	failpoint.Inject("failed-after-checkpoint-flushes-checksum", func(_ failpoint.Value) {
		failpoint.Return(errors.Errorf("failpoint: failed after checkpoint flushes checksum"))
	})
	return nil
}

// flush the meta to the external storage
func (r *CheckpointRunner[K, V]) doFlush(ctx context.Context, meta map[K]*RangeGroup[K, V]) error {
	if len(meta) == 0 {
		return nil
	}

	checkpointData := &CheckpointData{
		DureTime:        summary.NowDureTime(),
		RangeGroupMetas: make([]*RangeGroupData, 0, len(meta)),
	}

	var fname []byte = nil

	for _, group := range meta {
		if len(group.Group) == 0 {
			continue
		}

		// use the first item's group-key and sub-range-key as the filename
		if len(fname) == 0 {
			fname = append([]byte(fmt.Sprint(group.GroupKey, '.', '.')), group.Group[0].IdentKey()...)
		}

		// Flush the metaFile to storage
		content, err := r.valueMarshaler(group)
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

	failpoint.Inject("failed-after-checkpoint-flushes", func(_ failpoint.Value) {
		failpoint.Return(errors.Errorf("failpoint: failed after checkpoint flushes"))
	})
	return nil
}

type CheckpointLock struct {
	LockId   uint64 `json:"lock-id"`
	ExpireAt int64  `json:"expire-at"`
}

// get ts with retry
func (r *CheckpointRunner[K, V]) getTS(ctx context.Context) (int64, int64, error) {
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
func (r *CheckpointRunner[K, V]) flushLock(ctx context.Context, p int64) error {
	lock := &CheckpointLock{
		LockId:   r.lockId,
		ExpireAt: p + lockTimeToLive.Milliseconds(),
	}
	log.Info("start to flush the checkpoint lock", zap.Int64("lock-at", p),
		zap.Int64("expire-at", lock.ExpireAt))
	data, err := json.Marshal(lock)
	if err != nil {
		return errors.Trace(err)
	}

	err = r.storage.WriteFile(ctx, r.CheckpointLockPath, data)
	return errors.Trace(err)
}

// check whether this lock belongs to this BR
func (r *CheckpointRunner[K, V]) checkLockFile(ctx context.Context, now int64) error {
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
			log.Warn("The lock has expired.",
				zap.Int64("expire-at(ms)", lock.ExpireAt), zap.Int64("now(ms)", now))
		}
	} else if lock.LockId != r.lockId {
		return errors.Errorf("The existing lock will expire in %d seconds. "+
			"There may be another BR(%d) running. If not, you can wait for the lock to expire, "+
			"or delete the file `%s%s` manually.",
			(lock.ExpireAt-now)/1000, lock.LockId, strings.TrimRight(r.storage.URI(), "/"), r.CheckpointLockPath)
	}

	return nil
}

// generate a new lock and flush the lock to the external storage
func (r *CheckpointRunner[K, V]) updateLock(ctx context.Context) error {
	p, _, err := r.getTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if err = r.checkLockFile(ctx, p); err != nil {
		return errors.Trace(err)
	}
	if err = r.flushLock(ctx, p); err != nil {
		return errors.Trace(err)
	}

	failpoint.Inject("failed-after-checkpoint-updates-lock", func(_ failpoint.Value) {
		failpoint.Return(errors.Errorf("failpoint: failed after checkpoint updates lock"))
	})

	return nil
}

// Attempt to initialize the lock. Need to stop the backup when there is an unexpired locks.
func (r *CheckpointRunner[K, V]) initialLock(ctx context.Context) error {
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

// walk the whole checkpoint range files and retrieve the metadata of backed up/restored ranges
// and return the total time cost in the past executions
func walkCheckpointFile[K KeyType, V ValueType](
	ctx context.Context,
	s storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	subDir string,
	fn func(groupKey K, value V),
) (time.Duration, error) {
	// records the total time cost in the past executions
	var pastDureTime time.Duration = 0
	err := s.WalkDir(ctx, &storage.WalkOption{SubDir: subDir}, func(path string, size int64) error {
		if strings.HasSuffix(path, ".cpt") {
			content, err := s.ReadFile(ctx, path)
			if err != nil {
				return errors.Trace(err)
			}

			checkpointData := &CheckpointData{}
			if err = json.Unmarshal(content, checkpointData); err != nil {
				log.Error("failed to unmarshal the checkpoint data info, skip it", zap.Error(err))
				return nil
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

				group := &RangeGroup[K, V]{}
				if err = json.Unmarshal(decryptContent, group); err != nil {
					return errors.Trace(err)
				}

				for _, g := range group.Group {
					fn(group.GroupKey, g)
				}
			}
		}
		return nil
	})

	return pastDureTime, errors.Trace(err)
}

// load checkpoint meta data from external storage and unmarshal back
func loadCheckpointMeta[T any](ctx context.Context, s storage.ExternalStorage, path string, m *T) error {
	data, err := s.ReadFile(ctx, path)
	if err != nil {
		return errors.Trace(err)
	}

	err = json.Unmarshal(data, m)
	return errors.Trace(err)
}

// walk the whole checkpoint checksum files and retrieve checksum information of tables calculated
func loadCheckpointChecksum(
	ctx context.Context,
	s storage.ExternalStorage,
	subDir string,
) (map[int64]*ChecksumItem, time.Duration, error) {
	var pastDureTime time.Duration = 0
	checkpointChecksum := make(map[int64]*ChecksumItem)
	err := s.WalkDir(ctx, &storage.WalkOption{SubDir: subDir}, func(path string, size int64) error {
		data, err := s.ReadFile(ctx, path)
		if err != nil {
			return errors.Trace(err)
		}
		info := &ChecksumInfo{}
		err = json.Unmarshal(data, info)
		if err != nil {
			log.Error("failed to unmarshal the checkpoint checksum info, skip it", zap.Error(err))
			return nil
		}

		checksum := sha256.Sum256(info.Content)
		if !bytes.Equal(info.Checksum, checksum[:]) {
			log.Error("checkpoint checksum info's checksum mismatch, skip it",
				zap.ByteString("expect", info.Checksum),
				zap.ByteString("got", checksum[:]),
			)
			return nil
		}

		if info.DureTime > pastDureTime {
			pastDureTime = info.DureTime
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
	return checkpointChecksum, pastDureTime, errors.Trace(err)
}

func saveCheckpointMetadata[T any](ctx context.Context, s storage.ExternalStorage, meta *T, path string) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	err = s.WriteFile(ctx, path, data)
	return errors.Trace(err)
}

func removeCheckpointData(ctx context.Context, s storage.ExternalStorage, subDir string) error {
	var (
		// Generate one file every 30 seconds, so there are only 1200 files in 10 hours.
		removedFileNames = make([]string, 0, 1200)

		removeCnt  int   = 0
		removeSize int64 = 0
	)
	err := s.WalkDir(ctx, &storage.WalkOption{SubDir: subDir}, func(path string, size int64) error {
		if !strings.HasSuffix(path, ".cpt") && !strings.HasSuffix(path, ".meta") && !strings.HasSuffix(path, ".lock") {
			return nil
		}
		removedFileNames = append(removedFileNames, path)
		removeCnt += 1
		removeSize += size
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("start to remove checkpoint data",
		zap.String("checkpoint task", subDir),
		zap.Int("remove-count", removeCnt),
		zap.Int64("remove-size", removeSize),
	)

	maxFailedFilesNum := int64(16)
	var failedFilesCount atomic.Int64
	pool := util.NewWorkerPool(4, "checkpoint remove worker")
	eg, gCtx := errgroup.WithContext(ctx)
	for _, filename := range removedFileNames {
		name := filename
		pool.ApplyOnErrorGroup(eg, func() error {
			if err := s.DeleteFile(gCtx, name); err != nil {
				log.Warn("failed to remove the file", zap.String("filename", name), zap.Error(err))
				if failedFilesCount.Add(1) >= maxFailedFilesNum {
					return errors.Annotate(err, "failed to delete too many files")
				}
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	log.Info("all the checkpoint data has been removed", zap.String("checkpoint task", subDir))
	return nil
}
