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
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
)

const (
	CheckpointMetaPath = "checkpoint.meta"
	CheckpointDir      = "/checkpoints"

	CheckpointDataDir     = CheckpointDir + "/data"
	CheckpointChecksumDir = CheckpointDir + "/checksum"
)

const MaxChecksumTotalCost float64 = 60.0

const tickDuration = 30 * time.Second

type CheckpointMessage struct {
	// start-key of the origin range
	GroupKey string

	Group *rtree.Range
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

	checksumItems ChecksumItems

	// when the total time cost is large than the threshold,
	// begin to flush checksum
	totalCost float64

	err        error
	wg         sync.WaitGroup
	workerPool utils.WorkerPool
}

func NewChecksumRunner() *ChecksumRunner {
	return &ChecksumRunner{
		workerPool: *utils.NewWorkerPool(4, "checksum flush worker"),
	}
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
		recordErr := func(err error) {
			cr.Lock()
			cr.err = err
			cr.Unlock()
		}

		content, err := json.Marshal(toBeFlushedChecksumItems)
		if err != nil {
			recordErr(err)
			return
		}

		checksum := sha256.Sum256(content)
		checksumInfo := &ChecksumInfo{
			Content:  content,
			Checksum: checksum[:],
		}

		data, err := json.Marshal(checksumInfo)
		if err != nil {
			recordErr(err)
			return
		}

		fname := fmt.Sprintf("%s/t%d_and__", CheckpointChecksumDir, tableID)
		err = s.WriteFile(ctx, fname, data)
		if err != nil {
			recordErr(err)
			return
		}
	})
	return nil
}

type CheckpointRunner struct {
	meta map[string]*RangeGroups

	checksumRunner *ChecksumRunner

	storage storage.ExternalStorage
	cipher  *backuppb.CipherInfo

	appendCh chan *CheckpointMessage
	metaCh   chan map[string]*RangeGroups
	errCh    chan error

	wg sync.WaitGroup
}

// only for test
func StartCheckpointRunnerForTest(ctx context.Context, storage storage.ExternalStorage, cipher *backuppb.CipherInfo, tick time.Duration) *CheckpointRunner {
	runner := &CheckpointRunner{
		meta: make(map[string]*RangeGroups),

		checksumRunner: NewChecksumRunner(),

		storage: storage,
		cipher:  cipher,

		appendCh: make(chan *CheckpointMessage),
		metaCh:   make(chan map[string]*RangeGroups),
		errCh:    make(chan error),
	}

	runner.startCheckpointLoop(ctx, tick)
	return runner
}

func StartCheckpointRunner(ctx context.Context, storage storage.ExternalStorage, cipher *backuppb.CipherInfo) *CheckpointRunner {
	runner := &CheckpointRunner{
		meta: make(map[string]*RangeGroups),

		checksumRunner: NewChecksumRunner(),

		storage: storage,
		cipher:  cipher,

		appendCh: make(chan *CheckpointMessage),
		metaCh:   make(chan map[string]*RangeGroups),
		errCh:    make(chan error),
	}

	runner.startCheckpointLoop(ctx, tickDuration)
	return runner
}

func (r *CheckpointRunner) FlushChecksum(ctx context.Context, tableID int64, crc64xor uint64, totalKvs uint64, totalBytes uint64, timeCost float64) error {
	return r.checksumRunner.FlushChecksum(ctx, r.storage, tableID, crc64xor, totalKvs, totalBytes, timeCost)
}

func (r *CheckpointRunner) Append(
	ctx context.Context,
	groupKey string,
	startKey []byte,
	endKey []byte,
	files []*backuppb.File,
) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.errCh:
		return err
	case r.appendCh <- &CheckpointMessage{
		GroupKey: groupKey,
		Group: &rtree.Range{
			StartKey: startKey,
			EndKey:   endKey,
			Files:    files,
		},
	}:
		return nil
	}
}

// Note: Cannot be parallel with `Append` function
func (r *CheckpointRunner) WaitForFinish() {
	// can not append anymore
	close(r.appendCh)
	// wait the range flusher exit
	r.wg.Wait()
	// wait the checksum flusher exit
	r.checksumRunner.wg.Wait()
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

// start a goroutine to flush the meta, which is sent from `checkpoint looper`, to the external storage
func (r *CheckpointRunner) startCheckpointRunner(ctx context.Context, wg *sync.WaitGroup) chan error {
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
			}
		}
	}

	go flushWorker(ctx, errCh)
	return errCh
}

func (r *CheckpointRunner) startCheckpointLoop(ctx context.Context, tickDuration time.Duration) {
	r.wg.Add(1)
	checkpointLoop := func(ctx context.Context) {
		defer r.wg.Done()
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		errCh := r.startCheckpointRunner(cctx, &wg)
		ticker := time.NewTicker(tickDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.flushMeta(ctx, errCh); err != nil {
					r.errCh <- err
					return
				}
			case msg, ok := <-r.appendCh:
				if !ok {
					log.Info("stop checkpoint runner")
					if err := r.flushMeta(ctx, errCh); err != nil {
						r.errCh <- err
					}
					// close the channel to flush worker
					// and wait it to consumes all the metas
					close(r.metaCh)
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
				groups.Groups = append(groups.Groups, msg.Group)
			case err := <-errCh:
				// pass flush worker's error back
				r.errCh <- err
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
		path := fmt.Sprintf("%s/%s_%d.cpt", CheckpointDataDir, checksumEncoded, rand.Uint64())
		if err := r.storage.WriteFile(ctx, path, data); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// walk the whole checkpoint range files and retrieve the metadatat of backed up ranges
// and return the total time cost in the past executions
func WalkCheckpointFile(ctx context.Context, s storage.ExternalStorage, cipher *backuppb.CipherInfo, fn func(groupKey string, rg *rtree.Range)) (time.Duration, error) {
	// records the total time cost in the past executions
	var pastDureTime time.Duration = 0
	err := s.WalkDir(ctx, &storage.WalkOption{SubDir: CheckpointDataDir}, func(path string, size int64) error {
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
	data, err := s.ReadFile(ctx, CheckpointMetaPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m := &CheckpointMetadata{}
	err = json.Unmarshal(data, m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m.CheckpointChecksum, err = loadCheckpointChecksum(ctx, s)
	return m, errors.Trace(err)
}

// walk the whole checkpoint checksum files and retrieve checksum information of tables calculated
func loadCheckpointChecksum(ctx context.Context, s storage.ExternalStorage) (map[int64]*ChecksumItem, error) {
	checkpointChecksum := make(map[int64]*ChecksumItem)

	err := s.WalkDir(ctx, &storage.WalkOption{SubDir: CheckpointChecksumDir}, func(path string, size int64) error {
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
