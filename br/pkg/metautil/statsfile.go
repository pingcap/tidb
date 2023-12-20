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

package metautil

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"golang.org/x/sync/errgroup"
)

const maxStatsJsonTableSize = 32 * 1024 * 1024 // 32 MiB

func getStatsFileName(physicalID int64) string {
	return fmt.Sprintf("backupmeta.schema.stats.%09d", physicalID)
}

// A lightweight function wrapper to dump the statistic
type StatsWriter struct {
	storage storage.ExternalStorage
	cipher  *backuppb.CipherInfo

	// final stats file indexes
	statsFileIndexes []*backuppb.StatsFileIndex

	// temporary variables, clear after each flush
	totalSize   int
	statsFile   *backuppb.StatsFile
	physicalIDs []int64
}

func newStatsWriter(
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
) *StatsWriter {
	return &StatsWriter{
		storage: storage,
		cipher:  cipher,

		statsFileIndexes: make([]*backuppb.StatsFileIndex, 0),

		totalSize:   0,
		statsFile:   &backuppb.StatsFile{},
		physicalIDs: make([]int64, 0),
	}
}

func (s *StatsWriter) writeStatsFileAndClear(ctx context.Context, physicalID int64) error {
	fileName := getStatsFileName(physicalID)
	content, err := proto.Marshal(s.statsFile)
	if err != nil {
		return errors.Trace(err)
	}
	checksum := sha256.Sum256(content)

	encryptedContent, iv, err := Encrypt(content, s.cipher)
	if err != nil {
		return errors.Trace(err)
	}

	s.statsFileIndexes = append(s.statsFileIndexes, &backuppb.StatsFileIndex{
		Name:        fileName,
		Sha256:      checksum[:],
		SizeEnc:     uint64(len(encryptedContent)),
		SizeOri:     uint64(len(content)),
		CipherIv:    iv,
		PhysicalIds: s.physicalIDs,
	})

	// clear the temporary variables
	s.totalSize = 0
	s.statsFile = &backuppb.StatsFile{}
	s.physicalIDs = make([]int64, 0)
	return nil
}

func (s *StatsWriter) BackupStats(ctx context.Context, jsonTable *statsutil.JSONTable, physicalID int64) error {
	if jsonTable == nil {
		return nil
	}

	statsBytes, err := json.Marshal(jsonTable)
	if err != nil {
		return errors.Trace(err)
	}

	s.totalSize += len(statsBytes)
	s.physicalIDs = append(s.physicalIDs, physicalID)
	s.statsFile.Blocks = append(s.statsFile.Blocks, &backuppb.StatsBlock{
		PhysicalId: physicalID,
		JsonTable:  statsBytes,
	})

	// check whether need to flush
	if s.totalSize > maxStatsJsonTableSize {
		if err := s.writeStatsFileAndClear(ctx, physicalID); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *StatsWriter) BackupStatsDone(ctx context.Context) ([]*backuppb.StatsFileIndex, error) {
	if len(s.physicalIDs) == 0 || s.totalSize == 0 {
		return s.statsFileIndexes, nil
	}

	if err := s.writeStatsFileAndClear(ctx, s.physicalIDs[0]); err != nil {
		return nil, errors.Trace(err)
	}
	return s.statsFileIndexes, nil
}

func RestoreStats(
	ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	statsFileIndexes []*backuppb.StatsFileIndex,
	rewriteIDMap map[int64]int64,
	taskCh chan<- *statstypes.PartitionStatisticLoadTask,
) error {
	eg, ectx := errgroup.WithContext(ctx)
	downloadWorkerpool := utils.NewWorkerPool(4, "download stats for each partition")
	for _, statsFileIndex := range statsFileIndexes {
		if ectx.Err() != nil {
			break
		}
		statsFile := statsFileIndex
		downloadWorkerpool.ApplyOnErrorGroup(eg, func() error {
			content, err := storage.ReadFile(ectx, statsFile.Name)
			if err != nil {
				return errors.Trace(err)
			}

			decryptContent, err := Decrypt(content, cipher, statsFile.CipherIv)
			if err != nil {
				return errors.Trace(err)
			}

			checksum := sha256.Sum256(decryptContent)
			if !bytes.Equal(statsFile.Sha256, checksum[:]) {
				return berrors.ErrInvalidMetaFile.GenWithStackByArgs(fmt.Sprintf(
					"checksum mismatch expect %x, got %x", statsFile.Sha256, checksum[:]))
			}

			statsFileBlocks := &backuppb.StatsFile{}
			if err := proto.Unmarshal(decryptContent, statsFileBlocks); err != nil {
				return errors.Trace(err)
			}

			for _, block := range statsFileBlocks.Blocks {
				physicalId, ok := rewriteIDMap[block.PhysicalId]
				if !ok {
					return berrors.ErrRestoreInvalidRewrite.GenWithStackByArgs(fmt.Sprintf(
						"not rewrite rule matched, old physical id: %d", block.PhysicalId))
				}
				jsonTable := &statsutil.JSONTable{}
				if err := json.Unmarshal(block.JsonTable, jsonTable); err != nil {
					return errors.Trace(err)
				}
				select {
				case <-ectx.Done():
					return nil
				case taskCh <- &statstypes.PartitionStatisticLoadTask{
					PhysicalID: physicalId,
					JsonTable:  jsonTable,
				}:

				}
			}

			return nil
		})
	}

	return eg.Wait()
}
