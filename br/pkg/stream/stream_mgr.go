// Copyright 2021 PingCAP, Inc.
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

package stream

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	filter "github.com/pingcap/tidb/util/table-filter"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	streamBackupMetaPrefix = "v1/backupmeta"

	metaDataWorkerPoolSize = 128
)

func GetStreamBackupMetaPrefix() string {
	return streamBackupMetaPrefix
}

// appendTableObserveRanges builds key ranges corresponding to `tblIDS`.
func appendTableObserveRanges(tblIDs []int64) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(tblIDs))
	for _, tid := range tblIDs {
		startKey := tablecodec.GenTableRecordPrefix(tid)
		endKey := startKey.PrefixNext()
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs
}

// buildObserveTableRange builds key ranges to observe data KV that belongs to `table`.
func buildObserveTableRange(table *model.TableInfo) []kv.KeyRange {
	pis := table.GetPartitionInfo()
	if pis == nil {
		// Short path, no partition.
		return appendTableObserveRanges([]int64{table.ID})
	}

	tblIDs := make([]int64, 0, len(pis.Definitions))
	// whether we shoud append tbl.ID into tblIDS ?
	for _, def := range pis.Definitions {
		tblIDs = append(tblIDs, def.ID)
	}
	return appendTableObserveRanges(tblIDs)
}

// buildObserveTableRanges builds key ranges to observe table kv-events.
func buildObserveTableRanges(
	storage kv.Storage,
	tableFilter filter.Filter,
	backupTS uint64,
) ([]kv.KeyRange, error) {
	snapshot := storage.GetSnapshot(kv.NewVersion(backupTS))
	m := meta.NewSnapshotMeta(snapshot)

	dbs, err := m.ListDatabases()
	if err != nil {
		return nil, errors.Trace(err)
	}

	ranges := make([]kv.KeyRange, 0, len(dbs)+1)
	for _, dbInfo := range dbs {
		if !tableFilter.MatchSchema(dbInfo.Name.O) || util.IsMemDB(dbInfo.Name.L) {
			continue
		}

		tables, err := m.ListTables(dbInfo.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(tables) == 0 {
			log.Warn("It's not necessary to observe empty database",
				zap.Stringer("db", dbInfo.Name))
			continue
		}

		for _, tableInfo := range tables {
			if !tableFilter.MatchTable(dbInfo.Name.O, tableInfo.Name.O) {
				// Skip tables other than the given table.
				continue
			}

			log.Info("observer table schema", zap.String("table", dbInfo.Name.O+"."+tableInfo.Name.O))
			tableRanges := buildObserveTableRange(tableInfo)
			ranges = append(ranges, tableRanges...)
		}
	}

	return ranges, nil
}

// buildObserverAllRange build key range to observe all data kv-events.
func buildObserverAllRange() []kv.KeyRange {
	var startKey []byte
	startKey = append(startKey, tablecodec.TablePrefix()...)

	sk := kv.Key(startKey)
	ek := sk.PrefixNext()

	rgs := make([]kv.KeyRange, 0, 1)
	return append(rgs, kv.KeyRange{StartKey: sk, EndKey: ek})
}

// BuildObserveDataRanges builds key ranges to observe data KV.
func BuildObserveDataRanges(
	storage kv.Storage,
	filterStr []string,
	tableFilter filter.Filter,
	backupTS uint64,
) ([]kv.KeyRange, error) {
	if len(filterStr) == 1 && filterStr[0] == string("*.*") {
		return buildObserverAllRange(), nil
	} else {
		return buildObserveTableRanges(storage, tableFilter, backupTS)
	}
}

// BuildObserveMetaRange specifies build key ranges to observe meta KV(contains all of metas)
func BuildObserveMetaRange() *kv.KeyRange {
	var startKey []byte
	startKey = append(startKey, tablecodec.MetaPrefix()...)
	sk := kv.Key(startKey)
	ek := sk.PrefixNext()

	return &kv.KeyRange{StartKey: sk, EndKey: ek}
}

// FastUnmarshalMetaData used a 128 worker pool to speed up
// read metadata content from external_storage.
func FastUnmarshalMetaData(
	ctx context.Context,
	s storage.ExternalStorage,
	fn func(path string, m *backuppb.Metadata) error,
) error {
	log.Info("use workers to speed up reading metadata files", zap.Int("workers", metaDataWorkerPoolSize))
	pool := utils.NewWorkerPool(metaDataWorkerPoolSize, "metadata")
	eg, ectx := errgroup.WithContext(ctx)
	opt := &storage.WalkOption{SubDir: GetStreamBackupMetaPrefix()}
	err := s.WalkDir(ectx, opt, func(path string, size int64) error {
		readPath := path
		pool.ApplyOnErrorGroup(eg, func() error {
			log.Info("fast read meta file from storage", zap.String("path", readPath))
			b, err := s.ReadFile(ectx, readPath)
			if err != nil {
				return errors.Trace(err)
			}
			m := &backuppb.Metadata{}
			err = m.Unmarshal(b)
			if err != nil {
				if !strings.HasSuffix(path, ".meta") {
					return nil
				} else {
					return err
				}
			}
			return fn(readPath, m)
		})
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	return eg.Wait()
}
