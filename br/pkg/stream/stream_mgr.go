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

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/metautil"
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
	streamBackupMetaPrefix   = "v1/backupmeta"
	streamBackupMetaV2Prefix = "v2/backupmeta"

	streamBackupGlobalCheckpointPrefix = "v1/global_checkpoint"

	metaDataWorkerPoolSize = 128
)

func GetStreamBackupMetaPrefix() string {
	return streamBackupMetaPrefix
}

func GetStreamBackupMetaV2Prefix() string {
	return streamBackupMetaV2Prefix
}

func GetStreamBackupGlobalCheckpointPrefix() string {
	return streamBackupGlobalCheckpointPrefix
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
	}
	return buildObserveTableRanges(storage, tableFilter, backupTS)
}

// BuildObserveMetaRange specifies build key ranges to observe meta KV(contains all of metas)
func BuildObserveMetaRange() *kv.KeyRange {
	var startKey []byte
	startKey = append(startKey, tablecodec.MetaPrefix()...)
	sk := kv.Key(startKey)
	ek := sk.PrefixNext()

	return &kv.KeyRange{StartKey: sk, EndKey: ek}
}

// MetaDataHelper make restore compatible with metadataV1 and metadataV2.
type MetaDataHelper interface {
	ReadFile(ctx context.Context, path string, offset uint64, length uint64, storage storage.ExternalStorage) ([]byte, error)
	GetStreamBackupMetaPrefix() string
	ParseToMetaDataV2(rawMetaData []byte) (*backuppb.MetadataV2, error)
	ParseToMetaDataV2Hard(rawMetaData []byte) (*backuppb.MetadataV2, error)
	Marshal(meta *backuppb.MetadataV2) ([]byte, error)
}

type MetaDataV1Helper struct {
}

func (m *MetaDataV1Helper) ReadFile(ctx context.Context, path string, _ uint64, _ uint64, storage storage.ExternalStorage) ([]byte, error) {
	return storage.ReadFile(ctx, path)
}

func (m *MetaDataV1Helper) GetStreamBackupMetaPrefix() string {
	return streamBackupMetaPrefix
}

func (m *MetaDataV1Helper) ParseToMetaDataV2(rawMetaData []byte) (*backuppb.MetadataV2, error) {
	meta := &backuppb.Metadata{}
	err := meta.Unmarshal(rawMetaData)
	if err != nil {
		return nil, err
	}
	group := &backuppb.DataFileGroup{
		// For MetaDataV2, file's path is stored in it.
		Path: "",
		// In fact, each file in MetaDataV1 can be regard
		// as a file group in MetaDataV2. But for simplicity,
		// the files in MetaDataV1 are considered as a group.
		DataFilesInfo: meta.Files,
		// Other fields are Unused.
	}
	metaV2 := &backuppb.MetadataV2{
		FileGroups: []*backuppb.DataFileGroup{group},
		StoreId:    meta.StoreId,
		ResolvedTs: meta.ResolvedTs,
		MaxTs:      meta.MaxTs,
		MinTs:      meta.MinTs,
	}
	return metaV2, nil
}

// Only for deleting, after MetadataV1 is deprecated, we can remove it.
// Hard means convert to MetaDataV2 deeply.
func (m *MetaDataV1Helper) ParseToMetaDataV2Hard(rawMetaData []byte) (*backuppb.MetadataV2, error) {
	meta := &backuppb.Metadata{}
	err := meta.Unmarshal(rawMetaData)
	if err != nil {
		return nil, err
	}
	groups := make([]*backuppb.DataFileGroup, 0, len(meta.Files))
	for _, d := range meta.Files {
		groups = append(groups, &backuppb.DataFileGroup{
			// For MetaDataV2, file's path is stored in it.
			Path: d.Path,
			// In fact, each file in MetaDataV1 can be regard
			// as a file group in MetaDataV2. But for simplicity,
			// the files in MetaDataV1 are considered as a group.
			DataFilesInfo: []*backuppb.DataFileInfo{d},
			MaxTs:         d.MaxTs,
			MinTs:         d.MinTs,
			// Other fields are Unused.
		})
	}

	metaV2 := &backuppb.MetadataV2{
		FileGroups: groups,
		StoreId:    meta.StoreId,
		ResolvedTs: meta.ResolvedTs,
		MaxTs:      meta.MaxTs,
		MinTs:      meta.MinTs,
	}
	return metaV2, nil
}

func (m *MetaDataV1Helper) Marshal(meta *backuppb.MetadataV2) ([]byte, error) {
	files := make([]*backuppb.DataFileInfo, 0, len(meta.FileGroups))
	for _, ds := range meta.FileGroups {
		if len(ds.DataFilesInfo) != 1 {
			return nil, errors.Errorf("This is not fake MetadataV2, need metadataV1")
		}
		files = append(files, ds.DataFilesInfo[0])
	}
	metaV1 := &backuppb.Metadata{
		Files:      files,
		StoreId:    meta.StoreId,
		ResolvedTs: meta.ResolvedTs,
		MaxTs:      meta.MaxTs,
		MinTs:      meta.MinTs,
	}

	return metaV1.Marshal()
}

func NewMetaDataV1Helper() *MetaDataV1Helper {
	return &MetaDataV1Helper{}
}

type MetaDataV2Helper struct {
	decoder *zstd.Decoder
}

func (m *MetaDataV2Helper) ReadFile(ctx context.Context, path string, offset uint64, length uint64, storage storage.ExternalStorage) ([]byte, error) {
	data, err := storage.ReadRangeFile(ctx, path, int64(offset), int64(length))
	if err != nil {
		return nil, errors.Trace(err)
	}
	data, err = m.decoder.DecodeAll(data, nil)
	return data, errors.Trace(err)
}

func (m *MetaDataV2Helper) GetStreamBackupMetaPrefix() string {
	return streamBackupMetaV2Prefix
}

func (m *MetaDataV2Helper) ParseToMetaDataV2(rawMetaData []byte) (*backuppb.MetadataV2, error) {
	meta := &backuppb.MetadataV2{}
	err := meta.Unmarshal(rawMetaData)
	return meta, errors.Trace(err)
}

func (*MetaDataV2Helper) ParseToMetaDataV2Hard(rawMetaData []byte) (*backuppb.MetadataV2, error) {
	meta := &backuppb.MetadataV2{}
	err := meta.Unmarshal(rawMetaData)
	return meta, errors.Trace(err)
}

func (*MetaDataV2Helper) Marshal(meta *backuppb.MetadataV2) ([]byte, error) {
	return meta.Marshal()
}

func NewMetaDataV2Helper() *MetaDataV2Helper {
	decoder, _ := zstd.NewReader(nil)
	return &MetaDataV2Helper{
		decoder: decoder,
	}
}

func BuildMetaDataHelper(ctx context.Context, storage storage.ExternalStorage) (MetaDataHelper, error) {
	data, err := storage.ReadFile(ctx, metautil.LockFile)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if strings.Contains(string(data), "V2") {
		log.Info("use storage v2, restore backup files in v2/")
		return NewMetaDataV2Helper(), nil
	}

	log.Info("use storage v1, restore backup files in v1/")
	return NewMetaDataV1Helper(), nil
}

// FastUnmarshalMetaData used a 128 worker pool to speed up
// read metadata content from external_storage.
func FastUnmarshalMetaData(
	ctx context.Context,
	s storage.ExternalStorage,
	helper MetaDataHelper,
	fn func(path string, rawMetaData []byte) error,
) error {
	log.Info("use workers to speed up reading metadata files", zap.Int("workers", metaDataWorkerPoolSize))
	pool := utils.NewWorkerPool(metaDataWorkerPoolSize, "metadata")
	eg, ectx := errgroup.WithContext(ctx)
	opt := &storage.WalkOption{SubDir: helper.GetStreamBackupMetaPrefix()}
	err := s.WalkDir(ectx, opt, func(path string, size int64) error {
		if !strings.HasSuffix(path, ".meta") {
			return nil
		}
		readPath := path
		pool.ApplyOnErrorGroup(eg, func() error {
			log.Info("fast read meta file from storage", zap.String("path", readPath))
			b, err := s.ReadFile(ectx, readPath)
			if err != nil {
				log.Error("failed to read file", zap.String("file", readPath))
				return errors.Annotatef(err, "during reading meta file %s from storage", readPath)
			}

			return fn(readPath, b)
		})
		return nil
	})
	if err != nil {
		readErr := eg.Wait()
		if readErr != nil {
			return errors.Annotatef(readErr, "scanning metadata meets error %s", err)
		}
		return errors.Annotate(err, "scanning metadata meets error")
	}
	return eg.Wait()
}
