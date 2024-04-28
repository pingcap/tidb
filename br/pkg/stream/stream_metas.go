// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"context"
	"math"
	"strconv"
	"sync"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const notDeletedBecameFatalThreshold = 128

type StreamMetadataSet struct {
	// if set true, the metadata and datafile won't be removed
	DryRun bool

	// keeps the meta-information of metadata as little as possible
	// to save the memory
	metadataInfos             map[string]*MetadataInfo
	MetadataDownloadBatchSize uint

	// a parser of metadata
	Helper *MetadataHelper

	// for test
	BeforeDoWriteBack func(path string, replaced *backuppb.Metadata) (skip bool)
}

// keep these meta-information for statistics and filtering
type FileGroupInfo struct {
	MaxTS   uint64
	Length  uint64
	KVCount int64
}

// keep these meta-information for statistics and filtering
type MetadataInfo struct {
	MinTS          uint64
	FileGroupInfos []*FileGroupInfo
}

func (ms *StreamMetadataSet) TEST_GetMetadataInfos() map[string]*MetadataInfo {
	return ms.metadataInfos
}

// LoadUntilAndCalculateShiftTS loads the metadata until the specified timestamp and calculate
// the shift-until-ts by the way. This would record all metadata files that *may* contain data
// from transaction committed before that TS.
func (ms *StreamMetadataSet) LoadUntilAndCalculateShiftTS(
	ctx context.Context,
	s storage.ExternalStorage,
	until uint64,
) (uint64, error) {
	metadataMap := struct {
		sync.Mutex
		metas        map[string]*MetadataInfo
		shiftUntilTS uint64
	}{}
	metadataMap.metas = make(map[string]*MetadataInfo)
	// `shiftUntilTS` must be less than `until`
	metadataMap.shiftUntilTS = until
	err := FastUnmarshalMetaData(ctx, s, ms.MetadataDownloadBatchSize, func(path string, raw []byte) error {
		m, err := ms.Helper.ParseToMetadataHard(raw)
		if err != nil {
			return err
		}
		// If the meta file contains only files with ts grater than `until`, when the file is from
		// `Default`: it should be kept, because its corresponding `write` must has commit ts grater
		//            than it, which should not be considered.
		// `Write`: it should trivially not be considered.
		if m.MinTs <= until {
			// record these meta-information for statistics and filtering
			fileGroupInfos := make([]*FileGroupInfo, 0, len(m.FileGroups))
			for _, group := range m.FileGroups {
				var kvCount int64 = 0
				for _, file := range group.DataFilesInfo {
					kvCount += file.NumberOfEntries
				}
				fileGroupInfos = append(fileGroupInfos, &FileGroupInfo{
					MaxTS:   group.MaxTs,
					Length:  group.Length,
					KVCount: kvCount,
				})
			}
			metadataMap.Lock()
			metadataMap.metas[path] = &MetadataInfo{
				MinTS:          m.MinTs,
				FileGroupInfos: fileGroupInfos,
			}
			metadataMap.Unlock()
		}
		// filter out the metadatas whose ts-range is overlap with [until, +inf)
		// and calculate their minimum begin-default-ts
		ts, ok := UpdateShiftTS(m, until, mathutil.MaxUint)
		if ok {
			metadataMap.Lock()
			if ts < metadataMap.shiftUntilTS {
				metadataMap.shiftUntilTS = ts
			}
			metadataMap.Unlock()
		}
		return nil
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	ms.metadataInfos = metadataMap.metas
	if metadataMap.shiftUntilTS != until {
		log.Warn("calculate shift-ts", zap.Uint64("start-ts", until), zap.Uint64("shift-ts", metadataMap.shiftUntilTS))
	}
	return metadataMap.shiftUntilTS, nil
}

// LoadFrom loads data from an external storage into the stream metadata set. (Now only for test)
func (ms *StreamMetadataSet) LoadFrom(ctx context.Context, s storage.ExternalStorage) error {
	_, err := ms.LoadUntilAndCalculateShiftTS(ctx, s, math.MaxUint64)
	return err
}

func (ms *StreamMetadataSet) iterateDataFiles(f func(d *FileGroupInfo) (shouldBreak bool)) {
	for _, m := range ms.metadataInfos {
		for _, d := range m.FileGroupInfos {
			if f(d) {
				return
			}
		}
	}
}

// IterateFilesFullyBefore runs the function over all files contain data before the timestamp only.
//
//	0                                          before
//	|------------------------------------------|
//	 |-file1---------------| <- File contains records in this TS range would be found.
//	                               |-file2--------------| <- File contains any record out of this won't be found.
//
// This function would call the `f` over file1 only.
func (ms *StreamMetadataSet) IterateFilesFullyBefore(before uint64, f func(d *FileGroupInfo) (shouldBreak bool)) {
	ms.iterateDataFiles(func(d *FileGroupInfo) (shouldBreak bool) {
		if d.MaxTS >= before {
			return false
		}
		return f(d)
	})
}

// RemoveDataFilesAndUpdateMetadataInBatch concurrently remove datafilegroups and update metadata.
// Only one metadata is processed in each thread, including deleting its datafilegroup and updating it.
// Returns the not deleted datafilegroups.
func (ms *StreamMetadataSet) RemoveDataFilesAndUpdateMetadataInBatch(
	ctx context.Context,
	from uint64,
	storage storage.ExternalStorage,
	updateFn func(num int64),
) ([]string, error) {
	var notDeleted struct {
		item []string
		sync.Mutex
	}
	worker := util.NewWorkerPool(ms.MetadataDownloadBatchSize, "delete files")
	eg, cx := errgroup.WithContext(ctx)
	for path, metaInfo := range ms.metadataInfos {
		path := path
		minTS := metaInfo.MinTS
		// It's safety to remove the item within a range loop
		delete(ms.metadataInfos, path)
		if minTS >= from {
			// That means all the datafiles wouldn't be removed,
			// so that the metadata is skipped.
			continue
		}
		worker.ApplyOnErrorGroup(eg, func() error {
			if cx.Err() != nil {
				return cx.Err()
			}

			data, err := storage.ReadFile(ctx, path)
			if err != nil {
				return err
			}

			meta, err := ms.Helper.ParseToMetadataHard(data)
			if err != nil {
				return err
			}

			num, notDeletedItems, err := ms.removeDataFilesAndUpdateMetadata(ctx, storage, from, meta, path)
			if err != nil {
				return err
			}

			updateFn(num)

			notDeleted.Lock()
			notDeleted.item = append(notDeleted.item, notDeletedItems...)
			notDeleted.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}

	return notDeleted.item, nil
}

// removeDataFilesAndUpdateMetadata removes some datafilegroups of the metadata, if their max-ts is less than `from`
func (ms *StreamMetadataSet) removeDataFilesAndUpdateMetadata(
	ctx context.Context,
	storage storage.ExternalStorage,
	from uint64,
	meta *backuppb.Metadata,
	metaPath string,
) (num int64, notDeleted []string, err error) {
	removed := make([]*backuppb.DataFileGroup, 0)
	remainedDataFiles := make([]*backuppb.DataFileGroup, 0)
	notDeleted = make([]string, 0)
	// can we assume those files are sorted to avoid traversing here? (by what?)
	for _, ds := range meta.FileGroups {
		if ds.MaxTs < from {
			removed = append(removed, ds)
		} else {
			// That means some kvs in the datafilegroup shouldn't be removed,
			// so it will be kept out being removed.
			remainedDataFiles = append(remainedDataFiles, ds)
		}
	}

	num = int64(len(removed))

	if ms.DryRun {
		log.Info("dry run, skip deletion ...")
		return num, notDeleted, nil
	}

	// remove data file groups
	for _, f := range removed {
		log.Info("Deleting file", zap.String("path", f.Path))
		if err := storage.DeleteFile(ctx, f.Path); err != nil {
			log.Warn("File not deleted.", zap.String("path", f.Path), logutil.ShortError(err))
			notDeleted = append(notDeleted, f.Path)
			if len(notDeleted) > notDeletedBecameFatalThreshold {
				return num, notDeleted, errors.Annotatef(berrors.ErrPiTRMalformedMetadata, "too many failure when truncating")
			}
		}
	}

	// update metadata
	if len(remainedDataFiles) != len(meta.FileGroups) {
		// rewrite metadata
		log.Info("Updating metadata.", zap.String("file", metaPath),
			zap.Int("data-file-before", len(meta.FileGroups)),
			zap.Int("data-file-after", len(remainedDataFiles)))

		// replace the filegroups and update the ts of the replaced metadata
		ReplaceMetadata(meta, remainedDataFiles)

		if ms.BeforeDoWriteBack != nil && ms.BeforeDoWriteBack(metaPath, meta) {
			log.Info("Skipped writeback meta by the hook.", zap.String("meta", metaPath))
			return num, notDeleted, nil
		}

		if err := ms.doWriteBackForFile(ctx, storage, metaPath, meta); err != nil {
			// NOTE: Maybe we'd better roll back all writebacks? (What will happen if roll back fails too?)
			return num, notDeleted, errors.Annotatef(err, "failed to write back file %s", metaPath)
		}
	}

	return num, notDeleted, nil
}

func (ms *StreamMetadataSet) doWriteBackForFile(
	ctx context.Context,
	s storage.ExternalStorage,
	path string,
	meta *backuppb.Metadata,
) error {
	// If the metadata file contains no data file, remove it due to it is meanless.
	if len(meta.FileGroups) == 0 {
		if err := s.DeleteFile(ctx, path); err != nil {
			return errors.Annotatef(err, "failed to remove the empty meta %s", path)
		}
		return nil
	}

	bs, err := ms.Helper.Marshal(meta)
	if err != nil {
		return errors.Annotatef(err, "failed to marshal the file %s", path)
	}
	return truncateAndWrite(ctx, s, path, bs)
}

func truncateAndWrite(ctx context.Context, s storage.ExternalStorage, path string, data []byte) error {
	// Performance hack: the `Write` implemention would truncate the file if it exists.
	if err := s.WriteFile(ctx, path, data); err != nil {
		return errors.Annotatef(err, "failed to save the file %s to %s", path, s.URI())
	}
	return nil
}

const (
	// TruncateSafePointFileName is the filename that the ts(the log have been truncated) is saved into.
	TruncateSafePointFileName = "v1_stream_trancate_safepoint.txt"
)

// GetTSFromFile gets the current truncate safepoint.
// truncate safepoint is the TS used for last truncating:
// which means logs before this TS would probably be deleted or incomplete.
func GetTSFromFile(
	ctx context.Context,
	s storage.ExternalStorage,
	filename string,
) (uint64, error) {
	exists, err := s.FileExists(ctx, filename)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	data, err := s.ReadFile(ctx, filename)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return 0, berrors.ErrInvalidMetaFile.GenWithStackByArgs("failed to parse the truncate safepoint")
	}
	return value, nil
}

// SetTSToFile overrides the current truncate safepoint.
// truncate safepoint is the TS used for last truncating:
// which means logs before this TS would probably be deleted or incomplete.
func SetTSToFile(
	ctx context.Context,
	s storage.ExternalStorage,
	safepoint uint64,
	filename string,
) error {
	content := strconv.FormatUint(safepoint, 10)
	return truncateAndWrite(ctx, s, filename, []byte(content))
}

func UpdateShiftTS(m *backuppb.Metadata, startTS uint64, restoreTS uint64) (uint64, bool) {
	var (
		minBeginTS uint64
		isExist    bool
	)
	if len(m.FileGroups) == 0 || m.MinTs > restoreTS || m.MaxTs < startTS {
		return 0, false
	}

	for _, ds := range m.FileGroups {
		for _, d := range ds.DataFilesInfo {
			if d.Cf == DefaultCF || d.MinBeginTsInDefaultCf == 0 {
				continue
			}
			if d.MinTs > restoreTS || d.MaxTs < startTS {
				continue
			}
			if d.MinBeginTsInDefaultCf < minBeginTS || !isExist {
				isExist = true
				minBeginTS = d.MinBeginTsInDefaultCf
			}
		}
	}
	return minBeginTS, isExist
}

// replace the filegroups and update the ts of the replaced metadata
func ReplaceMetadata(meta *backuppb.Metadata, filegroups []*backuppb.DataFileGroup) {
	// replace the origin metadata
	meta.FileGroups = filegroups

	if len(meta.FileGroups) == 0 {
		meta.MinTs = 0
		meta.MaxTs = 0
		meta.ResolvedTs = 0
		return
	}

	meta.MinTs = meta.FileGroups[0].MinTs
	meta.MaxTs = meta.FileGroups[0].MaxTs
	meta.ResolvedTs = meta.FileGroups[0].MinResolvedTs
	for _, group := range meta.FileGroups {
		if group.MinTs < meta.MinTs {
			meta.MinTs = group.MinTs
		}
		if group.MaxTs > meta.MaxTs {
			meta.MaxTs = group.MaxTs
		}
		if group.MinResolvedTs < meta.ResolvedTs {
			meta.ResolvedTs = group.MinResolvedTs
		}
	}
}
