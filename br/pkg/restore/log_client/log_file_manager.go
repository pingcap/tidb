// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap"
)

// MetaIter is the type of iterator of metadata files' content.
type MetaIter = iter.TryNextor[*backuppb.Metadata]

type LogDataFileInfo struct {
	*backuppb.DataFileInfo
	MetaDataGroupName   string
	OffsetInMetaGroup   int
	OffsetInMergedGroup int
}

// LogIter is the type of iterator of each log files' meta information.
type LogIter = iter.TryNextor[*LogDataFileInfo]

// MetaGroupIter is the iterator of flushes of metadata.
type MetaGroupIter = iter.TryNextor[DDLMetaGroup]

// Meta is the metadata of files.
type Meta = *backuppb.Metadata

// Log is the metadata of one file recording KV sequences.
type Log = *backuppb.DataFileInfo

// LogFileManager is the manager for log files of a certain restoration,
// which supports read / filter from the log backup archive with static start TS / restore TS.
type LogFileManager struct {
	// startTS and restoreTS are used for kv file restore.
	// TiKV will filter the key space that don't belong to [startTS, restoreTS].
	startTS   uint64
	restoreTS uint64

	// If the commitTS of txn-entry belong to [startTS, restoreTS],
	// the startTS of txn-entry may be smaller than startTS.
	// We need maintain and restore more entries in default cf
	// (the startTS in these entries belong to [shiftStartTS, startTS]).
	shiftStartTS uint64

	storage storage.ExternalStorage
	helper  *stream.MetadataHelper

	metadataDownloadBatchSize uint
}

// LogFileManagerInit is the config needed for initializing the log file manager.
type LogFileManagerInit struct {
	StartTS   uint64
	RestoreTS uint64
	Storage   storage.ExternalStorage

	MetadataDownloadBatchSize uint
}

type DDLMetaGroup struct {
	Path      string
	FileMetas []*backuppb.DataFileInfo
}

// CreateLogFileManager creates a log file manager using the specified config.
// Generally the config cannot be changed during its lifetime.
func CreateLogFileManager(ctx context.Context, init LogFileManagerInit) (*LogFileManager, error) {
	fm := &LogFileManager{
		startTS:   init.StartTS,
		restoreTS: init.RestoreTS,
		storage:   init.Storage,
		helper:    stream.NewMetadataHelper(),

		metadataDownloadBatchSize: init.MetadataDownloadBatchSize,
	}
	err := fm.loadShiftTS(ctx)
	if err != nil {
		return nil, err
	}
	return fm, nil
}

func (rc *LogFileManager) ShiftTS() uint64 {
	return rc.shiftStartTS
}

func (rc *LogFileManager) loadShiftTS(ctx context.Context) error {
	shiftTS := struct {
		sync.Mutex
		value  uint64
		exists bool
	}{}
	err := stream.FastUnmarshalMetaData(ctx, rc.storage, rc.metadataDownloadBatchSize, func(path string, raw []byte) error {
		m, err := rc.helper.ParseToMetadata(raw)
		if err != nil {
			return err
		}
		log.Info("read meta from storage and parse", zap.String("path", path), zap.Uint64("min-ts", m.MinTs),
			zap.Uint64("max-ts", m.MaxTs), zap.Int32("meta-version", int32(m.MetaVersion)))

		ts, ok := stream.UpdateShiftTS(m, rc.startTS, rc.restoreTS)
		shiftTS.Lock()
		if ok && (!shiftTS.exists || shiftTS.value > ts) {
			shiftTS.value = ts
			shiftTS.exists = true
		}
		shiftTS.Unlock()

		return nil
	})
	if err != nil {
		return err
	}
	if !shiftTS.exists {
		rc.shiftStartTS = rc.startTS
		return nil
	}
	rc.shiftStartTS = shiftTS.value
	return nil
}

func (rc *LogFileManager) streamingMeta(ctx context.Context) (MetaIter, error) {
	return rc.streamingMetaByTS(ctx, rc.restoreTS)
}

func (rc *LogFileManager) streamingMetaByTS(ctx context.Context, restoreTS uint64) (MetaIter, error) {
	it, err := rc.createMetaIterOver(ctx, rc.storage)
	if err != nil {
		return nil, err
	}
	filtered := iter.FilterOut(it, func(metadata *backuppb.Metadata) bool {
		return restoreTS < metadata.MinTs || metadata.MaxTs < rc.shiftStartTS
	})
	return filtered, nil
}

func (rc *LogFileManager) createMetaIterOver(ctx context.Context, s storage.ExternalStorage) (MetaIter, error) {
	opt := &storage.WalkOption{SubDir: stream.GetStreamBackupMetaPrefix()}
	names := []string{}
	err := s.WalkDir(ctx, opt, func(path string, size int64) error {
		if !strings.HasSuffix(path, ".meta") {
			return nil
		}
		names = append(names, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	namesIter := iter.FromSlice(names)
	readMeta := func(ctx context.Context, name string) (*backuppb.Metadata, error) {
		f, err := s.ReadFile(ctx, name)
		if err != nil {
			return nil, errors.Annotatef(err, "failed during reading file %s", name)
		}
		meta, err := rc.helper.ParseToMetadata(f)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to parse metadata of file %s", name)
		}
		return meta, nil
	}
	// TODO: maybe we need to be able to adjust the concurrency to download files,
	// which currently is the same as the chunk size
	reader := iter.Transform(namesIter, readMeta,
		iter.WithChunkSize(rc.metadataDownloadBatchSize), iter.WithConcurrency(rc.metadataDownloadBatchSize))
	return reader, nil
}

func (rc *LogFileManager) FilterDataFiles(ms MetaIter) LogIter {
	return iter.FlatMap(ms, func(m *backuppb.Metadata) LogIter {
		return iter.FlatMap(iter.Enumerate(iter.FromSlice(m.FileGroups)), func(gi iter.Indexed[*backuppb.DataFileGroup]) LogIter {
			return iter.Map(
				iter.FilterOut(iter.Enumerate(iter.FromSlice(gi.Item.DataFilesInfo)), func(di iter.Indexed[*backuppb.DataFileInfo]) bool {
					// Modify the data internally, a little hacky.
					if m.MetaVersion > backuppb.MetaVersion_V1 {
						di.Item.Path = gi.Item.Path
					}
					return di.Item.IsMeta || rc.ShouldFilterOut(di.Item)
				}),
				func(di iter.Indexed[*backuppb.DataFileInfo]) *LogDataFileInfo {
					return &LogDataFileInfo{
						DataFileInfo: di.Item,

						// Since there is a `datafileinfo`, the length of `m.FileGroups`
						// must be larger than 0. So we use the first group's name as
						// metadata's unique key.
						MetaDataGroupName:   m.FileGroups[0].Path,
						OffsetInMetaGroup:   gi.Index,
						OffsetInMergedGroup: di.Index,
					}
				},
			)
		})
	})
}

// ShouldFilterOut checks whether a file should be filtered out via the current client.
func (rc *LogFileManager) ShouldFilterOut(d *backuppb.DataFileInfo) bool {
	return d.MinTs > rc.restoreTS ||
		(d.Cf == stream.WriteCF && d.MaxTs < rc.startTS) ||
		(d.Cf == stream.DefaultCF && d.MaxTs < rc.shiftStartTS)
}

func (rc *LogFileManager) collectDDLFilesAndPrepareCache(
	ctx context.Context,
	files MetaGroupIter,
) ([]Log, error) {
	fs := iter.CollectAll(ctx, files)
	if fs.Err != nil {
		return nil, errors.Annotatef(fs.Err, "failed to collect from files")
	}

	dataFileInfos := make([]*backuppb.DataFileInfo, 0)
	for _, g := range fs.Item {
		rc.helper.InitCacheEntry(g.Path, len(g.FileMetas))
		dataFileInfos = append(dataFileInfos, g.FileMetas...)
	}

	return dataFileInfos, nil
}

// LoadDDLFilesAndCountDMLFiles loads all DDL files needs to be restored in the restoration.
// At the same time, if the `counter` isn't nil, counting the DML file needs to be restored into `counter`.
// This function returns all DDL files needing directly because we need sort all of them.
func (rc *LogFileManager) LoadDDLFilesAndCountDMLFiles(ctx context.Context, counter *int) ([]Log, error) {
	m, err := rc.streamingMeta(ctx)
	if err != nil {
		return nil, err
	}
	if counter != nil {
		m = iter.Tap(m, func(m Meta) {
			for _, fg := range m.FileGroups {
				for _, f := range fg.DataFilesInfo {
					if !f.IsMeta && !rc.ShouldFilterOut(f) {
						*counter += 1
					}
				}
			}
		})
	}
	mg := rc.FilterMetaFiles(m)

	return rc.collectDDLFilesAndPrepareCache(ctx, mg)
}

// LoadDMLFiles loads all DML files needs to be restored in the restoration.
// This function returns a stream, because there are usually many DML files need to be restored.
func (rc *LogFileManager) LoadDMLFiles(ctx context.Context) (LogIter, error) {
	m, err := rc.streamingMeta(ctx)
	if err != nil {
		return nil, err
	}

	mg := rc.FilterDataFiles(m)
	return mg, nil
}

func (rc *LogFileManager) FilterMetaFiles(ms MetaIter) MetaGroupIter {
	return iter.FlatMap(ms, func(m Meta) MetaGroupIter {
		return iter.Map(iter.FromSlice(m.FileGroups), func(g *backuppb.DataFileGroup) DDLMetaGroup {
			metas := iter.FilterOut(iter.FromSlice(g.DataFilesInfo), func(d Log) bool {
				// Modify the data internally, a little hacky.
				if m.MetaVersion > backuppb.MetaVersion_V1 {
					d.Path = g.Path
				}
				return !d.IsMeta || rc.ShouldFilterOut(d)
			})
			return DDLMetaGroup{
				Path: g.Path,
				// NOTE: the metas iterator is pure. No context or cancel needs.
				FileMetas: iter.CollectAll(context.Background(), metas).Item,
			}
		})
	})
}

// the kv entry with ts, the ts is decoded from entry.
type KvEntryWithTS struct {
	E  kv.Entry
	Ts uint64
}

func getKeyTS(key []byte) (uint64, error) {
	if len(key) < 8 {
		return 0, errors.Annotatef(berrors.ErrInvalidArgument,
			"the length of key is smaller than 8, key:%s", redact.Key(key))
	}

	_, ts, err := codec.DecodeUintDesc(key[len(key)-8:])
	return ts, err
}

// ReadAllEntries loads content of a log file, with filtering out no needed entries.
func (rc *LogFileManager) ReadAllEntries(
	ctx context.Context,
	file Log,
	filterTS uint64,
) ([]*KvEntryWithTS, []*KvEntryWithTS, error) {
	kvEntries := make([]*KvEntryWithTS, 0)
	nextKvEntries := make([]*KvEntryWithTS, 0)

	buff, err := rc.helper.ReadFile(ctx, file.Path, file.RangeOffset, file.RangeLength, file.CompressionType, rc.storage)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if checksum := sha256.Sum256(buff); !bytes.Equal(checksum[:], file.GetSha256()) {
		return nil, nil, berrors.ErrInvalidMetaFile.GenWithStackByArgs(fmt.Sprintf(
			"checksum mismatch expect %x, got %x", file.GetSha256(), checksum[:]))
	}

	iter := stream.NewEventIterator(buff)
	for iter.Valid() {
		iter.Next()
		if iter.GetError() != nil {
			return nil, nil, errors.Trace(iter.GetError())
		}

		txnEntry := kv.Entry{Key: iter.Key(), Value: iter.Value()}

		if !stream.MaybeDBOrDDLJobHistoryKey(txnEntry.Key) {
			// only restore mDB and mDDLHistory
			continue
		}

		ts, err := getKeyTS(txnEntry.Key)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		// The commitTs in write CF need be limited on [startTs, restoreTs].
		// We can restore more key-value in default CF.
		if ts > rc.restoreTS {
			continue
		} else if file.Cf == stream.WriteCF && ts < rc.startTS {
			continue
		} else if file.Cf == stream.DefaultCF && ts < rc.shiftStartTS {
			continue
		}

		if len(txnEntry.Value) == 0 {
			// we might record duplicated prewrite keys in some conor cases.
			// the first prewrite key has the value but the second don't.
			// so we can ignore the empty value key.
			// see details at https://github.com/pingcap/tiflow/issues/5468.
			log.Warn("txn entry is null", zap.Uint64("key-ts", ts), zap.ByteString("tnxKey", txnEntry.Key))
			continue
		}

		if ts < filterTS {
			kvEntries = append(kvEntries, &KvEntryWithTS{E: txnEntry, Ts: ts})
		} else {
			nextKvEntries = append(nextKvEntries, &KvEntryWithTS{E: txnEntry, Ts: ts})
		}
	}

	return kvEntries, nextKvEntries, nil
}
