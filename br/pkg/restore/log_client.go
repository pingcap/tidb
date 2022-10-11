// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
)

const (
	readMetaConcurrency = 128
	readMetaBatchSize   = 512
)

// MetaIter is the type of iterator of metadata files' content.
type MetaIter = iter.TryNextor[*backuppb.Metadata]

// LogIter is the type of iterator of each log files' meta information.
type LogIter = iter.TryNextor[*backuppb.DataFileInfo]

// MetaGroupIter is the iterator of flushes of metadata.
type MetaGroupIter = iter.TryNextor[DDLMetaGroup]

// logFileManager is the manager for log files, which supports read / filter from the
// log backup archive.
type logFileManager struct {
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
}

type LogFileManagerInit struct {
	StartTS   uint64
	RestoreTS uint64
	Storage   storage.ExternalStorage
}

func CreateLogFileManager(ctx context.Context, init LogFileManagerInit) (*logFileManager, error) {
	fm := &logFileManager{
		startTS:   init.StartTS,
		restoreTS: init.RestoreTS,
		storage:   init.Storage,
		helper:    stream.NewMetadataHelper(),
	}
	err := fm.loadShiftTS(ctx)
	if err != nil {
		return nil, err
	}
	return fm, nil
}

func (rc *logFileManager) ShiftTS() uint64 {
	return rc.shiftStartTS
}

func (rc *logFileManager) loadShiftTS(ctx context.Context) error {
	shiftTS := struct {
		sync.Mutex
		value  uint64
		exists bool
	}{}
	err := stream.FastUnmarshalMetaData(ctx, rc.storage, func(path string, raw []byte) error {
		m, err := rc.helper.ParseToMetadata(raw)
		if err != nil {
			return err
		}
		shiftTS.Lock()
		defer shiftTS.Unlock()

		ts, ok := UpdateShiftTS(m, rc.startTS, rc.restoreTS)
		if ok && (!shiftTS.exists || shiftTS.value > ts) {
			shiftTS.value = ts
			shiftTS.exists = true
		}
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

func (rc *logFileManager) StreamingMetaByTS(ctx context.Context, restoreTS uint64) (MetaIter, error) {
	it, err := rc.ReadStreamMetadata(ctx, rc.storage)
	if err != nil {
		return nil, err
	}
	filtered := iter.FilterOut(it, func(metadata *backuppb.Metadata) bool {
		return restoreTS < metadata.MinTs || metadata.MaxTs < rc.shiftStartTS
	})
	return filtered, nil
}

func (rc *logFileManager) ReadStreamMetadata(ctx context.Context, s storage.ExternalStorage) (MetaIter, error) {
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
			return nil, err
		}
		meta, err := rc.helper.ParseToMetadata(f)
		if err != nil {
			return nil, err
		}
		return meta, nil
	}
	reader := iter.Transform(namesIter, readMeta,
		iter.WithChunkSize(readMetaBatchSize), iter.WithConcurrency(readMetaConcurrency))
	return reader, nil
}

func (rc *logFileManager) FilterDataFiles(ms MetaIter) LogIter {
	return iter.FlatMap(ms, func(m *backuppb.Metadata) LogIter {
		return iter.FlatMap(iter.FromSlice(m.FileGroups), func(g *backuppb.DataFileGroup) LogIter {
			return iter.FilterOut(iter.FromSlice(g.DataFilesInfo), func(d *backuppb.DataFileInfo) bool {
				// Modify the data internally, a little hacky.
				if m.MetaVersion > backuppb.MetaVersion_V1 {
					d.Path = g.Path
				}
				return d.IsMeta || rc.ShouldFilterOut(d)
			})
		})
	})
}

// ShouldFilterOut checks whether a file should be filtered out via the current client.
func (rc *logFileManager) ShouldFilterOut(d *backuppb.DataFileInfo) bool {
	return d.MinTs > rc.restoreTS ||
		(d.Cf == stream.WriteCF && d.MaxTs < rc.startTS) ||
		(d.Cf == stream.DefaultCF && d.MaxTs < rc.shiftStartTS)
}

type DDLMetaGroup struct {
	Path      string
	FileMetas []*backuppb.DataFileInfo
}

func (rc *logFileManager) FilterMetaFiles(ms MetaIter) MetaGroupIter {
	return iter.FlatMap(ms, func(m *backuppb.Metadata) MetaGroupIter {
		return iter.Map(iter.FromSlice(m.FileGroups), func(g *backuppb.DataFileGroup) DDLMetaGroup {
			metas := iter.FilterOut(iter.FromSlice(g.DataFilesInfo), func(d *backuppb.DataFileInfo) bool {
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

// ReadStreamMetaByTS is used for streaming task. collect all meta file by TS.
func (rc *logFileManager) ReadStreamMetaByTS(ctx context.Context, restoreTS uint64) ([]*backuppb.Metadata, error) {
	metas, err := rc.StreamingMetaByTS(ctx, restoreTS)
	if err != nil {
		return nil, err
	}
	r := iter.CollectAll(ctx, metas)
	if r.Err != nil {
		return nil, errors.Trace(r.Err)
	}
	return r.Item, nil
}
