// Copyright 2024 PingCAP, Inc.
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

package logclient

import (
	"context"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/stream"
	"go.uber.org/zap"
)

// BatchMetaKVProcessor defines how to process a batch of files
type BatchMetaKVProcessor interface {
	// ProcessBatch processes a batch of files and with a filterTS and return what's not processed for next iteration
	ProcessBatch(
		ctx context.Context,
		files []*backuppb.DataFileInfo,
		entries []*KvEntryWithTS,
		filterTS uint64,
		cf string,
	) ([]*KvEntryWithTS, error)
}

// RestoreMetaKVProcessor implements BatchMetaKVProcessor for restoring files in batches
type RestoreMetaKVProcessor struct {
	client         *LogClient
	schemasReplace *stream.SchemasReplace
	updateStats    func(kvCount uint64, size uint64)
	progressInc    func()
}

func NewRestoreMetaKVProcessor(client *LogClient, schemasReplace *stream.SchemasReplace,
	updateStats func(kvCount uint64, size uint64),
	progressInc func()) *RestoreMetaKVProcessor {
	return &RestoreMetaKVProcessor{
		client:         client,
		schemasReplace: schemasReplace,
		updateStats:    updateStats,
		progressInc:    progressInc,
	}
}

// RestoreAndRewriteMetaKVFiles tries to restore files about meta kv-event from stream-backup.
func (rp *RestoreMetaKVProcessor) RestoreAndRewriteMetaKVFiles(
	ctx context.Context,
	hasExplicitFilter bool,
	files []*backuppb.DataFileInfo,
) error {
	// starts gc row collector
	rp.client.RunGCRowsLoader(ctx)

	// separate the files by CF and sort each group by TS
	filesInDefaultCF, filesInWriteCF := SeparateAndSortFilesByCF(files)

	log.Info("start to restore meta files",
		zap.Int("total files", len(files)),
		zap.Int("default files", len(filesInDefaultCF)),
		zap.Int("write files", len(filesInWriteCF)))

	if err := LoadAndProcessMetaKVFilesInBatch(
		ctx,
		filesInDefaultCF,
		filesInWriteCF,
		rp,
	); err != nil {
		return errors.Trace(err)
	}

	if !hasExplicitFilter {
		// global schema version to trigger a full reload so every TiDB node in the cluster will get synced with
		// the latest schema update.
		log.Info("updating schema version to do full reload")
		if err := rp.client.UpdateSchemaVersionFullReload(ctx); err != nil {
			return errors.Trace(err)
		}
	} else {
		log.Info("skip doing full reload for filtered PiTR")
	}
	return nil
}

func (rp *RestoreMetaKVProcessor) ProcessBatch(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	entries []*KvEntryWithTS,
	filterTS uint64,
	cf string,
) ([]*KvEntryWithTS, error) {
	return rp.client.RestoreBatchMetaKVFiles(
		ctx, files, rp.schemasReplace, entries,
		filterTS, rp.updateStats, rp.progressInc, cf,
	)
}

// MetaKVInfoProcessor implements BatchMetaKVProcessor to iterate meta kv and collect information.
//
// 1. It collects table renaming information. The table rename operation will not change the table id, and the process
// will drop the original table and create a new one with the same table id, so in DDL history there will be two events
// that corresponds to the same table id.
//
// 2. It builds the id mapping from upstream to downstream. This logic was nested into table rewrite previously and now
// separated out to its own component.
type MetaKVInfoProcessor struct {
	client              *LogClient
	tableHistoryManager *stream.LogBackupTableHistoryManager
	tableMappingManager *stream.TableMappingManager
}

func NewMetaKVInfoProcessor(client *LogClient) *MetaKVInfoProcessor {
	return &MetaKVInfoProcessor{
		client:              client,
		tableHistoryManager: stream.NewTableHistoryManager(),
		tableMappingManager: stream.NewTableMappingManager(),
	}
}

func (mp *MetaKVInfoProcessor) ReadMetaKVFilesAndBuildInfo(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
) error {
	// separate the files by CF and sort each group by TS
	filesInDefaultCF, filesInWriteCF := SeparateAndSortFilesByCF(files)

	if err := LoadAndProcessMetaKVFilesInBatch(
		ctx,
		filesInDefaultCF,
		filesInWriteCF,
		mp,
	); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (mp *MetaKVInfoProcessor) ProcessBatch(
	ctx context.Context,
	files []*backuppb.DataFileInfo,
	entries []*KvEntryWithTS,
	filterTS uint64,
	cf string,
) ([]*KvEntryWithTS, error) {
	curSortedEntries, filteredEntries, err := mp.client.filterAndSortKvEntriesFromFiles(ctx, files, entries, filterTS)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// process entries to collect table IDs
	for _, entry := range curSortedEntries {
		// parse entry and do the table mapping, using tableHistoryManager as the collector
		if err = mp.tableMappingManager.ParseMetaKvAndUpdateIdMapping(&entry.E, cf, mp.tableHistoryManager); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return filteredEntries, nil
}

func (mp *MetaKVInfoProcessor) GetTableMappingManager() *stream.TableMappingManager {
	return mp.tableMappingManager
}

func (mp *MetaKVInfoProcessor) GetTableHistoryManager() *stream.LogBackupTableHistoryManager {
	return mp.tableHistoryManager
}
