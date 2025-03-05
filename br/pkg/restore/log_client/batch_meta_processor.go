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
	"encoding/json"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
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
	isOnline bool,
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

	if !isOnline {
		// TODO, use same technique to update schema as online
		// global schema version to trigger a full reload so every TiDB node in the cluster will get synced with
		// the latest schema update.
		log.Info("updating schema version to do full reload")
		if err := rp.client.UpdateSchemaVersionFullReload(ctx); err != nil {
			return errors.Trace(err)
		}
	} else {
		log.Info("skip doing full reload for online restore")
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
		// parse entry and do the table mapping
		if err = mp.tableMappingManager.ParseMetaKvAndUpdateIdMapping(&entry.E, cf); err != nil {
			return nil, errors.Trace(err)
		}

		// Process DDL job history keys (for deleted tables)
		if utils.IsMetaDDLJobHistoryKey(entry.E.Key) && cf == consts.DefaultCF {
			if err := mp.processDeletedTablesFromDDLJob(entry.E.Value); err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}

		// collect rename/partition exchange history
		// get value from default cf and get the short value if possible from write cf
		value, err := stream.ExtractValue(&entry.E, cf)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// write cf doesn't have short value in it
		if value == nil {
			continue
		}

		if utils.IsMetaDBKey(entry.E.Key) {
			rawKey, err := stream.ParseTxnMetaKeyFrom(entry.E.Key)
			if err != nil {
				return nil, errors.Trace(err)
			}

			if meta.IsDBkey(rawKey.Field) {
				var dbInfo model.DBInfo
				if err := json.Unmarshal(value, &dbInfo); err != nil {
					return nil, errors.Trace(err)
				}
				// collect db id -> name mapping during log backup, it will contain information about newly created db
				if dbInfo.Name.O == "" {
					log.Warn("db info doesn't contain db name", zap.Int64("dbID", dbInfo.ID))
				}
				mp.tableHistoryManager.RecordDBIdToName(dbInfo.ID, dbInfo.Name.O)

			} else if !meta.IsDBkey(rawKey.Key) {
				// also see RewriteMetaKvEntry
				continue
			} else if meta.IsTableKey(rawKey.Field) {
				// collect table history indexed by table id, same id may have different table names in history
				var tableInfo model.TableInfo
				if err := json.Unmarshal(value, &tableInfo); err != nil {
					return nil, errors.Trace(err)
				}
				// cannot use dbib in the parsed table info cuz it might not set so default to 0
				dbID, err := meta.ParseDBKey(rawKey.Key)
				if err != nil {
					return nil, errors.Trace(err)
				}

				// add to table rename history
				if tableInfo.Name.O == "" {
					log.Warn("table info doesn't contain table name", zap.Int64("tableID", tableInfo.ID))
				}
				mp.tableHistoryManager.AddTableHistory(tableInfo.ID, tableInfo.Name.O, dbID)

				// track partitions if this is a partitioned table
				if tableInfo.Partition != nil {
					for _, def := range tableInfo.Partition.Definitions {
						mp.tableHistoryManager.AddPartitionHistory(def.ID, tableInfo.Name.String(), dbID, tableInfo.ID)
					}
				}
			}
		}
	}
	return filteredEntries, nil
}

// processDeletedTablesFromDDLJob processes a DDL job and marks tables as deleted when appropriate
func (mp *MetaKVInfoProcessor) processDeletedTablesFromDDLJob(value []byte) error {
	// Check if this is a DDL job that deletes tables
	var job model.Job
	if err := json.Unmarshal(value, &job); err != nil {
		return errors.Trace(err)
	}

	// Handle multi-schema jobs
	if job.MultiSchemaInfo != nil {
		for i, sub := range job.MultiSchemaInfo.SubJobs {
			proxyJob := sub.ToProxyJob(&job, i)
			if err := mp.processDeletedTablesFromSingleJob(&proxyJob); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	// Process single job
	return mp.processDeletedTablesFromSingleJob(&job)
}

// processDeletedTablesFromSingleJob processes a single DDL job and marks tables as deleted when appropriate
func (mp *MetaKVInfoProcessor) processDeletedTablesFromSingleJob(job *model.Job) error {
	// Process DDL jobs that delete tables
	switch job.Type {
	case model.ActionDropTable:
		// Mark the table as deleted
		mp.tableHistoryManager.MarkTableDeleted(job.TableID)

		// Check for partitions that might be dropped
		args, err := model.GetDropTableArgs(job)
		if err == nil && len(args.OldPartitionIDs) > 0 {
			mp.tableHistoryManager.MarkTablesDeleted(args.OldPartitionIDs)
		}
	case model.ActionDropSchema:
		// When dropping a schema, all tables in it are dropped
		args, err := model.GetDropSchemaArgs(job)
		if err == nil && len(args.AllDroppedTableIDs) > 0 {
			mp.tableHistoryManager.MarkTablesDeleted(args.AllDroppedTableIDs)
		}
	case model.ActionTruncateTable:
		// Truncate table creates a new table with a new ID, so the old one is effectively dropped
		args, err := model.GetTruncateTableArgs(job)
		if err == nil {
			mp.tableHistoryManager.MarkTableDeleted(job.TableID)
			if len(args.OldPartitionIDs) > 0 {
				mp.tableHistoryManager.MarkTablesDeleted(args.OldPartitionIDs)
			}
		}
	case model.ActionDropTablePartition:
		// When dropping a partition, mark the partition IDs as deleted
		args, err := model.GetTablePartitionArgs(job)
		if err == nil && len(args.OldPhysicalTblIDs) > 0 {
			mp.tableHistoryManager.MarkTablesDeleted(args.OldPhysicalTblIDs)
		}
	case model.ActionReorganizePartition, model.ActionRemovePartitioning, model.ActionAlterTablePartitioning:
		// These actions can delete partitions
		args, err := model.GetTablePartitionArgs(job)
		if err == nil && len(args.OldPhysicalTblIDs) > 0 {
			mp.tableHistoryManager.MarkTablesDeleted(args.OldPhysicalTblIDs)
		}
	}
	return nil
}

func (mp *MetaKVInfoProcessor) GetTableMappingManager() *stream.TableMappingManager {
	return mp.tableMappingManager
}

func (mp *MetaKVInfoProcessor) GetTableHistoryManager() *stream.LogBackupTableHistoryManager {
	return mp.tableHistoryManager
}
