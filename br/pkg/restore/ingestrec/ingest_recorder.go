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

package ingestrec

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

// IngestIndexInfo records the information used to generate index drop/re-add SQL.
type IngestIndexInfo struct {
	SchemaName ast.CIStr
	TableName  ast.CIStr
	ColumnList string
	ColumnArgs []any
	IsPrimary  bool
	IndexInfo  *model.IndexInfo
	Updated    bool
}

// IngestRecorder records the indexes information that use ingest mode to construct kvs.
// Currently log backup cannot backed up these ingest kvs. So need to re-construct them.
type IngestRecorder struct {
	// Table ID -> Index ID -> Index info
	items map[int64]map[int64]*IngestIndexInfo
	// need to drop the constraints at first
	foreignKeyRecordManager *ForeignKeyRecordManager
}

// Return an empty IngestRecorder
func New() *IngestRecorder {
	return &IngestRecorder{
		items: make(map[int64]map[int64]*IngestIndexInfo),
	}
}

func notIngestJob(job *model.Job) bool {
	return job.ReorgMeta == nil ||
		job.ReorgMeta.ReorgTp != model.ReorgTypeIngest
}

func notReorgTypeJob(job *model.Job) bool {
	/* support new index using accelerated indexing in future:
	 * // 1. skip if the new index didn't generate new kvs
	 * // 2. shall the ReorgTp of ModifyColumnJob be ReorgTypeLitMerge if use accelerated indexing?
	 * if job.RowCount > 0 && notIngestJob(job) {
	 *   // ASSERT: select new indexes, which have the highest IDs in this job's BinlogInfo
	 *   newIndexesInfo := getIndexesWithTheHighestIDs(len(indexIDs))
	 *   for _, newIndexInfo := range newIndexesInfo {
	 *     tableindexes[newIndexInfo.ID] = ...
	 *   }
	 * }
	 */
	return job.Type != model.ActionAddIndex &&
		job.Type != model.ActionAddPrimaryKey && job.Type != model.ActionModifyColumn
}

// the final state of the sub jobs is done instead of synced.
// +-----+-------------------------------------+--------------+-----+--------+
// | ... | JOB_TYPE                            | SCHEMA_STATE | ... | STATE  |
// +-----+-------------------------------------+--------------+-----+--------+
// | ... | add index /* ingest */              | public       | ... | synced |
// +-----+-------------------------------------+--------------+-----+--------+
// | ... | alter table multi-schema change     | none         | ... | synced |
// +-----+-------------------------------------+--------------+-----+--------+
// | ... | add index /* subjob */ /* ingest */ | public       | ... | done   |
// +-----+-------------------------------------+--------------+-----+--------+
func notSynced(job *model.Job, isSubJob bool) bool {
	return (job.State != model.JobStateSynced) && !(isSubJob && job.State == model.JobStateDone)
}

// TryAddJob firstly filters the ingest index add operation job, and records it into IngestRecorder.
func (i *IngestRecorder) TryAddJob(job *model.Job, isSubJob bool) error {
	if job == nil || notIngestJob(job) || notReorgTypeJob(job) || notSynced(job, isSubJob) {
		return nil
	}

	switch job.Type {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		args, err := model.GetFinishedModifyIndexArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		tableindexes, exists := i.items[job.TableID]
		if !exists {
			tableindexes = make(map[int64]*IngestIndexInfo)
			i.items[job.TableID] = tableindexes
		}
		// the current information of table/index might be modified by other ddl jobs,
		// therefore update the index information at last
		for _, a := range args.IndexArgs {
			tableindexes[a.IndexID] = &IngestIndexInfo{
				IsPrimary: job.Type == model.ActionAddPrimaryKey,
				Updated:   false,
			}
		}
	case model.ActionModifyColumn:
		args, err := model.GetFinishedModifyColumnArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		tableindexes, exists := i.items[job.TableID]
		if !exists {
			tableindexes = make(map[int64]*IngestIndexInfo)
			i.items[job.TableID] = tableindexes
		}
		for _, idxID := range args.NewIndexIDs {
			tableindexes[idxID] = &IngestIndexInfo{
				IsPrimary: false, // Unsupported modify column: this column has primary key flag
				Updated:   false,
			}
		}
	}

	return nil
}

// RewriteTableID rewrites the table id of the items in the IngestRecorder
func (i *IngestRecorder) RewriteTableID(rewriteFunc func(tableID int64) (int64, bool, error)) error {
	newItems := make(map[int64]map[int64]*IngestIndexInfo)
	for tableID, item := range i.items {
		newTableID, skip, err := rewriteFunc(tableID)
		if err != nil {
			return errors.Annotatef(err, "failed to rewrite table id: %d", tableID)
		}
		if skip {
			continue
		}
		newItems[newTableID] = item
	}
	i.items = newItems
	return nil
}

// UpdateIndexInfo uses the newest schemas to update the ingest index's information
func (i *IngestRecorder) UpdateIndexInfo(ctx context.Context, infoSchema infoschema.InfoSchema) error {
	log.Info("start to update index information for ingest index")
	start := time.Now()
	defer func() {
		log.Info("finish updating index information for ingest index", zap.Duration("takes", time.Since(start)))
	}()
	finalForeignKeyManager := NewForeignKeyRecordManager()
	for tableID, tableIndexes := range i.items {
		tblInfo, tblexists := infoSchema.TableInfoByID(tableID)
		if !tblexists || tblInfo == nil {
			log.Info("skip repair ingest index, table is dropped", zap.Int64("table id", tableID))
			continue
		}
		// TODO: here only need an interface function like `SchemaNameByID`
		dbInfo, dbexists := infoSchema.SchemaByID(tblInfo.DBID)
		if !dbexists || dbInfo == nil {
			return errors.Errorf("failed to repair ingest index because table exists but cannot find database."+
				"[table-id:%d][db-id:%d]", tableID, tblInfo.DBID)
		}
		tableForeignKeyManager, err := NewForeignKeyRecordManagerForTables(ctx, infoSchema, dbInfo.Name, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}
		for _, indexInfo := range tblInfo.Indices {
			index, idxexists := tableIndexes[indexInfo.ID]
			if !idxexists {
				tableForeignKeyManager.RemoveForeignKeys(tblInfo, indexInfo)
				continue
			}
			var columnListBuilder strings.Builder
			var columnListArgs []any = make([]any, 0, len(indexInfo.Columns))
			var isFirst bool = true
			for _, column := range indexInfo.Columns {
				if !isFirst {
					columnListBuilder.WriteByte(',')
				}
				isFirst = false

				// expression / column
				col := tblInfo.Columns[column.Offset]
				if col.Hidden {
					// (expression)
					// the generated expression string can be directly add into sql
					columnListBuilder.WriteByte('(')
					columnListBuilder.WriteString(col.GeneratedExprString)
					columnListBuilder.WriteByte(')')
				} else {
					// columnName
					columnListBuilder.WriteString("%n")
					columnListArgs = append(columnListArgs, column.Name.O)
					if column.Length != types.UnspecifiedLength {
						columnListBuilder.WriteString(fmt.Sprintf("(%d)", column.Length))
					}
				}
			}
			index.ColumnList = columnListBuilder.String()
			index.ColumnArgs = columnListArgs
			index.IndexInfo = indexInfo
			index.SchemaName = dbInfo.Name
			index.TableName = tblInfo.Name
			index.Updated = true
		}
		finalForeignKeyManager.Merge(tableForeignKeyManager)
	}
	i.foreignKeyRecordManager = finalForeignKeyManager
	return nil
}

// Iterate iterates all the ingest index.
func (i *IngestRecorder) Iterate(f func(tableID int64, indexID int64, info *IngestIndexInfo) error) error {
	for tableID, is := range i.items {
		for indexID, info := range is {
			if !info.Updated {
				continue
			}
			if err := f(tableID, indexID, info); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// IterateForeignKeys iterates all the foreign keys need to be dropped before repair indexes
func (i *IngestRecorder) IterateForeignKeys(f func(*ForeignKeyRecord) error) error {
	for _, fkRecord := range i.foreignKeyRecordManager.fkRecordMap {
		if err := f(fkRecord); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ExportItems returns a snapshot of ingest items keyed by table ID and index ID.
func (i *IngestRecorder) ExportItems() map[int64]map[int64]bool {
	items := make(map[int64]map[int64]bool, len(i.items))
	for tableID, indexes := range i.items {
		if len(indexes) == 0 {
			continue
		}
		tableItems := make(map[int64]bool, len(indexes))
		for indexID, info := range indexes {
			if info == nil {
				continue
			}
			tableItems[indexID] = info.IsPrimary
		}
		if len(tableItems) > 0 {
			items[tableID] = tableItems
		}
	}
	return items
}

// MergeItems merges the provided ingest items into the recorder.
func (i *IngestRecorder) MergeItems(items map[int64]map[int64]bool) {
	if len(items) == 0 {
		return
	}
	if i.items == nil {
		i.items = make(map[int64]map[int64]*IngestIndexInfo)
	}
	for tableID, indexMap := range items {
		if len(indexMap) == 0 {
			continue
		}
		tableIndexes, exists := i.items[tableID]
		if !exists {
			tableIndexes = make(map[int64]*IngestIndexInfo, len(indexMap))
			i.items[tableID] = tableIndexes
		}
		for indexID, isPrimary := range indexMap {
			info, exists := tableIndexes[indexID]
			if !exists {
				tableIndexes[indexID] = &IngestIndexInfo{
					IsPrimary: isPrimary,
					Updated:   false,
				}
				continue
			}
			if isPrimary && !info.IsPrimary {
				info.IsPrimary = true
			}
		}
	}
}
