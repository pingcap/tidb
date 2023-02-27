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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
)

// IngestIndexInfo records the information used to generate index drop/re-add SQL.
type IngestIndexInfo struct {
	SchemaName string
	TableName  string
	IndexName  string
	ColumnList string
	IsPrimary  bool
	IsUnique   bool
}

// IngestRecorder records the indexes information that use ingest mode to construct kvs.
// Currently log backup cannot backed up these ingest kvs. So need to re-construct them.
type IngestRecorder struct {
	// Table ID -> Index ID -> Index info
	items map[int64]map[int64]IngestIndexInfo
}

// Return an empty IngestRecorder
func New() *IngestRecorder {
	return &IngestRecorder{
		items: make(map[int64]map[int64]IngestIndexInfo),
	}
}

func notIngestJob(job *model.Job) bool {
	return job.ReorgMeta == nil ||
		job.ReorgMeta.ReorgTp != model.ReorgTypeLitMerge
}

func notAddIndexJob(job *model.Job) bool {
	return job.Type != model.ActionAddIndex &&
		job.Type != model.ActionAddPrimaryKey
}

func notSynced(job *model.Job) bool {
	return job.State != model.JobStateSynced
}

// AddJob firstly filters the ingest index add operation job, and records it into IngestRecorder.
func (i *IngestRecorder) AddJob(job *model.Job) error {
	if job == nil || notIngestJob(job) || notAddIndexJob(job) || notSynced(job) {
		return nil
	}

	// If the add-index operation affects no row, the index doesn't need to be repair.
	if job.RowCount == 0 {
		return nil
	}

	var indexID int64 = 0
	if err := job.DecodeArgs(&indexID); err != nil {
		return errors.Trace(err)
	}

	if job.BinlogInfo == nil || job.BinlogInfo.TableInfo == nil {
		return nil
	}
	var indexInfo *model.IndexInfo = nil
	for _, index := range job.BinlogInfo.TableInfo.Indices {
		if indexID == index.ID {
			indexInfo = index
			break
		}
	}

	if indexInfo != nil && len(indexInfo.Columns) > 0 {
		tableindexes, exists := i.items[job.TableID]
		if !exists {
			tableindexes = make(map[int64]IngestIndexInfo)
			i.items[job.TableID] = tableindexes
		}

		var columnListBuilder strings.Builder
		var isFirst bool = true
		for _, column := range indexInfo.Columns {
			if !isFirst {
				columnListBuilder.WriteByte(',')
			}
			isFirst = false
			columnListBuilder.WriteByte('`')
			columnListBuilder.WriteString(column.Name.O)
			columnListBuilder.WriteByte('`')
		}
		tableindexes[indexID] = IngestIndexInfo{
			SchemaName: job.SchemaName,
			TableName:  job.TableName,
			IndexName:  indexInfo.Name.O,
			ColumnList: columnListBuilder.String(),
			IsPrimary:  job.Type == model.ActionAddPrimaryKey,
			IsUnique:   indexInfo.Unique,
		}
	}
	return nil
}

// DelJob firstly filters the ingest index add operation job, and removes it from IngestRecorder.
func (i *IngestRecorder) DelJob(job *model.Job) error {
	if job == nil || notSynced(job) {
		return nil
	}

	switch job.Type {
	case model.ActionDropSchema:
		var tableIDs []int64
		if err := job.DecodeArgs(&tableIDs); err != nil {
			return errors.Trace(err)
		}
		for _, tableID := range tableIDs {
			delete(i.items, tableID)
		}
		return nil
	case model.ActionDropTable, model.ActionTruncateTable:
		// ActionTruncateTable creates new indexes with empty rows,
		// so no need to repair them.
		delete(i.items, job.TableID)
		return nil
	}

	tableindexes, exists := i.items[job.TableID]
	if !exists {
		return nil
	}
	var indexIDs []int64
	switch job.Type {
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		var indexName interface{}
		var ifExists bool
		var indexID int64
		if err := job.DecodeArgs(&indexName, &ifExists, &indexID); err != nil {
			return errors.Trace(err)
		}
		indexIDs = []int64{indexID}
	case model.ActionDropIndexes: // Deprecated, we use ActionMultiSchemaChange instead
		if err := job.DecodeArgs(&indexIDs); err != nil {
			return errors.Trace(err)
		}
	case model.ActionDropColumn:
		var colName model.CIStr
		var ifExists bool
		if err := job.DecodeArgs(&colName, &ifExists, &indexIDs); err != nil {
			return errors.Trace(err)
		}
	case model.ActionDropColumns:
		var colNames []model.CIStr
		var ifExists []bool
		if err := job.DecodeArgs(&colNames, &ifExists, &indexIDs); err != nil {
			return errors.Trace(err)
		}
	case model.ActionModifyColumn:
		if err := job.DecodeArgs(&indexIDs); err != nil {
			return errors.Trace(err)
		}
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
	}

	for _, indexID := range indexIDs {
		delete(tableindexes, indexID)
	}
	return nil
}

// Iterate iterates all the ingest index.
func (i *IngestRecorder) Iterate(f func(tableID int64, indexID int64, info IngestIndexInfo) error) error {
	for tableID, is := range i.items {
		for indexID, info := range is {
			if err := f(tableID, indexID, info); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}
