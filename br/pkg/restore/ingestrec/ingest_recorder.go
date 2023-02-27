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
	ColumnList string
	IsPrimary  bool
	IndexInfo  *model.IndexInfo
	Updated    bool
}

// IngestRecorder records the indexes information that use ingest mode to construct kvs.
// Currently log backup cannot backed up these ingest kvs. So need to re-construct them.
type IngestRecorder struct {
	// Table ID -> Index ID -> Index info
	items map[int64]map[int64]*IngestIndexInfo
}

// Return an empty IngestRecorder
func New() *IngestRecorder {
	return &IngestRecorder{
		items: make(map[int64]map[int64]*IngestIndexInfo),
	}
}

func notIngestJob(job *model.Job) bool {
	return job.ReorgMeta == nil ||
		job.ReorgMeta.ReorgTp != model.ReorgTypeLitMerge
}

func notAddIndexJob(job *model.Job) bool {
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
			tableindexes = make(map[int64]*IngestIndexInfo)
			i.items[job.TableID] = tableindexes
		}

		tableindexes[indexID] = &IngestIndexInfo{
			IsPrimary: job.Type == model.ActionAddPrimaryKey,
			Updated:   false,
		}
	}
	return nil
}

func (i *IngestRecorder) UpdateIndexInfo(dbInfos []*model.DBInfo) {
	for _, dbInfo := range dbInfos {
		for _, tblInfo := range dbInfo.Tables {
			tableindexes, tblexists := i.items[tblInfo.ID]
			if !tblexists {
				continue
			}
			for _, indexInfo := range tblInfo.Indices {
				index, idxexists := tableindexes[indexInfo.ID]
				if !idxexists {
					continue
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
				index.ColumnList = columnListBuilder.String()
				index.IndexInfo = indexInfo
				index.SchemaName = dbInfo.Name.O
				index.TableName = tblInfo.Name.O
				index.Updated = true
			}
		}
	}
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
