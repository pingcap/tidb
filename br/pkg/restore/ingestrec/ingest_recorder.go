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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/types"
)

// IngestIndexInfo records the information used to generate index drop/re-add SQL.
type IngestIndexInfo struct {
	SchemaName model.CIStr
	TableName  model.CIStr
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
	if job == nil || notIngestJob(job) || notAddIndexJob(job) || notSynced(job, isSubJob) {
		return nil
	}

	allIndexIDs := make([]int64, 1)
	// The job.Args is either `Multi-index: ([]int64, ...)`,
	// or `Single-index: (int64, ...)`.
	// TODO: it's better to use the public function to parse the
	// job's Args.
	if err := job.DecodeArgs(&allIndexIDs[0]); err != nil {
		if err = job.DecodeArgs(&allIndexIDs); err != nil {
			return errors.Trace(err)
		}
	}

	tableindexes, exists := i.items[job.TableID]
	if !exists {
		tableindexes = make(map[int64]*IngestIndexInfo)
		i.items[job.TableID] = tableindexes
	}

	// the current information of table/index might be modified by other ddl jobs,
	// therefore update the index information at last
	for _, indexID := range allIndexIDs {
		tableindexes[indexID] = &IngestIndexInfo{
			IsPrimary: job.Type == model.ActionAddPrimaryKey,
			Updated:   false,
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
