// Copyright 2023-present PingCAP, Inc.
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
}

// IngestRecorder records the indices information that use ingest mode to construct kvs.
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

func notDropIndexJob(job *model.Job) bool {
	return job.Type != model.ActionDropIndex &&
		job.Type != model.ActionDropIndexes
}

func notSynced(job *model.Job) bool {
	return job.State != model.JobStateSynced
}

func parseIndexID(job *model.Job) (int64, error) {
	switch job.Type {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		var indexID int64
		if err := job.DecodeArgs(&indexID); err != nil {
			return 0, errors.Trace(err)
		}
		return indexID, nil
	}

	return 0, nil
}

// AddJob firstly filters the ingest index add operation job, and records it into IngestRecorder.
func (i *IngestRecorder) AddJob(job *model.Job) error {
	if notIngestJob(job) && notAddIndexJob(job) && notSynced(job) {
		return nil
	}

	// If the add-index operation affects no row, the index doesn't need to be repair.
	if job.RowCount == 0 {
		return nil
	}

	indexID, err := parseIndexID(job)
	if err != nil {
		return errors.Trace(err)
	}

	var indexInfo *model.IndexInfo = nil
	for _, index := range job.BinlogInfo.TableInfo.Indices {
		if indexID == index.ID {
			indexInfo = index
			break
		}
	}

	if indexInfo != nil && len(indexInfo.Columns) > 0 {
		tableIndices, exists := i.items[job.TableID]
		if !exists {
			tableIndices = make(map[int64]IngestIndexInfo)
			i.items[job.TableID] = tableIndices
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
		tableIndices[indexID] = IngestIndexInfo{
			SchemaName: job.SchemaName,
			TableName:  job.TableName,
			IndexName:  indexInfo.Name.O,
			ColumnList: columnListBuilder.String(),
			IsPrimary:  job.Type == model.ActionAddPrimaryKey,
		}
	}
	return nil
}

// DelJob firstly filters the ingest index add operation job, and removes it from IngestRecorder.
func (i *IngestRecorder) DelJob(job *model.Job) error {
	if notIngestJob(job) && notDropIndexJob(job) && notSynced(job) {
		return nil
	}

	tableIndices, exists := i.items[job.TableID]
	if !exists {
		return nil
	}

	indexID, err := parseIndexID(job)
	if err != nil {
		return errors.Trace(err)
	}

	delete(tableIndices, indexID)
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
