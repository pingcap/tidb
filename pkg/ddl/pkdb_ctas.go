// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// CTASLoadSubtaskMeta is the disttask payload for the CTAS data-loading phase.
type CTASLoadSubtaskMeta struct {
	JobID               int64                          `json:"job_id"`
	SchemaID            int64                          `json:"schema_id"`
	TableID             int64                          `json:"table_id"`
	CTASSnapshotTS      uint64                         `json:"ctas_snapshot_ts"`
	CTASSql             string                         `json:"ctas_sql"`
	CTASOnDuplicate     ast.OnDuplicateKeyHandlingType `json:"ctas_on_duplicate"`
	CTASSubmitterExecID string                         `json:"ctas_submitter_exec_id"`
	CTASSubmitterConnID uint64                         `json:"ctas_submitter_conn_id"`
}

func getRollbackCTASDeleteRangeTableIDs(job *model.Job) ([]int64, error) {
	if job == nil || job.Type != model.ActionCreateTable || !job.IsRollbackDone() {
		return nil, nil
	}

	args, err := model.GetCreateTableArgs(job)
	if err != nil {
		return nil, err
	}
	if args == nil || args.CTASSnapshotTS == 0 || args.TableInfo == nil {
		return nil, nil
	}

	tableIDs := make([]int64, 0, 1)
	if pi := args.TableInfo.GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			tableIDs = append(tableIDs, def.ID)
		}
	}

	tableID := job.TableID
	if tableID == 0 {
		tableID = args.TableInfo.ID
	}
	if tableID != 0 {
		tableIDs = append(tableIDs, tableID)
	}
	return tableIDs, nil
}
