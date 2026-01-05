// Copyright 2025 PingCAP, Inc.
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

package globalindexcleanup

import (
	"encoding/json"

	"github.com/pingcap/tidb/pkg/meta/model"
)

// CleanupTaskMeta is the task meta for global index cleanup.
type CleanupTaskMeta struct {
	Job model.Job `json:"job"`
	// TableInfo is the table info with dropping partitions visible.
	// For DROP PARTITION: use getTableInfoWithDroppingPartitions result.
	// For TRUNCATE PARTITION: use tblInfo with DroppingDefinitions + DDLAction=ActionTruncateTablePartition.
	TableInfo *model.TableInfo `json:"table_info"`
	// OldPartitionIDs are the partition IDs to clean up global index entries for.
	OldPartitionIDs []int64 `json:"old_partition_ids"`
	// GlobalIndexIDs are the global index IDs to clean up.
	GlobalIndexIDs []int64 `json:"global_index_ids"`
	// Version is used for compatibility.
	Version int `json:"version"`
}

// CleanupSubtaskMeta is the subtask meta for global index cleanup.
type CleanupSubtaskMeta struct {
	// PhysicalTableID is the old partition physical ID to scan.
	PhysicalTableID int64 `json:"physical_table_id"`
	// RowStart is the start key of the record range to scan.
	RowStart []byte `json:"row_start"`
	// RowEnd is the end key of the record range to scan.
	RowEnd []byte `json:"row_end"`
}

// Marshal marshals CleanupTaskMeta to JSON.
func (m *CleanupTaskMeta) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal unmarshals CleanupTaskMeta from JSON.
func (m *CleanupTaskMeta) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

// Marshal marshals CleanupSubtaskMeta to JSON.
func (m *CleanupSubtaskMeta) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal unmarshals CleanupSubtaskMeta from JSON.
func (m *CleanupSubtaskMeta) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}
