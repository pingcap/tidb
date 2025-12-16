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

package model

import (
	"github.com/pingcap/tidb/pkg/parser/model"
)

// TableGroupInfo provides meta data describing a TableGroup.
type TableGroupInfo struct {
	ID             int64       `json:"id"`
	Name           model.CIStr `json:"name"`
	Tables         []TableName `json:"tables"`
	State          SchemaState `json:"state"`
	AffinityGroups []AffinityGroupInfo
	GroupState     any
}

// TableName contains the lowercase name of db and table name.
type TableName struct {
	DB    string `json:"db"`
	Table string `json:"table"`
}

// AffinityGroupInfo represents information about an affinity group.
type AffinityGroupInfo struct {
	Name   string              `json:"name"`
	Tables []PhysicalTableInfo `json:"tables"`
}

// PhysicalTableInfo represents information about a physical table in an affinity group.
type PhysicalTableInfo struct {
	TableName
	PartitionName   string `json:"partition_name,omitempty"`
	PhysicalTableID int64  `json:"physical_table_id"`
}

// Valid returns true if the TableGroupInfo has valid ID and name.
func (tg *TableGroupInfo) Valid() bool {
	return tg != nil && tg.ID > 0 && tg.Name.O != ""
}

// GetAffinityGroupIDs returns all affinity group IDs in this table group.
func (tg *TableGroupInfo) GetAffinityGroupIDs() []string {
	ids := make([]string, 0, len(tg.AffinityGroups))
	for _, ag := range tg.AffinityGroups {
		ids = append(ids, ag.Name)
	}
	return ids
}
