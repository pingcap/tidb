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

package model

// MaterializedViewInfo is stored in TableInfo for a materialized view table.
type MaterializedViewInfo struct {
	// BaseTableID is the table ID of the base table.
	BaseTableID int64 `json:"base_table_id"`

	// MLogID is the table ID of the materialized view log used by this MV.
	MLogID int64 `json:"mlog_id"`

	// SQLContent is the SELECT statement in CREATE MATERIALIZED VIEW.
	SQLContent string `json:"sql_content"`
}

// MaterializedViewLogInfo is stored in TableInfo for a materialized view log table.
type MaterializedViewLogInfo struct {
	// BaseTableID is the table ID of the base table.
	BaseTableID int64 `json:"base_table_id"`

	// Columns is the base table column list recorded in the log (user-specified columns).
	Columns []string `json:"columns"`
}
