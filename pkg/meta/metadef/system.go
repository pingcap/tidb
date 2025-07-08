// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadef

const (
	// ReservedSchemaIDUpperBound is the max value of any physical schema object ID.
	// due to history reasons, the first 2 bytes are planned to be used for multi
	// tenancy, but it's replaced by keyspace.
	ReservedSchemaIDUpperBound = 0x0000FFFFFFFFFFFF
	// ReservedSchemaIDLowerBound reserves 1000 IDs.
	// valid usable ID range for user schema objects is [1, ReservedSchemaIDLowerBound].
	//
	// (ReservedSchemaIDLowerBound, ReservedSchemaIDUpperBound] is reserved for
	// system schema objects.
	ReservedSchemaIDLowerBound = ReservedSchemaIDUpperBound - 1000
)

const (
	// TiDBDDLJobTableID is the table ID of `tidb_ddl_job`.
	TiDBDDLJobTableID = ReservedSchemaIDUpperBound - 1
	// TiDBDDLReorgTableID is the table ID of `tidb_ddl_reorg`.
	TiDBDDLReorgTableID = ReservedSchemaIDUpperBound - 2
	// TiDBDDLHistoryTableID is the table ID of `tidb_ddl_history`.
	TiDBDDLHistoryTableID = ReservedSchemaIDUpperBound - 3
	// TiDBMDLInfoTableID is the table ID of `tidb_mdl_info`.
	TiDBMDLInfoTableID = ReservedSchemaIDUpperBound - 4
	// TiDBBackgroundSubtaskTableID is the table ID of `tidb_background_subtask`.
	TiDBBackgroundSubtaskTableID = ReservedSchemaIDUpperBound - 5
	// TiDBBackgroundSubtaskHistoryTableID is the table ID of `tidb_background_subtask_history`.
	TiDBBackgroundSubtaskHistoryTableID = ReservedSchemaIDUpperBound - 6
	// TiDBDDLNotifierTableID is the table ID of `tidb_ddl_notifier`.
	TiDBDDLNotifierTableID = ReservedSchemaIDUpperBound - 7
)
