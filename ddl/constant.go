// Copyright 2022 PingCAP, Inc.
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
	"github.com/pingcap/tidb/meta"
)

const (
	// JobTable stores the information of DDL jobs.
	JobTable = "tidb_ddl_job"
	// ReorgTable stores the information of DDL reorganization.
	ReorgTable = "tidb_ddl_reorg"
	// HistoryTable stores the history DDL jobs.
	HistoryTable = "tidb_ddl_history"

	// JobTableID is the table ID of `tidb_ddl_job`.
	JobTableID = meta.MaxInt48 - 1
	// ReorgTableID is the table ID of `tidb_ddl_reorg`.
	ReorgTableID = meta.MaxInt48 - 2
	// HistoryTableID is the table ID of `tidb_ddl_history`.
	HistoryTableID = meta.MaxInt48 - 3
)
