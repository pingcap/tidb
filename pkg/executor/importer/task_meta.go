// Copyright 2019 PingCAP, Inc.
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

package importer

import (
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
)

// TaskMeta is the task of IMPORT INTO.
// All the field should be serializable.
type TaskMeta struct {
	// IMPORT INTO job id, see mysql.tidb_import_jobs.
	JobID int64
	Plan  Plan
	Stmt  string

	// Summary is the summary of the whole import task.
	Summary Summary

	// eligible instances to run this task, we run on all instances if it's empty.
	// we only need this when run IMPORT INTO without distributed option now, i.e.
	// running on the instance that initiate the IMPORT INTO.
	EligibleInstances []*serverinfo.ServerInfo
	// the file chunks to import, when import from server file, we need to pass those
	// files to the framework scheduler which might run on another instance.
	// we use a map from engine ID to chunks since we need support split_file for CSV,
	// so need to split them into engines before passing to scheduler.
	ChunkMap map[int32][]Chunk
	// PreparedMetaExternalPath points to external chunk metadata prepared by
	// framework OnPrepare for nextgen global-sort path.
	PreparedMetaExternalPath string `json:"prepared_meta_external_path,omitempty"`
}
