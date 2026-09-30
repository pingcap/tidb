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

// Package conflictpath defines the object-store path layout of IMPORT INTO
// conflict rows.
package conflictpath

import (
	"fmt"

	"github.com/google/uuid"
)

const (
	// StorageDir is the top-level directory under the global-sort URI where
	// IMPORT INTO stores conflict rows.
	StorageDir = "conflicted-rows"
	// StoragePrefix is StorageDir with a trailing slash, matching the object
	// keys returned by Storage.WalkDir.
	StoragePrefix = StorageDir + "/"
)

// NewFileNamePrefix returns a new file name prefix used to store the conflict
// rows for the given task and subtask. All files under StoragePrefix must use a
// prefix returned by this function; CleanConflictRowFiles treats malformed paths
// in that namespace as invalid files and deletes them.
func NewFileNamePrefix(taskID, subtaskID int64) string {
	// Keep these files available for user inspection. They must not live directly
	// under '<task-id>/', where global-sort cleanup would delete them with temp data.
	return fmt.Sprintf("%s/%d/%d-%s", StorageDir, taskID, subtaskID, uuid.NewString())
}
