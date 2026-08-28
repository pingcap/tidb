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

package conflictrows

import (
	"fmt"

	"github.com/google/uuid"
)

const (
	storageDir    = "conflicted-rows"
	storagePrefix = storageDir + "/"
)

// NewFileNamePrefix returns a new file name prefix used to store the conflict
// rows for the given task and subtask. All files under storagePrefix must use a
// prefix returned by this function; CleanConflictRowFiles treats malformed paths
// in that namespace as invalid files and deletes them.
func NewFileNamePrefix(taskID, subtaskID int64) string {
	// Keep these files available for user inspection. They must not live directly
	// under '<task-id>/', where global-sort cleanup would delete them with temp data.
	return fmt.Sprintf("%s/%d/%d-%s", storageDir, taskID, subtaskID, uuid.NewString())
}
