// Copyright 2015 PingCAP, Inc.
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

package taskexecutor

import (
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
)

// Callback is used for task executor.
type Callback interface {
	OnInitBefore(task *proto.Task)
	OnSubtaskRunBefore(subtask *proto.Subtask)
	OnSubtaskRunAfter(subtask *proto.Subtask)
	OnSubtaskFinishedBefore(subtask *proto.Subtask)
	OnSubtaskFinishedAfter(subtask *proto.Subtask)
	//		// OnChanged is called after a ddl statement is finished.
	//		OnChanged(err error) error
	//		// OnSchemaStateChanged is called after a schema state is changed.
	//		OnSchemaStateChanged(schemaVer int64)
	//		// OnJobRunBefore is called before running job.
	//		OnJobRunBefore(job *model.Job)
	//		// OnJobRunAfter is called after running job.
	//		OnJobRunAfter(job *model.Job)
	//		// OnJobUpdated is called after the running job is updated.
	//		OnJobUpdated(job *model.Job)
	//		// OnWatched is called after watching owner is completed.
	//		OnWatched(ctx context.Context)
	//		// OnGetJobBefore is called before getting job.
	//		OnGetJobBefore(jobType string)
	//		// OnGetJobAfter is called after getting job.
	//		OnGetJobAfter(jobType string, job *model.Job)
	//	}
}
