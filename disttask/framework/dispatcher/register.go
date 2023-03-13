// Copyright 2023 PingCAP, Inc.
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

package dispatcher

import (
	"github.com/pingcap/tidb/disttask/framework/proto"
)

// TaskFlowHandle is used to control the process operations for each global task.
type TaskFlowHandle interface {
	ProcessNormalFlow(d Dispatch, gTask *proto.Task, fromPending bool) (finished bool, metas [][]byte, err error)
	ProcessErrFlow(d Dispatch, gTask *proto.Task, receive string) (meta []byte, err error)
}

var taskDispatcherHandleMap = make(map[string]TaskFlowHandle)

// RegisterTaskFlowHandle is used to register the global task handle.
func RegisterTaskFlowHandle(taskType string, dispatcherHandle TaskFlowHandle) {
	taskDispatcherHandleMap[taskType] = dispatcherHandle
}

// GetTaskFlowHandle is used to get the global task handle.
func GetTaskFlowHandle(taskType string) TaskFlowHandle {
	return taskDispatcherHandleMap[taskType]
}

var MockTiDBIDs []string
