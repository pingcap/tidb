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
	"github.com/pingcap/tidb/distribute_framework/proto"
	"github.com/pingcap/tidb/kv"
)

type Splitter interface {
	// SplitTask splits the task into subtasks.
	SplitTask() (task []*proto.Subtask, err error)
}

type TaskDispatcherHandle interface {
	Progress(d *Dispatcher, gTask *proto.Task) (finished bool, subTasks []*proto.Subtask, err error)
	HandleError(d *Dispatcher, gTask *proto.Task, receive string) error
}

var (
	SplitterConstructors             = make(map[proto.TaskType]Splitter)
	TaskDispatcherHandleConstructors = make(map[proto.TaskType]TaskDispatcherHandle)
)

func RegisterSplitter(taskType proto.TaskType, splitter Splitter) {
	SplitterConstructors[taskType] = splitter
}

func RegisterTaskDispatcherHandle(taskType proto.TaskType, dispatcherHandle TaskDispatcherHandle) {
	TaskDispatcherHandleConstructors[taskType] = dispatcherHandle
}

func GetTaskDispatcherHandle(taskType proto.TaskType) TaskDispatcherHandle {
	return TaskDispatcherHandleConstructors[taskType]
}

type AddIndexSplitter struct {
	startKey kv.Key
	endKey   kv.Key
}

func (splitter *AddIndexSplitter) SplitTask() (task []*proto.Subtask, err error) {
	return nil, nil
}

func init() {
	// TODO: Implement AddIndexSplitter
	addIdxSplitter := &AddIndexSplitter{}
	RegisterSplitter(proto.TaskTypeCreateIndex, addIdxSplitter)
}
