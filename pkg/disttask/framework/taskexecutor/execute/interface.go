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

package execute

import (
	"context"
	"reflect"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
)

// StepExecutor defines the executor for subtasks of a task step.
// the calling sequence is:
//
//	Init
//	for every subtask of this step:
//		if RunSubtask failed then break
//		else OnFinished
//	Cleanup
type StepExecutor interface {
	StepExecFrameworkInfo

	// Init is used to initialize the environment.
	// if failed, task executor will retry later.
	Init(context.Context) error
	// RunSubtask is used to run the subtask.
	RunSubtask(ctx context.Context, subtask *proto.Subtask) error

	// RealtimeSummary returns the realtime summary of the running subtask by this executor.
	RealtimeSummary() *SubtaskSummary

	// OnFinished is used to handle the subtask when it is finished.
	// The subtask meta can be updated in place.
	OnFinished(ctx context.Context, subtask *proto.Subtask) error
	// Cleanup is used to clean up the environment.
	Cleanup(context.Context) error
}

// SubtaskSummary contains the summary of a subtask.
type SubtaskSummary struct {
	RowCount int64
}

// StepExecFrameworkInfo is an interface that should be embedded into the
// implementation of StepExecutor. It's set by the framework automatically and
// the implementation can use it to access necessary information. The framework
// will init it before `StepExecutor.Init`, before that you cannot call methods
// in this interface.
type StepExecFrameworkInfo interface {
	// restricted is a private method to prevent other package mistakenly implements
	// StepExecFrameworkInfo. So when StepExecFrameworkInfo is composed with other
	// interfaces, the implementation of other interface must embed
	// StepExecFrameworkInfo.
	restricted()
	// GetResource returns the expected resource of this step executor.
	GetResource() *proto.StepResource
}

var stepExecFrameworkInfoName = reflect.TypeOf((*StepExecFrameworkInfo)(nil)).Elem().Name()

type frameworkInfo struct {
	resource *proto.StepResource
}

func (*frameworkInfo) restricted() {}

func (f *frameworkInfo) GetResource() *proto.StepResource {
	return f.resource
}

// SetFrameworkInfo sets the framework info for the StepExecutor.
func SetFrameworkInfo(exec StepExecutor, resource *proto.StepResource) {
	if exec == nil {
		return
	}
	toInject := &frameworkInfo{resource: resource}
	// use reflection to set the framework info
	e := reflect.ValueOf(exec)
	if e.Kind() == reflect.Ptr || e.Kind() == reflect.Interface {
		e = e.Elem()
	}
	info := e.FieldByName(stepExecFrameworkInfoName)
	// if `exec` embeds StepExecutor rather than StepExecFrameworkInfo, the field
	// will not be found. This is happened in mock generated code.
	if info.IsValid() && info.CanSet() {
		info.Set(reflect.ValueOf(toInject))
	}
}
