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
	"time"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"go.uber.org/atomic"
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
	// task executor will retry if the returned error is retryable, see
	// IsRetryableError in TaskExecutor.Extension, else framework will mark random
	// subtask as failed, to trigger task failure.
	Init(context.Context) error
	// RunSubtask is used to run the subtask.
	// The subtask meta can be updated in place, if no error returned, the subtask
	// meta will be updated in the task table.
	RunSubtask(ctx context.Context, subtask *proto.Subtask) error

	// RealtimeSummary returns the realtime summary of the running subtask by this executor.
	RealtimeSummary() *SubtaskSummary

	// Cleanup is used to clean up the environment for this step.
	// the returned error will not affect task/subtask state, it's only logged,
	// so don't put code that's prone to error in it.
	Cleanup(context.Context) error
	// TaskMetaModified is called when the task meta is modified, if any error
	// happen, framework might recreate the step executor, so don't put code
	// that's prone to error in it.
	TaskMetaModified(ctx context.Context, newMeta []byte) error
	// ResourceModified is called when the resource allowed to be used is modified
	// and there is a subtask running. Note: if no subtask running, framework will
	// call SetResource directly.
	// application must make sure the resource in use conforms to the new resource
	// before returning. When reducing resources, the framework depends on this
	// to make sure current instance won't OOM.
	ResourceModified(ctx context.Context, newResource *proto.StepResource) error
}

// SubtaskSummary contains the summary of a subtask
// These fields represent the number of data/rows inputed to the subtask.
type SubtaskSummary struct {
	RowCnt     atomic.Int64 `json:"row_count,omitempty"`
	Bytes      atomic.Int64 `json:"bytes,omitempty"`
	UpdateTime time.Time    `json:"update_time,omitempty"`
}

// Reset resets the summary to the given row count and bytes.
func (s *SubtaskSummary) Reset() {
	s.RowCnt.Store(0)
	s.Bytes.Store(0)
}

// Collector is the interface for collecting subtask metrics.
type Collector interface {
	// Add is used collects metrics.
	// `bytes` is the number of bytes processed, and `rows` is the number of rows processed.
	// The meaning of `bytes` may vary by scenario, for example:
	//   - During encoding, it represents the number of bytes read from the source data file.
	//   - During merge sort, it represents the number of bytes merged.
	Add(bytes, rows int64)
}

// TestCollector is an implementation used for test.
type TestCollector struct {
	Bytes atomic.Int64
	Rows  atomic.Int64
}

// Add implements Collector.Add
func (c *TestCollector) Add(bytes, rows int64) {
	c.Bytes.Add(bytes)
	c.Rows.Add(rows)
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
	// GetStep returns the step.
	GetStep() proto.Step
	// GetResource returns the expected resource of this step executor.
	GetResource() *proto.StepResource
	// SetResource sets the resource of this step executor.
	SetResource(resource *proto.StepResource)
	// GetCheckpointUpdateFunc returns the checkpoint update function
	GetCheckpointUpdateFunc() func(context.Context, int64, any) error
	// GetCheckpointunc returns the checkpoint get function
	GetCheckpointFunc() func(context.Context, int64) (string, error)
}

var stepExecFrameworkInfoName = reflect.TypeFor[StepExecFrameworkInfo]().Name()

type frameworkInfo struct {
	step     proto.Step
	resource atomic.Pointer[proto.StepResource]

	// updateCheckpointFunc is used to update checkpoint for the current subtask
	updateCheckpointFunc func(context.Context, int64, any) error
	// getCheckpointFunc is used to get checkpoint for the current subtask
	getCheckpointFunc func(context.Context, int64) (string, error)
}

var _ StepExecFrameworkInfo = (*frameworkInfo)(nil)

func (*frameworkInfo) restricted() {}

func (f *frameworkInfo) GetStep() proto.Step {
	return f.step
}

func (f *frameworkInfo) GetResource() *proto.StepResource {
	return f.resource.Load()
}

func (f *frameworkInfo) SetResource(resource *proto.StepResource) {
	f.resource.Store(resource)
}

// GetCheckpointUpdateFunc returns the checkpoint update function
func (f *frameworkInfo) GetCheckpointUpdateFunc() func(context.Context, int64, any) error {
	return f.updateCheckpointFunc
}

// GetCheckpointFunc returns the checkpoint get function
func (f *frameworkInfo) GetCheckpointFunc() func(context.Context, int64) (string, error) {
	return f.getCheckpointFunc
}

// SetFrameworkInfo sets the framework info for the StepExecutor.
func SetFrameworkInfo(
	exec StepExecutor,
	step proto.Step,
	resource *proto.StepResource,
	updateCheckpointFunc func(context.Context, int64, any) error,
	getCheckpointFunc func(context.Context, int64) (string, error),
) {
	if exec == nil {
		return
	}
	toInject := &frameworkInfo{
		step:                 step,
		updateCheckpointFunc: updateCheckpointFunc,
		getCheckpointFunc:    getCheckpointFunc,
	}
	toInject.resource.Store(resource)
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
