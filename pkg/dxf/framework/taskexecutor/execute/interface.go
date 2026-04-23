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

	"github.com/pingcap/tidb/pkg/dxf/framework/metering"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/objstore/recording"
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

	// ResetSummary resets the summary of the running subtask by this executor.
	ResetSummary()

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

const (
	// UpdateSubtaskSummaryInterval is the interval for updating the subtask summary to
	// subtask table.
	UpdateSubtaskSummaryInterval = 3 * time.Second

	// maxProgressInSummary is the number of progress stored in subtask summary
	maxProgressInSummary = 5

	// SubtaskSpeedUpdateInterval is the interval for updating the subtasks' speed.
	SubtaskSpeedUpdateInterval = UpdateSubtaskSummaryInterval * maxProgressInSummary
)

// Progress represents the progress of a subtask at a specific time.
type Progress struct {
	// For now, RowCnt is not used, but as it's collected by the collector,
	// we still keep it here for future possible usage.
	RowCnt int64 `json:"row_count,omitempty"`
	Bytes  int64 `json:"bytes,omitempty"`

	// UpdateTime is the time when this progress is stored.
	UpdateTime time.Time `json:"update_time,omitempty"`
}

// SubtaskSummary contains the summary of a subtask.
// It tracks the runtime summary of the subtask.
type SubtaskSummary struct {
	// RowCnt and Bytes are updated by the collector.
	RowCnt atomic.Int64 `json:"row_count,omitempty"`
	// Bytes is the number of bytes to process.
	Bytes atomic.Int64 `json:"bytes,omitempty"`
	// ReadBytes is the number of bytes that read from the source.
	ReadBytes atomic.Int64 `json:"read_bytes,omitempty"`
	// GetReqCnt is the number of get requests to the external storage.
	// Note: Import-into also do GET on the source data bucket, but that's not
	// recorded.
	GetReqCnt atomic.Uint64 `json:"get_request_count,omitempty"`
	// PutReqCnt is the number of put requests to the external storage.
	PutReqCnt atomic.Uint64 `json:"put_request_count,omitempty"`

	// Progresses are the history of data processed, which is used to get a
	// smoother speed for each subtask.
	// It's updated each time we store the latest summary into subtask table.
	Progresses []Progress `json:"progresses,omitempty"`
}

// MergeObjStoreRequests merges the recording requests into the summary.
func (s *SubtaskSummary) MergeObjStoreRequests(reqs *recording.Requests) {
	s.GetReqCnt.Add(reqs.Get.Load())
	s.PutReqCnt.Add(reqs.Put.Load())
}

// Update stores the latest progress of the subtask.
func (s *SubtaskSummary) Update() {
	s.Progresses = append(s.Progresses, Progress{
		RowCnt:     s.RowCnt.Load(),
		Bytes:      s.Bytes.Load(),
		UpdateTime: time.Now(),
	})

	if len(s.Progresses) > maxProgressInSummary {
		s.Progresses = s.Progresses[len(s.Progresses)-maxProgressInSummary:]
	}
}

// GetSpeedInTimeRange returns the speed in the specified time range.
func (s *SubtaskSummary) GetSpeedInTimeRange(endTime time.Time, duration time.Duration) int64 {
	if len(s.Progresses) < 2 {
		return 0
	}

	startTime := endTime.Add(-duration)
	if endTime.Before(s.Progresses[0].UpdateTime) || startTime.After(s.Progresses[len(s.Progresses)-1].UpdateTime) {
		return 0
	}

	// The number of point is small, so we can afford to iterate through all points.
	var totalBytes float64
	for i := range len(s.Progresses) - 1 {
		rangeStart := s.Progresses[i].UpdateTime
		rangeEnd := s.Progresses[i+1].UpdateTime
		rangeBytes := float64(s.Progresses[i+1].Bytes - s.Progresses[i].Bytes)
		if endTime.Before(rangeStart) || startTime.After(rangeEnd) {
			continue
		} else if startTime.Before(rangeStart) && endTime.After(rangeEnd) {
			totalBytes += rangeBytes
			continue
		}

		iStart := rangeStart
		if startTime.After(rangeStart) {
			iStart = startTime
		}

		iEnd := rangeEnd
		if endTime.Before(rangeEnd) {
			iEnd = endTime
		}

		totalBytes += rangeBytes * float64(iEnd.Sub(iStart)) / float64(rangeEnd.Sub(rangeStart))
	}

	return int64(totalBytes / duration.Seconds())
}

// UpdateTime returns the last update time of the summary.
func (s *SubtaskSummary) UpdateTime() time.Time {
	if len(s.Progresses) == 0 {
		return time.Time{}
	}
	return s.Progresses[len(s.Progresses)-1].UpdateTime
}

// Reset resets the summary to zero values and clears history data.
func (s *SubtaskSummary) Reset() {
	s.RowCnt.Store(0)
	s.Bytes.Store(0)
	s.ReadBytes.Store(0)
	s.PutReqCnt.Store(0)
	s.GetReqCnt.Store(0)
	s.Progresses = s.Progresses[:0]
	s.Update()
}

// Collector is the interface for collecting subtask metrics.
type Collector interface {
	// Accepted is used collects metrics.
	// The difference between Accepted and Processed is that Accepted is called
	// when the data is accepted to be processed.
	Accepted(bytes int64)
	// Processed is used collects metrics.
	// `bytes` is the number of bytes processed, and `rows` is the number of rows processed.
	// The meaning of `bytes` may vary by scenario, for example:
	//   - During encoding, it represents the number of bytes read from the source data file.
	//   - During merge sort, it represents the number of bytes merged.
	Processed(bytes, rows int64)
}

// NoopCollector is a no-op implementation of Collector.
type NoopCollector struct{}

// Accepted implements Collector.Accepted
func (*NoopCollector) Accepted(_ int64) {}

// Processed implements Collector.Processed
func (*NoopCollector) Processed(_, _ int64) {}

// TestCollector is an implementation used for test.
type TestCollector struct {
	NoopCollector
	ReadBytes atomic.Int64
	Bytes     atomic.Int64
	Rows      atomic.Int64
}

// Accepted implements Collector.Accepted
func (c *TestCollector) Accepted(bytes int64) {
	c.ReadBytes.Add(bytes)
}

// Processed implements Collector.Processed
func (c *TestCollector) Processed(bytes, rows int64) {
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
	// GetMeterRecorder returns the meter recorder for the corresponding task.
	GetMeterRecorder() *metering.Recorder
	// GetCheckpointUpdateFunc returns the checkpoint update function
	GetCheckpointUpdateFunc() func(context.Context, int64, any) error
	// GetCheckpointunc returns the checkpoint get function
	GetCheckpointFunc() func(context.Context, int64) (string, error)
}

var stepExecFrameworkInfoName = reflect.TypeFor[StepExecFrameworkInfo]().Name()

type frameworkInfo struct {
	step          proto.Step
	meterRecorder *metering.Recorder
	resource      atomic.Pointer[proto.StepResource]

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

func (f *frameworkInfo) GetMeterRecorder() *metering.Recorder {
	return f.meterRecorder
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
	task *proto.Task,
	resource *proto.StepResource,
	updateCheckpointFunc func(context.Context, int64, any) error,
	getCheckpointFunc func(context.Context, int64) (string, error),
) {
	if exec == nil {
		return
	}
	toInject := &frameworkInfo{
		step:                 task.Step,
		meterRecorder:        metering.RegisterRecorder(&task.TaskBase),
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
