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

package testkit

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/util/taskstop"
	"github.com/stretchr/testify/require"
)

// TaskFunc is the func for the task
type TaskFunc func(ch *taskstop.Chan)

// StoppableTask is a task object that can be paused
type StoppableTask struct {
	t       *testing.T
	name    string
	runner  *StoppableTasksRunner
	ch      *taskstop.Chan
	runFunc TaskFunc

	currentStop *taskstop.StopPoint
	path        []*taskstop.StopPoint
}

func newStoppableTask(name string, runner *StoppableTasksRunner) *StoppableTask {
	return &StoppableTask{
		t:      runner.t,
		name:   name,
		runner: runner,
	}
}

func (t *StoppableTask) setup(fn TaskFunc) {
	t.ch = taskstop.NewChan()
	t.runFunc = fn

	go func() {
		require.NoError(t.t, t.ch.SignalOnStopAt("START"))
		<-t.ch.WaitStepSignal()
		defer func() {
			require.NoError(t.t, t.ch.SignalOnStopAt("DONE"))
		}()
		t.runFunc(t.ch)
	}()
	t.waitNextStop()
	t.record(t.currentStop)
}

// CurrentStop returns the task's current stop
func (t *StoppableTask) CurrentStop() *taskstop.StopPoint {
	return t.currentStop
}

// Start starts the task
func (t *StoppableTask) Start() *StoppableTask {
	return t.ExpectWaitingStart().Continue()
}

// Continue resumes the task
func (t *StoppableTask) Continue() *StoppableTask {
	t.step()
	t.record(t.currentStop)
	return t
}

// ExpectStoppedAt will check the task stops at the specified stop point
func (t *StoppableTask) ExpectStoppedAt(name string) *StoppableTask {
	require.Equal(t.t, name, t.currentStop.Name())
	return t
}

// ExpectWaitingStart will check the task is waiting for start
func (t *StoppableTask) ExpectWaitingStart() *StoppableTask {
	require.Truef(t.t, t.IsWaitingStart(), "current stop: '%s'", t.currentStop.Name())
	return t
}

// ExpectDone will check the task is done
func (t *StoppableTask) ExpectDone() *StoppableTask {
	require.Truef(t.t, t.IsDone(), "current stop: '%s'", t.currentStop.Name())
	return t
}

// IsWaitingStart returns whether the current stop is waiting for start
func (t *StoppableTask) IsWaitingStart() bool {
	return t.currentStop.Name() == "START"
}

// IsDone returns whether the current stop is done
func (t *StoppableTask) IsDone() bool {
	return t.currentStop.Name() == "DONE"
}

func (t *StoppableTask) record(stop *taskstop.StopPoint) {
	t.path = append(t.path, stop)
	t.runner.recordPath(t.name, stop)
}

func (t *StoppableTask) step() {
	if t.IsDone() {
		t.t.Fatal("cannot step a done task")
	}
	require.NoError(t.t, t.ch.SignalStep())
	t.waitNextStop()
}

func (t *StoppableTask) waitNextStop() {
	select {
	case stopPoint := <-t.ch.WaitOnStop():
		t.currentStop = stopPoint
	case <-time.After(time.Second * 10):
		t.t.Fatal("timeout")
	}
}

// StoppableTasksRunner is used to manage all StoppableTasks
type StoppableTasksRunner struct {
	t     *testing.T
	tasks map[string]*StoppableTask
	path  []struct {
		task *StoppableTask
		stop *taskstop.StopPoint
	}
}

// NewStoppableTasksRunner creates a new StoppableTasksRunner
func NewStoppableTasksRunner(t *testing.T) *StoppableTasksRunner {
	return &StoppableTasksRunner{
		t:     t,
		tasks: make(map[string]*StoppableTask),
	}
}

// CreateTask creates a new task
func (r *StoppableTasksRunner) CreateTask(name string, fn TaskFunc) *StoppableTask {
	if _, ok := r.tasks[name]; ok {
		r.t.Fatalf("task '%s' already exists", name)
	}

	task := newStoppableTask(name, r)
	r.tasks[name] = task
	task.setup(fn)
	return task
}

func (r *StoppableTasksRunner) recordPath(taskName string, stop *taskstop.StopPoint) {
	task, ok := r.tasks[taskName]
	if !ok {
		r.t.Fatalf("task '%s' not exist", task.name)
	}

	r.path = append(r.path, struct {
		task *StoppableTask
		stop *taskstop.StopPoint
	}{task: task, stop: stop})
}
