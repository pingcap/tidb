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

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/taskstop"
	"github.com/stretchr/testify/require"
)

// SteppedTaskFunc is the func for the task
type SteppedTaskFunc func(ch *taskstop.Chan)

// SteppedTask is a task object that can be paused
type SteppedTask struct {
	t      *testing.T
	name   string
	runner *SteppedTasksRunner
	ch     *taskstop.Chan
	run    SteppedTaskFunc

	currentStop *taskstop.StopPoint
	path        []*taskstop.StopPoint
}

func newSteppedTask(name string, runner *SteppedTasksRunner) *SteppedTask {
	return &SteppedTask{
		t:      runner.t,
		name:   name,
		runner: runner,
	}
}

func (t *SteppedTask) setup(fn SteppedTaskFunc) {
	t.ch = taskstop.NewChan()
	t.run = fn

	go func() {
		defer func() {
			require.NoError(t.t, t.ch.SignalOnStopAt(taskstop.DonePointName))
		}()

		require.NoError(t.t, t.ch.SignalOnStopAt(taskstop.StartPointName))
		<-t.ch.WaitStepSignal()
		t.run(t.ch)
	}()
	t.waitNextStop()
	t.record(t.currentStop)
}

// CurrentStop returns the task's current stop
func (t *SteppedTask) CurrentStop() *taskstop.StopPoint {
	return t.currentStop
}

// Start starts the task
func (t *SteppedTask) Start() *SteppedTask {
	return t.ExpectWaitingStart().Continue()
}

// Continue resumes the task
func (t *SteppedTask) Continue() *SteppedTask {
	return t.ContinueWithValue(nil)
}

// ContinueWithValue resumes the task with the value
func (t *SteppedTask) ContinueWithValue(val any) *SteppedTask {
	t.step(val)
	t.record(t.currentStop)
	return t
}

// ExpectStoppedAt will check the task stops at the specified stop point
func (t *SteppedTask) ExpectStoppedAt(name string) *SteppedTask {
	require.Equal(t.t, name, t.currentStop.Name())
	return t
}

// ExpectWaitingStart will check the task is waiting for start
func (t *SteppedTask) ExpectWaitingStart() *SteppedTask {
	return t.ExpectStoppedAt(taskstop.StartPointName)
}

// ExpectDone will check the task is done
func (t *SteppedTask) ExpectDone() *SteppedTask {
	return t.ExpectStoppedAt(taskstop.DonePointName)
}

// IsWaitingStart returns whether the current stop is waiting for start
func (t *SteppedTask) IsWaitingStart() bool {
	return t.currentStop.Name() == taskstop.StartPointName
}

// IsDone returns whether the current stop is done
func (t *SteppedTask) IsDone() bool {
	return t.currentStop.Name() == taskstop.DonePointName
}

func (t *SteppedTask) record(stop *taskstop.StopPoint) {
	t.path = append(t.path, stop)
	t.runner.recordPath(t.name, stop)
}

func (t *SteppedTask) step(val any) {
	if t.IsDone() {
		t.t.Fatal("cannot step a done task")
	}
	require.NoError(t.t, t.ch.SignalStep(val))
	t.waitNextStop()
}

func (t *SteppedTask) waitNextStop() {
	select {
	case stopPoint := <-t.ch.WaitOnStop():
		t.currentStop = stopPoint
	case <-time.After(time.Second * 10):
		t.t.Fatal("timeout")
	}
}

// SteppedTasksRunner is used to manage all SteppedTasks
type SteppedTasksRunner struct {
	t     *testing.T
	tasks map[string]*SteppedTask
	path  []struct {
		task *SteppedTask
		stop *taskstop.StopPoint
	}
}

// NewSteppedTasksRunner creates a new SteppedTasksRunner
func NewSteppedTasksRunner(t *testing.T) *SteppedTasksRunner {
	return &SteppedTasksRunner{
		t:     t,
		tasks: make(map[string]*SteppedTask),
	}
}

// CreateTask creates a new task
func (r *SteppedTasksRunner) CreateTask(name string, fn SteppedTaskFunc) *SteppedTask {
	if _, ok := r.tasks[name]; ok {
		r.t.Fatalf("task '%s' already exists", name)
	}

	task := newSteppedTask(name, r)
	r.tasks[name] = task
	task.setup(fn)
	return task
}

// CreateSteppedTestKit creates a new SteppedTestKit
func (r *SteppedTasksRunner) CreateSteppedTestKit(name string, store kv.Storage) *SteppedTestKit {
	return newSteppedTestKit(r.t, name, store, r)
}

func (r *SteppedTasksRunner) recordPath(taskName string, stop *taskstop.StopPoint) {
	task, ok := r.tasks[taskName]
	if !ok {
		r.t.Fatalf("task '%s' not exist", taskName)
	}

	r.path = append(r.path, struct {
		task *SteppedTask
		stop *taskstop.StopPoint
	}{task: task, stop: stop})
}

// SteppedCommandTask is a stepped task for sql command
type SteppedCommandTask struct {
	t          *testing.T
	task       *SteppedTask
	done       bool
	resultChan chan any
	result     any
}

func (t *SteppedCommandTask) updateState() *SteppedCommandTask {
	switch t.task.currentStop.Name() {
	case waitingCommandStopPointName:
		t.done = true
		select {
		case result := <-t.resultChan:
			t.result = result
		case <-time.After(time.Second * 10):
			require.FailNow(t.t, "timeout")
		}
	case taskstop.DonePointName:
		t.done = true
	}
	return t
}

// CurrentStop returns the task's current stop
func (t *SteppedCommandTask) CurrentStop() *taskstop.StopPoint {
	if t.done {
		return taskstop.NewStopPoint(taskstop.DonePointName)
	}
	return t.task.CurrentStop()
}

// Continue resumes the task
func (t *SteppedCommandTask) Continue() *SteppedCommandTask {
	t.ExpectNotDone()
	t.task.Continue()
	return t.updateState()
}

// IsDone returns whether the current stop is done
func (t *SteppedCommandTask) IsDone() bool {
	return t.CurrentStop().Name() == taskstop.DonePointName
}

// ExpectStoppedAt will check the task stops at the specified stop poin
func (t *SteppedCommandTask) ExpectStoppedAt(name string) *SteppedCommandTask {
	require.Equal(t.t, name, t.CurrentStop().Name())
	return t
}

// ExpectDone will check the task is done
func (t *SteppedCommandTask) ExpectDone() *SteppedCommandTask {
	require.Equal(t.t, taskstop.DonePointName, t.CurrentStop().Name())
	return t
}

// ExpectNotDone will check the task not done yet
func (t *SteppedCommandTask) ExpectNotDone() *SteppedCommandTask {
	require.NotEqual(t.t, taskstop.DonePointName, t.CurrentStop().Name())
	return t
}

// GetResult returns the result of command
func (t *SteppedCommandTask) GetResult() any {
	return t.ExpectDone().result
}

// GetQueryResult returns the result of query
func (t *SteppedCommandTask) GetQueryResult() *Result {
	return t.GetResult().(*Result)
}

const waitingCommandStopPointName = "steppedTestKitWaitingCommand"

// SteppedTestKitCommand is what we want to run for the stepped task
type SteppedTestKitCommand func(t *testing.T, tk *TestKit, ch *taskstop.Chan) any

// SteppedTestKit is the testkit that can be paused
type SteppedTestKit struct {
	t          *testing.T
	name       string
	store      kv.Storage
	resultChan chan any

	task *SteppedTask
}

func newSteppedTestKit(t *testing.T, name string, store kv.Storage, runner *SteppedTasksRunner) *SteppedTestKit {
	tk := &SteppedTestKit{
		t:          t,
		name:       name,
		store:      store,
		resultChan: make(chan any, 1),
	}
	tk.start(runner)
	return tk
}

func (tk *SteppedTestKit) start(runner *SteppedTasksRunner) {
	tk.task = runner.CreateTask(tk.name, tk.run)
	tk.task.Start()
}

func (tk *SteppedTestKit) run(ch *taskstop.Chan) {
	rawTestKit := NewTestKit(tk.t, tk.store)
	defer func() {
		taskstop.DisableSessionStopPoint(rawTestKit.Session())
		rawTestKit.MustExec("rollback")
	}()

	for {
		require.NoError(tk.t, ch.SignalOnStopAt(waitingCommandStopPointName))
		switch fn := (<-ch.WaitStepSignal()).(type) {
		case SteppedTestKitCommand:
			select {
			case tk.resultChan <- fn(tk.t, rawTestKit, ch):
			default:
				require.FailNow(tk.t, "the previous value not consumed")
			}
		default:
			return
		}
	}
}

// Close closes the current test kit
func (tk *SteppedTestKit) Close() {
	tk.task.ContinueWithValue(struct{}{})
}

// SteppedCommand create a new stepped task for the command
func (tk *SteppedTestKit) SteppedCommand(fn SteppedTestKitCommand) *SteppedCommandTask {
	tk.task.ExpectStoppedAt(waitingCommandStopPointName)
	tk.task.ContinueWithValue(fn)
	cmd := &SteppedCommandTask{
		t:          tk.t,
		task:       tk.task,
		resultChan: tk.resultChan,
	}
	return cmd.updateState()
}

// Command executes a command
func (tk *SteppedTestKit) Command(fn SteppedTestKitCommand) any {
	cmd := tk.SteppedCommand(fn)
	for !cmd.IsDone() {
		cmd.Continue()
	}
	return cmd.GetResult()
}

// EnableSessionStopPoint enables the session's stop point
func (tk *SteppedTestKit) EnableSessionStopPoint(point ...string) {
	tk.Command(func(t *testing.T, tk *TestKit, ch *taskstop.Chan) any {
		taskstop.EnableSessionStopPoint(tk.Session(), ch, point...)
		return nil
	})
}

// DisableSessionStopPoint disables the session's stop point
func (tk *SteppedTestKit) DisableSessionStopPoint() {
	tk.Command(func(t *testing.T, tk *TestKit, ch *taskstop.Chan) any {
		taskstop.DisableSessionStopPoint(tk.Session())
		return nil
	})
}

// MustExec executes MustExec
func (tk *SteppedTestKit) MustExec(sql string, args ...interface{}) {
	tk.Command(func(t *testing.T, tk *TestKit, ch *taskstop.Chan) any {
		tk.MustExec(sql, args...)
		return nil
	})
}

// MustQuery executes MustQuery
func (tk *SteppedTestKit) MustQuery(sql string, args ...interface{}) *Result {
	return tk.Command(func(t *testing.T, tk *TestKit, ch *taskstop.Chan) any {
		return tk.MustQuery(sql, args...)
	}).(*Result)
}

// SteppedMustExec creates a new stepped task for MustExec
func (tk *SteppedTestKit) SteppedMustExec(sql string, args ...interface{}) *SteppedCommandTask {
	return tk.SteppedCommand(func(t *testing.T, tk *TestKit, ch *taskstop.Chan) any {
		tk.MustExec(sql, args...)
		return nil
	})
}

// SteppedMustQuery creates a new stepped task for MustQuery
func (tk *SteppedTestKit) SteppedMustQuery(sql string, args ...interface{}) *SteppedCommandTask {
	return tk.SteppedCommand(func(t *testing.T, tk *TestKit, ch *taskstop.Chan) any {
		return tk.MustQuery(sql, args...)
	})
}
