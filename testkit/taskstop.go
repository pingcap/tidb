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

type TaskFunc func(ch *taskstop.Chan)
type SessionTaskFunc func(ch *taskstop.Chan, tk *TestKit)

type StoppableTask struct {
	t       *testing.T
	name    string
	runner  *StoppableTasksRunner
	ch      *taskstop.Chan
	runFunc TaskFunc

	currentStop *taskstop.StopPoint
	path        []*taskstop.StopPoint
	whiteList   bool
	stopList    map[string]struct{}
}

func newStoppableTask(name string, runner *StoppableTasksRunner) *StoppableTask {
	return &StoppableTask{
		t:      runner.t,
		name:   name,
		runner: runner,
	}
}

func (t *StoppableTask) StopWhen(stops []string) *StoppableTask {
	t.setStopList(true, stops)
	return t
}

func (t *StoppableTask) StopWhenNot(stops []string) *StoppableTask {
	t.setStopList(false, stops)
	return t
}

func (t *StoppableTask) StopEveryPoint() *StoppableTask {
	t.setStopList(false, nil)
	return t
}

func (t *StoppableTask) setStopList(whiteList bool, stops []string) {
	list := make(map[string]struct{})
	for _, stopName := range stops {
		list[stopName] = struct{}{}
	}

	t.stopList = list
	t.whiteList = whiteList
}

func (t *StoppableTask) Start(fn TaskFunc) *StoppableTask {
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
	t.runner.addTask(t)
	t.waitNextStop()
	t.record(t.currentStop)
	return t
}

func (t *StoppableTask) CurrentStop() *taskstop.StopPoint {
	return t.currentStop
}

func (t *StoppableTask) Step() *StoppableTask {
	t.step()
	t.record(t.currentStop)
	return t
}

func (t *StoppableTask) StepUntilDone() *StoppableTask {
	if t.IsDone() {
		return t
	}

	for !t.IsDone() {
		t.step()
	}
	t.record(t.currentStop)
	return t
}

func (t *StoppableTask) CheckStopAt(name string) *StoppableTask {
	require.Equal(t.t, name, t.currentStop.Name())
	return t
}

func (t *StoppableTask) CheckDone() *StoppableTask {
	require.Truef(t.t, t.IsDone(), "current stop: '%s'", t.currentStop.Name())
	return t
}

func (t *StoppableTask) IsDone() bool {
	return t.currentStop.Name() == "DONE"
}

func (t *StoppableTask) step() {
	for {
		if t.stepOne(); t.IsDone() {
			break
		}

		if _, ok := t.stopList[t.currentStop.Name()]; t.whiteList == ok {
			break
		}
	}
}

func (t *StoppableTask) record(stop *taskstop.StopPoint) {
	t.path = append(t.path, stop)
	t.runner.recordPath(t.name, stop)
}

func (t *StoppableTask) stepOne() {
	if t.IsDone() {
		panic("cannot step a done thread")
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

type StoppableTasksRunner struct {
	t     *testing.T
	tasks map[string]*StoppableTask
	path  []struct {
		task *StoppableTask
		stop *taskstop.StopPoint
	}
}

func NewStoppableTasksRunner(t *testing.T) *StoppableTasksRunner {
	return &StoppableTasksRunner{
		t:     t,
		tasks: make(map[string]*StoppableTask),
	}
}

func (r *StoppableTasksRunner) Task(name string) *StoppableTask {
	if thread, ok := r.tasks[name]; ok {
		return thread
	}

	return newStoppableTask(name, r)
}

func (r *StoppableTasksRunner) addTask(thread *StoppableTask) {
	if _, ok := r.tasks[thread.name]; ok {
		r.t.Fatalf("thread '%s' already exists", thread.name)
	}
	r.tasks[thread.name] = thread
}

func (r *StoppableTasksRunner) recordPath(threadName string, stop *taskstop.StopPoint) {
	task, ok := r.tasks[threadName]
	if !ok {
		r.t.Fatalf("thread '%s' not exist", task.name)
	}

	r.path = append(r.path, struct {
		task *StoppableTask
		stop *taskstop.StopPoint
	}{task: task, stop: stop})
}
