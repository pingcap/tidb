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

package multithreadtest

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/stringutil"
)

type Chan struct {
	ch1 chan *StopPoint
	ch2 chan any
}

func (c *Chan) Stop(name string, value ...any) {
	c.StopAtPoint(NewStopPoint(name, value))
}

func (c *Chan) StopAtPoint(point *StopPoint) {
	c.ch1 <- point
	<-c.ch2
}

func EnableSessionStopPoint(sctx sessionctx.Context, c *Chan) {
	sctx.SetValue(stringutil.StringerStr("multiThreadTestChain"), c)
}

func DisableSessionStopPoint(sctx sessionctx.Context) {
	sctx.SetValue(stringutil.StringerStr("multiThreadTestChain"), nil)
}

func SessionStop(sctx sessionctx.Context, stopName string, value ...any) {
	if ch, ok := sctx.Value(stringutil.StringerStr("multiThreadTestChain")).(*Chan); ok {
		ch.Stop(stopName, value)
	}
}

type ThreadFunc func(ch *Chan, args []any) []any

type StopPoint struct {
	name  string
	value []any
}

func NewStopPoint(name string, value ...any) *StopPoint {
	return &StopPoint{
		name:  name,
		value: value,
	}
}

func (p *StopPoint) Name() string {
	return p.name
}

func (p *StopPoint) Value() []any {
	return p.value
}

func StartPoint(value []any) *StopPoint {
	return &StopPoint{name: "START", value: value}
}

func DonePoint(value []any) *StopPoint {
	return &StopPoint{name: "DONE", value: value}
}

type Thread struct {
	name   string
	runner *ThreadsRunner
	ch     *Chan
	run    ThreadFunc

	currentStop *StopPoint
	path        []*StopPoint
	whiteList   bool
	stopList    map[string]struct{}
}

func newThread(name string, runner *ThreadsRunner) *Thread {
	return &Thread{
		name:   name,
		runner: runner,
	}
}

func (t *Thread) StopWhen(stops []string) *Thread {
	t.setStopList(true, stops)
	return t
}

func (t *Thread) StopWhenNot(stops []string) *Thread {
	t.setStopList(false, stops)
	return t
}

func (t *Thread) StopForAllPoints() *Thread {
	t.setStopList(false, nil)
	return t
}

func (t *Thread) setStopList(whiteList bool, stops []string) {
	list := make(map[string]struct{})
	for _, stop := range stops {
		list[stop] = struct{}{}
	}

	t.stopList = list
	t.whiteList = whiteList
}

func (t *Thread) Start(run ThreadFunc, args ...any) *Thread {
	t.ch = &Chan{
		ch1: make(chan *StopPoint),
		ch2: make(chan any),
	}
	t.run = run

	go func() {
		t.ch.StopAtPoint(StartPoint(args))
		var value []any
		defer func() {
			t.ch.ch1 <- DonePoint(value)
		}()
		value = t.run(t.ch, args)
	}()
	t.runner.addThread(t)
	t.waitNextStop()
	t.record(t.currentStop)
	return t
}

func (t *Thread) CurrentStop() *StopPoint {
	return t.currentStop
}

func (t *Thread) Step() *Thread {
	t.step()
	t.record(t.currentStop)
	return t
}

func (t *Thread) StepUntilDone() *Thread {
	if t.IsDone() {
		return t
	}

	for !t.IsDone() {
		t.step()
	}
	t.record(t.currentStop)
	return t
}

func (t *Thread) CheckCurrentStop(name string) *Thread {
	if t.currentStop.name != name {
		panic(fmt.Sprintf("Expected current stop '%s', actual '%s'", name, t.currentStop.name))
	}
	return t
}

func (t *Thread) CheckDone() *Thread {
	if !t.IsDone() {
		panic(fmt.Sprintf("Expected current stop is done, actual '%s'", t.currentStop.name))
	}
	return t
}

func (t *Thread) IsDone() bool {
	return t.currentStop.name == "DONE"
}

func (t *Thread) step() {
	for {
		if t.stepOne(); t.IsDone() {
			break
		}

		if _, ok := t.stopList[t.currentStop.Name()]; t.whiteList == ok {
			break
		}
	}
}

func (t *Thread) record(stop *StopPoint) {
	t.path = append(t.path, stop)
	t.runner.recordPath(t.name, stop)
}

func (t *Thread) stepOne() {
	if t.IsDone() {
		panic("cannot step a done thread")
	}
	t.ch.ch2 <- struct{}{}
	t.waitNextStop()
}

func (t *Thread) waitNextStop() {
	select {
	case stop := <-t.ch.ch1:
		t.currentStop = stop
	case <-time.After(time.Second * 10):
		panic("timeout")
	}
}

type ThreadsRunner struct {
	threads map[string]*Thread
	path    []struct {
		thread *Thread
		stop   *StopPoint
	}
}

func NewThreadsRunner() *ThreadsRunner {
	return &ThreadsRunner{
		threads: make(map[string]*Thread),
	}
}

func (r *ThreadsRunner) Thread(name string) *Thread {
	if thread, ok := r.threads[name]; ok {
		return thread
	}

	return newThread(name, r)
}

func (r *ThreadsRunner) addThread(thread *Thread) {
	if _, ok := r.threads[thread.name]; ok {
		panic(fmt.Sprintf("thread '%s' already exists", thread.name))
	}
	r.threads[thread.name] = thread
}

func (r *ThreadsRunner) recordPath(threadName string, stop *StopPoint) {
	thread, ok := r.threads[threadName]
	if !ok {
		panic(fmt.Sprintf("thread '%s' not exists", threadName))
	}

	r.path = append(r.path, struct {
		thread *Thread
		stop   *StopPoint
	}{thread: thread, stop: stop})
}
