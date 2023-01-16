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

package taskgroup

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/util/grunning"
	"github.com/pingcap/tidb/util/queue"
	atomicutil "go.uber.org/atomic"
)

type key int

const Key key = 0

type TaskGroup struct {
	cpuTime   atomicutil.Duration
	startTime time.Time

	// All tasks in this task group is yield and maintained in a link list
	mu struct {
		sync.RWMutex
		tasks *TaskContext
	}
}

type TaskContext struct {
	tg *TaskGroup
	wg sync.WaitGroup

	lastCheck time.Duration
	next      *TaskContext
}

func NewTaskGroup() *TaskGroup {
	return &TaskGroup{
		startTime: time.Now(),
	}
}

func fromContext(ctx context.Context) *TaskContext {
	val := ctx.Value(Key)
	if val == nil {
		return nil
	}
	ret, ok := val.(*TaskContext)
	if !ok {
		return nil
	}
	return ret
}

func NewContext(ctx context.Context, tg *TaskGroup) context.Context {
	return context.WithValue(ctx, Key, &TaskContext{
		tg: tg,
	})
}

func ContextWithSchedInfo(ctx context.Context) context.Context {
	tc := fromContext(ctx)
	if tc == nil {
		panic("no TaskContext found in current context!")
	}
	return NewContext(ctx, tc.tg)
}

func CheckPoint(ctx context.Context) {
	tc := fromContext(ctx)
	if tc == nil {
		return
	}

	tg := tc.tg
	tg.mu.RLock()
	scheduling := tg.mu.tasks != nil
	tg.mu.RUnlock()

	if !scheduling {
		runningTime := grunning.Time()
		elapse := runningTime - tc.lastCheck
		tc.lastCheck = runningTime
		cpuTime := tg.cpuTime.Add(elapse)
		if cpuTime < 20*time.Millisecond {
			return
		}
	}

	tc.wg.Add(1)
	s.ch <- tc
	tc.wg.Wait()
}

type sched struct {
	ch chan *TaskContext
	pq *queue.PriorityQueue[*TaskGroup]
}

var s = sched{
	ch: make(chan *TaskContext, 4096),
	pq: queue.NewPriorityQueue[*TaskGroup](200, func(a *TaskGroup, b *TaskGroup) bool {
		return a.startTime.Before(b.startTime)
	}),
}

func Scheduler() {
	lastTime := time.Now()
	const rate = 10
	capacity := 200 * time.Millisecond
	tokens := capacity
	for {
		select {
		case cp := <-s.ch:
			tg := cp.tg
			tg.mu.Lock()
			if tg.mu.tasks == nil {
				s.pq.Enqueue(tg)
			}
			cp.next = tg.mu.tasks
			tg.mu.tasks = cp
			tg.mu.Unlock()
		}

		// A token bucket algorithm to limit the total cpu usage.
		//
		// When a task group enqueue, it means a 20ms cpu time elapsed,
		// assume that the elapsed wall time is 100ms, it means the CPU usage is 20ms/100ms = 20%
		// assume that the elapsed wall time is 20ms, it means the CPU usage is 20ms/20ms = 100%
		// assume that the elapsed wall time is 5ms, it means the CPU usage is 20ms/5ms = 400%
		// The last case can happen in a multi-core environment.
		//
		// == How to decide the token generate rate? ==
		// Say, we want to control the CPU usage at 80%, there are 10 cores.
		// GC takes 25% of CPU resource, reserve that for it, we have actually 750% available in total.
		// 750% * 80% = 600%
		// CPU usage = cpu time / wall time, so the rate is: every 100ms, we can have 600ms cpu time,
		// 600ms / 20ms = 30, or we can say: every 100ms, we can have 30 token generated.

		now := time.Now()
		elapse := now.Sub(lastTime)
		lastTime = now

		// refill tokens
		tokens += rate * elapse
		if tokens > capacity {
			tokens = capacity
		}

		// == How to decide the priority of a task group? ==
		// If a task group A is swap-in to the queue, and its last running time is [startA, endA], the corresponding cpu time is 20ms.
		// It means the CPU resource usage rate for A is: 20ms / [now - startA], because [endA, now] does not take any CPU resource.
		//
		// If a task group B is swap-in to the queue, and its last running time is [startB, endB], the corresponding cpu time is 20ms.
		// It means the CPU resource usage rate for A is: 20ms / [now - startB], because [endB, now] does not take any CPU resource.
		//
		// Note that the 20ms cpu time is fixed for all the swaped-in task groups.
		// If startA < startB, task group A takes less CPU resource than task group B.
		// So we reach a conclusion that the smaller the start running time of a task group, the less CPU resource it uses, thus the higher priority.

		for !s.pq.Empty() {
			if tokens < 20*time.Millisecond {
				// Not enough tokens, rate limiter take effect.
				break
			}

			tg := s.pq.Dequeue()
			tokens -= 20 * time.Millisecond
			tg.startTime = now
			tg.cpuTime.Store(time.Duration(0))
			tg.mu.Lock()
			for tg.mu.tasks != nil {
				tc := tg.mu.tasks
				tg.mu.tasks = tg.mu.tasks.next
				tc.next = nil
				tc.wg.Done()
			}
			tg.mu.Unlock()
		}
	}
}
