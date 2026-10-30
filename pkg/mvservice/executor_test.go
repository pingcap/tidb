// Copyright 2026 PingCAP, Inc.
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

//go:build intest

package mvservice

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTaskExecutorMaxConcurrency(t *testing.T) {
	installMockTimeForTest(t)
	exec := NewTaskExecutor(1, 0)
	require.True(t, exec.Run())
	require.False(t, exec.Run()) // idempotent
	defer exec.Close()

	started := make(chan string, 2)
	block := make(chan struct{})

	task := func(name string) func() error {
		return func() error {
			started <- name
			<-block
			return nil
		}
	}

	exec.Submit("t1", task("t1"))
	exec.Submit("t2", task("t2"))

	first := waitForStart(t, started, time.Hour)
	require.True(t, first == "t1" || first == "t2")

	select {
	case got := <-started:
		t.Fatalf("unexpected concurrent start: %s", got)
	default:
	}

	assertTaskExecutorMetrics(t, exec, 2, 0, 0, 0, 0, 1, 1, 0)

	close(block)

	second := waitForStart(t, started, time.Hour)
	require.NotEqual(t, first, second)

	waitForCount(t, exec.metrics.counters.finishedCount.Load, 2)
	assertTaskExecutorMetrics(t, exec, 2, 2, 0, 0, 0, 0, 0, 0)
}

func TestTaskExecutorUpdateMaxConcurrency(t *testing.T) {
	installMockTimeForTest(t)
	exec := NewTaskExecutor(1, 0)
	exec.Run()
	defer exec.Close()

	started := make(chan struct{}, 2)
	block := make(chan struct{})

	task := func() error {
		started <- struct{}{}
		<-block
		return nil
	}

	exec.Submit("t1", task)
	exec.Submit("t2", task)

	waitForSignal(t, started, time.Hour)

	select {
	case got := <-started:
		t.Fatalf("unexpected concurrent start: %v", got)
	default:
	}

	assertTaskExecutorMetrics(t, exec, 2, 0, 0, 0, 0, 1, 1, 0)

	exec.setMaxConcurrency(2)

	waitForSignal(t, started, time.Hour)

	assertTaskExecutorMetrics(t, exec, 2, 0, 0, 0, 0, 2, 0, 0)

	close(block)

	waitForCount(t, exec.metrics.counters.finishedCount.Load, 2)
	assertTaskExecutorMetrics(t, exec, 2, 2, 0, 0, 0, 0, 0, 0)
}

func TestTaskExecutorTimeoutReleasesSlot(t *testing.T) {
	module := installMockTimeForTest(t)
	exec := NewTaskExecutor(1, 50*time.Millisecond)
	exec.Run()
	defer exec.Close()

	started := make(chan string, 2)
	block := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(block) })
	defer unblock()
	longStarted := make(chan struct{})

	exec.Submit("long", func() error {
		started <- "long"
		close(longStarted)
		<-block
		return nil
	})

	waitForSignal(t, longStarted, time.Hour)

	assertTaskExecutorMetrics(t, exec, 1, 0, 0, 0, 0, 1, 0, 0)

	module.Advance(500 * time.Millisecond)

	waitForCount(t, exec.metrics.counters.timeoutCount.Load, 1)
	waitForCount(t, exec.metrics.gauges.runningCount.Load, 0)
	waitForCount(t, exec.metrics.gauges.timedOutRunningCount.Load, 1)

	exec.Submit("short", func() error {
		started <- "short"
		return nil
	})

	waitForNamedStart(t, started, "short", time.Hour)

	unblock()

	waitForCount(t, exec.metrics.counters.finishedCount.Load, 2)
	assertTaskExecutorMetrics(t, exec, 2, 2, 0, 1, 0, 0, 0, 0)
}

func TestTaskExecutorUpdateTimeout(t *testing.T) {
	module := installMockTimeForTest(t)
	exec := NewTaskExecutor(1, 0)
	exec.Run()
	defer exec.Close()

	exec.setTimeout(40 * time.Millisecond)

	started := make(chan string, 2)
	block := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(block) })
	defer unblock()
	longStarted := make(chan struct{})

	exec.Submit("long", func() error {
		started <- "long"
		close(longStarted)
		<-block
		return nil
	})

	waitForSignal(t, longStarted, time.Hour)

	assertTaskExecutorMetrics(t, exec, 1, 0, 0, 0, 0, 1, 0, 0)

	exec.Submit("short", func() error {
		started <- "short"
		return nil
	})

	module.Advance(40 * time.Millisecond)
	waitForNamedStart(t, started, "short", 300*time.Millisecond)
	waitForCount(t, exec.metrics.gauges.timedOutRunningCount.Load, 1)

	unblock()

	waitForCount(t, exec.metrics.counters.timeoutCount.Load, 1)
	waitForCount(t, exec.metrics.counters.finishedCount.Load, 2)
	assertTaskExecutorMetrics(t, exec, 2, 2, 0, 1, 0, 0, 0, 0)
}

func TestTaskExecutorRejectAfterClose(t *testing.T) {
	exec := NewTaskExecutor(1, 0)
	require.True(t, exec.Close())
	require.False(t, exec.Close())

	exec.Submit("rejected", func() error { return nil })

	assertTaskExecutorMetrics(t, exec, 0, 0, 0, 0, 1, 0, 0, 0)
}

type toggleBackpressureController struct {
	blocked atomic.Bool
	delay   time.Duration
}

func (c *toggleBackpressureController) ShouldBackpressure() (bool, time.Duration) {
	if c.blocked.Load() {
		return true, c.delay
	}
	return false, 0
}

type blockingBackpressureController struct {
	entered chan struct{}
	release chan struct{}
}

func (c *blockingBackpressureController) ShouldBackpressure() (bool, time.Duration) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	return false, 0
}

type signalBackpressureController struct {
	entered chan struct{}
	delay   time.Duration
}

func (c *signalBackpressureController) ShouldBackpressure() (bool, time.Duration) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	return true, c.delay
}

func TestTaskExecutorBackpressure(t *testing.T) {
	module := installMockTimeForTest(t)
	exec := NewTaskExecutor(1, 0)
	exec.Run()
	defer exec.Close()

	controller := &toggleBackpressureController{
		delay: time.Second,
	}
	controller.blocked.Store(true)
	exec.SetBackpressureController(controller)

	started := make(chan struct{}, 1)
	done := make(chan struct{}, 1)

	exec.Submit("blocked", func() error {
		started <- struct{}{}
		done <- struct{}{}
		return nil
	})

	select {
	case <-started:
		t.Fatalf("task should not start under backpressure")
	default:
	}

	module.Advance(5 * time.Second)
	select {
	case <-started:
		t.Fatalf("task should still be blocked")
	default:
	}
	require.Eventually(t, func() bool {
		return exec.metrics.counters.backpressureCount.Load() > 0
	}, testEventuallyWait, testEventuallyTick)
	require.Equal(t, int64(1), exec.metrics.counters.backpressureCount.Load())

	controller.blocked.Store(false)
	module.Advance(time.Second)
	waitForSignal(t, done, time.Hour)
	require.Eventually(t, func() bool {
		return exec.metrics.gauges.backpressureBlocked.Load() == 0
	}, testEventuallyWait, testEventuallyTick)
}

func TestTaskExecutorBackpressureCheckDoesNotBlockSubmit(t *testing.T) {
	exec := NewTaskExecutor(1, 0)
	exec.Run()
	defer exec.Close()

	controller := &blockingBackpressureController{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	exec.SetBackpressureController(controller)

	started := make(chan string, 2)
	exec.Submit("t1", func() error {
		started <- "t1"
		return nil
	})

	select {
	case <-controller.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach backpressure controller")
	}

	submitDone := make(chan struct{})
	go func() {
		exec.Submit("t2", func() error {
			started <- "t2"
			return nil
		})
		close(submitDone)
	}()

	select {
	case <-submitDone:
	case <-time.After(time.Second):
		t.Fatal("submit should not be blocked by backpressure check")
	}

	close(controller.release)

	waitStarted := func(want string) {
		t.Helper()
		timeout := time.After(time.Second)
		for {
			select {
			case got := <-started:
				if got == want {
					return
				}
			case <-timeout:
				t.Fatalf("timeout waiting for %s to start", want)
			}
		}
	}
	waitStarted("t1")
	waitStarted("t2")
	require.Eventually(t, func() bool {
		return exec.metrics.counters.finishedCount.Load() == 2
	}, testEventuallyWait, testEventuallyTick)
}

func TestTaskExecutorCloseInterruptsBackpressureWait(t *testing.T) {
	exec := NewTaskExecutor(1, 0)
	exec.Run()

	controller := &signalBackpressureController{
		entered: make(chan struct{}, 1),
		delay:   time.Hour,
	}
	exec.SetBackpressureController(controller)

	exec.Submit("blocked", func() error {
		t.Fatal("task should not run under persistent backpressure")
		return nil
	})

	select {
	case <-controller.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach backpressure check")
	}

	closed := make(chan struct{})
	go func() {
		exec.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close should interrupt backpressure wait")
	}
	require.Eventually(t, func() bool {
		return exec.metrics.gauges.backpressureBlocked.Load() == 0
	}, testEventuallyWait, testEventuallyTick)
	require.Greater(t, exec.metrics.counters.backpressureCount.Load(), int64(0))
}

func TestCPUMemBackpressureController(t *testing.T) {
	controller := NewCPUMemBackpressureController(0.7, 0.8, 0)
	controller.getCPUUsage = func() (float64, bool) {
		return 0.9, false
	}
	controller.getMemTotal = func() uint64 { return 100 }
	controller.getMemUsed = func() (uint64, error) { return 20, nil }

	blocked, delay := controller.ShouldBackpressure()
	require.True(t, blocked)
	require.Equal(t, defaultTaskBackpressureDelay, delay)

	controller.getCPUUsage = func() (float64, bool) {
		return 0.1, false
	}
	controller.getMemUsed = func() (uint64, error) { return 90, nil }
	controller.Delay = 200 * time.Millisecond
	blocked, delay = controller.ShouldBackpressure()
	require.True(t, blocked)
	require.Equal(t, 200*time.Millisecond, delay)

	controller.getMemUsed = func() (uint64, error) { return 10, nil }
	blocked, delay = controller.ShouldBackpressure()
	require.False(t, blocked)
	require.Equal(t, time.Duration(0), delay)
}

func TestTaskQueueRingBufferFIFO(t *testing.T) {
	var q taskQueue
	mkReq := func(i int) taskRequest {
		return taskRequest{
			name: fmt.Sprintf("t%d", i),
			task: func() error { return nil },
		}
	}

	for i := 1; i <= 4; i++ {
		q.push(mkReq(i))
	}
	for i := 1; i <= 2; i++ {
		req, ok := q.pop()
		require.True(t, ok)
		require.Equal(t, fmt.Sprintf("t%d", i), req.name)
	}

	for i := 5; i <= 12; i++ {
		q.push(mkReq(i))
	}
	require.Equal(t, 10, q.length())

	for i := 3; i <= 12; i++ {
		req, ok := q.pop()
		require.True(t, ok)
		require.Equal(t, fmt.Sprintf("t%d", i), req.name)
	}
	require.Equal(t, 0, q.length())

	_, ok := q.pop()
	require.False(t, ok)
}

func TestTaskQueueClearsReferences(t *testing.T) {
	var q taskQueue
	fn := func() error { return nil }
	q.push(taskRequest{name: "a", task: fn})

	_, ok := q.pop()
	require.True(t, ok)
	require.Equal(t, 0, q.length())
	require.GreaterOrEqual(t, len(q.buf), 1)
	require.Empty(t, q.buf[0].name)
	require.Nil(t, q.buf[0].task)

	q.push(taskRequest{name: "b", task: fn})
	q.push(taskRequest{name: "c", task: fn})
	pending := q.clear()
	require.Equal(t, 2, pending)
	require.Equal(t, 0, q.length())
	require.Nil(t, q.buf)
	require.Equal(t, 0, q.head)
}

func waitForSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-mvsAfter(timeout):
		t.Fatalf("timeout waiting for signal")
	}
}

func assertTaskExecutorMetrics(
	t *testing.T,
	exec *TaskExecutor,
	submitted, finished, failed, timeout, rejected int64,
	running, waiting, timedOutRunning int64,
) {
	t.Helper()
	require.Equal(t, submitted, exec.metrics.counters.submittedCount.Load())
	require.Equal(t, finished, exec.metrics.counters.finishedCount.Load())
	require.Equal(t, failed, exec.metrics.counters.failedCount.Load())
	require.Equal(t, timeout, exec.metrics.counters.timeoutCount.Load())
	require.Equal(t, rejected, exec.metrics.counters.rejectedCount.Load())
	require.Equal(t, running, exec.metrics.gauges.runningCount.Load())
	require.Equal(t, waiting, exec.metrics.gauges.waitingCount.Load())
	require.Equal(t, timedOutRunning, exec.metrics.gauges.timedOutRunningCount.Load())
}

func waitForStart(t *testing.T, ch <-chan string, timeout time.Duration) string {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-mvsAfter(timeout):
		t.Fatalf("timeout waiting for start")
		return ""
	}
}

func waitForNamedStart(t *testing.T, ch <-chan string, want string, timeout time.Duration) {
	t.Helper()
	deadline := mvsAfter(timeout)
	for {
		select {
		case got := <-ch:
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("timeout waiting for %s to start", want)
		}
	}
}

func TestWaitForCountDoesNotAdvanceMockTime(t *testing.T) {
	module := installMockTimeForTest(t)
	start := module.Now()
	var polls int64
	waitForCount(t, func() int64 {
		polls++
		return polls
	}, 2)
	require.Equal(t, start, module.Now())
}

func waitForCount(t *testing.T, get func() int64, want int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return get() == want
	}, testEventuallyWait, testEventuallyTick)
}
