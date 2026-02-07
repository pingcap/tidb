package utils

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTaskExecutorMaxConcurrency(t *testing.T) {
	exec := NewTaskExecutor(context.Background(), 1, 0)
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

	wg := make(chan struct{})
	mockInjection = func() {
		close(wg)
	}

	exec.Submit("t1", task("t1"))
	exec.Submit("t2", task("t2"))

	first := waitForStart(t, started, 200*time.Millisecond)
	require.True(t, first == "t1" || first == "t2")

	select {
	case got := <-started:
		t.Fatalf("unexpected concurrent start: %s", got)
	case <-wg:
		break
	}

	require.Equal(t, int64(2), exec.metrics.submittedCount.Load())
	require.Equal(t, int64(1), exec.metrics.runningCount.Load())
	require.Equal(t, int64(1), exec.metrics.waitingCount.Load())
	require.Equal(t, int64(0), exec.metrics.completedCount.Load())
	require.Equal(t, int64(0), exec.metrics.failedCount.Load())
	require.Equal(t, int64(0), exec.metrics.timeoutCount.Load())
	require.Equal(t, int64(0), exec.metrics.rejectedCount.Load())

	close(block)

	second := waitForStart(t, started, 200*time.Millisecond)
	require.NotEqual(t, first, second)

	waitForCount(t, "completed", exec.metrics.completedCount.Load, 2)
	require.Equal(t, int64(0), exec.metrics.runningCount.Load())
	require.Equal(t, int64(0), exec.metrics.waitingCount.Load())
	require.Equal(t, int64(0), exec.metrics.failedCount.Load())
	require.Equal(t, int64(0), exec.metrics.timeoutCount.Load())
	require.Equal(t, int64(0), exec.metrics.rejectedCount.Load())
}

func TestTaskExecutorUpdateMaxConcurrency(t *testing.T) {
	exec := NewTaskExecutor(context.Background(), 1, 0)
	defer exec.Close()

	started := make(chan struct{}, 2)
	block := make(chan struct{})

	task := func() error {
		started <- struct{}{}
		<-block
		return nil
	}

	wg := make(chan struct{})
	mockInjection = func() {
		close(wg)
	}

	exec.Submit("t1", task)
	exec.Submit("t2", task)

	waitForSignal(t, started, 200*time.Millisecond)

	select {
	case got := <-started:
		t.Fatalf("unexpected concurrent start: %s", got)
	case <-wg:
		break
	}

	require.Equal(t, int64(2), exec.metrics.submittedCount.Load())
	require.Equal(t, int64(1), exec.metrics.runningCount.Load())
	require.Equal(t, int64(1), exec.metrics.waitingCount.Load())
	require.Equal(t, int64(0), exec.metrics.completedCount.Load())
	require.Equal(t, int64(0), exec.metrics.failedCount.Load())
	require.Equal(t, int64(0), exec.metrics.timeoutCount.Load())
	require.Equal(t, int64(0), exec.metrics.rejectedCount.Load())

	exec.setMaxConcurrency(2)

	waitForSignal(t, started, 200*time.Millisecond)

	require.Equal(t, int64(2), exec.metrics.runningCount.Load())
	require.Equal(t, int64(0), exec.metrics.waitingCount.Load())

	close(block)

	waitForCount(t, "completed", exec.metrics.completedCount.Load, 2)
	require.Equal(t, int64(0), exec.metrics.runningCount.Load())
	require.Equal(t, int64(0), exec.metrics.waitingCount.Load())
	require.Equal(t, int64(0), exec.metrics.failedCount.Load())
	require.Equal(t, int64(0), exec.metrics.timeoutCount.Load())
	require.Equal(t, int64(0), exec.metrics.rejectedCount.Load())
}

func TestTaskExecutorTimeoutReleasesSlot(t *testing.T) {
	exec := NewTaskExecutor(context.Background(), 1, 50*time.Millisecond)
	defer exec.Close()

	started := make(chan string, 2)
	block := make(chan struct{})
	longStarted := make(chan struct{})

	exec.Submit("long", func() error {
		started <- "long"
		close(longStarted)
		<-block
		return nil
	})

	mockCh := make(chan time.Time, 1)
	mockInjectionTimer = func(_ <-chan time.Time) <-chan time.Time {
		return mockCh
	}
	defer func() { mockInjectionTimer = nil }()

	waitForSignal(t, longStarted, 200*time.Millisecond)

	require.Equal(t, int64(1), exec.metrics.submittedCount.Load())
	require.Equal(t, int64(1), exec.metrics.runningCount.Load())
	require.Equal(t, int64(0), exec.metrics.waitingCount.Load())
	require.Equal(t, int64(0), exec.metrics.completedCount.Load())
	require.Equal(t, int64(0), exec.metrics.failedCount.Load())
	require.Equal(t, int64(0), exec.metrics.timeoutCount.Load())
	require.Equal(t, int64(0), exec.metrics.rejectedCount.Load())

	mockCh <- time.Time{}
	waitForCount(t, "timeout", exec.metrics.timeoutCount.Load, 1)
	waitForCount(t, "running", exec.metrics.runningCount.Load, 0)

	exec.Submit("short", func() error {
		started <- "short"
		return nil
	})

	waitForNamedStart(t, started, "short", 300*time.Millisecond)

	close(block)

	waitForCount(t, "completed", exec.metrics.completedCount.Load, 2)
	require.Equal(t, int64(0), exec.metrics.failedCount.Load())
	require.Equal(t, int64(1), exec.metrics.timeoutCount.Load())
	require.Equal(t, int64(0), exec.metrics.rejectedCount.Load())
}

func TestTaskExecutorUpdateTimeout(t *testing.T) {
	exec := NewTaskExecutor(context.Background(), 1, 0)
	defer exec.Close()

	exec.setTimeout(40 * time.Millisecond)

	started := make(chan string, 2)
	block := make(chan struct{})
	longStarted := make(chan struct{})

	mockCh := make(chan time.Time, 1)
	mockInjectionTimer = func(_ <-chan time.Time) <-chan time.Time {
		return mockCh
	}
	defer func() { mockInjectionTimer = nil }()

	exec.Submit("long", func() error {
		started <- "long"
		close(longStarted)
		<-block
		return nil
	})

	waitForSignal(t, longStarted, 200*time.Millisecond)

	require.Equal(t, int64(1), exec.metrics.submittedCount.Load())
	require.Equal(t, int64(1), exec.metrics.runningCount.Load())
	require.Equal(t, int64(0), exec.metrics.waitingCount.Load())
	require.Equal(t, int64(0), exec.metrics.timeoutCount.Load())

	exec.Submit("short", func() error {
		started <- "short"
		return nil
	})

	mockCh <- time.Time{}
	waitForNamedStart(t, started, "short", 300*time.Millisecond)

	close(block)

	waitForCount(t, "timeout", exec.metrics.timeoutCount.Load, 1)
	waitForCount(t, "completed", exec.metrics.completedCount.Load, 2)
	require.Equal(t, int64(0), exec.metrics.failedCount.Load())
	require.Equal(t, int64(0), exec.metrics.rejectedCount.Load())
}

func TestTaskExecutorRejectAfterClose(t *testing.T) {
	exec := NewTaskExecutor(context.Background(), 1, 0)
	exec.Close()

	exec.Submit("rejected", func() error { return nil })

	require.Equal(t, int64(1), exec.metrics.rejectedCount.Load())
	require.Equal(t, int64(0), exec.metrics.submittedCount.Load())
	require.Equal(t, int64(0), exec.metrics.waitingCount.Load())
	require.Equal(t, int64(0), exec.metrics.runningCount.Load())
	require.Equal(t, int64(0), exec.metrics.completedCount.Load())
	require.Equal(t, int64(0), exec.metrics.failedCount.Load())
	require.Equal(t, int64(0), exec.metrics.timeoutCount.Load())
}

func waitForSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for signal")
	}
}

func waitForStart(t *testing.T, ch <-chan string, timeout time.Duration) string {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for start")
		return ""
	}
}

func waitForNamedStart(t *testing.T, ch <-chan string, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
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

func waitForCount(t *testing.T, name string, get func() int64, want int64) {
	t.Helper()
	for {
		if got := get(); got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
