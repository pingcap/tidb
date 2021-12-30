package testutil

import (
	"context"
	"runtime/pprof"
)

// MockCPULoad exports for testing
func MockCPULoad(ctx context.Context, labels ...string) {
	lvs := []string{}
	for _, label := range labels {
		lvs = append(lvs, label)
		lvs = append(lvs, label+" value")
		// start goroutine with only 1 label.
		go mockCPULoadByGoroutineWithLabel(ctx, label, label+" value")
	}
	// start goroutine with all labels.
	go mockCPULoadByGoroutineWithLabel(ctx, lvs...)
}

func mockCPULoadByGoroutineWithLabel(ctx context.Context, labels ...string) {
	ctx = pprof.WithLabels(ctx, pprof.Labels(labels...))
	pprof.SetGoroutineLabels(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		sum := 0
		for i := 0; i < 1000000; i++ {
			sum = sum + i*2
		}
	}
}
