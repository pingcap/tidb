// Copyright 2021 PingCAP, Inc.
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

package testutil

import (
	"context"
	"encoding/hex"
	"runtime/pprof"
)

// MockCPULoad exports for testing
func MockCPULoad(ctx context.Context, labels ...string) {
	lvs := []string{}
	for _, label := range labels {
		lvs = append(lvs, label)
		val := hex.EncodeToString([]byte(label + " value"))
		lvs = append(lvs, val)
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
