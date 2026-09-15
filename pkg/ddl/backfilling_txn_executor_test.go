// Copyright 2025 PingCAP, Inc.
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

package ddl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackfillWorkerSendsInFlightResultAfterClose(t *testing.T) {
	t.Run("worker downscale preserves result", func(t *testing.T) {
		worker := newBackfillWorker(context.Background(), context.Background(), nil)
		worker.resultCh = make(chan *backfillResult)
		worker.Close()

		want := &backfillResult{taskID: 1}
		go worker.sendResult(want)

		select {
		case got := <-worker.resultCh:
			require.Same(t, want, got)
		case <-time.After(time.Second):
			t.Fatal("in-flight backfill result was dropped after worker close")
		}
	})

	t.Run("executor shutdown can discard result", func(t *testing.T) {
		resultCtx, cancelResult := context.WithCancel(context.Background())
		worker := newBackfillWorker(context.Background(), resultCtx, nil)
		worker.resultCh = make(chan *backfillResult)
		cancelResult()

		done := make(chan struct{})
		go func() {
			worker.sendResult(&backfillResult{taskID: 1})
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("backfill worker blocked while discarding a result during executor shutdown")
		}
	})
}

func TestExpectedIngestWorkerCnt(t *testing.T) {
	tests := []struct {
		concurrency   int
		avgRowSize    int
		useGlobalSort bool
		expReader     int
		expWriter     int
	}{
		// Global sort path
		{10, 100, true, 10, 10},
		{20, 500, true, 20, 20},

		// Non-global sort path, avgRowSize = 0
		{10, 0, false, 5, 7},
		{40, 0, false, 16, 16},
		{1, 0, false, 1, 2},

		// Non-global sort path, various avgRowSize
		{10, 100, false, 5, 10},
		{10, 300, false, 10, 10},
		{10, 600, false, 20, 10},
		{10, 2000, false, 40, 10},
		{10, 5000, false, 80, 10},
	}

	for _, tt := range tests {
		reader, writer := expectedIngestWorkerCnt(tt.concurrency, tt.avgRowSize, tt.useGlobalSort)
		require.Equal(t, tt.expReader, reader, "concurrency: %d, avgRowSize: %d, useGlobalSort: %v", tt.concurrency, tt.avgRowSize, tt.useGlobalSort)
		require.Equal(t, tt.expWriter, writer, "concurrency: %d, avgRowSize: %d, useGlobalSort: %v", tt.concurrency, tt.avgRowSize, tt.useGlobalSort)
	}
}
