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

package local

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func TestRegionJobBaseWorker(t *testing.T) {
	newWorker := func() *regionJobBaseWorker {
		return &regionJobBaseWorker{
			jobInCh:  make(chan *regionJob, 10),
			jobOutCh: make(chan *regionJob, 10),
			jobWg:    &sync.WaitGroup{},
			doRunJobFn: func(ctx context.Context, job *regionJob) error {
				job.convertStageTo(ingested)
				return nil
			},
			regenerateJobsFn: func(
				ctx context.Context, data common.IngestData, sortedJobRanges []common.Range,
				regionSplitSize, regionSplitKeys int64,
			) ([]*regionJob, error) {
				return []*regionJob{
					{}, {}, {},
				}, nil
			},
		}
	}

	t.Run("run on closed channel", func(t *testing.T) {
		w := newWorker()
		close(w.jobInCh)
		require.NoError(t, w.run(context.Background()))
	})

	t.Run("send job to out channel after run job", func(t *testing.T) {
		w := newWorker()
		job := &regionJob{}
		w.jobInCh <- job
		close(w.jobInCh)
		require.NoError(t, w.run(context.Background()))
		require.Equal(t, 1, len(w.jobOutCh))
	})

	t.Run("regenerate jobs", func(t *testing.T) {
		w := newWorker()
		w.doRunJobFn = func(ctx context.Context, job *regionJob) error {
			job.convertStageTo(needRescan)
			return nil
		}
		job := &regionJob{}
		w.jobInCh <- job
		close(w.jobInCh)
		require.NoError(t, w.run(context.Background()))
		require.Equal(t, 3, len(w.jobOutCh))
	})
}
