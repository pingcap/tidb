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
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestReorgCtxSetMaxProgress(t *testing.T) {
	rc := &reorgCtx{}

	require.Equal(t, float64(0), rc.maxProgress.Load())

	result := rc.setMaxProgress(0.5)
	require.Equal(t, 0.5, result)
	require.Equal(t, 0.5, rc.maxProgress.Load())

	result = rc.setMaxProgress(0.7)
	require.Equal(t, 0.7, result)
	require.Equal(t, 0.7, rc.maxProgress.Load())

	result = rc.setMaxProgress(0.3)
	require.Equal(t, 0.7, result)                // Returns old max
	require.Equal(t, 0.7, rc.maxProgress.Load()) // Value unchanged

	result = rc.setMaxProgress(0.7)
	require.Equal(t, 0.7, result)
	require.Equal(t, 0.7, rc.maxProgress.Load())

	result = rc.setMaxProgress(0.9)
	require.Equal(t, 0.9, result)
	require.Equal(t, 0.9, rc.maxProgress.Load())
}

func TestReorgCtxSetMaxProgressConcurrent(t *testing.T) {
	rc := &reorgCtx{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(progress float64) {
			defer wg.Done()
			rc.setMaxProgress(progress)
		}(float64(i) / 100.0)
	}

	wg.Wait()

	require.Equal(t, 0.99, rc.maxProgress.Load())
}

func TestRunReorgJobContextCause(t *testing.T) {
	buildErr := errors.New("build failed")
	for _, tc := range []struct {
		name                string
		cause, result, want error
	}{
		{"cancelled", dbterror.ErrCancelledDDLJob, errors.Trace(context.Canceled), dbterror.ErrCancelledDDLJob},
		{"paused", dbterror.ErrPausedDDLJob, context.Canceled, dbterror.ErrPausedDDLJob},
		{"owner change", dbterror.ErrNotOwner, context.Canceled, dbterror.ErrNotOwner},
		{"live context", nil, context.Canceled, context.Canceled},
		{"unrelated error", dbterror.ErrPausedDDLJob, buildErr, buildErr},
		{"completed", dbterror.ErrCancelledDDLJob, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			if tc.cause != nil {
				cancel(tc.cause)
			}
			w := &worker{ddlCtx: &ddlCtx{reorgCtx: reorgContexts{reorgCtxMap: make(map[int64]*reorgCtx)}}}
			job := &model.Job{ID: 1}
			jobCtx := &jobContext{stepCtx: ctx, oldDDLCtx: w.ddlCtx}
			err := w.runReorgJob(jobCtx, &reorgInfo{Job: job, jobCtx: jobCtx}, nil, func() error {
				// A later step may replace the context before consuming this result.
				jobCtx.stepCtx = context.Background()
				return tc.result
			})
			w.wg.Wait()
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
}
