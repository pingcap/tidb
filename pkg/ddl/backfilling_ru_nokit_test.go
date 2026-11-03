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

package ddl

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestAccountBackfillTxnRU(t *testing.T) {
	t.Cleanup(config.RestoreFunc())
	const txnKVWeight = 2
	config.UpdateGlobal(func(cfg *config.Config) {
		cfg.RUV2.DDLWeights.TxnKVBytes = txnKVWeight
	})

	const (
		jobID       = int64(1)
		writtenSize = 128
	)

	rc := &reorgCtx{}
	accountBackfillTxnRU(rc, jobID, writtenSize)

	want := 0.0
	if kerneltype.IsNextGen() {
		want = float64(writtenSize) * txnKVWeight
	}
	require.Equal(t, want, rc.getRU())

	// Accumulated RU is only persisted after the matching table-state
	// transition succeeds.
	jobCtx := &jobContext{}
	job := &model.Job{RU: 5}
	stageReorgResultRU(jobCtx, reorgFnResult{ru: rc.getRU()})
	accountPendingReorgRU(jobCtx, job, nil)
	require.Equal(t, 5+want, job.RU)

	t.Run("failed transition discards staged backfill RU", func(t *testing.T) {
		rc := &reorgCtx{}
		rc.increaseRU(7)
		rc.increaseRU(3)
		require.Equal(t, 10.0, rc.getRU())

		jobCtx := &jobContext{}
		job := &model.Job{RU: 5}
		stageReorgResultRU(jobCtx, reorgFnResult{ru: rc.getRU()})
		accountPendingReorgRU(jobCtx, job, errors.New("transition failed"))
		require.Equal(t, 5.0, job.RU)
		require.Zero(t, jobCtx.pendingReorgRU)
	})
}
