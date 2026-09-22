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

	tests := []struct {
		name       string
		accountTxn bool
	}{
		{name: "partition reorg accounts backfill txn bytes", accountTxn: true},
		{name: "other reorg keeps existing accounting", accountTxn: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &reorgCtx{}
			dc := &ddlCtx{}
			dc.reorgCtx.reorgCtxMap = map[int64]*reorgCtx{jobID: rc}
			bc := &backfillCtx{ddlCtx: dc, accountTxnRU: tt.accountTxn}

			bc.accountBackfillTxnRU(jobID, writtenSize)

			want := 0.0
			if kerneltype.IsNextGen() && tt.accountTxn {
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
		})
	}

	t.Run("missing reorg ctx is a no-op", func(t *testing.T) {
		bc := &backfillCtx{ddlCtx: &ddlCtx{}, accountTxnRU: true}
		require.NotPanics(t, func() {
			bc.accountBackfillTxnRU(jobID, writtenSize)
		})
	})

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

func TestReorgBackfillAccountsTxnRU(t *testing.T) {
	modifyColumnJob := func(tp byte) *model.Job {
		job := &model.Job{Version: model.JobVersion2, Type: model.ActionModifyColumn}
		job.FillArgs(&model.ModifyColumnArgs{ModifyColumnType: tp})
		return job
	}

	tests := []struct {
		name string
		job  *model.Job
		want bool
	}{
		{"reorganize partition", &model.Job{Type: model.ActionReorganizePartition}, true},
		{"remove partitioning", &model.Job{Type: model.ActionRemovePartitioning}, true},
		{"alter table partitioning", &model.Job{Type: model.ActionAlterTablePartitioning}, true},
		{"modify column reorg", modifyColumnJob(model.ModifyTypeReorg), true},
		{"modify column index reorg", modifyColumnJob(model.ModifyTypeIndexReorg), true},
		{"modify column meta only", modifyColumnJob(model.ModifyTypeNoReorg), false},
		{"modify column meta only with check", modifyColumnJob(model.ModifyTypeNoReorgWithCheck), false},
		{"modify column precheck", modifyColumnJob(model.ModifyTypePrecheck), false},
		{"modify column legacy none", modifyColumnJob(model.ModifyTypeNone), false},
		{"add index", &model.Job{Type: model.ActionAddIndex}, false},
		{"create table", &model.Job{Type: model.ActionCreateTable}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, reorgBackfillAccountsTxnRU(tt.job))
		})
	}
}
