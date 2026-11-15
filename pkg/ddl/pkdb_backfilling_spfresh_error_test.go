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
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBuildReadIndexErrorIsNonRetryable(t *testing.T) {
	originalErr := common.ErrKVServerIsBusy.GenWithStack("mock final PUT failure")
	require.True(t, common.IsRetryableError(originalErr))
	tblInfo := newSPFreshBackfillTableInfo()
	exec := &readIndexStepExecutor{
		job: &model.Job{
			ID:        101,
			ReorgMeta: &model.DDLReorgMeta{},
		},
		indexes:          []*model.IndexInfo{tblInfo.Indices[0]},
		spfreshRoutePlan: &backend.SPFreshBuildRoutePlan{},
	}

	for _, originalErr := range []error{
		originalErr,
		errors.New("invalid SPFresh build recovery response: retrying requires next-retry-at-ms"),
		errors.New("invalid SPFresh build recovery response: paused requires next-retry-at-ms"),
	} {
		t.Run(originalErr.Error(), func(t *testing.T) {
			err := exec.runSubtaskResult(originalErr)
			_, ok := errors.Cause(err).(*terror.Error)
			require.True(t, ok)
			require.ErrorContains(t, err, "original error: "+originalErr.Error())
			require.False(t, (&backfillDistExecutor{}).IsRetryableError(err))
			require.False(t, isRetryableError(err))
			// Reorg must take the rollback branch even on the first error.
			require.False(t, isRetryableJobError(err, 0))
		})
	}
}

func TestBackfillDistExecutorSPFreshReadIndexIsNotIdempotent(t *testing.T) {
	exec := &backfillDistExecutor{
		taskMeta: &BackfillTaskMeta{
			SPFreshBuildRoutePlan: &backend.SPFreshBuildRoutePlan{},
		},
	}

	require.False(t, exec.IsIdempotent(&proto.Subtask{
		SubtaskBase: proto.SubtaskBase{Step: proto.BackfillStepReadIndex},
	}))
	require.True(t, exec.IsIdempotent(&proto.Subtask{
		SubtaskBase: proto.SubtaskBase{Step: proto.BackfillStepWriteAndIngest},
	}))
	require.True(t, (&backfillDistExecutor{}).IsIdempotent(&proto.Subtask{
		SubtaskBase: proto.SubtaskBase{Step: proto.BackfillStepReadIndex},
	}))
}
